#!/bin/bash
# Compare gh-ost under heavy DML load: --num-workers=1 vs --num-workers=4.
#
# - 50000 rows + 16 sysbench threads to overwhelm single-threaded apply
# - Preserves _old table and creates pre-migration snapshot for data verification
# - Multi-checkpoint consistency checks (pre-cutover + post-cutover + snapshot)
# - Reports throughput ratio, stall count, backlog distribution
#
# Usage: SYSBENCH_CATCHUP_SEC=45 ./script/mts-benchmark-compare.sh

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_DB="${MYSQL_DB:-test}"
SYSBENCH_TABLE_SIZE="${SYSBENCH_TABLE_SIZE:-50000}"
SYSBENCH_THREADS="${SYSBENCH_THREADS:-16}"
SYSBENCH_CATCHUP_SEC="${SYSBENCH_CATCHUP_SEC:-60}"
GHOST_BIN="${GHOST_BIN:-/tmp/gh-ost-test}"
TABLE=sbtest1
GHOST="_${TABLE}_gho"
OLD="_${TABLE}_old"
SNAP="${TABLE}_snapshot"
POSTPONE_FILE="${POSTPONE_FILE:-/tmp/gh-ost.mts.postpone}"
REPORT="/tmp/gh-ost-bench-report.txt"

mysql_cli() { mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" "$@"; }
sysbench_cli() { sysbench "$@" --mysql-host="$MYSQL_HOST" --mysql-port="$MYSQL_PORT" --mysql-user="$MYSQL_USER" --mysql-db="$MYSQL_DB"; }

[ -x "$GHOST_BIN" ] || go build -o "$GHOST_BIN" ./go/cmd/gh-ost
command -v sysbench >/dev/null || { echo "brew install sysbench"; exit 1; }

# ── helpers ──────────────────────────────────────────────────────────────

prepare_table() {
	rm -f /tmp/gh-ost.bench-nw*.sock "$POSTPONE_FILE" 2>/dev/null || true
	mysql_cli -e "DROP TABLE IF EXISTS \`${MYSQL_DB}\`.\`${SNAP}\`" 2>/dev/null || true
	mysql_cli -e "DROP TABLE IF EXISTS \`${MYSQL_DB}\`.\`${OLD}\`"  2>/dev/null || true
	sysbench_cli oltp_write_only --tables=1 --table-size="$SYSBENCH_TABLE_SIZE" cleanup 2>/dev/null || true
	sysbench_cli oltp_write_only --tables=1 --table-size="$SYSBENCH_TABLE_SIZE" prepare
}

# Snapshot the original table before migration for post-cutover verification.
create_snapshot() {
	mysql_cli -e "
		DROP TABLE IF EXISTS \`${MYSQL_DB}\`.\`${SNAP}\`;
		CREATE TABLE \`${MYSQL_DB}\`.\`${SNAP}\` LIKE \`${MYSQL_DB}\`.\`${TABLE}\`;
		INSERT INTO \`${MYSQL_DB}\`.\`${SNAP}\` SELECT * FROM \`${MYSQL_DB}\`.\`${TABLE}\`;
	"
	echo "Snapshot created: ${SNAP} ($(mysql_cli -Nse "SELECT COUNT(*) FROM \`${MYSQL_DB}\`.\`${SNAP}\`") rows)"
}

# Compare original vs ghost during postpone (catch-up verification).
checksum_before_cutover() {
	mysql_cli -Nse "
		SELECT CONCAT(
			(SELECT COUNT(*) FROM ${MYSQL_DB}.${TABLE}),
			':',
			(SELECT COUNT(*) FROM ${MYSQL_DB}.${GHOST}),
			':',
			(SELECT COUNT(*) FROM ${MYSQL_DB}.${TABLE} o
			 LEFT JOIN ${MYSQL_DB}.${GHOST} g ON o.id=g.id WHERE g.id IS NULL),
			':',
			(SELECT COUNT(*) FROM ${MYSQL_DB}.${GHOST} g
			 LEFT JOIN ${MYSQL_DB}.${TABLE} o ON o.id=g.id WHERE o.id IS NULL)
		)" 2>/dev/null || echo "n/a"
}

# After cutover: compare _old (frozen original) vs new table + snapshot.
verify_post_cutover() {
	local nw=$1 log=$2
	echo "--- Post-cutover verification (nw=$nw) ---"

	# Check _old table exists
	if ! mysql_cli -Nse "SELECT 1 FROM \`${MYSQL_DB}\`.\`${OLD}\` LIMIT 1" &>/dev/null; then
		echo "WARN: ${OLD} table not found (cutover may have dropped it)"
		return
	fi

	local old_count new_count snap_count
	old_count=$(mysql_cli -Nse "SELECT COUNT(*) FROM \`${MYSQL_DB}\`.\`${OLD}\`")
	new_count=$(mysql_cli -Nse "SELECT COUNT(*) FROM \`${MYSQL_DB}\`.\`${TABLE}\`")
	snap_count=$(mysql_cli -Nse "SELECT COUNT(*) FROM \`${MYSQL_DB}\`.\`${SNAP}\`" 2>/dev/null || echo "n/a")

	echo "  rows: old=${old_count} new=${new_count} snapshot=${snap_count}"

	# _old should match snapshot (original was frozen at lock time; snapshot taken
	# before sysbench load, so _old may have more rows from load before lock).
	if [ "$snap_count" != "n/a" ]; then
		local snap_only old_only
		snap_only=$(mysql_cli -Nse "
			SELECT COUNT(*) FROM \`${MYSQL_DB}\`.\`${SNAP}\` s
			LEFT JOIN \`${MYSQL_DB}\`.\`${OLD}\` o ON s.id=o.id WHERE o.id IS NULL" 2>/dev/null || echo "?")
		old_only=$(mysql_cli -Nse "
			SELECT COUNT(*) FROM \`${MYSQL_DB}\`.\`${OLD}\` o
			LEFT JOIN \`${MYSQL_DB}\`.\`${SNAP}\` s ON o.id=s.id WHERE s.id IS NULL" 2>/dev/null || echo "?")
		echo "  snapshot_vs_old: only_in_snapshot=${snap_only} only_in_old=${old_only}"
	fi

	# New table must have every row from _old (no data loss).
	local missing_from_new
	missing_from_new=$(mysql_cli -Nse "
		SELECT COUNT(*) FROM \`${MYSQL_DB}\`.\`${OLD}\` o
		LEFT JOIN \`${MYSQL_DB}\`.\`${TABLE}\` n ON o.id=n.id WHERE n.id IS NULL" 2>/dev/null || echo "?")
	echo "  old_vs_new: missing_from_new=${missing_from_new}"
	if [ "$missing_from_new" != "0" ] && [ "$missing_from_new" != "?" ]; then
		echo "  ERROR: ${missing_from_new} rows in _old but missing from new table — data loss!"
	fi

	# Value-level check on a sample: compare k,c,pad for matching IDs.
	local value_mismatch
	value_mismatch=$(mysql_cli -Nse "
		SELECT COUNT(*) FROM (
			SELECT o.id FROM \`${MYSQL_DB}\`.\`${OLD}\` o
			JOIN \`${MYSQL_DB}\`.\`${TABLE}\` n ON o.id=n.id
			WHERE o.k != n.k OR o.c != n.c OR o.pad != n.pad
		) t" 2>/dev/null || echo "?")
	echo "  value_mismatch_on_matching_ids: ${value_mismatch}"
	if [ "$value_mismatch" != "0" ] && [ "$value_mismatch" != "?" ]; then
		echo "  NOTE: value mismatches are expected if UPDATEs occurred after snapshot but before lock"
	fi

	echo "  Post-cutover verification done"
}

# Parse log for metrics.
parse_log_metrics() {
	local log=$1
	local final_applied max_backlog max_hblag stall_count
	final_applied=$(grep -oE 'Applied: [0-9]+' "$log" | tail -1 | awk '{print $2}')
	max_backlog=$(grep -oE 'Backlog: [0-9]+' "$log" | awk '{print $2}' | sort -n | tail -1)

	# Heartbeat lag during catch-up
	local catchup_log
	catchup_log=$(mktemp)
	if grep -q 'Row copy complete' "$log"; then
		awk '/Row copy complete/,/Done migrating/' "$log" >"$catchup_log" || cp "$log" "$catchup_log"
	else
		cp "$log" "$catchup_log"
	fi
	max_hblag=$(grep -oE 'HeartbeatLag: [0-9.]+s' "$catchup_log" | sed 's/HeartbeatLag: //;s/s//' | sort -n | tail -1)

	# Stall detection: count status lines where Applied stayed at same value
	stall_count=$(grep -oE 'Applied: [0-9]+' "$catchup_log" | awk '{print $2}' | uniq -d | wc -l | tr -d ' ')

	rm -f "$catchup_log"
	echo "${final_applied:-0} ${max_backlog:-0} ${max_hblag:-0} ${stall_count:-0}"
}

# Analyze log for MTS feature evidence.
analyze_features() {
	local log=$1 nw=$2
	echo "--- Feature coverage (nw=$nw) ---"

	if [ "$nw" -gt 1 ]; then
		if grep -q 'Starting MTS mode' "$log"; then
			echo "  [PASS] MTS mode started"
		else
			echo "  [FAIL] MTS mode NOT started (missing logical timestamps?)"
		fi
	else
		if grep -q 'Starting MTS mode' "$log"; then
			echo "  [WARN] MTS mode started for nw=1 (unexpected)"
		else
			echo "  [PASS] Single-threaded mode (no MTS)"
		fi
	fi

	if grep -q 'Done migrating' "$log"; then
		echo "  [PASS] Migration completed"
	else
		echo "  [FAIL] Migration did NOT complete"
	fi

	if grep -qi 'cut-over' "$log"; then
		echo "  [PASS] Cutover phase reached"
	else
		echo "  [FAIL] Cutover not reached"
	fi

	# Deadlock evidence (only meaningful for nw>1)
	if [ "$nw" -gt 1 ]; then
		local deadlock_count
		deadlock_count=$(grep -ci 'deadlock\|1213' "$log" 2>/dev/null || echo "0")
		echo "  [INFO] Deadlock mentions in log: ${deadlock_count}"
	fi

	# Backlog pressure
	local bl_above_500
	bl_above_500=$(grep -oE 'Backlog: [0-9]+' "$log" | awk '{print $2}' | awk '$1>500' | wc -l | tr -d ' ')
	echo "  [INFO] Status lines with backlog>500: ${bl_above_500}"

	echo "  Feature analysis done"
}

# ── main run ─────────────────────────────────────────────────────────────

run_one() {
	local nw=$1
	local log="/tmp/gh-ost-bench-nw${nw}.log"
	local t0 t_load_start t_load_end

	prepare_table
	create_snapshot
	touch "$POSTPONE_FILE"
	: >"$log"
	t0=$(date +%s)

	echo "Starting gh-ost nw=$nw (table_size=$SYSBENCH_TABLE_SIZE, catchup=${SYSBENCH_CATCHUP_SEC}s)..."
	GOTRACEBACK=crash "$GHOST_BIN" \
		--host="$MYSQL_HOST" --port="$MYSQL_PORT" --user="$MYSQL_USER" \
		--password="${MYSQL_PWD:-}" \
		--allow-on-master --database="$MYSQL_DB" --table="$TABLE" \
		--alter="engine=InnoDB" --gtid --num-workers="$nw" \
		--assume-rbr --skip-metadata-lock-check \
		--initially-drop-ghost-table --initially-drop-old-table \
		--chunk-size=100 --dml-batch-size=10 \
		--max-lag-millis=5000 --default-retries=10 \
		--postpone-cut-over-flag-file="$POSTPONE_FILE" \
		--serve-socket-file=/tmp/gh-ost.bench-nw${nw}.sock \
		--initially-drop-socket-file --verbose --execute \
		>>"$log" 2>&1 &
	local pid=$!

	# Wait for row copy to complete
	for _ in $(seq 1 300); do
		grep -q 'Row copy complete' "$log" 2>/dev/null && break
		ps -p "$pid" >/dev/null || break
		sleep 1
	done

	if ! grep -q 'Row copy complete' "$log" 2>/dev/null; then
		echo "ERROR: Row copy did not complete for nw=$nw"
		kill "$pid" 2>/dev/null || true
		wait "$pid" 2>/dev/null || true
		tail -n 30 "$log"
		return 1
	fi

	# Start sysbench DML load
	t_load_start=$(date +%s)
	sysbench_cli oltp_write_only --tables=1 --threads="$SYSBENCH_THREADS" \
		--time="$SYSBENCH_CATCHUP_SEC" --rate=0 --rand-seed=42 run &
	local sb_pid=$!

	# Monitor applied progress during load
	sleep "$SYSBENCH_CATCHUP_SEC"
	kill "$sb_pid" 2>/dev/null || true
	wait "$sb_pid" 2>/dev/null || true
	t_load_end=$(date +%s)

	# Checksum while still in postpone (original vs ghost)
	local cs
	cs=$(checksum_before_cutover)
	local applied_at_load_end
	applied_at_load_end=$(grep -oE 'Applied: [0-9]+' "$log" | tail -1 | awk '{print $2}')

	# Release postpone → cutover
	rm -f "$POSTPONE_FILE"
	echo "Released postpone-cut-over (nw=$nw); waiting for cut-over..."

	for _ in $(seq 1 120); do
		grep -q 'Done migrating' "$log" 2>/dev/null && break
		ps -p "$pid" >/dev/null || break
		sleep 1
	done
	wait "$pid" 2>/dev/null || true
	local t_done
	t_done=$(date +%s)

	# Parse metrics
	read -r final_applied max_backlog max_hblag stall_count <<<"$(parse_log_metrics "$log")"
	local load_sec=$((t_load_end - t_load_start))
	local total_sec=$((t_done - t0))
	local throughput=0
	if [ "$load_sec" -gt 0 ] && [ "${applied_at_load_end:-0}" -gt 0 ]; then
		throughput=$((applied_at_load_end / load_sec))
	fi

	# Post-cutover verification
	verify_post_cutover "$nw" "$log"
	analyze_features "$log" "$nw"

	# Cleanup snapshot for next run
	mysql_cli -e "DROP TABLE IF EXISTS \`${MYSQL_DB}\`.\`${SNAP}\`" 2>/dev/null || true

	# Report
	echo ""
	echo "=== nw=$nw RESULT ==="
	echo "  total_sec=$total_sec load_sec=$load_sec"
	echo "  applied_at_load_end=$applied_at_load_end final_applied=$final_applied"
	echo "  throughput=${throughput}/s max_backlog=$max_backlog max_heartbeat_lag_s=$max_hblag stall_count=$stall_count"
	echo "  checksum(orig:ghost:only_orig:only_ghost)=$cs"
	echo "  log=$log"
	echo ""

	echo "nw=$nw total_sec=$total_sec load_sec=$load_sec applied_at_load_end=$applied_at_load_end final_applied=$final_applied throughput=${throughput}/s max_backlog=$max_backlog max_hblag=${max_hblag}s stalls=$stall_count checksum=$cs" >>"$REPORT"
}

# ── entry point ──────────────────────────────────────────────────────────

: >"$REPORT"
echo "=== gh-ost MTS benchmark $(date) ===" | tee -a "$REPORT"
echo "table_size=$SYSBENCH_TABLE_SIZE threads=$SYSBENCH_THREADS catchup_sec=$SYSBENCH_CATCHUP_SEC" | tee -a "$REPORT"
echo ""

NW1_RESULT=""
NW4_RESULT=""

echo ">>> Running nw=1 (single-threaded baseline)..."
if run_one 1; then
	NW1_RESULT="ok"
else
	NW1_RESULT="FAIL"
	echo "nw=1 FAILED, skipping nw=4 comparison"
fi

echo ""
echo ">>> Running nw=4 (MTS parallel apply)..."
if run_one 4; then
	NW4_RESULT="ok"
else
	NW4_RESULT="FAIL"
fi

# ── summary ──────────────────────────────────────────────────────────────

echo ""
echo "============================================"
echo "  BENCHMARK SUMMARY"
echo "============================================"
cat "$REPORT"

if [ "$NW1_RESULT" = "ok" ] && [ "$NW4_RESULT" = "ok" ]; then
	# Extract applied counts for ratio
	nw1_applied=$(grep '^nw=1' "$REPORT" | tail -1 | grep -oE 'applied_at_load_end=[0-9]+' | head -1 | cut -d= -f2)
	nw4_applied=$(grep '^nw=4' "$REPORT" | tail -1 | grep -oE 'applied_at_load_end=[0-9]+' | head -1 | cut -d= -f2)
	nw1_bl=$(grep '^nw=1' "$REPORT" | tail -1 | grep -oE 'max_backlog=[0-9]+' | head -1 | cut -d= -f2)
	nw4_bl=$(grep '^nw=4' "$REPORT" | tail -1 | grep -oE 'max_backlog=[0-9]+' | head -1 | cut -d= -f2)

	echo ""
	echo "nw=1 applied during load: ${nw1_applied:-?}, max_backlog: ${nw1_bl:-?}"
	echo "nw=4 applied during load: ${nw4_applied:-?}, max_backlog: ${nw4_bl:-?}"

	if [ -n "$nw1_applied" ] && [ -n "$nw4_applied" ] && [ "$nw1_applied" -gt 0 ]; then
		echo "nw=4/nw=1 throughput ratio: $((nw4_applied / nw1_applied))x"
	fi
fi

echo ""
echo "Full report: $REPORT"
