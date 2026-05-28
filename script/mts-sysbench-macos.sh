#!/bin/bash
# MTS + sysbench on native macOS MySQL (no Docker).
#
# Prerequisites (Homebrew):
#   brew install mysql sysbench
#   brew services start mysql    # or your own mysqld
#
# MySQL server must have:
#   binlog_format=ROW, log_bin=ON, gtid_mode=ON, enforce_gtid_consistency=ON
#
# Usage:
#   export MYSQL_PWD='your_root_password'   # if needed
#   ./script/mts-sysbench-macos.sh
#
# Optional env:
#   MYSQL_HOST=127.0.0.1  MYSQL_PORT=3306  MYSQL_USER=root  MYSQL_DB=test
#   NUM_WORKERS=4  SYSBENCH_THREADS=8  SYSBENCH_TABLE_SIZE=50000
#   GHOST_BIN=/tmp/gh-ost-test  SKIP_UNIT_TESTS=1

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_DB="${MYSQL_DB:-test}"
NUM_WORKERS="${NUM_WORKERS:-4}"
SYSBENCH_THREADS="${SYSBENCH_THREADS:-8}"
SYSBENCH_TABLE_SIZE="${SYSBENCH_TABLE_SIZE:-50000}"
SYSBENCH_TIME="${SYSBENCH_TIME:-120}"
MIGRATION_TIMEOUT="${MIGRATION_TIMEOUT:-600}"
GHOST_BIN="${GHOST_BIN:-/tmp/gh-ost-test}"
TABLE_NAME="${TABLE_NAME:-sbtest1}"
GHOST_TABLE="_${TABLE_NAME}_gho"
# gh-ost renames original to _del when --ok-to-drop-table is false (default)
OLD_TABLE="_${TABLE_NAME}_del"
TEST_LOG="/tmp/gh-ost-mts-sysbench.log"

mysql_cli() {
	mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" "$@"
}

sysbench_cli() {
	sysbench "$@" \
		--mysql-host="$MYSQL_HOST" \
		--mysql-port="$MYSQL_PORT" \
		--mysql-user="$MYSQL_USER" \
		--mysql-db="$MYSQL_DB"
}

ensure_sysbench() {
	if command -v sysbench &>/dev/null; then
		return
	fi
	if ! command -v brew &>/dev/null; then
		echo "sysbench not found and Homebrew unavailable. Install: brew install sysbench"
		exit 1
	fi
	echo "Installing sysbench via Homebrew..."
	brew install sysbench
}

ensure_ghost_binary() {
	if [ -x "$GHOST_BIN" ]; then
		return
	fi
	echo "Building gh-ost -> $GHOST_BIN"
	go build -o "$GHOST_BIN" ./go/cmd/gh-ost
}

check_mysql() {
	echo "Checking MySQL at ${MYSQL_HOST}:${MYSQL_PORT}..."
	if ! mysql_cli -e "SELECT 1" &>/dev/null; then
		echo "Cannot connect to MySQL. Check server, user, and MYSQL_PWD."
		echo "  brew services start mysql"
		echo "  export MYSQL_PWD='...'"
		exit 1
	fi

	read -r version gtid_mode binlog_format log_bin <<<"$(
		mysql_cli -Nse "
			SELECT @@version,
			       @@global.gtid_mode,
			       @@global.binlog_format,
			       @@global.log_bin
		" 2>/dev/null | tr '\t' ' '
	)"
	echo "  version=$version gtid_mode=$gtid_mode binlog_format=$binlog_format log_bin=$log_bin"

	if [ "$binlog_format" != "ROW" ]; then
		echo "ERROR: binlog_format must be ROW (current: $binlog_format)"
		exit 1
	fi
	if [ "$log_bin" != "1" ]; then
		echo "ERROR: log_bin must be ON for gh-ost"
		exit 1
	fi
	if [ "$gtid_mode" != "ON" ]; then
		echo "ERROR: gtid_mode must be ON for MTS (--gtid). Current: $gtid_mode"
		echo "  SET GLOBAL enforce_gtid_consistency=ON;"
		echo "  SET GLOBAL gtid_mode=ON;  -- may require restart on some versions"
		exit 1
	fi
}

setup_database() {
	mysql_cli -e "CREATE DATABASE IF NOT EXISTS \`${MYSQL_DB}\`"
	sysbench_cli oltp_write_only --tables=1 --table-size="$SYSBENCH_TABLE_SIZE" cleanup 2>/dev/null || true
	echo "Preparing sysbench table ${TABLE_NAME} (${SYSBENCH_TABLE_SIZE} rows)..."
	sysbench_cli oltp_write_only --tables=1 --table-size="$SYSBENCH_TABLE_SIZE" prepare
}

run_unit_tests() {
	if [ "${SKIP_UNIT_TESTS:-0}" = "1" ]; then
		return
	fi
	echo "Running MTS unit tests..."
	go test ./go/logic/ -run 'TestCommitBarrier|TestMTSSchedule|TestCollectTransaction|TestNotifyLogical|TestEnqueueApply|TestMigrator_Num' -count=1
}

run_migration() {
	: >"$TEST_LOG"

	sysbench_pid=""
	ghost_pid=""
	cleanup() {
		if [ -n "$sysbench_pid" ] && ps -p "$sysbench_pid" &>/dev/null; then
			kill "$sysbench_pid" 2>/dev/null || true
		fi
		if [ -n "$ghost_pid" ] && ps -p "$ghost_pid" &>/dev/null; then
			kill "$ghost_pid" 2>/dev/null || true
		fi
	}
	trap cleanup EXIT INT TERM

	# Record binlog state before
	binlogs_before=$(mysql_cli -Nse "SHOW BINARY LOGS" 2>/dev/null | wc -l | tr -d ' ')

	# Start sysbench write load FIRST so writes happen during the entire migration
	sysbench_cli oltp_write_only \
		--tables=1 \
		--threads="$SYSBENCH_THREADS" \
		--time="$SYSBENCH_TIME" \
		--rate=0 \
		--rand-seed=42 \
		run &
	sysbench_pid=$!
	echo "Started sysbench PID=$sysbench_pid (${SYSBENCH_THREADS} threads, ${SYSBENCH_TIME}s)"

	# Brief pause to let sysbench warm up and generate binlog events
	sleep 2

	# Now start gh-ost migration while sysbench is writing
	echo "Starting gh-ost (--num-workers=${NUM_WORKERS} --gtid --allow-on-master)..."
	GOTRACEBACK=crash "$GHOST_BIN" \
		--host="$MYSQL_HOST" \
		--port="$MYSQL_PORT" \
		--user="$MYSQL_USER" \
		--password="${MYSQL_PWD:-}" \
		--allow-on-master \
		--database="$MYSQL_DB" \
		--table="$TABLE_NAME" \
		--alter="engine=InnoDB" \
		--gtid \
		--num-workers="$NUM_WORKERS" \
		--assume-rbr \
		--skip-metadata-lock-check \
		--initially-drop-ghost-table \
		--initially-drop-old-table \
		--chunk-size=100 \
		--dml-batch-size=10 \
		--default-retries=3 \
		--serve-socket-file=/tmp/gh-ost.mts-sysbench.sock \
		--initially-drop-socket-file \
		--verbose \
		--execute >>"$TEST_LOG" 2>&1 &
	ghost_pid=$!

	for _ in $(seq 1 "$MIGRATION_TIMEOUT"); do
		if ! ps -p "$ghost_pid" &>/dev/null; then
			break
		fi
		if grep -q "Done migrating" "$TEST_LOG" 2>/dev/null; then
			break
		fi
		sleep 1
	done

	cleanup
	trap - EXIT INT TERM
	wait "$ghost_pid" 2>/dev/null || true

	if ! grep -q "Done migrating" "$TEST_LOG"; then
		echo "ERROR: gh-ost did not complete successfully"
		tail -n 60 "$TEST_LOG"
		exit 1
	fi
	if ! grep -q "Starting MTS mode with ${NUM_WORKERS} workers" "$TEST_LOG"; then
		echo "ERROR: MTS mode not started (need gtid + logical timestamps)"
		tail -n 60 "$TEST_LOG"
		exit 1
	fi
	echo "gh-ost finished; log: $TEST_LOG"

	# Report binlog rotations
	binlogs_after=$(mysql_cli -Nse "SHOW BINARY LOGS" 2>/dev/null | wc -l | tr -d ' ')
	binlog_rotations=$((binlogs_after - binlogs_before))
	if [ "$binlog_rotations" -gt 0 ]; then
		echo "  [binlog] ${binlog_rotations} rotation(s) detected during migration"
	else
		echo "  [binlog] no rotation detected (reduce max_binlog_size for stricter test)"
	fi
}

verify_checksum() {
	echo "Verifying data consistency (triple check)..."
	local errors=0

	if ! mysql_cli -Nse "SELECT 1 FROM \`${MYSQL_DB}\`.\`${OLD_TABLE}\` LIMIT 1" &>/dev/null; then
		echo "WARN: old table (${OLD_TABLE}) not found; verifying new table exists"
		mysql_cli -e "SELECT COUNT(*) FROM \`${MYSQL_DB}\`.\`${TABLE_NAME}\`"
		return
	fi

	# 1. Row count + missing rows check
	local old_count new_count missing
	old_count=$(mysql_cli -Nse "SELECT COUNT(*) FROM \`${MYSQL_DB}\`.\`${OLD_TABLE}\`")
	new_count=$(mysql_cli -Nse "SELECT COUNT(*) FROM \`${MYSQL_DB}\`.\`${TABLE_NAME}\`")
	missing=$(mysql_cli -Nse "
		SELECT COUNT(*) FROM \`${MYSQL_DB}\`.\`${OLD_TABLE}\` o
		LEFT JOIN \`${MYSQL_DB}\`.\`${TABLE_NAME}\` n ON o.id=n.id WHERE n.id IS NULL")
	echo "  [1/3] row count: _del=${old_count} new=${new_count} missing=${missing}"
	if [ "$missing" != "0" ]; then
		echo "ERROR: ${missing} rows missing - data loss!"
		errors=$((errors + 1))
	elif [ "$old_count" != "$new_count" ]; then
		echo "ERROR: row count mismatch _del=${old_count} vs new=${new_count}"
		errors=$((errors + 1))
	fi

	# 2. CHECKSUM TABLE (server-side row checksum, extract checksum value only)
	local old_cs new_cs
	old_cs=$(mysql_cli -Nse "CHECKSUM TABLE \`${MYSQL_DB}\`.\`${OLD_TABLE}\`" | awk '{print $2}')
	new_cs=$(mysql_cli -Nse "CHECKSUM TABLE \`${MYSQL_DB}\`.\`${TABLE_NAME}\`" | awk '{print $2}')
	echo "  [2/3] CHECKSUM TABLE: _del=${old_cs} new=${new_cs}"
	if [ "$old_cs" != "$new_cs" ]; then
		echo "ERROR: CHECKSUM TABLE mismatch!"
		errors=$((errors + 1))
	fi

	# 3. md5sum sorted content (row-level comparison)
	local orig_file="/tmp/gh-ost-mts-orig.csv"
	local ghost_file="/tmp/gh-ost-mts-ghost.csv"
	mysql_cli -Nse "SELECT id,k,c,pad FROM \`${MYSQL_DB}\`.\`${OLD_TABLE}\` ORDER BY id" >"$orig_file"
	mysql_cli -Nse "SELECT id,k,c,pad FROM \`${MYSQL_DB}\`.\`${TABLE_NAME}\` ORDER BY id" >"$ghost_file"
	local orig_md5 ghost_md5
	orig_md5=$(md5sum <"$orig_file" | awk '{print $1}')
	ghost_md5=$(md5sum <"$ghost_file" | awk '{print $1}')
	echo "  [3/3] md5sum: _del=${orig_md5} new=${ghost_md5}"
	if [ "$orig_md5" != "$ghost_md5" ]; then
		echo "ERROR: md5sum mismatch! Diff:"
		diff "$orig_file" "$ghost_file" | head -n 20
		errors=$((errors + 1))
	fi
	rm -f "$orig_file" "$ghost_file"

	if [ "$errors" -gt 0 ]; then
		echo "ERROR: ${errors}/3 consistency checks failed"
		exit 1
	fi
	echo "OK: All 3 consistency checks passed"
}

teardown() {
	# Drop old table left by gh-ost
	mysql_cli -e "DROP TABLE IF EXISTS \`${MYSQL_DB}\`.\`${OLD_TABLE}\`" 2>/dev/null || true
	sysbench_cli oltp_write_only --tables=1 cleanup 2>/dev/null || true
}

main() {
	ensure_sysbench
	ensure_ghost_binary
	run_unit_tests
	check_mysql
	setup_database
	run_migration
	verify_checksum
	teardown
	echo ""
	echo "OK: MTS sysbench test passed on ${MYSQL_HOST}:${MYSQL_PORT} (num-workers=${NUM_WORKERS})"
}

main "$@"
