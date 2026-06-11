#!/bin/bash
# MTS parallel apply under sysbench write load (requires --gtid from test.sh -g).

set -euo pipefail

if ! command -v sysbench &>/dev/null; then
    echo "sysbench not installed; skip"
    exit 0
fi

table_name="sbtest1"
ghost_table_name="_${table_name}_gho"
# --test-on-replica (from test.sh) cut-over then swap-reverts: migrated data stays on
# _gho, original name sbtest1 is restored. Compare like standard localtests, not _del.

sysbench_prepare() {
    sysbench oltp_write_only \
        --mysql-host="$master_host" \
        --mysql-port="$master_port" \
        --mysql-user=root \
        --mysql-password=opensesame \
        --mysql-db=test \
        --tables=1 \
        --table-size=5000 \
        prepare
}

# Load tuned for CI: 8 threads, 45s, rate=1200 trx/s (PR #1454 scale). Unbounded
# 16-thread load pegs the MTS backlog and exceeds the docker test timeout.
sysbench_run_cmd() {
    echo "sysbench oltp_write_only \
        --mysql-host=$master_host \
        --mysql-port=$master_port \
        --mysql-user=root \
        --mysql-password=opensesame \
        --mysql-db=test \
        --rand-seed=42 \
        --tables=1 \
        --threads=8 \
        --time=45 \
        --rate=1200 \
        run"
}

echo "Preparing sysbench sbtest1..."
sysbench_prepare "$master_host" "$master_port"

build_ghost_command

echo_dot
echo >"$test_logfile"

# --- Binlog rotation detection (PR #1454 root cause) ---
binlogs_before=$(gh-ost-test-mysql-master -Nse "SHOW BINARY LOGS" 2>/dev/null | wc -l | tr -d ' ')

# Start sysbench write load FIRST so writes happen during the entire migration
eval "$(sysbench_run_cmd)" &
sysbench_pid=$!
echo "Started sysbench PID=$sysbench_pid"
sleep 2

bash -c "$cmd" >>"$test_logfile" 2>&1 &
ghost_pid=$!

# Allow time for sysbench (45s) plus backlog drain and cut-over under MTS.
for _ in $(seq 1 300); do
    if ! ps -p "$ghost_pid" >/dev/null 2>&1; then
        break
    fi
    if grep -q "Done migrating" "$test_logfile" 2>/dev/null; then
        break
    fi
    sleep 1
    echo_dot
done

if ps -p "$sysbench_pid" >/dev/null 2>&1; then
    kill "$sysbench_pid" 2>/dev/null || true
fi
wait "$ghost_pid" 2>/dev/null || true
execution_result=$?

if [ "$execution_result" -ne 0 ]; then
    echo
    echo "ERROR mts-sysbench gh-ost failed"
    tail -n 80 "$test_logfile"
    exit 1
fi

if ! grep -q "Done migrating" "$test_logfile"; then
    echo
    echo "ERROR mts-sysbench: migration did not complete"
    tail -n 80 "$test_logfile"
    exit 1
fi

if ! grep -q "Starting MTS mode with 4 workers" "$test_logfile"; then
    echo
    echo "ERROR mts-sysbench: expected MTS mode in log (need --gtid and MySQL 5.7+ logical timestamps)"
    tail -n 80 "$test_logfile"
    exit 1
fi

# --- Binlog rotation report ---
binlogs_after=$(gh-ost-test-mysql-master -Nse "SHOW BINARY LOGS" 2>/dev/null | wc -l | tr -d ' ')
binlog_rotations=$((binlogs_after - binlogs_before))
if [ "$binlog_rotations" -gt 0 ]; then
    echo "  [binlog] ${binlog_rotations} rotation(s) detected during migration"
else
    echo "  [binlog] no rotation detected (reduce max_binlog_size for stricter test)"
fi

# --- Triple verification: row count + CHECKSUM TABLE + md5sum ---
# Replica after --test-on-replica: compare original table vs ghost table (see test.sh).

# 1. Row count
orig_count=$(gh-ost-test-mysql-replica --default-character-set=utf8mb4 test \
    -Nse "SELECT COUNT(*) FROM ${table_name}" 2>/dev/null || echo "MISSING")
ghost_count=$(gh-ost-test-mysql-replica --default-character-set=utf8mb4 test \
    -Nse "SELECT COUNT(*) FROM ${ghost_table_name}" 2>/dev/null || echo "MISSING")

if [ "$orig_count" = "MISSING" ] || [ "$ghost_count" = "MISSING" ]; then
    echo "ERROR mts-sysbench: table missing orig=${orig_count} ghost=${ghost_count}"
    exit 1
fi
if [ "$orig_count" != "$ghost_count" ]; then
    echo "ERROR mts-sysbench: row count mismatch orig=${orig_count} ghost=${ghost_count}"
    exit 1
fi
echo "  [verify] row count match: ${orig_count}"

# 2. CHECKSUM TABLE
orig_checksum=$(gh-ost-test-mysql-replica --default-character-set=utf8mb4 test \
    -Nse "CHECKSUM TABLE ${table_name}" 2>/dev/null | awk '{print $NF}')
ghost_checksum=$(gh-ost-test-mysql-replica --default-character-set=utf8mb4 test \
    -Nse "CHECKSUM TABLE ${ghost_table_name}" 2>/dev/null | awk '{print $NF}')
if [ "$orig_checksum" != "$ghost_checksum" ]; then
    echo "ERROR mts-sysbench: CHECKSUM TABLE mismatch orig=${orig_checksum} ghost=${ghost_checksum}"
    exit 1
fi
echo "  [verify] CHECKSUM TABLE match: ${orig_checksum}"

# 3. md5sum sorted content
orig_columns="id,k,c,pad"
ghost_columns="$orig_columns"
order_by="order by id"

gh-ost-test-mysql-replica --default-character-set=utf8mb4 test \
    -e "select ${orig_columns} from ${table_name} ${order_by}" -ss >"$orig_content_output_file"
gh-ost-test-mysql-replica --default-character-set=utf8mb4 test \
    -e "select ${ghost_columns} from ${ghost_table_name} ${order_by}" -ss >"$ghost_content_output_file"

orig_checksum=$(md5sum <"$orig_content_output_file")
ghost_checksum=$(md5sum <"$ghost_content_output_file")

if [ "$orig_checksum" != "$ghost_checksum" ]; then
    echo "ERROR mts-sysbench: md5sum mismatch"
    diff "$orig_content_output_file" "$ghost_content_output_file" | head -n 40
    exit 1
fi
echo "  [verify] md5sum match: ${orig_checksum%% *}"

sysbench oltp_write_only \
    --mysql-host="$master_host" \
    --mysql-port="$master_port" \
    --mysql-user=root \
    --mysql-password=opensesame \
    --mysql-db=test \
    --tables=1 \
    cleanup >/dev/null 2>&1 || true

exit 0
