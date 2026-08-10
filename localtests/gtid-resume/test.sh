#!/bin/bash
# Custom test: interrupt a --gtid migration mid-copy, then --resume it.
#
# Purpose: exercise the GTID checkpoint round-trip end-to-end --
# WriteCheckpoint persists the binlog coordinates as a GTID set, and on resume
# ReadLastCheckpoint parses them back via mysql.NewGTIDBinlogCoordinates(flavor,
# coords). On the MariaDB image this covers the MariaDB GTID dialect of that
# read path; on MySQL images it covers the MySQL dialect.
#
# Gated to GTID-on servers via the gtid_mode=ON file. Runs on all GTID flavors.
#
# Flow: run gh-ost with --checkpoint in the background; --nice-ratio stretches
# the row-copy so it reliably outlasts the first checkpoint (gh-ost enforces
# --checkpoint-seconds>=10), so a checkpoint is written mid-copy; abort with the
# interactive 'panic' command (aborts without cleanup, leaving the ghost/
# changelog/checkpoint tables in place); then start a second gh-ost with
# --resume and let it finish; finally checksum original vs ghost (the framework
# skips its own checksum for custom test.sh cases).
#
# Note: the copy must NOT be throttled while waiting for the checkpoint --
# throttling pauses DML apply, the backlog grows, and the checkpoint (2s
# internal timeout) never reaches a consistent point. --nice-ratio slows the
# copy by sleeping between chunks (it does not pause DML apply), so checkpoints
# still succeed.

# Force the harness throttle-query to never throttle (THROTTLE_SECONDS=0 -> its
# condition becomes "< 0"). Throttling pauses DML apply and makes checkpoints
# time out, so it must stay fully off for this test. Must be set before
# build_ghost_command bakes the throttle-query in.
export THROTTLE_SECONDS=0

table_name="gh_ost_test"
ghost_table_name="_gh_ost_test_gho"
ghost_socket="/tmp/gh-ost.test.sock"

stop_ghost() {
    # Abort via the interactive 'panic' command (no cleanup -> tables remain for
    # resume). Fall back to SIGKILL if the process does not exit.
    echo panic | nc -U "$ghost_socket" >/dev/null 2>&1
    for i in $(seq 1 30); do
        ps -p $ghost_pid >/dev/null || return 0
        sleep 0.5
    done
    kill -9 $ghost_pid 2>/dev/null
    pkill -9 -f "$ghost_binary" 2>/dev/null
}

build_ghost_command

# --- first run: interrupt mid-copy -----------------------------------------
echo >$test_logfile
echo_dot
bash -c "$cmd" >>$test_logfile 2>&1 &
ghost_pid=$!

# Wait for a checkpoint to be written while the row-copy is still in progress.
checkpoint_written=false
for i in $(seq 1 120); do
    if grep -q "checkpoint success at coords=" "$test_logfile"; then
        checkpoint_written=true
        break
    fi
    if grep -q "Row copy complete" "$test_logfile"; then
        echo
        echo "ERROR row copy completed before a checkpoint was written; increase --nice-ratio (extra_args) or the seed size in create.sql"
        stop_ghost
        return 1
    fi
    if ! ps -p $ghost_pid >/dev/null; then
        echo
        echo "ERROR gh-ost exited early during the first (pre-resume) run"
        print_log_excerpt
        return 1
    fi
    sleep 0.5
    echo_dot
done

if [ "$checkpoint_written" != true ]; then
    echo
    echo "ERROR no checkpoint was written within the wait window"
    print_log_excerpt
    stop_ghost
    return 1
fi

# Abort the migration to simulate an interruption. 'panic' leaves the ghost,
# changelog and checkpoint tables in place for the resume.
stop_ghost
wait $ghost_pid 2>/dev/null
echo_dot

# --- second run: resume from checkpoint ------------------------------------
# --initially-drop-ghost-table (in $cmd) is ignored under --resume: gh-ost keeps
# the partially-copied ghost table and continues from the checkpoint.
# Drop --nice-ratio (only needed to stretch the first run past the checkpoint)
# so the resume copies the remaining rows at full speed.
resume_cmd="$(echo "$cmd" | sed -E 's/--nice-ratio=[0-9.]+//g') --resume"
bash -c "$resume_cmd" >>$test_logfile 2>&1 &
resume_pid=$!
wait $resume_pid
execution_result=$?

if [ $execution_result -ne 0 ]; then
    echo
    echo "ERROR resume run failed (exit ${execution_result})"
    print_log_excerpt
    return 1
fi

if ! grep -q "Resuming from checkpoint" "$test_logfile"; then
    echo
    echo "ERROR resume run did not read a checkpoint (no 'Resuming from checkpoint' in log)"
    print_log_excerpt
    return 1
fi

# --- validate: original vs ghost table checksum on the replica --------------
echo_dot
gh-ost-test-mysql-replica --default-character-set=utf8mb4 test -e "select * from ${table_name} order by id" -ss >$orig_content_output_file
gh-ost-test-mysql-replica --default-character-set=utf8mb4 test -e "select * from ${ghost_table_name} order by id" -ss >$ghost_content_output_file
orig_checksum=$(cat $orig_content_output_file | md5sum)
ghost_checksum=$(cat $ghost_content_output_file | md5sum)

if [ "$orig_checksum" != "$ghost_checksum" ]; then
    echo
    echo "ERROR ${test_name}: checksum mismatch after resume"
    echo "---"
    diff $orig_content_output_file $ghost_content_output_file | head -50
    return 1
fi

return 0
