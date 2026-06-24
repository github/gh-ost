#!/bin/bash
# Custom test: kill a --parallel-copy migration mid-flight (after at least one
# checkpoint), then --resume it and verify the resulting ghost table matches the
# original.
#
# The test is robust to timing: if the first run happens to finish the row copy
# before we kill it, --resume simply has no gaps to fill and completes. Either
# way the ghost table must match the original. The deterministic gap-filling
# logic itself is covered by the unit tests in go/logic/parallel_test.go.

table_name="gh_ost_test"
ghost_table_name="_gh_ost_test_gho"
postpone_flag_file=/tmp/gh-ost-test.parallel-resume.postpone
rm -f $postpone_flag_file

# Connection/behaviour args shared by both runs. No --initially-drop-* flags
# (the resume run must keep the ghost/checkpoint tables produced by the first
# run) and no postpone flag (gh-ost re-creates a removed flag file at startup,
# which would re-postpone the resume run).
base_args="--user=gh-ost --password=gh-ost \
  --host=$replica_host --port=$replica_port \
  --assume-master-host=${master_host}:${master_port} \
  --database=test --table=${table_name} \
  --storage-engine=${storage_engine} --alter='engine=${storage_engine}' \
  --exact-rowcount --assume-rbr --skip-metadata-lock-check \
  --serve-socket-file=/tmp/gh-ost.test.sock --initially-drop-socket-file \
  --test-on-replica --default-retries=3 --chunk-size=10 \
  --checkpoint --checkpoint-seconds=10 --parallel-copy --parallel-copy-workers=4 \
  --verbose --debug --stack"

echo >$test_logfile

# --- First run: start, then simulate a crash after a checkpoint -------------
# The postpone flag keeps the first run alive past row-copy so a checkpoint is
# guaranteed to land and the kill cannot race the cut-over (which would otherwise
# leave replication stopped). --nice-ratio slows the copy so the checkpoint is
# likely to land mid-copy, exercising resume-with-gaps.
touch $postpone_flag_file
first_cmd="GOTRACEBACK=crash $ghost_binary $base_args \
  --nice-ratio=5 --postpone-cut-over-flag-file=$postpone_flag_file \
  --initially-drop-ghost-table --initially-drop-old-table --execute"
echo "$first_cmd" >>$test_logfile
bash -c "$first_cmd" >>$test_logfile 2>&1 &
ghost_pid=$!

# Wait for the first checkpoint, then kill -9 to simulate an uncatchable crash
# (no graceful table cleanup).
for i in {1..90}; do
    grep -q "checkpoint success" $test_logfile && break
    ps -p $ghost_pid >/dev/null 2>&1 || break
    sleep 1
    echo_dot
done
kill -9 $ghost_pid 2>/dev/null
wait $ghost_pid 2>/dev/null
rm -f $postpone_flag_file

if ! grep -q "checkpoint success" $test_logfile; then
    echo
    echo "ERROR $test_name: first run never wrote a parallel checkpoint; cannot test resume"
    print_log_excerpt
    return 1
fi

# --- Resume run: must reuse the checkpoint and complete the migration -------
resume_cmd="GOTRACEBACK=crash $ghost_binary $base_args --resume --execute"
echo "$resume_cmd" >>$test_logfile
timeout $test_timeout bash -c "$resume_cmd" >>$test_logfile 2>&1
execution_result=$?
if [ $execution_result -ne 0 ]; then
    echo
    echo "ERROR $test_name: resume run failed (exit ${execution_result})"
    print_log_excerpt
    return 1
fi

# --- Validate: ghost table matches original ---------------------------------
# With --test-on-replica the rename is rolled back, so the ghost table remains
# for comparison.
gh-ost-test-mysql-replica --default-character-set=utf8mb4 test -e "select * from ${table_name} order by id" -ss >$orig_content_output_file
gh-ost-test-mysql-replica --default-character-set=utf8mb4 test -e "select * from ${ghost_table_name} order by id" -ss >$ghost_content_output_file
orig_checksum=$(cat $orig_content_output_file | md5sum)
ghost_checksum=$(cat $ghost_content_output_file | md5sum)
if [ "$orig_checksum" != "$ghost_checksum" ]; then
    echo
    echo "ERROR $test_name: checksum mismatch after resume"
    diff $orig_content_output_file $ghost_content_output_file | head -40
    return 1
fi
gh-ost-test-mysql-master --default-character-set=utf8mb4 test -e "
    drop table if exists _gh_ost_test_gho;
    drop table if exists _gh_ost_test_ghc;
    drop table if exists _gh_ost_test_ghk;
    drop table if exists _gh_ost_test_del;
    drop event if exists gh_ost_test;
    drop table if exists gh_ost_test;
"
return 0
