#!/bin/bash
# Custom test: inject conflicting data AFTER row copy completes
# This tests the DML event application code path, not row copy

# Create postpone flag file (referenced in extra_args)
postpone_flag_file=/tmp/gh-ost-test.postpone-cutover
touch $postpone_flag_file

# Build gh-ost command using framework function
build_ghost_command

# Run in background
echo_dot
# Clear log file before starting gh-ost
echo > $test_logfile
bash -c "$cmd" >>$test_logfile 2>&1 &
ghost_pid=$!

# Wait for row copy to complete
echo_dot
for i in {1..30}; do
    grep -q "Row copy complete" $test_logfile && break
    ps -p $ghost_pid > /dev/null || { echo; echo "ERROR gh-ost exited early"; rm -f $postpone_flag_file; return 1; }
    sleep 1; echo_dot
done

# Inject conflicting SQL after row copy (UPDATE with PK change creates DELETE+INSERT in binlog)
echo_dot
gh-ost-test-mysql-master test -e "update gh_ost_test set id = 200, email = 'alice@example.com' where id = 2"

# Wait for binlog event to replicate and be applied
sleep 10; echo_dot

# Complete cutover by removing postpone flag
rm -f $postpone_flag_file

# Wait for gh-ost to complete
wait $ghost_pid
execution_result=$?
rm -f $postpone_flag_file

# Validate using framework function
validate_expected_failure
return $?
