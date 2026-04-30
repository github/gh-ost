#!/bin/bash
# Custom test: inject batched DML events AFTER row copy completes
# Tests that warnings in the middle of a DML batch are detected

# Create postpone flag file (referenced in extra_args)
postpone_flag_file=/tmp/gh-ost-test.postpone-cutover
touch $postpone_flag_file

# Set table names (required by build_ghost_command)
table_name="gh_ost_test"
ghost_table_name="_gh_ost_test_gho"

# Build gh-ost command using framework function
build_ghost_command

# Run in background
echo_dot
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

# Inject batched DML events that will create warnings
# These must be in a single transaction to be batched during binlog replay
echo_dot
gh-ost-test-mysql-master test << 'EOF'
BEGIN;
-- INSERT with duplicate PRIMARY KEY - warning on migration key (filtered by gh-ost)
INSERT IGNORE INTO gh_ost_test (id, email) VALUES (1, 'duplicate_pk@example.com');
-- INSERT with duplicate email - warning on unique index (should trigger failure)
INSERT IGNORE INTO gh_ost_test (email) VALUES ('alice@example.com');
-- INSERT with unique data - would succeed if not for previous warning
INSERT IGNORE INTO gh_ost_test (email) VALUES ('new@example.com');
COMMIT;
EOF

# Wait for binlog events to replicate and be applied
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
