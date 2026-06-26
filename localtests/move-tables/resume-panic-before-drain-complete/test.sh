
#!/bin/bash
# Custom test:
# - panic after RENAME (T1) and prior to drain completion (T3), prior to cutover completion
# - validate RENAME and source writes are not possible
# - resume and complete the migration

set -x

database=test
table_name=gh_ost_test

# Build gh-ost command from scratch using framework function  (required to inject failpoints)
rm $ghost_binary
build_binary

######################################################################################################
### Run #1: Should panic after RENAME (T1) and before drain completion (T3)
######################################################################################################

echo  "⚙️ Starting migration with failpoint (run #1)..."

# Build the gh-ost command using the framework function
GO_FAILPOINTS="github.com/github/gh-ost/go/base/move-tables-panic-before-drain-completion=return(true)" build_ghost_command

# queue up removal of the postpone cutover flag, otherwise gh-ost hangs on the cutover
(
    sleep 2;
    echo "⏩ Sending unpostpone cutover"
    rm $postpone_cutover_flag_file &> /dev/null;
) &

# Run the gh-ost command, expecting panic on the failpoint the first time
echo_dot
echo > $test_logfile
bash -c "$cmd" >>$test_logfile 2>&1
ghost_result=$?

if [ $ghost_result -eq 0 ]; then
    echo "ERROR: gh-ost should have failed but did not."
    return 1
fi

echo -e "\n\n\n\n\n"

######################################################################################################
### Intermediate validation
######################################################################################################

echo  "⚙️ Validating checkpointed state on unexpected exit..."

# Table was renamed on source
mysql-exec source primary $database -sNe "SELECT 1 FROM ${table_name} LIMIT 1;"
if [ $? -eq 0 ]; then
    echo "ERROR: Table '${table_name}' exists on source but show have been renamed."
    return 1
fi

mysql-exec source primary $database -sNe "SELECT 1 FROM _${table_name}_del LIMIT 1;"
if [ $? -gt 0 ]; then
    echo "ERROR: Renamed table '_${table_name}_del' does not exist on source."
    return 1
fi

# Table not writeable on source
mysql-exec source primary $database -e "INSERT INTO ${table_name} VALUES (NULL, 1021, 2001, 2400001, 201, 1700000041, 1700000041);"
if [ $? -eq 0 ]; then
    echo "ERROR: Table '${table_name}' was writeable on source but should not be!."
    return 1
fi

# Table still exists on target
mysql-exec target primary $database -sNe "SELECT 1 FROM ${table_name} LIMIT 1;"
if [ $? -gt 0 ]; then
    echo "ERROR: Table '${table_name}' does not exist on target."
    return 1
fi

# validate last checkpoint (cutover started and drain GTID are set). The
# checkpoint table is named from the run token (_gho_<token>_ghk), so look it up
# by pattern rather than a static per-table name.
checkpoint_table=$(mysql-exec target primary $database -sNe "SELECT table_name FROM information_schema.tables WHERE table_schema='${database}' AND table_name LIKE '\\_gho\\_%\\_ghk' LIMIT 1;")
if [ -z "$checkpoint_table" ]; then
    echo "ERROR: Checkpoint table does not exist."
    return 1
fi

cutover_started=$(mysql-exec target primary $database -Ne "SELECT gh_ost_move_tables_cutover_started FROM \`${checkpoint_table}\` ORDER BY gh_ost_chk_id DESC LIMIT 1;")
if [ "$cutover_started" != 1 ]; then
    echo "ERROR: Expected cutover started to be set in last checkpoint."
    return 1
fi

drain_gtid=$(mysql-exec target primary $database -Ne "SELECT gh_ost_move_tables_drain_gtid FROM \`${checkpoint_table}\` ORDER BY gh_ost_chk_id DESC LIMIT 1;")
if [ "$drain_gtid" == "" ]; then
    echo "ERROR: Expected drain GTID to be set in last checkpoint."
    return 1
fi

echo  "✅ Validated checkpointed state on unexpected exit..."

echo -e "\n\n\n\n\n"

######################################################################################################
### Run #2: Resume and complete the migration
######################################################################################################

echo  "⚙️ Resuming migration (run #2)..."

# resume migration
build_ghost_command
cmd="$cmd --resume"

# queue up removal of the postpone cutover flag, otherwise gh-ost hangs on the cutover
(
    sleep 2;
    echo "⏩ Sending unpostpone cutover"
    rm $postpone_cutover_flag_file &> /dev/null;
) &

bash -c "$cmd" >>$test_logfile 2>&1
ghost_result=$?

if [ $ghost_result -ne 0 ]; then
    echo "ERROR: gh-ost should have succeeded but did not. ($ghost_result)"
    return 1
fi

echo -e "\n\n\n\n\n"
