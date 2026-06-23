
#!/bin/bash
# Custom test:
# - panic after drain (T4) and prior to on-success (T5), prior to cutover completion
# - validate RENAME and source writes are not possible
# - validate contents of source and target are the same
# - resume and complete the migration

database=test
table_name=gh_ost_test

# Build gh-ost command from scratch using framework function  (required to inject failpoints)
rm $ghost_binary
build_binary

# ensure hook files are executable
chmod +x $tests_path/$test_name/hooks/*

# clean up any existing test hook files
rm -rf /tmp/gh-ost-hooks/
mkdir -p /tmp/gh-ost-hooks/

######################################################################################################
### Run #1: Should panic after drain (T4) and before on-success (T5)
######################################################################################################


echo  "⚙️ Starting migration with failpoint (run #1)..."

# Build the gh-ost command using the framework function
GO_FAILPOINTS="github.com/github/gh-ost/go/base/move-tables-panic-before-on-success-hook=return(true)" build_ghost_command
cmd="$cmd --hooks-path=$tests_path/$test_name/hooks"

# queue up removal of the postpone cutover flag, otherwise gh-ost hangs on the cutover
(
    sleep 2;
    echo "⏩ Sending unpostpone cutover"
    rm $postpone_cutover_flag_file &> /dev/null;
) &

# drive some concurrent writes to the table to exercise queue drain (T3/T4)
(
    DATABASE=test script/move-tables/insert-source-primary-loop 100 0.1 10 &>/dev/null &
    writes_pid=$!
    sleep 3
    kill $writes_pid
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
mysql-exec source primary $database -sNe "INSERT INTO ${table_name} VALUES (NULL, 1021, 2001, 2400001, 201, 1700000041, 1700000041);"
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

# contents of table on source and target are the same
source_contents_file=/tmp/gh-ost-test.resume-move-tables-panic-before-on-success-hook-source_contents.txt
target_contents_file=/tmp/gh-ost-test.resume-move-tables-panic-before-on-success-hook-target_contents.txt
mysql-exec source primary $database -sNe "SELECT * FROM _${table_name}_del;" > $source_contents_file
mysql-exec target primary $database -sNe "SELECT * FROM ${table_name};" > $target_contents_file

if ! diff $source_contents_file $target_contents_file; then
    echo "ERROR: Contents of table '${table_name}' are not the same on source and target."
    echo "---- DIFF -----"
    diff --side-by-side $source_contents_file $target_contents_file
    echo "---------------"
    return 1
fi

# validate on-success hook was not called
if [ -f /tmp/gh-ost-hooks/on-success ]; then
    echo "ERROR: on-success hook was called when it should not have been."
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
cmd="$cmd --resume --hooks-path=$tests_path/$test_name/hooks"

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

# validate on-success hook was was called
if [ ! -f /tmp/gh-ost-hooks/on-success ]; then
    echo "ERROR: on-success hook was not called when it should have been."
fi

echo -e "\n\n\n\n\n"
