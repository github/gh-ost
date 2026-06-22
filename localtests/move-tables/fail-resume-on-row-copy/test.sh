
#!/bin/bash
# Custom test: 
# - panic during row copy stage, prior to cutover
# - resume and complete the migration

database=test
table_name=gh_ost_test

# Build gh-ost command from scratch using framework function  (required to inject failpoints)
rm $ghost_binary
build_binary

######################################################################################################
### Run #1: Should panic after first row copy and migration will not complete
######################################################################################################

echo  "⚙️ Starting migration with failpoint (run #1)..."

# Build the gh-ost command using the framework function
GO_FAILPOINTS="github.com/github/gh-ost/go/base/panic-after-row-copy=return(true)" build_ghost_command
cmd="$cmd --checkpoint-seconds=1"

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

# checkpoint file exists on target and is non-empty
results=$(mysql-exec target primary $database -sNe "SELECT 1 FROM _${table_name}_ghk LIMIT 1;")
if [ -z "$results" ]; then
    echo "ERROR: Checkpoint file is empty or does not exist."
    return 1
fi

# original table still exists on source
results=$(mysql-exec source replica $database -sNe "SELECT 1 FROM ${table_name} LIMIT 1;")
if [ -z "$results" ]; then
    echo "ERROR: Table '${table_name}' does not exist on the source cluster."
    return 1
fi

# original table exists on the target
results=$(mysql-exec target replica $database -sNe "SELECT 1 FROM ${table_name} LIMIT 1;")
if [ -z "$results" ]; then
    echo "ERROR: Table '${table_name}' does not exist on the target cluster."
    return 1
fi

echo  "✅ Validating checkpointed state on unexpected exit..."

echo -e "\n\n\n\n\n"

######################################################################################################
### Run #2: Resume and complete the migration
######################################################################################################

echo  "⚙️ Resuming migration (run #2)..."

# queue up removal of the postpone cutover flag, otherwise gh-ost hangs on the cutover
(
    sleep 1; 
    echo "⏩ Sending unpostpone cutover"
    rm $postpone_cutover_flag_file &> /dev/null;
) &

# resume migration
build_ghost_command
cmd="$cmd --resume"

bash -c "$cmd" >>$test_logfile 2>&1
ghost_result=$?

if [ $ghost_result -neq 0 ]; then
    echo "ERROR: gh-ost should have succeeded but did not. ($ghost_result)"
    return 1
fi

echo -e "\n\n\n\n\n"
