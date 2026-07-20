
#!/bin/bash
# Custom test:
#   Executes a MULTI-table move-tables migration with custom hooks
#   (on-row-copy-complete, on-before-cut-over, on-success). The hooks assert the
#   environment-variable contract gh-ost exposes for multi-table moves — most
#   importantly that GH_OST_TABLE_NAME (and the ghost/target/old/tables vars) are
#   comma-joined lists. This is the exact contract the db-infra-scripts
#   move-tables ACL hooks depend on; getting it wrong caused a per-table GRANT
#   bug that a single-table test could not catch.

database=test

# Build gh-ost command from scratch using framework function
build_binary

######################################################################################################
### Run gh-ost with custom hooks enabled
######################################################################################################

echo  "Running gh-ost (multi-table) with custom hooks..."

# ensure hook files are executable
chmod +x $tests_path/$test_name/hooks/*

# clean up any existing test hook files
rm -rf /tmp/gh-ost-hooks/
mkdir -p /tmp/gh-ost-hooks/

# Build the gh-ost command using the framework function (moves every table listed
# in tables.txt, which test_single loaded into tables_to_migrate).
build_ghost_command
cmd="$cmd --hooks-path=$tests_path/$test_name/hooks"

# queue up removal of the postpone cutover flag, otherwise gh-ost hangs on the cutover
(
    sleep 2;
    echo "Sending unpostpone cutover"
    rm $postpone_cutover_flag_file &> /dev/null;
) &

# Run the gh-ost command
echo_dot
echo > $test_logfile
bash -c "$cmd" >>$test_logfile 2>&1
ghost_result=$?

if [ $ghost_result -ne 0 ]; then
    echo "ERROR: gh-ost failed unexpectedly."
    return 1
fi

echo  "gh-ost move-tables succeeded!"

echo -e "\n\n\n\n\n"


######################################################################################################
### Validate hook status
######################################################################################################

echo  "Validating hook status after execution..."

for expected in on-row-copy-complete on-before-cut-over on-success; do
    if [ ! -f "/tmp/gh-ost-hooks/$expected" ]; then
        echo "ERROR: Expected test hook file '/tmp/gh-ost-hooks/$expected' was not found."
        return 1
    fi
done

echo  "Hook status validated successfully."
