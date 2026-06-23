
#!/bin/bash
# Custom test:
# - panic during row copy stage, prior to cutover
# - resume and complete the migration

database=test
table_name=gh_ost_test

# Build gh-ost command from scratch using framework function
build_binary

######################################################################################################
### Run gh-ost with custom hooks neabled
######################################################################################################

echo  "⚙️ Running gh-ost with custom hooks..."

# ensure hook files are executable
chmod +x $tests_path/$test_name/hooks/*

# clean up any existing test hook files
rm -rf /tmp/gh-ost-hooks/
mkdir -p /tmp/gh-ost-hooks/

# Build the gh-ost command using the framework function
build_ghost_command
cmd="$cmd --hooks-path=$tests_path/$test_name/hooks"

# queue up removal of the postpone cutover flag, otherwise gh-ost hangs on the cutover
(
    sleep 2;
    echo "⏩ Sending unpostpone cutover"
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

echo  "✅ gh-ost move-tables succeeded!"

echo -e "\n\n\n\n\n"


######################################################################################################
### Validate hook status
######################################################################################################

echo  "⚙️ Validating hook status after execution..."

for expected in on-row-copy-complete on-before-cut-over on-success; do
    if [ ! -f "/tmp/gh-ost-hooks/$expected" ]; then
        echo "ERROR: Expected test hook file '/tmp/gh-ost-hooks/$expected' was not found."
        return 1
    fi
done

echo  "✅ Hook status validated successfully."
