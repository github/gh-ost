#!/bin/bash

# Local integration tests. To be used by CI.
# See https://github.com/github/gh-ost/tree/doc/local-tests.md
#

# Usage: localtests/test/sh [filter]
# By default, runs all move-tables tests. Given filter, will only run tests matching given regep

# TODO(chriskirkland):
# - (nice-to-have) remove trailing newline requirement for tables.txt
# - ...

repo_root=$(git rev-parse --show-toplevel)
script_path="$repo_root/script/move-tables"
tests_path=$(dirname $0)/move-tables
test_logfile=/tmp/gh-ost-test.log
default_ghost_binary=/tmp/gh-ost-test
ghost_binary=""
database=test
docker=false
gtid=false
storage_engine=innodb
exec_command_file=/tmp/gh-ost-test.bash
orig_structure_output_file=/tmp/gh-ost-test.orig.structure.sql
ghost_structure_output_file=/tmp/gh-ost-test.ghost.structure.sql
orig_content_output_file=/tmp/gh-ost-test.orig.content.csv
ghost_content_output_file=/tmp/gh-ost-test.ghost.content.csv
postpone_cutover_flag_file=/tmp/gh-ost-test.ghost.postpone.flag

source_master_host=
source_master_port=
source_replica_host=
source_replica_port=
target_master_host=
target_master_port=
target_replica_host=
target_replica_port=
original_sql_mode=
current_gtid_mode=
test_timeout=120
test_failure_log_tail_lines=50

OPTIND=1
while getopts "b:s:dg" OPTION; do
    case $OPTION in
    b)
        ghost_binary="$OPTARG"
        ;;
    s)
        storage_engine="$OPTARG"
        ;;
    d)
        docker=true
        ;;
    g)
        gtid=true
        ;;
    esac
done
shift $((OPTIND - 1))

test_pattern="${1:-.}"

mysql-exec() {
    cluster=$1
    role=$2
    shift 2

    if [[ $TEST_MYSQL_IMAGE =~ "mysql:8.4" ]]; then
        $script_path/mysql-$cluster-$role --ssl-mode=required "$@"
    else
        $script_path/mysql-$cluster-$role "$@"
    fi
}

verify_master_and_replica() {
    cluster=$1
    echo "Verifying $cluster cluster..."

    if [ "$(mysql-exec $cluster primary -e "select 1" -ss)" != "1" ]; then
        echo "Cannot verify $cluster primary"
        exit 1
    fi
    read master_host master_port <<<$(mysql-exec $cluster primary -e "select @@hostname, @@port" -ss)
    [ "$master_host" == "$(hostname)" ] && master_host="127.0.0.1"
    echo "# master verified at $master_host:$master_port"
    if ! mysql-exec $cluster primary -e "set global event_scheduler := 1"; then
        echo "Cannot enable event_scheduler on master"
        exit 1
    fi
    original_sql_mode="$(mysql-exec $cluster primary -e "select @@global.sql_mode" -s -s)"
    echo "sql_mode on master is ${original_sql_mode}"

    current_gtid_mode=$(mysql-exec $cluster primary -s -s -e "select @@global.gtid_mode" 2>/dev/null || echo unsupported)
    current_enforce_gtid_consistency=$(mysql-exec $cluster primary -s -s -e "select @@global.enforce_gtid_consistency" 2>/dev/null || echo unsupported)
    current_master_server_uuid=$(mysql-exec $cluster primary -s -s -e "select @@global.server_uuid" 2>/dev/null || echo unsupported)
    current_replica_server_uuid=$(mysql-exec $cluster replica -s -s -e "select @@global.server_uuid" 2>/dev/null || echo unsupported)
    echo "gtid_mode on master is ${current_gtid_mode} with enforce_gtid_consistency=${current_enforce_gtid_consistency}"
    echo "server_uuid on master is ${current_master_server_uuid}, replica is ${current_replica_server_uuid}"

    echo "Gracefully sleeping for 3 seconds while replica is setting up..."
    sleep 3

    if [ "$(mysql-exec $cluster replica -e "select 1" -ss)" != "1" ]; then
        echo "Cannot verify gh-ost-test-mysql-replica"
        exit 1
    fi
    if [ "$(mysql-exec $cluster replica -e "select @@global.binlog_format" -ss)" != "ROW" ]; then
        echo "Expecting test replica to have binlog_format=ROW"
        exit 1
    fi
    read replica_host replica_port <<<$(mysql-exec $cluster replica -e "select @@hostname, @@port" -ss)
    [ "$replica_host" == "$(hostname)" ] && replica_host="127.0.0.1"
    echo "# replica verified at $replica_host:$replica_port"

    if [ "$docker" = true ]; then
        master_host="0.0.0.0"
        if [ "$cluster" == "source" ]; then
            master_port="3307"
        elif [ "$cluster" == "target" ]; then
            master_port="3309"
        fi
        echo "# using docker master at $master_host:$master_port"
        replica_host="0.0.0.0"
        if [ "$cluster" == "source" ]; then
            replica_port="3308"
        elif [ "$cluster" == "target" ]; then
            replica_port="3310"
        fi
        echo "# using docker replica at $replica_host:$replica_port"
    fi

    if [ "$cluster" == "source" ]; then
        source_master_host=$master_host
        source_master_port=$master_port
        source_replica_host=$replica_host
        source_replica_port=$replica_port
    elif [ "$cluster" == "target" ]; then
        target_master_host=$master_host
        target_master_port=$master_port
        target_replica_host=$replica_host
        target_replica_port=$replica_port
    fi
}

echo_dot() {
    echo -n "."
}

start_replication() {
    cluster=$1

    echo "Starting replication for $cluster..."
    mysql_version="$(mysql-exec $cluster replica -e "select @@version")"
    if [[ $mysql_version =~ "8.4" ]]; then
        seconds_behind_source="Seconds_Behind_Source"
        replica_terminology="replica"
    else
        seconds_behind_source="Seconds_Behind_Master"
        replica_terminology="slave"
    fi
    mysql-exec $cluster replica -e "stop $replica_terminology; start $replica_terminology;"

    num_attempts=0
    while mysql-exec $cluster replica -e "show $replica_terminology status\G" | grep $seconds_behind_source | grep -q NULL; do
        ((num_attempts = num_attempts + 1))
        if [ $num_attempts -gt 10 ]; then
            echo
            echo "ERROR replication failure"
            exit 1
        fi
        echo_dot
        sleep 1
    done
}

build_ghost_command() {
    # Build gh-ost command with all standard options
    #
    # expected $1 to be a comma-separated list of tables to move
    cmd="GOTRACEBACK=crash $ghost_binary \
    --move-tables=$1 \
    --user=root \
    --password=opensesame \
    --host=$source_replica_host \
    --port=$source_replica_port \
    --database=$database \
    --target-user=root \
    --target-password=opensesame \
    --target-host=$target_master_host \
    --target-port=$target_master_port \
    --target-database=$database \
    --serve-socket-file=/tmp/gh-ost.test.sock \
    --initially-drop-socket-file \
    --default-retries=3 \
    --chunk-size=10 \
    --verbose \
    --debug \
    --stack \
    --checkpoint \
    --postpone-cut-over-flag-file=$postpone_cutover_flag_file \
    --execute ${extra_args[@]}"
}

print_log_excerpt() {
    echo "=== Last $test_failure_log_tail_lines lines of $test_logfile ==="
    tail -n $test_failure_log_tail_lines $test_logfile
    echo "=== End log excerpt ==="
}

validate_expected_failure() {
    # Check if test expected to fail and validate error message
    # Expects: tests_path, test_name, execution_result, test_logfile
    if [ -f $tests_path/$test_name/expect_failure ]; then
        if [ $execution_result -eq 0 ]; then
            echo
            echo "ERROR $test_name execution was expected to exit on error but did not."
            print_log_excerpt
            return 1
        fi
        if [ -s $tests_path/$test_name/expect_failure ]; then
            # 'expect_failure' file has content. We expect to find this content in the log.
            expected_error_message="$(cat $tests_path/$test_name/expect_failure)"
            if grep -q "$expected_error_message" $test_logfile; then
                return 0
            fi
            echo
            echo "ERROR $test_name execution was expected to exit with error message '${expected_error_message}' but did not."
            print_log_excerpt
            return 1
        fi
        # 'expect_failure' file has no content. We generally agree that the failure is correct
        return 0
    fi

    if [ $execution_result -ne 0 ]; then
        echo
        echo "ERROR $test_name execution failure. cat $test_logfile:"
        cat $test_logfile
        return 1
    fi
    return 0
}

cleanup() {
    # reset test database
    mysql-exec source primary --default-character-set=utf8mb4 $database -e "drop database if exists $database; create database $database;"
    mysql-exec target primary --default-character-set=utf8mb4 $database -e "drop database if exists $database; create database $database;"
}

test_single() {
    local test_name
    test_name="$1"

    # Read the list of tables to migrate from the test's tables.txt
    if [ ! -f $tests_path/$test_name/tables.txt ]; then
        echo "🐛 ERROR: $tests_path/$test_name/tables.txt not found"
        return 1
    fi
    echo "----"
    cat $tests_path/$test_name/tables.txt
    echo "----"
    tables_to_migrate=()
    while IFS='' read -r line; do
        tables_to_migrate+=("$line")
    done < <(cat $tests_path/$test_name/tables.txt)

    if [ -f $tests_path/$test_name/ignore_versions ]; then
        ignore_versions=$(cat $tests_path/$test_name/ignore_versions)
        mysql_version=$(mysql-exec source primary -s -s -e "select @@version")
        mysql_version_comment=$(mysql-exec source primary -s -s -e "select @@version_comment")
        if echo "$mysql_version" | egrep -q "^${ignore_versions}"; then
            echo -n "Skipping: $test_name"
            return 0
        elif echo "$mysql_version_comment" | egrep -i -q "^${ignore_versions}"; then
            echo -n "Skipping: $test_name"
            return 0
        fi
    fi

    echo -n "Testing: $test_name (${#tables_to_migrate[@]} table(s))"

    echo_dot
    start_replication source
    start_replication target
    echo_dot

    if [ -f $tests_path/$test_name/gtid_mode ]; then
        target_gtid_mode=$(cat $tests_path/$test_name/gtid_mode)
        if [ "$current_gtid_mode" != "$target_gtid_mode" ]; then
            echo "gtid_mode is ${current_gtid_mode}, expected ${target_gtid_mode}"
            exit 1
        fi
    fi

    if [ -f $tests_path/$test_name/sql_mode ]; then
        mysql-exec source primary --default-character-set=utf8mb4 $database -e "set @@global.sql_mode='$(cat $tests_path/$test_name/sql_mode)'"
        mysql-exec source replica --default-character-set=utf8mb4 $database -e "set @@global.sql_mode='$(cat $tests_path/$test_name/sql_mode)'"
        mysql-exec target primary --default-character-set=utf8mb4 $database -e "set @@global.sql_mode='$(cat $tests_path/$test_name/sql_mode)'"
        mysql-exec target replica --default-character-set=utf8mb4 $database -e "set @@global.sql_mode='$(cat $tests_path/$test_name/sql_mode)'"
    fi

    mysql-exec source primary --default-character-set=utf8mb4 $database <$tests_path/$test_name/create.sql
    test_create_result=$?

    if [ $test_create_result -ne 0 ]; then
        echo
        echo "ERROR $test_name create failure. cat $tests_path/$test_name/create.sql:"
        cat $tests_path/$test_name/create.sql
        return 1
    fi

    extra_args=""
    if [ -f $tests_path/$test_name/extra_args ]; then
        extra_args=$(cat $tests_path/$test_name/extra_args)
    fi
    if [ "$gtid" = true ]; then
        extra_args+=" --gtid"
    fi

    # graceful sleep for replica to catch up
    echo_dot
    sleep 1

    # Check for custom test script
    if [ -f $tests_path/$test_name/test.sh ]; then
        # Run the custom test script in a subshell with timeout monitoring
        # The subshell inherits all functions and variables from the current shell
        (source $tests_path/$test_name/test.sh) &
        test_pid=$!

        # Monitor the test with timeout
        timeout_counter=0
        while kill -0 $test_pid 2>/dev/null; do
            if [ $timeout_counter -ge $test_timeout ]; then
                kill -TERM $test_pid 2>/dev/null
                sleep 1
                kill -KILL $test_pid 2>/dev/null
                wait $test_pid 2>/dev/null
                echo
                echo "ERROR $test_name execution timed out"
                print_log_excerpt
                return 1
            fi
            sleep 1
            ((timeout_counter++))
        done

        # Get the exit code
        wait $test_pid 2>/dev/null
        execution_result=$?
        return $execution_result
    fi

    # kick off the on_test script for the test. this enables arbitrary custom logic 
    # concurrent with the gh-ost process. this enables additional scenarios like
    # streaming of writes prior to the write cutover.
    #
    # IMPORTANT: The on-test script is executed in the background and will be killed as soon
    # as the gh-ost process terminates.
    if [ -f $tests_path/$test_name/on_test.sh ]; then
        $tests_path/$test_name/on_test.sh &> /dev/null &
        on_test_pid=$!
    fi

    # queue up removal of the postpone cutover flag, otherwise gh-ost hangs on the cutover
    (
        sleep 1; 
        echo "⏩ Sending unpostpone cutover"
        rm $postpone_cutover_flag_file &> /dev/null;
    ) &

    # Build and execute gh-ost command
    move_tables_arg=$(IFS=, ; echo "${tables_to_migrate[*]}")
    build_ghost_command "$move_tables_arg"
    echo_dot
    echo $cmd >$exec_command_file
    echo_dot
    timeout $test_timeout bash $exec_command_file >$test_logfile 2>&1

    execution_result=$?

    if [ -n "$on_test_pid" ]; then
        kill -KILL $on_test_pid &>/dev/null
    fi

    # Check for timeout (exit code 124)
    if [ $execution_result -eq 124 ]; then
        echo
        echo "ERROR $test_name execution timed out"
        print_log_excerpt
        return 1
    fi

    if [ -f $tests_path/$test_name/sql_mode ]; then
        mysql-exec source primary --default-character-set=utf8mb4 $database -e "set @@global.sql_mode='${original_sql_mode}'"
        mysql-exec source replica --default-character-set=utf8mb4 $database -e "set @@global.sql_mode='${original_sql_mode}'"
        mysql-exec target primary --default-character-set=utf8mb4 $database -e "set @@global.sql_mode='${original_sql_mode}'"
        mysql-exec target replica --default-character-set=utf8mb4 $database -e "set @@global.sql_mode='${original_sql_mode}'"
    fi

    # TODO(chriskirkland): use this reset schemas and/or ACLs/permissions on each cluster?
    if [ -f $tests_path/$test_name/destroy.sql ]; then
        mysql-exec source primary --default-character-set=utf8mb4 $database <$tests_path/$test_name/destroy.sql
        mysql-exec target primary --default-character-set=utf8mb4 $database <$tests_path/$test_name/destroy.sql
    fi

    # Validate expected failure or success
    if ! validate_expected_failure; then
        return 1
    fi

    # If this was an expected failure test, we're done (no need to validate structure/checksums)
    if [ -f $tests_path/$test_name/expect_failure ]; then
        return 0
    fi

    # Test succeeded - now validate structure/checksums and contents
    for table_name in "${tables_to_migrate[@]}"; do
        echo "⚙️ Validating table: $table_name"

        # Validate that the structure of the table matches on the source and target clusters
        mysql-exec source replica --default-character-set=utf8mb4 $database -e "show create table _${table_name}_del\G" -ss >$orig_structure_output_file
        mysql-exec target replica --default-character-set=utf8mb4 $database -e "show create table ${table_name}\G" -ss >$ghost_structure_output_file

        if ! diff $orig_structure_output_file $ghost_structure_output_file > /dev/null 2>&1 ; then
            echo "ERROR $test_name: structure mismatch on table $table_name"
            echo "---"
            diff $orig_structure_output_file $ghost_structure_output_file

            echo "diff $orig_structure_output_file $ghost_structure_output_file"

            return 1
        fi
        echo "✅ Table $table_name: structures match"

        echo_dot

        # validate contents match
        mysql-exec source replica --default-character-set=utf8mb4 $database -e "select * from _${table_name}_del" -ss >$orig_content_output_file
        mysql-exec target replica --default-character-set=utf8mb4 $database -e "select * from ${table_name}" -ss >$ghost_content_output_file
        orig_content_checksum=$(cat $orig_content_output_file | md5sum)
        ghost_content_checksum=$(cat $ghost_content_output_file | md5sum)

        if [ "$orig_content_checksum" != "$ghost_content_checksum" ]; then
            mysql-exec source replica --default-character-set=utf8mb4 $database -e "select * from _${table_name}_del" -ss >$orig_content_output_file
            mysql-exec target replica --default-character-set=utf8mb4 $database -e "select * from ${table_name}" -ss >$ghost_content_output_file
            echo "ERROR $test_name: checksum mismatch on table $table_name"
            echo "---"
            diff $orig_content_output_file $ghost_content_output_file

            echo "diff $orig_content_output_file $ghost_content_output_file"

            return 1
        fi
        echo "✅ Table $table_name: content checksums match"

        echo_dot
        echo_dot
    done
}

build_binary() {
    echo "Building"
    rm -f $default_ghost_binary
    [ "$ghost_binary" == "" ] && ghost_binary="$default_ghost_binary"
    if [ -f "$ghost_binary" ]; then
        echo "Using binary: $ghost_binary"
        return 0
    fi

    go build -o $ghost_binary go/cmd/gh-ost/main.go

    if [ $? -ne 0 ]; then
        echo "Build failure"
        exit 1
    fi
}

test_all() {
    build_binary
    test_dirs=$(find "$tests_path" -mindepth 1 -maxdepth 1 ! -path . -type d | grep "$test_pattern" | sort)
    while read -r test_dir; do
        test_name=$(basename "$test_dir")
        local test_start_time=$(date +%s)
        if ! test_single "$test_name"; then
            local test_end_time=$(date +%s)
            local test_duration=$((test_end_time - test_start_time))
            echo "+ FAIL (${test_duration}s)"
            return 1
        else
            local test_end_time=$(date +%s)
            local test_duration=$((test_end_time - test_start_time))
            echo
            echo "+ pass (${test_duration}s)"
        fi

        cleanup

        for cluster in source target; do
            mysql_version="$(mysql-exec $cluster replica -e "select @@version")"
            replica_terminology="slave"
            if [[ $mysql_version =~ "8.4" ]]; then
                replica_terminology="replica"
            fi
            mysql-exec $cluster replica -e "start $replica_terminology"
        done
    done <<<"$test_dirs"

    echo "✅ All tests completed."
}

verify_master_and_replica source
verify_master_and_replica target
test_all
