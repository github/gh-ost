#!/bin/bash

# Local integration tests. To be used by CI.
# See https://github.com/github/gh-ost/tree/doc/local-tests.md
#

# Usage: localtests/test/sh [filter]
# By default, runs all tests. Given filter, will only run tests matching given regep

tests_path=$(dirname $0)
test_logfile=/tmp/gh-ost-test.log
default_ghost_binary=/tmp/gh-ost-test
ghost_binary=""
docker=false
toxiproxy=false
gtid=false
storage_engine=innodb
exec_command_file=/tmp/gh-ost-test.bash
ghost_structure_output_file=/tmp/gh-ost-test.ghost.structure.sql
orig_content_output_file=/tmp/gh-ost-test.orig.content.csv
ghost_content_output_file=/tmp/gh-ost-test.ghost.content.csv
throttle_flag_file=/tmp/gh-ost-test.ghost.throttle.flag
table_name=
ghost_table_name=

master_host=
master_port=
replica_host=
replica_port=
original_sql_mode=
current_gtid_mode=
sysbench_pid=

OPTIND=1
while getopts "b:s:dtg" OPTION; do
    case $OPTION in
    b)
        ghost_binary="$OPTARG"
        ;;
    s)
        storage_engine="$OPTARG"
        ;;
    t)
        toxiproxy=true
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

verify_master_and_replica() {
    if [ "$(gh-ost-test-mysql-master -e "select 1" -ss)" != "1" ]; then
        echo "Cannot verify gh-ost-test-mysql-master"
        exit 1
    fi
    read master_host master_port <<<$(gh-ost-test-mysql-master -e "select @@hostname, @@port" -ss)
    [ "$master_host" == "$(hostname)" ] && master_host="127.0.0.1"
    echo "# master verified at $master_host:$master_port"
    if ! gh-ost-test-mysql-master -e "set global event_scheduler := 1"; then
        echo "Cannot enable event_scheduler on master"
        exit 1
    fi
    original_sql_mode="$(gh-ost-test-mysql-master -e "select @@global.sql_mode" -s -s)"
    echo "sql_mode on master is ${original_sql_mode}"

    current_gtid_mode=$(gh-ost-test-mysql-master -s -s -e "select @@global.gtid_mode" 2>/dev/null || echo unsupported)
    current_enforce_gtid_consistency=$(gh-ost-test-mysql-master -s -s -e "select @@global.enforce_gtid_consistency" 2>/dev/null || echo unsupported)
    current_master_server_uuid=$(gh-ost-test-mysql-master -s -s -e "select @@global.server_uuid" 2>/dev/null || echo unsupported)
    current_replica_server_uuid=$(gh-ost-test-mysql-replica -s -s -e "select @@global.server_uuid" 2>/dev/null || echo unsupported)
    echo "gtid_mode on master is ${current_gtid_mode} with enforce_gtid_consistency=${current_enforce_gtid_consistency}"
    echo "server_uuid on master is ${current_master_server_uuid}, replica is ${current_replica_server_uuid}"

    echo "Gracefully sleeping for 3 seconds while replica is setting up..."
    sleep 3

    if [ "$(gh-ost-test-mysql-replica -e "select 1" -ss)" != "1" ]; then
        echo "Cannot verify gh-ost-test-mysql-replica"
        exit 1
    fi
    if [ "$(gh-ost-test-mysql-replica -e "select @@global.binlog_format" -ss)" != "ROW" ]; then
        echo "Expecting test replica to have binlog_format=ROW"
        exit 1
    fi
    read replica_host replica_port <<<$(gh-ost-test-mysql-replica -e "select @@hostname, @@port" -ss)
    [ "$replica_host" == "$(hostname)" ] && replica_host="127.0.0.1"
    echo "# replica verified at $replica_host:$replica_port"

    if [ "$docker" = true ]; then
        master_host="0.0.0.0"
        master_port="3307"
        echo "# using docker master at $master_host:$master_port"
        replica_host="0.0.0.0"
        if [ "$toxiproxy" = true ]; then
            replica_port="23308"
            echo "# using toxiproxy replica at $replica_host:$replica_port"
        else
            replica_port="3308"
            echo "# using docker replica at $replica_host:$replica_port"
        fi
    fi
}

exec_cmd() {
    echo "$@"
    command "$@" 1>$test_logfile 2>&1
    return $?
}

echo_dot() {
    echo -n "."
}

start_replication() {
    mysql_version="$(gh-ost-test-mysql-replica -e "select @@version")"
    if [[ $mysql_version =~ "8.4" ]]; then
        seconds_behind_source="Seconds_Behind_Source"
        replica_terminology="replica"
    else
        seconds_behind_source="Seconds_Behind_Master"
        replica_terminology="slave"
    fi
    gh-ost-test-mysql-replica -e "stop $replica_terminology; start $replica_terminology;"

    num_attempts=0
    while gh-ost-test-mysql-replica -e "show $replica_terminology status\G" | grep $seconds_behind_source | grep -q NULL; do
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

sysbench_prepare() {
    local mysql_host="$1"
    local mysql_port="$2"
    sysbench oltp_write_only \
        --mysql-host="$mysql_host" \
        --mysql-port="$mysql_port" \
        --mysql-user=root \
        --mysql-password=opensesame \
        --mysql-db=test \
        --tables=1 \
        --table-size=20000 \
        prepare
}

sysbench_run_cmd() {
    local mysql_host="$1"
    local mysql_port="$2"
    cmd="sysbench oltp_write_only \
    --mysql-host="$mysql_host" \
    --mysql-port="$mysql_port" \
    --mysql-user=root \
    --mysql-password=opensesame \
    --mysql-db=test \
    --rand-seed=163 \
    --tables=1 \
    --threads=2 \
    --time=30 \
    --report-interval=10 \
    --rate=200 \
    run"
    echo $cmd
}

cleanup() {
    if ! [ -z $sysbench_pid ] && ps -p $sysbench_pid >/dev/null; then
        kill $sysbench_pid
    fi
}

test_single() {
    local test_name
    test_name="$1"

    if [ -f $tests_path/$test_name/ignore_versions ]; then
        ignore_versions=$(cat $tests_path/$test_name/ignore_versions)
        mysql_version=$(gh-ost-test-mysql-master -s -s -e "select @@version")
        mysql_version_comment=$(gh-ost-test-mysql-master -s -s -e "select @@version_comment")
        if echo "$mysql_version" | egrep -q "^${ignore_versions}"; then
            echo -n "Skipping: $test_name"
            return 0
        elif echo "$mysql_version_comment" | egrep -i -q "^${ignore_versions}"; then
            echo -n "Skipping: $test_name"
            return 0
        fi
    fi

    echo -n "Testing: $test_name"

    echo_dot
    start_replication
    echo_dot

    if [ -f $tests_path/$test_name/gtid_mode ]; then
        target_gtid_mode=$(cat $tests_path/$test_name/gtid_mode)
        if [ "$current_gtid_mode" != "$target_gtid_mode" ]; then
            echo "gtid_mode is ${current_gtid_mode}, expected ${target_gtid_mode}"
            exit 1
        fi
    fi

    if [ -f $tests_path/$test_name/sql_mode ]; then
        gh-ost-test-mysql-master --default-character-set=utf8mb4 test -e "set @@global.sql_mode='$(cat $tests_path/$test_name/sql_mode)'"
        gh-ost-test-mysql-replica --default-character-set=utf8mb4 test -e "set @@global.sql_mode='$(cat $tests_path/$test_name/sql_mode)'"
    fi

    gh-ost-test-mysql-master --default-character-set=utf8mb4 test <$tests_path/$test_name/create.sql
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
    if [ "$toxiproxy" = true ]; then
        extra_args+=" --skip-port-validation"
    fi
    orig_columns="*"
    ghost_columns="*"
    order_by=""
    if [ -f $tests_path/$test_name/orig_columns ]; then
        orig_columns=$(cat $tests_path/$test_name/orig_columns)
    fi
    if [ -f $tests_path/$test_name/ghost_columns ]; then
        ghost_columns=$(cat $tests_path/$test_name/ghost_columns)
    fi
    if [ -f $tests_path/$test_name/order_by ]; then
        order_by="order by $(cat $tests_path/$test_name/order_by)"
    fi
    # graceful sleep for replica to catch up
    echo_dot
    sleep 1

    table_name="gh_ost_test"
    ghost_table_name="_gh_ost_test_gho"
    # test with sysbench oltp write load
    if [[ "$test_name" == "sysbench" ]]; then
        if ! command -v sysbench &>/dev/null; then
            echo "skipping"
            return 0
        fi
        table_name="sbtest1"
        ghost_table_name="_${table_name}_gho"
        echo "Preparing sysbench..."
        sysbench_prepare "$master_host" "$master_port"

        load_cmd="$(sysbench_run_cmd $master_host $master_port)"
        eval "$load_cmd" &
        sysbench_pid=$!
        echo
        echo -n "Started sysbench (PID $sysbench_pid):  "
        echo $load_cmd
    fi
    trap cleanup SIGINT

    #
    cmd="GOTRACEBACK=crash $ghost_binary \
    --user=gh-ost \
    --password=gh-ost \
    --host=$replica_host \
    --port=$replica_port \
    --assume-master-host=${master_host}:${master_port}
    --database=test \
    --table=${table_name} \
    --storage-engine=${storage_engine} \
    --alter='engine=${storage_engine}' \
    --exact-rowcount \
    --assume-rbr \
    --skip-metadata-lock-check \
    --initially-drop-old-table \
    --initially-drop-ghost-table \
    --throttle-query='select timestampdiff(second, min(last_update), now()) < 5 from _${table_name}_ghc' \
    --throttle-flag-file=$throttle_flag_file \
    --serve-socket-file=/tmp/gh-ost.test.sock \
    --initially-drop-socket-file \
    --test-on-replica \
    --default-retries=3 \
    --chunk-size=10 \
    --verbose \
    --debug \
    --stack \
    --checkpoint \
    --execute ${extra_args[@]}"
    echo_dot
    echo $cmd >$exec_command_file
    echo_dot
    bash $exec_command_file 1>$test_logfile 2>&1

    execution_result=$?
    cleanup

    if [ -f $tests_path/$test_name/sql_mode ]; then
        gh-ost-test-mysql-master --default-character-set=utf8mb4 test -e "set @@global.sql_mode='${original_sql_mode}'"
        gh-ost-test-mysql-replica --default-character-set=utf8mb4 test -e "set @@global.sql_mode='${original_sql_mode}'"
    fi

    if [ -f $tests_path/$test_name/destroy.sql ]; then
        gh-ost-test-mysql-master --default-character-set=utf8mb4 test <$tests_path/$test_name/destroy.sql
    fi

    if [ -f $tests_path/$test_name/expect_failure ]; then
        if [ $execution_result -eq 0 ]; then
            echo
            echo "ERROR $test_name execution was expected to exit on error but did not. cat $test_logfile"
            return 1
        fi
        if [ -s $tests_path/$test_name/expect_failure ]; then
            # 'expect_failure' file has content. We expect to find this content in the log.
            expected_error_message="$(cat $tests_path/$test_name/expect_failure)"
            if grep -q "$expected_error_message" $test_logfile; then
                return 0
            fi
            echo
            echo "ERROR $test_name execution was expected to exit with error message '${expected_error_message}' but did not. cat $test_logfile"
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

    gh-ost-test-mysql-replica --default-character-set=utf8mb4 test -e "show create table ${ghost_table_name}\G" -ss >$ghost_structure_output_file

    if [ -f $tests_path/$test_name/expect_table_structure ]; then
        expected_table_structure="$(cat $tests_path/$test_name/expect_table_structure)"
        if ! grep -q "$expected_table_structure" $ghost_structure_output_file; then
            echo
            echo "ERROR $test_name: table structure was expected to include ${expected_table_structure} but did not. cat $ghost_structure_output_file:"
            cat $ghost_structure_output_file
            return 1
        fi
    fi

    echo_dot
    gh-ost-test-mysql-replica --default-character-set=utf8mb4 test -e "select ${orig_columns} from ${table_name} ${order_by}" -ss >$orig_content_output_file
    gh-ost-test-mysql-replica --default-character-set=utf8mb4 test -e "select ${ghost_columns} from ${ghost_table_name} ${order_by}" -ss >$ghost_content_output_file
    orig_checksum=$(cat $orig_content_output_file | md5sum)
    ghost_checksum=$(cat $ghost_content_output_file | md5sum)

    if [ "$orig_checksum" != "$ghost_checksum" ]; then
        gh-ost-test-mysql-replica --default-character-set=utf8mb4 test -e "select ${orig_columns} from ${table_name}" -ss >$orig_content_output_file
        gh-ost-test-mysql-replica --default-character-set=utf8mb4 test -e "select ${ghost_columns} from ${ghost_table_name}" -ss >$ghost_content_output_file
        echo "ERROR $test_name: checksum mismatch"
        echo "---"
        diff $orig_content_output_file $ghost_content_output_file

        echo "diff $orig_content_output_file $ghost_content_output_file"

        return 1
    fi
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
        if ! test_single "$test_name"; then
            create_statement=$(gh-ost-test-mysql-replica test -t -e "show create table ${ghost_table_name} \G")
            echo "$create_statement" >>$test_logfile
            echo "+ FAIL"
            return 1
        else
            echo
            echo "+ pass"
        fi
        mysql_version="$(gh-ost-test-mysql-replica -e "select @@version")"
        replica_terminology="slave"
        if [[ $mysql_version =~ "8.4" ]]; then
            replica_terminology="replica"
        fi
        gh-ost-test-mysql-replica -e "start $replica_terminology"
    done <<<"$test_dirs"
}

verify_master_and_replica
test_all
