#!/bin/bash

# Local integration tests. To be used by CI.
# See https://github.com/github/gh-ost/tree/doc/local-tests.md
#

# Usage: localtests/test/sh [filter]
# By default, runs all tests. Given filter, will only run tests matching given regep

tests_path=$(dirname $0)
test_logfile=/tmp/gh-ost-test.log
ghost_binary=/tmp/gh-ost-test
exec_command_file=/tmp/gh-ost-test.bash
orig_table_csv_file=/tmp/gh-ost-test.orig.csv
ghost_table_csv_file=/tmp/gh-ost-test.ghost.csv

test_pattern="${1:-.}"

master_host=
master_port=
replica_host=
replica_port=

verify_master_and_replica() {
  if [ "$(gh-ost-test-mysql-master -e "select 1" -ss)" != "1" ] ; then
    echo "Cannot verify gh-ost-test-mysql-master"
    exit 1
  fi
  read master_host master_port <<< $(gh-ost-test-mysql-master -e "select @@hostname, @@port" -ss)
  if [ "$(gh-ost-test-mysql-replica -e "select 1" -ss)" != "1" ] ; then
    echo "Cannot verify gh-ost-test-mysql-replica"
    exit 1
  fi
  if [ "$(gh-ost-test-mysql-replica -e "select @@global.binlog_format" -ss)" != "ROW" ] ; then
    echo "Expecting test replica to have binlog_format=ROW"
    exit 1
  fi
  read replica_host replica_port <<< $(gh-ost-test-mysql-replica -e "select @@hostname, @@port" -ss)
}

exec_cmd() {
  echo "$@"
  command "$@" 1> $test_logfile 2>&1
  return $?
}

echo_dot() {
  echo -n "."
}

start_replication() {
  gh-ost-test-mysql-replica -e "stop slave; start slave;"
  num_attempts=0
  while gh-ost-test-mysql-replica -e "show slave status\G" | grep Seconds_Behind_Master | grep -q NULL ; do
    ((num_attempts=num_attempts+1))
    if [ $num_attempts -gt 10 ] ; then
      echo
      echo "ERROR replication failure"
      exit 1
    fi
    echo_dot
    sleep 1
  done
}

test_single() {
  local test_name
  test_name="$1"

  echo -n "Testing: $test_name"

  echo_dot
  start_replication
  echo_dot
  gh-ost-test-mysql-master --default-character-set=utf8mb4 test < $tests_path/$test_name/create.sql

  extra_args=""
  if [ -f $tests_path/$test_name/extra_args ] ; then
    extra_args=$(cat $tests_path/$test_name/extra_args)
  fi
  orig_columns="*"
  ghost_columns="*"
  order_by=""
  if [ -f $tests_path/$test_name/orig_columns ] ; then
    orig_columns=$(cat $tests_path/$test_name/orig_columns)
  fi
  if [ -f $tests_path/$test_name/ghost_columns ] ; then
    ghost_columns=$(cat $tests_path/$test_name/ghost_columns)
  fi
  if [ -f $tests_path/$test_name/order_by ] ; then
    order_by="order by $(cat $tests_path/$test_name/order_by)"
  fi
  # graceful sleep for replica to catch up
  echo_dot
  sleep 1
  #
  cmd="$ghost_binary \
    --user=gh-ost \
    --password=gh-ost \
    --host=$replica_host \
    --port=$replica_port \
    --database=test \
    --table=gh_ost_test \
    --alter='engine=innodb' \
    --exact-rowcount \
    --assume-rbr \
    --initially-drop-old-table \
    --initially-drop-ghost-table \
    --throttle-query='select timestampdiff(second, min(last_update), now()) < 5 from _gh_ost_test_ghc' \
    --serve-socket-file=/tmp/gh-ost.test.sock \
    --initially-drop-socket-file \
    --test-on-replica \
    --default-retries=1 \
    --chunk-size=10 \
    --verbose \
    --debug \
    --stack \
    --execute ${extra_args[@]}"
  echo_dot
  echo $cmd > $exec_command_file
  echo_dot
  bash $exec_command_file 1> $test_logfile 2>&1

  execution_result=$?

  if [ -f $tests_path/$test_name/destroy.sql ] ; then
    gh-ost-test-mysql-master --default-character-set=utf8mb4 test < $tests_path/$test_name/destroy.sql
  fi

  if [ -f $tests_path/$test_name/expect_failure ] ; then
    if [ $execution_result -eq 0 ] ; then
      echo
      echo "ERROR $test_name execution was expected to exit on error but did not. cat $test_logfile"
      return 1
    fi
    if [ -s $tests_path/$test_name/expect_failure ] ; then
      # 'expect_failure' file has content. We expect to find this content in the log.
      expected_error_message="$(cat $tests_path/$test_name/expect_failure)"
      if grep -q "$expected_error_message" $test_logfile ; then
          return 0
      fi
      echo
      echo "ERROR $test_name execution was expected to exit with error message '${expected_error_message}' but did not. cat $test_logfile"
      return 1
    fi
    # 'expect_failure' file has no content. We generally agree that the failure is correct
    return 0
  fi

  if [ $execution_result -ne 0 ] ; then
    echo
    echo "ERROR $test_name execution failure. cat $test_logfile"
    return 1
  fi

  echo_dot
  orig_checksum=$(gh-ost-test-mysql-replica --default-character-set=utf8mb4 test -e "select ${orig_columns} from gh_ost_test ${order_by}" -ss | md5sum)
  ghost_checksum=$(gh-ost-test-mysql-replica --default-character-set=utf8mb4 test -e "select ${ghost_columns} from _gh_ost_test_gho ${order_by}" -ss | md5sum)

  if [ "$orig_checksum" != "$ghost_checksum" ] ; then
    gh-ost-test-mysql-replica --default-character-set=utf8mb4 test -e "select ${orig_columns} from gh_ost_test" -ss > ${orig_table_csv_file}
    gh-ost-test-mysql-replica --default-character-set=utf8mb4 test -e "select ${ghost_columns} from _gh_ost_test_gho" -ss > ${ghost_table_csv_file}
    echo "ERROR $test_name: checksum mismatch"
    echo "---"
    cat ${orig_table_csv_file}
    echo "---"
    cat ${ghost_table_csv_file}
    echo "---"
    echo "diff ${orig_table_csv_file} ${ghost_table_csv_file}"
    return 1
  fi
}

build_binary() {
  echo "Building"
  go build -o $ghost_binary go/cmd/gh-ost/main.go
}

test_all() {
  build_binary
  find $tests_path ! -path . -type d -mindepth 1 -maxdepth 1 | cut -d "/" -f 3 | egrep "$test_pattern" | while read test_name ; do
    test_single "$test_name"
    if [ $? -ne 0 ] ; then
      create_statement=$(gh-ost-test-mysql-replica test -t -e "show create table _gh_ost_test_gho \G")
      echo "$create_statement" >> $test_logfile
      echo "+ FAIL"
      return 1
    else
      echo
      echo "+ pass"
    fi
    gh-ost-test-mysql-replica -e "start slave"
  done
}

verify_master_and_replica
test_all
