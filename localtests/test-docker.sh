#!/bin/bash

. script/common
./build.sh

exec_command_file=/tmp/gh-ost-test.bash
orig_content_output_file=/tmp/gh-ost-test.orig.content.csv
ghost_content_output_file=/tmp/gh-ost-test.ghost.content.csv
throttle_flag_file=/tmp/gh-ost-test.ghost.throttle.flag
test_pattern="${1:-.}"

master_host=127.0.0.1
replica_host=127.0.0.1

mysql_version=
test_logfile=
run_master=
run_replica=
master_port=
replica_port=
original_sql_mode=

if [ $(uname -s) = "Darwin" ]; then
  ghost_binary=build/bin/gh-ost-darwin-amd64
else
  ghost_binary=build/bin/gh-ost-linux-amd64
fi

versions=(
  5.7.21
  5.6.33
)

containers=()
function at_exit() {
  echo "### Stopping docker containers.."
  for container in "${containers[@]}"; do
    docker kill ${container}
  done
}
trap at_exit EXIT

tests_path=$(dirname $0)

echo_dot() {
  echo -n "."
}

start_replication() {
  ${run_replica} -e "stop slave; start slave;"
  num_attempts=0
  while ${run_replica} -e "show slave status\G" | grep Seconds_Behind_Master | grep -q NULL ; do
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

  if [ -f $tests_path/$test_name/ignore_versions ] ; then
    ignore_versions=$(cat $tests_path/$test_name/ignore_versions)
    if echo "$mysql_version" | egrep -q "^${ignore_versions}" ; then
      echo -n "Skipping: $test_name"
      return 0
    fi
  fi

  echo -n "Testing: $test_name"

  echo_dot
  start_replication "$run_replica"
  echo_dot

  if [ -f $tests_path/$test_name/sql_mode ] ; then
    ${run_master} --default-character-set=utf8mb4 test -e "set @@global.sql_mode='$(cat $tests_path/$test_name/sql_mode)'"
    ${run_replica} --default-character-set=utf8mb4 test -e "set @@global.sql_mode='$(cat $tests_path/$test_name/sql_mode)'"
  fi

  ${run_master} --default-character-set=utf8mb4 test < $tests_path/$test_name/create.sql

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

  cmd="$ghost_binary \
    --user=gh-ost \
    --password=gh-ost \
    --host=$replica_host \
    --port=$replica_port \
    --assume-master-host=${master_host}:${master_port}
    --database=test \
    --table=gh_ost_test \
    --alter='engine=innodb' \
    --exact-rowcount \
    --assume-rbr \
    --initially-drop-old-table \
    --initially-drop-ghost-table \
    --throttle-query='select timestampdiff(second, min(last_update), now()) < 5 from _gh_ost_test_ghc' \
    --throttle-flag-file=$throttle_flag_file \
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

  if [ -f $tests_path/$test_name/sql_mode ] ; then
    ${run_master} --default-character-set=utf8mb4 test -e "set @@global.sql_mode='${original_sql_mode}'"
    ${run_replica} --default-character-set=utf8mb4 test -e "set @@global.sql_mode='${original_sql_mode}'"
  fi

  if [ -f $tests_path/$test_name/destroy.sql ] ; then
    ${run_master} --default-character-set=utf8mb4 test < $tests_path/$test_name/destroy.sql
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
    echo "ERROR $test_name execution failure. cat $test_logfile:"
    cat $test_logfile
    return 1
  fi

  echo_dot
  ${run_replica} --default-character-set=utf8mb4 test -e "select ${orig_columns} from gh_ost_test ${order_by}" -ss > $orig_content_output_file
  ${run_replica} --default-character-set=utf8mb4 test -e "select ${ghost_columns} from _gh_ost_test_gho ${order_by}" -ss > $ghost_content_output_file
  orig_checksum=$(cat $orig_content_output_file | md5sum)
  ghost_checksum=$(cat $ghost_content_output_file | md5sum)

  if [ "$orig_checksum" != "$ghost_checksum" ] ; then
    echo "ERROR $test_name: checksum mismatch"
    echo "---"
    diff $orig_content_output_file $ghost_content_output_file

    echo "diff $orig_content_output_file $ghost_content_output_file"

    return 1
  fi
}

function set_globals() {
  local container_name="$1"
  run_master="docker exec -i ${container_name} /sandboxes/rsandbox/m"
  run_replica="docker exec -i ${container_name} /sandboxes/rsandbox/s1"

  original_sql_mode="$(${run_master} -e "select @@global.sql_mode" -s -s)"
  echo "sql_mode on master is ${original_sql_mode}"

  if [ "$(${run_master} -e "select 1" -ss)" != "1" ] ; then
    echo "Cannot verify gh-ost master"
    exit 1
  fi

  if [ "$(${run_replica} -e "select 1" -ss)" != "1" ] ; then
    echo "Cannot verify gh-ost replica"
    exit 1
  fi

  if [ "$(${run_replica} -e "select @@global.binlog_format" -ss)" != "ROW" ] ; then
    echo "Expecting test replica to have binlog_format=ROW"
    exit 1
  fi

  local port
  read port <<< $(${run_master} -e "select @@port" -ss)
  if [ "$master_port" != "$port" ]; then
    echo "Master port expected to be ${master_port} and instead is ${port}"
    exit 1
  fi

  read port <<< $(${run_replica} -e "select @@port" -ss)
  if [ "$replica_port" != "$port" ]; then
    echo "Master port expected to be ${replica_port} and instead is ${port}"
    exit 1
  fi
}

function test_all() {
  local container_name=gh-ost-${mysql_version}

  master_port=$(echo ${mysql_version} | tr -d '.')
  replica_port=$(expr ${master_port} + 1)

  docker run --rm -d -p ${master_port}:${master_port} -p ${replica_port}:${replica_port} --name gh-ost-${mysql_version} gh-ost/dbdeployer:${mysql_version}
  sleep 3 # TODO wait for server to startup
  containers+=($container_name)
  set_globals "${container_name}"

  for test_name in `find $tests_path -mindepth 1 -maxdepth 1 -type d | cut -d "/" -f 3 | egrep "$test_pattern"`; do
    test_single "$test_name"
    if [ $? -ne 0 ] ; then
      create_statement=$(${run_replica} test -t -e "show create table _gh_ost_test_gho \G")
      echo "$create_statement" >> $test_logfile
      echo "+ FAIL"
      return 1
    else
      echo
      echo "+ pass"
    fi
    ${run_replica} -e "start slave"
  done
}

for version in "${versions[@]}"; do
  mysql_version=$version
  echo "### Building docker image for ${mysql_version}"
  docker build -t gh-ost/dbdeployer:${mysql_version} \
    -f docker/dbdeployer/Dockerfile \
    --build-arg MYSQL_VERSION=${mysql_version} \
    docker/dbdeployer

  echo "### Running gh-ost tests for ${mysql_version}"
  test_logfile=/tmp/gh-ost-test-${mysql_version}.log
  test_all
done
