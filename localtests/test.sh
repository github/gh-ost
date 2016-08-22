#!/bin/bash

# set -e

tests_path=$(dirname $0)
test_logfile=/tmp/gh-ost-test.log

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
  read replica_host replica_port <<< $(gh-ost-test-mysql-replica -e "select @@hostname, @@port" -ss)
}

exec_cmd() {
  echo "$@"
  command "$@" 1> $test_logfile 2>&1
  return $?
}

test_single() {
  local test_name
  test_name="$1"

  echo "Testing: $test_name"

  gh-ost-test-mysql-replica -e "start slave"
  gh-ost-test-mysql-master test < $tests_path/$test_name/create.sql

  extra_args=""
  if [ -f $tests_path/$test_name/extra_args ] ; then
    # mapfile -t <$tests_path/$test_name/extra_args
    # echo "${MAPFILE[@]}"
    extra_args=($(cat $tests_path/$test_name/extra_args))
    echo ${extra_args[@]}
  fi
  columns="*"
  if [ -f $tests_path/$test_name/test_columns ] ; then
    columns=$(cat $tests_path/$test_name/test_columns)
  fi

  go run go/cmd/gh-ost/main.go \
    --user=gh-ost \
    --password=gh-ost \
    --host=$replica_host \
    --port=$replica_port \
    --database=test \
    --table=gh_ost_test \
    --alter="engine=innodb" \
    --exact-rowcount \
    --switch-to-rbr \
    --initially-drop-old-table \
    --initially-drop-ghost-table \
    --throttle-query="select timestampdiff(second, min(last_update), now()) < 5 from _gh_ost_test_ghc" \
    --serve-socket-file=/tmp/gh-ost.test.sock \
    --initially-drop-socket-file \
    --postpone-cut-over-flag-file=/tmp/gh-ost.postpone.flag \
    --test-on-replica \
    --verbose \
    --debug \
    --stack \
    --execute "${extra_args[@]}"

    if [ $? -ne 0 ] ; then
      echo "ERROR $test_name execution failure. See $test_logfile"
      return 1
    fi

    orig_checksum=$(gh-ost-test-mysql-replica test -e "select ${columns} from gh_ost_test" -ss | md5sum)
    ghost_checksum=$(gh-ost-test-mysql-replica test -e "select ${columns} from _gh_ost_test_gho" -ss | md5sum)

    gh-ost-test-mysql-replica -e "start slave"

    if [ "$orig_checksum" != "$ghost_checksum" ] ; then
      echo "ERROR $test_name: checksum mismatch"
      return 1
    fi
}

test_all() {
  find $tests_path ! -path . -type d -mindepth 1 -maxdepth 1 | cut -d "/" -f 3 | while read test_name ; do
    test_single "$test_name"
    if [ $? -ne 0 ] ; then
      echo "+ FAIL"
      return 1
    else
      echo "+ pass"
    fi
  done
}

verify_master_and_replica
test_all
