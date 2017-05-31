#!/bin/bash

source travis/00-_common.sh

tests_path=$PWD/localtests
test_logfile=/tmp/gh-ost-test.log
exec_command_file=/tmp/gh-ost-test.bash

echo_dot() {
  echo -n "."
}

test_single() {
  local test_name
  test_name="$1"

  echo -n "Testing: $test_name"

  echo_dot
  $SLAVE_MYSQL -e "stop slave; start slave; do sleep(1)"
  echo_dot
  $MASTER_MYSQL --default-character-set=utf8mb4 test < $tests_path/$test_name/create.sql

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
  cmd="$GHOST_BIN \
    --user=gh-ost \
    --password=gh-ost \
    --host=localhost \
    --port=20002 \
    --database=test \
    --table=gh_ost_test \
    --alter='engine=innodb' \
    --exact-rowcount \
    --switch-to-rbr \
    --initially-drop-old-table \
    --initially-drop-ghost-table \
    --throttle-query='select timestampdiff(second, min(last_update), now()) < 5 from _gh_ost_test_ghc' \
    --serve-socket-file=/tmp/gh-ost.test.sock \
    --initially-drop-socket-file \
    --postpone-cut-over-flag-file=/tmp/gh-ost.test.postpone.flag \
    --test-on-replica \
    --default-retries=1 \
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
    $MASTER_MYSQL --default-character-set=utf8mb4 test < $tests_path/$test_name/destroy.sql
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
  orig_checksum=$($SLAVE_MYSQL --default-character-set=utf8mb4 test -e "select ${orig_columns} from gh_ost_test ${order_by}" -ss | md5sum)
  ghost_checksum=$($SLAVE_MYSQL --default-character-set=utf8mb4 test -e "select ${ghost_columns} from _gh_ost_test_gho ${order_by}" -ss | md5sum)

  if [ "$orig_checksum" != "$ghost_checksum" ] ; then
    echo "ERROR $test_name: checksum mismatch"
    echo "---"
    $SLAVE_MYSQL --default-character-set=utf8mb4 test -e "select ${orig_columns} from gh_ost_test" -ss
    echo "---"
    $SLAVE_MYSQL --default-character-set=utf8mb4 test -e "select ${ghost_columns} from _gh_ost_test_gho" -ss
    return 1
  fi
}

test_all() {
  for test_name in $tests_path/*/ ; do
    test_name=$(basename $test_name)
    test_single "$test_name"
    if [ $? -ne 0 ] ; then
      create_statement=$($SLAVE_MYSQL test -t -e "show create table _gh_ost_test_gho \G")
      echo "$create_statement" >> $test_logfile
      echo "+ FAIL"
      return 1
    else
      echo
      echo "+ pass"
    fi
    $SLAVE_MYSQL -e "start slave"
    echo "LOG FOR $test_name"
    cat $test_logfile
    break
  done
}

test_all
