#!/bin/bash
#
#/ Usage: itest.sh [opts] [test_pattern]
#/
#/ Options:
#/    --gh-ost-binary | -b path to gh-ost binary. defaults to /tmp/gh-ost-test
#/    --test-dir           path to test directories. defaults to /localtests

usage() {
  code="$1"
  u="$(grep "^#/" "$0" | cut -c"4-")"
  if [ "$code" -ne 0 ]; then echo "$u" >&2; else echo "$u"; fi

  exit "$code"
}

GHOST_BINARY=/tmp/gh-ost-test
TEST_DIR=/localtests

while [ "$#" -gt 0 ];
do
  case "$1" in
    --gh-ost-binary|-b) GHOST_BINARY="$2"; shift 2;;
    --gh-ost-binary=*) GHOST_BINARY="$(echo "$1" | cut -d"=" -f"2-")"; shift;;
    --test-dir) TEST_DIR="$2"; shift 2;;
    --test-dir=*) TEST_DIR="$(echo "$1" | cut -d"=" -f"2-")"; shift;;
    --help|-h) usage 0;;
    -*) usage 1;;
    *) break ;;
  esac
done

TEST_PATTERN="${1:-.}"

if [ -f /mysql_version ];
then
  MYSQL_VERSION="$(cat /mysql_version)"
else
  MYSQL_VERSION="5.7.26"
fi

echo "Creating replication sandbox"
dbdeployer deploy replication $MYSQL_VERSION \
  --my-cnf-options log_slave_updates \
  --my-cnf-options log_bin \
  --my-cnf-options binlog_format=ROW \
  --sandbox-directory gh-ost-test

echo '#!/bin/bash' > /usr/local/bin/gh-ost-test-mysql-primary
echo '/root/sandboxes/gh-ost-test/m "$@"' >> /usr/local/bin/gh-ost-test-mysql-primary
chmod +x /usr/local/bin/gh-ost-test-mysql-primary

echo '#!/bin/bash' > /usr/local/bin/gh-ost-test-mysql-replica
echo '/root/sandboxes/gh-ost-test/s1 "$@"' > /usr/local/bin/gh-ost-test-mysql-replica
chmod +x /usr/local/bin/gh-ost-test-mysql-replica

echo "Creating gh-ost user"
gh-ost-test-mysql-primary -uroot -e"CREATE USER IF NOT EXISTS 'gh-ost'@'%' IDENTIFIED BY 'gh-ost'"
gh-ost-test-mysql-primary -uroot -e"GRANT ALL PRIVILEGES ON *.* TO 'gh-ost'@'%'"

echo "Reading database topology"
master_host="127.0.0.1"
master_port="$(/root/sandboxes/gh-ost-test/m -e"select @@port" -ss)"
replica_host="127.0.0.1"
replica_port="$(/root/sandboxes/gh-ost-test/s1 -e"select @@port" -ss)"

start_replication() {
  gh-ost-test-mysql-replica -e "stop slave; start slave;"
  num_attempts=0
  while gh-ost-test-mysql-replica -e "show slave status\G" | grep Seconds_Behind_Master | grep -q NULL ; do
    ((num_attempts=num_attempts+1))
    if [ "$num_attempts" -gt 10 ] ; then
      echo
      echo "ERROR replication failure"
      exit 1
    fi
    sleep 1
  done
}

test_single() {
  local test_name="$1"

  test_path="${TEST_DIR}/${test_name}"
  test_logfile="/tmp/gh-ost-test.${test_name}.log"
  exec_command_file="/tmp/gh-ost-test.${test_name}.bash"
  orig_content_output_file="/tmp/gh-ost-test.${test_name}.orig.content.csv"
  ghost_content_output_file="/tmp/gh-ost-test.${test_name}.ghost.content.csv"
  throttle_flag_file="/tmp/gh-ost-test.${test_name}.ghost.throttle.flag"

  if [ -f "${test_path}/ignore_versions" ] ; then
    ignore_versions=$(cat "${test_path}/ignore_versions")
    if echo "$MYSQL_VERSION" | egrep -q "^${ignore_versions}" ; then
      echo "Skipping: $test_name"
      return 0
    fi
  fi

  original_sql_mode="$(gh-ost-test-mysql-primary -e 'select @@global.sql_mode' -ss)"

  echo -n "Testing: $test_name..."
  start_replication

  if [ -f "${test_path}/sql_mode" ] ; then
    gh-ost-test-mysql-primary --default-character-set=utf8mb4 test -e "set @@global.sql_mode='$(cat "${test_path}/sql_mode")'"
    gh-ost-test-mysql-replica --default-character-set=utf8mb4 test -e "set @@global.sql_mode='$(cat "${test_path}/sql_mode")'"
  fi

  gh-ost-test-mysql-primary --default-character-set=utf8mb4 test <"${test_path}/create.sql"

  extra_args=""
  if [ -f "${test_path}/extra_args" ] ; then
    extra_args=$(cat "${test_path}/extra_args")
  fi

  orig_columns="*"
  ghost_columns="*"
  order_by=""
  if [ -f "${test_path}/orig_columns" ] ; then
    orig_columns="$(cat "${test_path}/orig_columns")"
  fi
  if [ -f "${test_path}/ghost_columns" ] ; then
    ghost_columns="$(cat "${test_path}/ghost_columns")"
  fi
  if [ -f "${test_path}/order_by" ] ; then
    order_by="order by $(cat "${test_path}/order_by")"
  fi

  cat <<EOF > "$exec_command_file"
  $GHOST_BINARY \
    --user=gh-ost \
    --password=gh-ost \
    --host=$replica_host \
    --port=$replica_port \
    --assume-master-host=${master_host}:${master_port} \
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
    --default-retries=3 \
    --chunk-size=10 \
    --verbose \
    --debug \
    --stack \
    --execute ${extra_args[@]}
EOF

  bash "$exec_command_file" 1> "$test_logfile" 2>&1

  execution_result=$?

  if [ -f "${test_path}/sql_mode" ] ; then
    gh-ost-test-mysql-primary --default-character-set=utf8mb4 test -e "set @@global.sql_mode='${original_sql_mode}'"
    gh-ost-test-mysql-replica --default-character-set=utf8mb4 test -e "set @@global.sql_mode='${original_sql_mode}'"
  fi

  if [ -f "${test_path}/destroy.sql" ] ; then
    gh-ost-test-mysql-primary --default-character-set=utf8mb4 test < "${test_path}/destroy.sql"
  fi

  if [ -f "${test_path}/expect_failure" ] ; then
    if [ $execution_result -eq 0 ] ; then
      echo " FAIL"
      echo "ERROR $test_name execution was expected to exit on error but did not. cat $test_logfile"
      return 1
    fi
    if [ -s "${test_path}/expect_failure" ] ; then
      # 'expect_failure' file has content. We expect to find this content in the log.
      expected_error_message="$(cat "${test_path}/expect_failure")"
      if grep -q "$expected_error_message" "$test_logfile" ; then
          echo " pass"
          return 0
      fi
      echo " FAIL"
      echo "ERROR $test_name execution was expected to exit with error message '${expected_error_message}' but did not. cat $test_logfile"
      return 1
    fi
    # 'expect_failure' file has no content. We generally agree that the failure is correct
    echo " pass"
    return 0
  fi

  if [ $execution_result -ne 0 ] ; then
    echo " FAIL"
    echo "ERROR $test_name execution failure. cat $test_logfile:"
    cat "$test_logfile"
    return 1
  fi

  gh-ost-test-mysql-replica --default-character-set=utf8mb4 test -e "select ${orig_columns} from gh_ost_test ${order_by}" -ss > "$orig_content_output_file"
  gh-ost-test-mysql-replica --default-character-set=utf8mb4 test -e "select ${ghost_columns} from _gh_ost_test_gho ${order_by}" -ss > "$ghost_content_output_file"
  orig_checksum=$(cat "$orig_content_output_file" | md5sum)
  ghost_checksum=$(cat "$ghost_content_output_file" | md5sum)

  if [ "$orig_checksum" != "$ghost_checksum" ] ; then
    echo " FAIL"
    echo "ERROR $test_name: checksum mismatch"
    echo "---"
    diff "$orig_content_output_file" "$ghost_content_output_file"

    echo "diff $orig_content_output_file $ghost_content_output_file"

    return 1
  fi

  echo " pass"
}

find "$TEST_DIR" -mindepth 1 ! -path . -type d | cut -d "/" -f 3 | grep -E "$TEST_PATTERN" | while read test_name ; do
  test_single "$test_name"
done
