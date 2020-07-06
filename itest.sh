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

test_path=localtests/trivial
throttle_flag_file=trivial.trottle-flag
extra_args=""
if [ -f $test_path/extra_args ];
then
  extra_args="$(cat $test_path/extra_args)"
fi

echo "Setting up test case"
/root/sandboxes/gh-ost-test/m -uroot test <$test_path/create.sql

echo "Running gh-ost"
/usr/local/bin/gh-ost \
    --user=gh-ost \
    --password=gh-ost \
    --host="$replica_host" \
    --port="$replica_port" \
    --assume-master-host="${master_host}:${master_port}" \
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
    --execute "${extra_args[@]}" 1>trivial.log 2>&1

result=$?
if [ $result -ne 0 ];
then
  echo
  echo "ERROR execution failure"
  cat trivial.log
  exit 1
fi

echo "Success!"
