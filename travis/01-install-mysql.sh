#!/bin/bash

source travis/00-_common.sh

set -xe

# Shutdown the existing percona server if possible.
if [ "$(which mysqladmin)" != "" ]; then
  mysqladmin -u root shutdown || true
fi

# Initialize mysql data

rm -rf $MASTER_MYSQL_DIR
rm -rf $SLAVE_MYSQL_DIR

mkdir -p $MASTER_MYSQL_DIR/data
mkdir -p $SLAVE_MYSQL_DIR/data

$MASTER_MYSQLD --initialize-insecure
$SLAVE_MYSQLD --initialize-insecure

# Start mysqld
$MASTER_MYSQLD &
$SLAVE_MYSQLD &

# Wait for mysqld to start
while ! $MASTER_MYSQLADMIN ping; do
  sleep 1
done

while ! $SLAVE_MYSQLADMIN ping; do
  sleep 1
done

# Sanity check to see we're initialized correctly
$MASTER_MYSQL -e "SELECT @@port"
$MASTER_MYSQL -e "SELECT VERSION()"
$SLAVE_MYSQL -e "SELECT @@port"
$SLAVE_MYSQL -e "SELECT VERSION()"

# Setup replication on mysql
$MASTER_MYSQL -e "RESET MASTER"
$MASTER_MYSQL -e "CREATE USER 'repl'@'%' IDENTIFIED BY 'repl'"
$MASTER_MYSQL -e "GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%'"

$SLAVE_MYSQL -e "RESET MASTER"
$SLAVE_MYSQL -e "CHANGE MASTER TO MASTER_HOST='127.0.0.1', MASTER_PORT=20001, MASTER_USER='repl', MASTER_PASSWORD='repl', MASTER_AUTO_POSITION=1"
$SLAVE_MYSQL -e "START SLAVE"

# Setup credential for gh-ost

$MASTER_MYSQL -e "CREATE USER 'gh-ost'@'%' IDENTIFIED BY 'gh-ost'"
$MASTER_MYSQL -e "GRANT ALL ON *.* TO 'gh-ost'@'%'"

$SLAVE_MYSQL -e "CREATE USER IF NOT EXISTS 'gh-ost'@'%' IDENTIFIED BY 'gh-ost'"
$SLAVE_MYSQL -e "GRANT ALL ON *.* TO 'gh-ost'@'%'"
# ,ALTER,CREATE,DELETE,DROP,INDEX,LOCK TABLES,SELECT,TRIGGER,UPDATE,REPLICATION CLIENT,REPLICATION SLAVE

# Ensure replication chain is correctly setup

$MASTER_MYSQL -e "CREATE DATABASE ghostdata"
$MASTER_MYSQL -e "CREATE TABLE ghostdata.data123 (id bigint(20) AUTO_INCREMENT, str VARCHAR(255), PRIMARY KEY(id))"
$MASTER_MYSQL -e "INSERT INTO ghostdata.data123 VALUES (1, 'hi')"

while ! $SLAVE_MYSQL -sNe 'SELECT str FROM ghostdata.data123'; do
  echo "waiting for slave to catchup..."
  sleep 1
done

$MASTER_MYSQL -e "DROP DATABASE ghostdata"
$MASTER_MYSQL -e "CREATE DATABASE IF NOT EXISTS test"
