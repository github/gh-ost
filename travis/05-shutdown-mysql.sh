#!/bin/bash

source travis/00-_common.sh

set -x
set +e

# Unset the trap if this is executed from a trap
trap '' ERR

$SLAVE_MYSQLADMIN shutdown
$MASTER_MYSQLADMIN shutdown

cat $MASTER_MYSQL_DIR/data/mysql.err
cat $SLAVE_MYSQL_DIR/data/mysql.err
