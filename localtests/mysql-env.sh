#!/bin/bash

DIR=$(readlink -f $(dirname $0))
FILE=$DIR/mysql.env

(
  echo 'MYSQL_ALLOW_EMPTY_PASSWORD=true'

  echo "TEST_STORAGE_ENGINE=${TEST_STORAGE_ENGINE}"
  if [ "$TEST_STORAGE_ENGINE" == "rocksdb" ]; then
    echo 'INIT_ROCKSDB=true'
  fi
) | tee $FILE

echo "Wrote env file to $FILE"
