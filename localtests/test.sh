#!/bin/bash

set -e

tests_path=$(dirname $0)
cd $tests_path

gh-ost-test-mysql-master -e "select 1"
gh-ost-test-mysql-replica -e "select 1"


find . ! -path . -type d | cut -d "/" -f 2 | while read test_name ; do
  echo "Testing: $test_name"
done
