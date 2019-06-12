#!/bin/bash
# shellcheck disable=SC2168

sleep 1 # let any any replication catch up
local replica_server_uuid gtid_executed

replica_server_uuid=$(gh-ost-test-mysql-replica test -e "SELECT @@global.server_uuid" -ss)
gtid_executed=$(gh-ost-test-mysql-replica test -e "SELECT @@global.gtid_executed" -ss)

# shellcheck disable=SC2154
if [ "$gtid_executed" = "$gtid_executed_pristine" ] ; then
  echo
  echo "ERROR gtid_executed shouldn't have been rolled back"
  echo "prior gtid_executed: $gtid_executed_pristine"
  echo "final gtid_executed: $gtid_executed"
  return 1
fi

if ! echo "$gtid_executed" | grep -q "$replica_server_uuid" ; then
  echo
  echo "ERROR gtid_executed doesn't contains gh-ost gtids"
  echo "replica server_uuid $replica_server_uuid"
  echo "prior gtid_executed:   $gtid_executed_pristine"
  echo "current gtid_executed: $gtid_executed"
  return 1
fi
