#!/bin/bash
# shellcheck disable=SC2168

sleep 1 # let any any replication catch up

local master_server_uuid replica_server_uuid gtid_executed gtid_purged

master_server_uuid=$(gh-ost-test-mysql-master test -e "SELECT @@global.server_uuid" -ss)
replica_server_uuid=$(gh-ost-test-mysql-replica test -e "SELECT @@global.server_uuid" -ss)
gtid_executed=$(gh-ost-test-mysql-replica test -e "SELECT @@global.gtid_executed" -ss)
gtid_purged=$(gh-ost-test-mysql-replica test -e "SELECT @@global.gtid_purged" -ss)

if ! echo "$gtid_executed" | grep -q "$replica_server_uuid" ; then
  echo
  echo "ERROR gtid_executed is missing original gh-ost gtids"
  # shellcheck disable=SC2154
  echo "prior gtid_executed:   $gtid_executed_pristine"
  echo "current gtid_executed: $gtid_executed"
  return 1
fi

if [ "$gtid_executed" != "$gtid_purged" ] ; then
  echo
  echo "ERROR gtid_executed and gtid_purged don't match"
  echo "gtid_executed: $gtid_executed"
  echo "gtid_purged:   $gtid_purged"
  return 1
fi

local max_pristine_master_tx max_current_master_tx
max_pristine_master_tx=$(echo "$gtid_executed_pristine" | grep -o -P "${master_server_uuid}:\d+-\d+" | cut -d ":" -f 2 | cut -d "-" -f 2)
max_current_master_tx=$(echo "$gtid_executed" | grep -o -P "${master_server_uuid}:\d+-\d+" | cut -d ":" -f 2 | cut -d "-" -f 2)

if (( max_current_master_tx <= max_pristine_master_tx )) ; then
  echo
  echo "ERROR gtid_executed trimmed legitimate master gtids."
  echo "current gtid_executed should > prior gtid_executed"
  echo "prior gtid_executed:   $gtid_executed_pristine"
  echo "current gtid_executed: $gtid_executed"
  return 1
fi
