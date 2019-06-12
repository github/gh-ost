#!/bin/bash
# shellcheck disable=SC2168

local gtid_executed_pristine

# Write something to the replica-only. Make sure it stays even after GTID surgery
gh-ost-test-mysql-replica test -e "CREATE TABLE replica_only (id INTEGER); INSERT INTO replica_only VALUES (1), (2), (3);"

# shellcheck disable=SC2034
gtid_executed_pristine=$(gh-ost-test-mysql-replica test -e "SELECT @@global.gtid_executed" -ss)
