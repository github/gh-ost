#!/bin/bash
# shellcheck disable=SC2168

local gtid_executed_pristine
# shellcheck disable=SC2034
gtid_executed_pristine=$(gh-ost-test-mysql-replica test -e "SELECT @@global.gtid_executed" -ss)
