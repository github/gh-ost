#!/bin/bash
# Atomic multi-table cutover test.
#
# A workload commits transactions that each write the SAME txn_id into BOTH
# migrated tables, in a tight loop, right up to the cutover. Because gh-ost
# renames every migrated table in ONE atomic `RENAME TABLE t1 TO ..., t2 TO ...`,
# each cross-table transaction lands on the target entirely or not at all.
#
# Verification is deterministic (final-state set + checksum comparison, no timing
# assertions): the set of txn_ids in target gh_ost_test must exactly equal the set
# in target gh_ost_test_other. A regression from the atomic multi-table RENAME to
# a per-table sequential RENAME splits a boundary transaction across the two
# tables and leaves an orphan txn_id, failing this test.

database=test

build_binary
build_ghost_command

######################################################################################################
### Drive cross-table transactions, then cut over while they are still committing
######################################################################################################

echo "⚙️ Starting cross-table transaction workload..."

# Each iteration commits one transaction touching BOTH tables with a shared
# txn_id. The loop runs with no delay until the tables are renamed at cutover
# (the INSERT then errors and the loop exits), so cross-table transactions are
# committing continuously while the cutover happens. Note: we do NOT (and cannot,
# from a shell) control whether a transaction is literally mid-commit at the
# RENAME instant -- that is timing-dependent. Correctness is asserted
# deterministically on the final state below (no orphaned txn_id across the pair
# + per-table checksums), not on hitting that instant.
(
    n=1
    while true; do
        mysql-exec source primary $database -e \
            "START TRANSACTION; \
             INSERT INTO gh_ost_test (txn_id, payload) VALUES ($n, 'a'); \
             INSERT INTO gh_ost_test_other (txn_id, payload) VALUES ($n, 'b'); \
             COMMIT;" 2>/dev/null || break
        n=$((n + 1))
    done
) &
workload_pid=$!

# Remove the postpone flag so cutover proceeds while the workload is still
# committing cross-table transactions. This is a best-effort time-based overlap,
# not a guarantee that a transaction is mid-commit at the exact RENAME; the
# atomicity guarantee is verified on the final target state below.
(
    sleep 4
    echo "⏩ Sending unpostpone cutover"
    rm $postpone_cutover_flag_file &> /dev/null
) &

echo > $test_logfile
bash -c "$cmd" >> $test_logfile 2>&1
ghost_result=$?

kill $workload_pid &> /dev/null

if [ $ghost_result -ne 0 ]; then
    echo "ERROR: gh-ost should have succeeded but did not. ($ghost_result)"
    return 1
fi

echo -e "\n\n\n\n\n"

######################################################################################################
### Validate atomicity + data integrity (read primaries to avoid replication lag)
######################################################################################################

echo "⚙️ Validating atomic multi-table cutover..."

# Sanity: the workload must have landed cross-table rows on the target, otherwise
# the atomicity assertion below would be vacuously true.
paired=$(mysql-exec target primary $database -sNe "SELECT COUNT(*) FROM gh_ost_test WHERE txn_id > 0;")
if [ -z "$paired" ] || [ "$paired" -lt 1 ]; then
    echo "ERROR: workload produced no cross-table rows on target; test would be vacuous."
    return 1
fi

# Atomicity invariant: every cross-table transaction landed entirely or not at
# all, i.e. the txn_id sets match across the two target tables (no orphans).
orphans_a=$(mysql-exec target primary $database -sNe \
    "SELECT COUNT(*) FROM gh_ost_test t1 WHERE t1.txn_id > 0 \
       AND NOT EXISTS (SELECT 1 FROM gh_ost_test_other t2 WHERE t2.txn_id = t1.txn_id);")
orphans_b=$(mysql-exec target primary $database -sNe \
    "SELECT COUNT(*) FROM gh_ost_test_other t2 WHERE t2.txn_id > 0 \
       AND NOT EXISTS (SELECT 1 FROM gh_ost_test t1 WHERE t1.txn_id = t2.txn_id);")

if [ "$orphans_a" != "0" ] || [ "$orphans_b" != "0" ]; then
    echo "ERROR: non-atomic cutover: ${orphans_a} txn_id(s) in gh_ost_test missing from gh_ost_test_other; ${orphans_b} the other way."
    return 1
fi

# Full data integrity: each migrated table matches its source rollback handle.
for table_name in gh_ost_test gh_ost_test_other; do
    src_checksum=$(mysql-exec source primary $database -ss -e "SELECT * FROM _${table_name}_del ORDER BY id" | md5sum)
    dst_checksum=$(mysql-exec target primary $database -ss -e "SELECT * FROM ${table_name} ORDER BY id" | md5sum)
    if [ "$src_checksum" != "$dst_checksum" ]; then
        echo "ERROR: checksum mismatch on ${table_name} between source _del and target."
        return 1
    fi
done

echo "✅ Atomic multi-table cutover validated: ${paired} cross-table transactions, no orphans, checksums match."
