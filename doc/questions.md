# How?

### How does the cut-over work? Is it really atomic?

The cut-over phase, where the original table is swapped away, and the _ghost_ table takes its place, is an atomic, blocking, controlled operation.

- Atomic: the tables are swapped together. There is no gap where your table does not exist.
- Blocking: all app queries involving the migrated (original) table are either operate on the original table, or are blocked, or proceed to operate on the _new_ table (formerly the _ghost_ table, now swapped in).
- Controlled: the cut-over times out at pre-defined threshold, and is atomically aborted, then re-attempted. Cut-over only takes place when no lags are present, and otherwise no throttling reason is found. Cut-over step itself gets high priority and is never throttled.

Read more on [cut-over](cut-over.md) and on the [cut-over design Issue](https://github.com/github/gh-ost/issues/82)


# Is it possible to?

### Is it possible to add a UNIQUE KEY?

Adding a `UNIQUE KEY` is possible, in the condition that no violation will occur. That is, you must make sure there aren't any violating rows on your table before, and during the migration.

At this time there is no equivalent to `ALTER IGNORE`, where duplicates are implicitly and silently thrown away. The MySQL `5.7` docs say:

> As of MySQL 5.7.4, the IGNORE clause for ALTER TABLE is removed and its use produces an error.

It is therefore unlikely that `gh-ost` will support this behavior.

### Run concurrent migrations?

Yes. TL;DR if running all on same replica/master, make sure to provide `--replica-server-id`. [Read more](cheatsheet.md#concurrent-migrations)

# Why

### Why Is the "Connect to Replica" mode preferred? 

To avoid placing extra load on the master. `gh-ost` connects as a replication client. Each additional replica adds some load to the master. 

To monitor replication lag from a replica. This makes the replication lag throttle, `--max-lag-millis`, more representative of the lag experienced by other replicas following the master (perhaps N levels deep in a tree of replicas).
