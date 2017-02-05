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

At this time there is no equivalent to `ALETER IGNORE`, where duplicates are implicitly and silently thrown away. This behavior may be supported in the future.

# Why
