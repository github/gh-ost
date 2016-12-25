# Resurrection

`gh-ost` supports resurrection of a failed migration, continuing the migration from last known good position, potentially saving hours of clock-time.

A migration may fail as follows:

- On meeting with `--critical-load`
- On successively meeting with a specific error (e.g. recurring locks)
- Being `kill -9`'d by a user
- MySQL crash
- Server crash
- Robots taking over the world and other reasons.

### --resurrect

One may resurrect such a migration by running the exact same command, adding the `--resurrect` flag.

The terms for resurrection are:

- Exact same database/table/alter
- Previous migration ran for at least one minute
- Previous migration began looking at row-copy and event handling (by `1` minute of execution you may expect this to be the case)

### How does it work?

`gh-ost` dumps its migration status (context) once per minute, onto the _changelog table_. The changelog table is used for internal bookkeeping, and manages heartbeat and internal message passing.

When `--resurrect` is provided,`gh-ost` attempts to find such status dump in the changelog table. Most interestingly this status included:

- Last handled binlog event coordinates (any event up to that point has been applied to _ghost_ table)
- Last copied chunk range
- Other useful information

Resurrection reconnects the streamer at last handled binlog coordinates, and skips rowcopy to proceed from last copied chunk range.

Noteworthy is that it is not important to resume from _exact same_ coordinates and chunk as last applied; the context dump only runs once per minute, and resurrection may re-apply a minute's worth of binary logs, and re-iterate a minute's work of copied chunks.

Row-based replication has the property of being idempotent for DML events. There is no damage in reapplying contiguous binlog events starting at some point in the past.

Chunk-reiteration likewise poses no integrity concern and there is no harm in re-copying same range of rows.

The only concern is to never skip binlog events, and never skip a row range. By virtue of only dumping events and ranges that have been applied, and by virtue of only processing binlog events and chunks moving forward, `gh-ost` keeps integrity intact.
