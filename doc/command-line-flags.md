# Command line flags

A more in-depth discussion of various `gh-ost` command line flags: implementation, implication, use cases.

##### conf

`--conf=/path/to/my.cnf`: file where credentials are specified. Should be in (or contain) the following format:

  ```
[client]
user=gromit
password=123456
  ```

##### cut-over

Required. You are asked to explicitly state which cut-over algorithm you wish to use. Please see more discussion on [cut-over](cut-over.md)

##### exact-rowcount

A `gh-ost` execution need to copy whatever rows you have in your existing table onto the ghost table. This can, and often be, a large number. Exactly what that number is?
`gh-ost` initially estimates the number of rows in your table by issuing an `explain select * from your_table`. This will use statistics on your table and return with a rough estimate. How rough? It might go as low as half or as high as double the actual number of rows in your table. This is the same method as used in [`pt-online-schema-change`](https://www.percona.com/doc/percona-toolkit/2.2/pt-online-schema-change.html).

`gh-ost` also supports the `--exact-rowcount` flag. When this flag is given, two things happen:
- An initial, authoritative `select count(*) from your_table`.
  This query may take a long time to complete, but is performed before we begin the massive operations.
- A continuous update to the estimate as we make progress applying events.
  We heuristically update the number of rows based on the queries we process from the binlogs.

While the ongoing estimated number of rows is still heuristic, it's almost exact, such that the reported  [ETA](understanding-output.md) or percentage progress is typically accurate to the second throughout a multiple-hour operation.

##### execute

Without this parameter, migration is a _noop_: testing table creation and validity of migration, but not touching data.

##### initially-drop-ghost-table

`gh-ost` maintains two tables while migrating: the _ghost_ table (which is synced from your original table and finally replaces it) and a changelog table, which is used internally for bookkeeping. By default, it panics and aborts if it sees those tables upon startup. Provide `--initially-drop-ghost-table` and `--initially-drop-old-table` to let `gh-ost` know it's OK to drop them beforehand.

We think `gh-ost` should not take chances or make assumptions about the user's tables. Dropping tables can be a dangerous, locking operation. We let the user explicitly approve such operations.

##### initially-drop-old-table

See #initially-drop-ghost-table

##### test-on-replica

Issue the migration on a replica; do not modify data on master. Useful for validating, testing and benchmarking. See [test-on-replica](test-on-replica.md)
