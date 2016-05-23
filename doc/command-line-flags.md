# Command line flags

A more in-depth discussion of various `gh-ost` command line flags: implementation, implication, use cases.

##### exact-rowcount

A `gh-ost` execution need to copy whatever rows you have in your existing table onto the ghost table. This can, and often be, a large number. Exactly what that number is?
`gh-ost` initially estimates the number of rows in your table by issuing an `explain select * from your_table`. This will use statistics on your table and return with a rough estimate. How rough? It might go as low as half or as high as double the actual number of rows in your table. This is the same method as used in [`pt-online-schema-change`](https://www.percona.com/doc/percona-toolkit/2.2/pt-online-schema-change.html).

`gh-ost` also supports the `--exact-rowcount` flag. When this flag is given, two things happen:
- An initial, authoritative `select count(*) from your_table`.
  This query may take a long time to complete, but is performed before we begin the massive operations.
- A continuous update to the estimate as we make progress applying events.
  We heuristically update the number of rows based on the queries we process from the binlogs.

While the ongoing estimated number of rows is still heuristic, it's almost exact, such that the reported  [ETA](understanding-output.md) or percentage progress is typically accurate to the second throughout a multiple-hour operation.
