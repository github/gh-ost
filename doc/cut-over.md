# Cut-over step

The cut-over is the final major step of the migration: it's the moment where your original table is pushed aside, and the ghost table (the one we secretly altered and operated on throughout the process) takes its place.

MySQL poses some limitations on how the table swap can take place. While it supports an atomic swap, it does not allow a connection to swap tables it holds under lock.

The [facebook OSC](https://www.facebook.com/notes/mysql-at-facebook/online-schema-change-for-mysql/430801045932/) tool documents this nicely. Look for **"Cut-over phase"**. The Facebook solution uses a non-atomic swap: the original table is first renamed and pushed aside, then the ghost table is renamed to take its place. In between the two renames there's a brief period of time where your table just does not exist, and queries will fail.

Another option to support a atomic swap without the use of a lock is the use of triggers only in the cut-over phase,
gh-ost philosophy is avoid the use of triggers but in some environments like a Galera cluster isn't possible use the
lock command, and like the cut-over should take a little time it shouldn't be a problem. Triggers cut-over works like the following:

- A stop writes event is injected in the binlog and gh-ost disable the writes once it receive it.
- The triggers are created to handle the modifications in the MySQL side.
- A created triggers event is injected in the binlog and gh-ost wait until receive it.
- The affected rows will be in an inconsistent stata during the time between the first and the second event. For this reason, this events are checked and, the values of the fields that are part of the unique key used to do the online alter are saved to sanitize that rows.

`gh-ost` solves this by using an atomic, two-step blocking swap: while one connection holds the lock, another attempts the atomic `RENAME`. The `RENAME` is guaranteed to not be executed prematurely by positioning a sentry table which blocks the `RENAME` operation until `gh-ost` is satisfied all is in order.

This solution either:
- executes successfully, in which case the tables are swapped atomically and pending connections are blocked for a brief period of time, proceeding to operate on the newly migrated table
- or fails, due to timeout or death of some connection, in which case we are naturally returning to pre-cut-over phase, where the original table is still in place and accessible. This releases the pending connections, which are able again to write to the table, and `gh-ost` is then able to make another attempt at the cut-over.

Also note:
- With `--migrate-on-replica` the cut-over is executed in exactly the same way as on master.
- With `--test-on-replica` the replication is first stopped; then the cut-over is executed just as on master, but then reverted (tables rename forth then back again).

Internals of the atomic cut-over are discussed in [Issue #82](https://github.com/github/gh-ost/issues/82).

At this time the command-line argument `--cut-over` is supported, and defaults to the atomic cut-over algorithm described above. Also supported is `--cut-over=two-step`, which uses the FB non-atomic algorithm and the `--cut-over=trigger`, which use the trigger algorithm. We recommend using the default cut-over that has been battle tested in our production environments.
