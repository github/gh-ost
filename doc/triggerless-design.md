# Triggerless design

A breakdown of the logic and algorithm behind `gh-ost`'s triggerless design, followed by the implications, advantages and disadvantages of such design.

### Trigger-based migrations background

It is worthwhile to consider two popular existing online schema change solutions:

- [pt-online-schema-change](https://www.percona.com/doc/percona-toolkit/2.2/pt-online-schema-change.html)
- [Facebook OSC](https://www.facebook.com/notes/mysql-at-facebook/online-schema-change-for-mysql/430801045932/)

The former uses a synchronous design: it adds three triggers (`AFTER INSERT`, `AFTER UPDATE`, `AFTER DELETE`) on the original table. Each such trigger relays the operation onto the ghost table. So for every `UPDATE` on the original table, an `UPDATE` executes on the ghost table. A `DELETE` on the original table triggers a `DELETE` on the ghost table. Same for `INSERT`. The triggers live in the same transaction space as the original query.

The latter uses an asynchronous design: it adds three triggers (`AFTER INSERT`, `AFTER UPDATE`, `AFTER DELETE`) on the original table. It also creates a _changelog_ table. The triggers do not relay operations directly to the ghost table. Instead, they each add an entry to the changelog table. An `UPDATE` on the original table makes for an `INSERT` on the changelog table saying "There was an UPDATE on the original table with this and that values"; likewise for `INSERT` and `DELETE`.
A background process tails the changelog table and applies the changes onto the ghost table. This approach is asynchronous in that the applier does not live in the same transaction space as the original table, and may operate on a change event seconds or more after said event was written.
It is noteworthy that the writes to the changelog table still live in the same transaction space as the writes on the original table.

### Triggerless based asynchronous migrations

`gh-ost`'s triggerless design uses an asynchronous approach. However it does not require triggers because it does not require having a _changelog_ table like the FB tool does. The reason it does not require a changelog table is that it finds the changelog in another place: the binary logs.

In particular, it reads Row Based Replication (RBR) entries (you can still [use it with Statement Based Replication!](migrating-with-sbr.md)) and searches for entries that apply to the original table.

RBR entries are very convenient for this job: they break complex statements, potentially multi-table, into distinct, per-table, per-row entries, which are easy to read and apply.

`gh-ost` pretends to be a MySQL replica: it connects to the MySQL server and begins requesting for binlog events as though it were a real replication server. Thus, it gets a continuous streaming of the binary logs, and filters out those events that apply to the original table.

`gh-ost` can connect directly to the master, but prefers to connect to one of its replicas. Such a replica would need to use `log-slave-updates` and use `binlog-format=ROW` (`gh-ost` can change the latter setting for you).

Reading from the binary log, specially in the case of reading those on a replica, further stresses the asynchronous nature of the algorithm. While the transaction _may_ (based on configuration) be synced with the binlog entry write, it will take time until `gh-ost` - pretending to be a replica - will get notification for that, copy the event downstream and apply it.

The asynchronous design implies many noteworthy outcomes, to be discussed later on.

### Workflow overview

The workflow includes reading table data from the server, reading event data from the binary log, checking for replication lag or other throttling parameters, applying changes onto the server (typically the master), sending hints through the binary log stream and more.

Some flow breakdown:

#### Initial setup & validation
Initial setup is a no-concurrency operation

- Connecting to replica/master, detecting master identify
- Pre-validating `alter` statement
- Initial sanity: privileges, existence of tables
- Creation of changelog and ghost tables.
- Applying `alter` on ghost table
- Comparing structure of original & ghost table. Looking for shared columns, shared unique keys, validating foreign keys. Choosing shared unique key, the key by which we chunk the table and process it.
- Setting up the binlog listener; begin listening on changelog events
- Injecting a "good to go" entry onto the changelog table (to be intercepted via binary logs)
- Begin listening on binlog events for original table DMLs
- Reading original table's chosen key min/max values

#### Copy flow
This setup includes multiple moving parts, all acting concurrently with some coordination

- Setting up a heartbeat mechanism: frequent writes on the changelog table (we consider this to be low, negligible write load for throttling purposes)
- Continuously updating status
- Periodically (frequently) checking for potential throttle scenarios or hints
- Work through the original table's rows range, chunk by chunk, queueing copy tasks onto the ghost table
- Reading DML events from the binlogs, queueing apply tasks onto the ghost table
- Processing the copy tasks queue and the apply tasks queue and sequentially applying onto ghost table
  - Suspending by throttle state
- Injecting/intercepting "copy all done" once full row-copy range has been exhausted
- Stall/postpone while `postpone-cut-over-flag-file` exists (we keep apply ongoing DMLs)

#### Cut-over and completion

- Locking the original table for writes, working on what remains on the binlog event backlog (recall this is an asynchronous operation, and so even as the table is locked, we still have unhandled events in our pipe).
- Swapping the original table out, the ghost table in
- Cleanup: potential drop of tables

### Asynchronous design implications

#### Cut-over phase

A complication the asynchronous approach presents is the cut-over phase: the swapping of the tables. In the synchronous approach, the two tables are kept in sync thanks to the transaction-space in which the triggers operate. Thus, a simple, atomic `rename table original to _original_old, ghost to original` suffices and is valid.

In the asynchronous approach, as we lock the original table, we often still have events in the pipeline, changes in the binary log we still need to apply onto the ghost table. An atomic swap would be a premature and incorrect solution, since it would imply the write load would immediately proceed to operate on what used to be the ghost table, even before we completed applying those last changes.

The Facebook solution uses an "outage", two-step rename:

- Lock the original table, work on backlog
- Rename original table to `_old`
- Rename ghost table to original

In between those two renames there's a point in time where the table does not exist, hence there's a "table outage".

`gh-ost` solves this by using a two-step algorithm that blocks writes to the table, then issues an atomic swap. It uses safety latches such that the operation either succeeds, atomically, or fails, bringing us back to pre-cut-over stage.

Read more on the [cut-over](cut-over.md) documentation.

#### Decoupling

The most impacting change the triggerless, asynchronous approach provides is the decoupling of workload. With triggers, either synchronous or asynchronous, every write on your table implied an immediate write on another table.

We will break down the meaning of workload decoupling, shortly. But it is important to understand that `gh-ost` interprets the situation in its own time and acts in its own time, yet still makes this an online operation.

The decoupling is important not only as the tool's logic goes, but very importantly as the master server sees it. As far as the master knows, write to the table and writes to the ghost table are unrelated.

#### Writer load

Not using triggers means the master no longer needs to overload multiple, concurrent writes with stored routine interpretation combined with lock contention on the ghost table.

The responsibility for applying data to the ghost table is completely `gh-ost`'s. As such, `gh-ost` decides which data gets to be written to the ghost table and when. We are decoupled from the original table's write load, and choose to write to the ghost table in a single thread.

MySQL does not perform well on multiple concurrent massive writes to a specific table. Locking becomes an issue. This is why we choose to alternate between the massive row-copy and the ongoing binlog events backlog such that the server only sees writes from a single connection.

It is also interesting to observe that `gh-ost` is the only application writing to the ghost table. No one else is even aware of its existence. Thus, the trigger originated problem of high concurrency, high contention writes simply does not exist in `gh-ost`.

#### Pausability

When `gh-ost` pauses (throttles), it issues no writes on the ghost table. Because there are no triggers, write workload is decoupled from the `gh-ost` write workload. And because we're using an asynchronous approach, the algorithm already handles a time difference between a master write time and the ghost apply time. A difference of a few microseconds is no different from a difference of minutes or hours.

When `gh-ost` [throttles](throttle.md), either by replication lag, `max-load` setting or and explicit [interactive user command](interactive-commands.md), the master is back to normal. It sees no more writes on the ghost table.
An exception is the ongoing heartbeat writes onto the changelog table, which we consider to be negligible.

#### Testability

We are able to test the migration process: as we've decoupled the migration operation from the master's workload, we are good to apply the changes not to the master, but to one of its replicas. We are able to migrate a table on a replica.

This in itself is a nice feature; but it also presents us with testability: just as we complete the migration, we stop replication on the replica. We cut-over but rollback again. We do not drop any table. The result is both the original and ghost table exist on the replica, which is not taking any further changes. We have time to examine the two tables and compare them to our satisfaction.

This is the method used by GitHub to continuously validate the tool's integrity: multiple production replicas are continuously and repeatedly doing a "trivial migration" (no actually change of column) on all our production tables. Each migration is followed by a checksum of the entire table data, on both original and ghost tables. We expect the checksums to be identical and we log the results. We expect zero failures.

#### Multiple, concurrent migrations

`gh-ost` was designed with having multiple concurrent migration running in parallel (no two on the same table, of course). The asynchronous approach supports that design by not caring when data is being shipped to the ghost table. The fact no triggers exist means multiple migrations appear to the master (or other migrated host) just as multiple connections, each writing to some otherwise unknown table. Each can throttle in its own time, or we can throttle all together.

#### Going outside the server space

More to come as we make progress.

### No free meals

#### Increased traffic

The existing tools utilize triggers to propagate data changes. `gh-ost` takes upon itself to read the data, then write it back. `gh-ost` actually prefers to read from a replica and write to the master. This implies data transfers between hosts, and certainly in/out the MySQL server daemon. At this time the MySQL client library used by `gh-ost` does not support compression, and so during a migration you can expect the full volume of a table to transfer on the wire.

#### Code complexity

With the synchronous, trigger based approach, the role of the migration tool is relatively small. A lot of the migration is based on the triggers doing their job within the transaction space. Issues such as rollback, datatypes, cut-over are implicitly taken care of by the database. With `gh-ost`'s asynchronous approach, the tool turns complex. It connects to the master and onto a replica; it imposes as a replicating server; it writes heartbeat events; it reads binlog data into the app to be written again onto the migrated host; it need to manage connection failures, replication lag, and more.

The tool has therefore a larger codebase and a more complicated asynchronous, concurrent logic. But we jumped the opportunity to add some [perks](perks.md) and completely redesign how an online migration tool should work.
