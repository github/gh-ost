# Why triggerless?

Existing MySQL schema migration tools:

- [pt-online-schema-change](https://www.percona.com/doc/percona-toolkit/2.2/pt-online-schema-change.html)
- [Facebook OSC](https://www.facebook.com/notes/mysql-at-facebook/online-schema-change-for-mysql/430801045932/)
- [LHM](https://github.com/soundcloud/lhm)
- [oak-online-alter-table](https://github.com/shlomi-noach/openarkkit)

are all using [triggers](http://dev.mysql.com/doc/refman/5.6/en/triggers.html) to propagate live changes on your table onto a ghost/shadow table that is slowly being synchronized. The tools not all work the same: while most use a synchronous approach (all changes applied on the ghost table), the Facebook tool uses an asynchronous approach (changes are appended to a changelog table, later reviewed and applied on ghost table).

Use of triggers simplifies a lot of the flow in doing a live table migration, but also poses some limitations or difficulties. Here are reasons why we choose to [design a triggerless solution](triggerless-design.md) to schema migrations.


## Overview

Triggers are stored routines which are invoked on a per-row operation upon `INSERT`, `DELETE`, `UPDATE` on a table.
They were introduced in MySQL `5.0`.
A trigger may contain a set of queries, and these queries run in the same transaction space as the query that manipulates the table. This makes for an atomicity of both the original operation on the table and the trigger-invoked operations.

### Triggers, overhead

A trigger in MySQL is a stored routine. MySQL stored routines are interpreted, never compiled. With triggers, for every `INSERT`, `DELETE`, `UPDATE` on our often busy table, we pay the necessary price of the additional write (onto ghost or changelog table), but also the price of interpreting the trigger body.

We know this to be a visible overhead on very busy or very large tables.

### Triggers, locks

When a table with triggers is concurrently being written to, the triggers, being in same transaction space as the incoming queries, are also executed concurrently. While concurrent queries compete for resources via locks (e.g. the `auto_increment` value), the triggers need to _simultaneously_ compete for their own locks (e.g., likewise on the `auto_increment` value on the ghost table, in a synchronous solution). These competitions are non-coordinated.

We have evidenced near or complete lock downs in production, to the effect of rendering the table or the entire database inaccessible due to lock contention.

### Trigger based migration, no pause

The triggers are used to either apply or record ongoing changes to your original table.

Existing online migration tools (notably `pt-online-schema-change`) support the notion of _throttling_: suspending execution when the master becomes too busy, or replication is unable to catch up.

However this throttling is partial. The tool may only suspend the tedious and long task of copying the millions of rows from the original table onto the ghost table, but the tool may not, at any stage, cancel the triggers. Cancelling the triggers means loss of information, leading to incorrect data.

Thus, triggers must keep operating. On busy servers, we have seen that even as the online operation throttles, the master is brought down by the load of the triggers.

Read more about [`gh-ost` throttling](throttle.md)

### Triggers, multiple migrations

We are interested in being able to run multiple concurrent migrations (not on the same table, of course). Given all the above, we do not have trust that running multiple trigger-based migrations is a safe operation. In our current, past and shared experiences we have never done so; we are unaware of anyone who is doing so.

### Trigger based migration, no reliable production test

We sometimes wish to experiment with a migration, or know in advance how much time it would take. A trigger-based solution allows us to run a migration on a replica, provided it uses Statement Based Replication.

With Row Based Replication there is no option at all to use triggers on a replica, since the triggers only execute on the master (replica only gets notified of the _effect_ of the trigger).

But even with Statement Based Replication we do not get a reliable representation of the migration as it would have executed on the master. MySQL replication (up to and including `5.6`) is single threaded, given a table, and even in `5.7` we do not expect to find concurrency on a single table. The triggers on the replica will invoke sequentially ; they will not simulate the same load, and they will not suffer from the same concurrency and locking problems depicted above, as on the master.

### Trigger based migration, bound to server

The trigger space is within a MySQL service. It cannot go beyond that space. We are working towards a multi-server solution. More discussion as we make progress.

## Triggerless design

Proceed to read about the [triggerless design](triggerless-design.md)
