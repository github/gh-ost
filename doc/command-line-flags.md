# Command line flags

A more in-depth discussion of various `gh-ost` command line flags: implementation, implication, use cases.

### allow-on-master

By default, `gh-ost` would like you to connect to a replica, from where it figures out the master by itself. This wiring is required should your master execute using `binlog_format=STATEMENT`.

If, for some reason, you do not wish `gh-ost` to connect to a replica, you may connect it directly to the master and approve this via `--allow-on-master`.

### approve-renamed-columns

When your migration issues a column rename (`change column old_name new_name ...`) `gh-ost` analyzes the statement to try an associate the old column name with new column name. Otherwise the new structure may also look like some column was dropped and another was added.

`gh-ost` will print out what it thinks the _rename_ implied, but will not issue the migration unless you provide with `--approve-renamed-columns`.

If you think `gh-ost` is mistaken and that there's actually no _rename_ involved, you may pass `--skip-renamed-columns` instead. This will cause `gh-ost` to disassociate the column values; data will not be copied between those columns.

### assume-master-host

`gh-ost` infers the identity of the master server by crawling up the replication topology. You may explicitly tell `gh-ost` the identity of the master host via `--assume-master-host=the.master.com`. This is useful in:

- master-master topologies (together with `--allow-master-master`), where `gh-ost` can arbitrarily pick one of the co-master and you prefer that it picks a specific one
- _tungsten replicator_ topologies (together with `--tungsten`), where `gh-ost` is unable to crawl and detect the master

### assume-rbr

If you happen to _know_ your servers use RBR (Row Based Replication, i.e. `binlog_format=ROW`), you may specify `--assume-rbr`. This skips a verification step where `gh-ost` would issue a `STOP SLAVE; START SLAVE`.
Skipping this step means `gh-ost` would not need the `SUPER` privilege in order to operate.
You may want to use this on Amazon RDS

### conf

`--conf=/path/to/my.cnf`: file where credentials are specified. Should be in (or contain) the following format:

  ```
[client]
user=gromit
password=123456
  ```

### concurrent-rowcount

See `exact-rowcount`

### critical-load-interval-millis

`--critical-load` defines a threshold that, when met, `gh-ost` panics and bails out. The default behavior is to bail out immediately when meeting this threshold.

This may sometimes lead to migrations bailing out on a very short spike, that, while in itself is impacting production and is worth investigating, isn't reason enough to kill a 10 hour migration.

When `--critical-load-interval-millis` is specified (e.g. `--critical-load-interval-millis=2500`), `gh-ost` gives a second chance: when it meets `critical-load` threshold, it doesn't bail out. Instead, it starts a timer (in this example: `2.5` seconds) and re-checks `critical-load` when the timer expires. If `critical-load` is met again, `gh-ost` panics and bails out. If not, execution continues.

This is somewhat similar to a Nagios `n`-times test, where `n` in our case is always `2`.

### cut-over

Optional. Default is `safe`. See more discussion in [cut-over](cut-over.md)

### discard-foreign-keys

**Danger**: this flag will _silently_ discard any foreign keys existing on your table.

At this time (10-2016) `gh-ost` does not support foreign keys on migrated tables (it bails out when it notices a FK on the migrated table). However, it is able to support _dropping_ of foreign keys via this flag. If you're trying to get rid of foreign keys in your environment, this is a useful flag.

### exact-rowcount

A `gh-ost` execution need to copy whatever rows you have in your existing table onto the ghost table. This can, and often be, a large number. Exactly what that number is?
`gh-ost` initially estimates the number of rows in your table by issuing an `explain select * from your_table`. This will use statistics on your table and return with a rough estimate. How rough? It might go as low as half or as high as double the actual number of rows in your table. This is the same method as used in [`pt-online-schema-change`](https://www.percona.com/doc/percona-toolkit/2.2/pt-online-schema-change.html).

`gh-ost` also supports the `--exact-rowcount` flag. When this flag is given, two things happen:
- An initial, authoritative `select count(*) from your_table`.
  This query may take a long time to complete, but is performed before we begin the massive operations.
  When `--concurrent-rowcount` is also specified, this runs in parallel to row copy.
  Note: `--concurrent-rowcount` now defaults to `true`.
- A continuous update to the estimate as we make progress applying events.
  We heuristically update the number of rows based on the queries we process from the binlogs.

While the ongoing estimated number of rows is still heuristic, it's almost exact, such that the reported  [ETA](understanding-output.md) or percentage progress is typically accurate to the second throughout a multiple-hour operation.

### execute

Without this parameter, migration is a _noop_: testing table creation and validity of migration, but not touching data.

### initially-drop-ghost-table

`gh-ost` maintains two tables while migrating: the _ghost_ table (which is synced from your original table and finally replaces it) and a changelog table, which is used internally for bookkeeping. By default, it panics and aborts if it sees those tables upon startup. Provide `--initially-drop-ghost-table` and `--initially-drop-old-table` to let `gh-ost` know it's OK to drop them beforehand.

We think `gh-ost` should not take chances or make assumptions about the user's tables. Dropping tables can be a dangerous, locking operation. We let the user explicitly approve such operations.

### initially-drop-old-table

See #initially-drop-ghost-table

### max-lag-millis

On a replication topology, this is perhaps the most important migration throttling factor: the maximum lag allowed for migration to work. If lag exceeds this value, migration throttles.

When using [Connect to replica, migrate on master](cheatsheet.md), this lag is primarily tested on the very replica `gh-ost` operates on. Lag is measured by checking the heartbeat events injected by `gh-ost` itself on the utility changelog table. That is, to measure this replica's lag, `gh-ost` doesn't need to issue `show slave status` nor have any external heartbeat mechanism.

When `--throttle-control-replicas` is provided, throttling also considers lag on specified hosts. Measuring lag on these hosts works as follows:

- If `--replication-lag-query` is provided, use the query, trust its result to indicate lag seconds (fraction, i.e. float, allowed)
- Otherwise, issue `show slave status` and read `Seconds_behind_master` (`1sec` granularity)

See also: [Sub-second replication lag throttling](subsecond-lag.md)

### migrate-on-replica

Typically `gh-ost` is used to migrate tables on a master. If you wish to only perform the migration in full on a replica, connect `gh-ost` to said replica and pass `--migrate-on-replica`. `gh-ost` will briefly connect to the master but other issue no changes on the master. Migration will be fully executed on the replica, while making sure to maintain a small replication lag.

### skip-renamed-columns

See `approve-renamed-columns`

### test-on-replica

Issue the migration on a replica; do not modify data on master. Useful for validating, testing and benchmarking. See [testing-on-replica](testing-on-replica.md)
