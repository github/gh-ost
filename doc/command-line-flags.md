# Command line flags

A more in-depth discussion of various `gh-ost` command line flags: implementation, implication, use cases.

### aliyun-rds

Add this flag when executing on Aliyun RDS.

### allow-master-master

See [`--assume-master-host`](#assume-master-host).

### allow-on-master

By default, `gh-ost` would like you to connect to a replica, from where it figures out the master by itself. This wiring is required should your master execute using `binlog_format=STATEMENT`.

If, for some reason, you do not wish `gh-ost` to connect to a replica, you may connect it directly to the master and approve this via `--allow-on-master`.

### approve-renamed-columns

When your migration issues a column rename (`change column old_name new_name ...`) `gh-ost` analyzes the statement to try and associate the old column name with new column name. Otherwise the new structure may also look like some column was dropped and another was added.

`gh-ost` will print out what it thinks the _rename_ implied, but will not issue the migration unless you provide with `--approve-renamed-columns`.

If you think `gh-ost` is mistaken and that there's actually no _rename_ involved, you may pass [`--skip-renamed-columns`](#skip-renamed-columns) instead. This will cause `gh-ost` to disassociate the column values; data will not be copied between those columns.

### assume-master-host

`gh-ost` infers the identity of the master server by crawling up the replication topology. You may explicitly tell `gh-ost` the identity of the master host via `--assume-master-host=the.master.com`. This is useful in:

- _master-master_ topologies (together with [`--allow-master-master`](#allow-master-master)), where `gh-ost` can arbitrarily pick one of the co-masters and you prefer that it picks a specific one
- _tungsten replicator_ topologies (together with [`--tungsten`](#tungsten)), where `gh-ost` is unable to crawl and detect the master

### assume-rbr

If you happen to _know_ your servers use RBR (Row Based Replication, i.e. `binlog_format=ROW`), you may specify `--assume-rbr`. This skips a verification step where `gh-ost` would issue a `STOP SLAVE; START SLAVE`.
Skipping this step means `gh-ost` would not need the `SUPER` privilege in order to operate.
You may want to use this on Amazon RDS.

### conf

`--conf=/path/to/my.cnf`: file where credentials are specified. Should be in (or contain) the following format:

  ```
[client]
user=gromit
password=123456
  ```

### concurrent-rowcount

Defaults to `true`. See [`exact-rowcount`](#exact-rowcount)

### critical-load

Comma delimited status-name=threshold, same format as [`--max-load`](#max-load).

`--critical-load` defines a threshold that, when met, `gh-ost` panics and bails out. The default behavior is to bail out immediately when meeting this threshold.

This may sometimes lead to migrations bailing out on a very short spike, that, while in itself is impacting production and is worth investigating, isn't reason enough to kill a 10 hour migration.

### critical-load-interval-millis

When `--critical-load-interval-millis` is specified (e.g. `--critical-load-interval-millis=2500`), `gh-ost` gives a second chance: when it meets `critical-load` threshold, it doesn't bail out. Instead, it starts a timer (in this example: `2.5` seconds) and re-checks `critical-load` when the timer expires. If `critical-load` is met again, `gh-ost` panics and bails out. If not, execution continues.

This is somewhat similar to a Nagios `n`-times test, where `n` in our case is always `2`.

### cut-over

Optional. Default is `safe`. See more discussion in [`cut-over`](cut-over.md)

### cut-over-lock-timeout-seconds

Default `3`.  Max number of seconds to hold locks on tables while attempting to cut-over (retry attempted when lock exceeds timeout).

### discard-foreign-keys

**Danger**: this flag will _silently_ discard any foreign keys existing on your table.

At this time (10-2016) `gh-ost` does not support foreign keys on migrated tables (it bails out when it notices a FK on the migrated table). However, it is able to support _dropping_ of foreign keys via this flag. If you're trying to get rid of foreign keys in your environment, this is a useful flag.

See also: [`skip-foreign-key-checks`](#skip-foreign-key-checks)


### dml-batch-size

`gh-ost` reads event from the binary log and applies them onto the _ghost_ table. It does so in batched writes: grouping multiple events to apply in a single transaction. This gives better write throughput as we don't need to sync the transaction log to disk for each event.

The `--dml-batch-size` flag controls the size of the batched write. Allowed values are `1 - 100`, where `1` means no batching (every event from the binary log is applied onto the _ghost_ table on its own transaction). Default value is `10`.

Why is this behavior configurable? Different workloads have different characteristics. Some workloads have very large writes, such that aggregating even `50` writes into a transaction makes for a significant transaction size. On other workloads write rate is high such that one just can't allow for a hundred more syncs to disk per second. The default value of `10` is a modest compromise that should probably work very well for most workloads. Your mileage may vary.

Noteworthy is that setting `--dml-batch-size` to higher value _does not_ mean `gh-ost` blocks or waits on writes. The batch size is an upper limit on transaction size, not a minimal one. If `gh-ost` doesn't have "enough" events in the pipe, it does not wait on the binary log, it just writes what it already has. This conveniently suggests that if write load is light enough for `gh-ost` to only see a few events in the binary log at a given time, then it is also light enough for `gh-ost` to apply a fraction of the batch size.

### exact-rowcount

A `gh-ost` execution need to copy whatever rows you have in your existing table onto the ghost table. This can, and often be, a large number. Exactly what that number is?
`gh-ost` initially estimates the number of rows in your table by issuing an `explain select * from your_table`. This will use statistics on your table and return with a rough estimate. How rough? It might go as low as half or as high as double the actual number of rows in your table. This is the same method as used in [`pt-online-schema-change`](https://www.percona.com/doc/percona-toolkit/2.2/pt-online-schema-change.html).

`gh-ost` also supports the `--exact-rowcount` flag. When this flag is given, two things happen:
- An initial, authoritative `select count(*) from your_table`.
  This query may take a long time to complete, but is performed before we begin the massive operations.
  When [`--concurrent-rowcount`](#concurrent-rowcount) is also specified, this runs in parallel to row copy.
  Note: [`--concurrent-rowcount`](#concurrent-rowcount) now defaults to `true`.
- A continuous update to the estimate as we make progress applying events.
  We heuristically update the number of rows based on the queries we process from the binlogs.

While the ongoing estimated number of rows is still heuristic, it's almost exact, such that the reported  [ETA](understanding-output.md) or percentage progress is typically accurate to the second throughout a multiple-hour operation.

### execute

Without this parameter, migration is a _noop_: testing table creation and validity of migration, but not touching data.

### force-named-cut-over

If given, a `cut-over` command must name the migrated table, or else ignored.

### force-named-panic

If given, a `panic` command must name the migrated table, or else ignored.

### force-table-names

Table name prefix to be used on the temporary tables.

### gcp

Add this flag when executing on a 1st generation Google Cloud Platform (GCP).

### heartbeat-interval-millis

Default 100. See [`subsecond-lag`](subsecond-lag.md) for details.

### initially-drop-ghost-table

`gh-ost` maintains two tables while migrating: the _ghost_ table (which is synced from your original table and finally replaces it) and a changelog table, which is used internally for bookkeeping. By default, it panics and aborts if it sees those tables upon startup. Provide `--initially-drop-ghost-table` and `--initially-drop-old-table` to let `gh-ost` know it's OK to drop them beforehand.

We think `gh-ost` should not take chances or make assumptions about the user's tables. Dropping tables can be a dangerous, locking operation. We let the user explicitly approve such operations.

### initially-drop-old-table

See [`initially-drop-ghost-table`](#initially-drop-ghost-table)

### initially-drop-socket-file

Default False. Should `gh-ost` forcibly delete an existing socket file. Be careful: this might drop the socket file of a running migration!

### max-lag-millis

On a replication topology, this is perhaps the most important migration throttling factor: the maximum lag allowed for migration to work. If lag exceeds this value, migration throttles.

When using [Connect to replica, migrate on master](cheatsheet.md#a-connect-to-replica-migrate-on-master), this lag is primarily tested on the very replica `gh-ost` operates on. Lag is measured by checking the heartbeat events injected by `gh-ost` itself on the utility changelog table. That is, to measure this replica's lag, `gh-ost` doesn't need to issue `show slave status` nor have any external heartbeat mechanism.

When [`--throttle-control-replicas`](#throttle-control-replicas) is provided, throttling also considers lag on specified hosts. Lag measurements on listed hosts is done by querying `gh-ost`'s _changelog_ table, where `gh-ost` injects a heartbeat.

See also: [Sub-second replication lag throttling](subsecond-lag.md)

### max-load

List of metrics and threshold values; topping the threshold of any will cause throttler to kick in. See also: [`throttling`](throttle.md#status-thresholds)

### migrate-on-replica

Typically `gh-ost` is used to migrate tables on a master. If you wish to only perform the migration in full on a replica, connect `gh-ost` to said replica and pass `--migrate-on-replica`. `gh-ost` will briefly connect to the master but otherwise will make no changes on the master. Migration will be fully executed on the replica, while making sure to maintain a small replication lag.

### postpone-cut-over-flag-file

Indicate a file name, such that the final [cut-over](cut-over.md) step does not take place as long as the file exists.
When this flag is set, `gh-ost` expects the file to exist on startup, or else tries to create it. `gh-ost` exits with error if the file does not exist and `gh-ost` is unable to create it.
With this flag set, the migration will cut-over upon deletion of the file or upon `cut-over` [interactive command](interactive-commands.md).

### replica-server-id

Defaults to 99999. If you run multiple migrations then you must provide a different, unique `--replica-server-id` for each `gh-ost` process.
Optionally involve the process ID, for example: `--replica-server-id=$((1000000000+$$))`.

It's on you to choose a number that does not collide with another `gh-ost` or another running replica.
See also: [`concurrent-migrations`](cheatsheet.md#concurrent-migrations) on the cheatsheet.

### skip-foreign-key-checks

By default `gh-ost` verifies no foreign keys exist on the migrated table. On servers with large number of tables this check can take a long time. If you're absolutely certain no foreign keys exist (table does not reference other table nor is referenced by other tables) and wish to save the check time, provide with `--skip-foreign-key-checks`.

### skip-strict-mode

By default `gh-ost` enforces STRICT_ALL_TABLES sql_mode as a safety measure. In some cases this changes the behaviour of other modes (namely ERROR_FOR_DIVISION_BY_ZERO, NO_ZERO_DATE, and NO_ZERO_IN_DATE) which may lead to errors during migration. Use `--skip-strict-mode` to explicitly tell `gh-ost` not to enforce this. **Danger** This may have some unexpected disastrous side effects.

### skip-renamed-columns

See [`approve-renamed-columns`](#approve-renamed-columns)

### ssl

By default `gh-ost` does not use ssl/tls connections to the database servers when performing migrations. This flag instructs `gh-ost` to use encrypted connections. If enabled, `gh-ost` will use the system's ca certificate pool for server certificate verification. If a different certificate is needed for server verification, see `--ssl-ca`. If you wish to skip server verification, but still use encrypted connections, use with `--ssl-allow-insecure`.

### ssl-allow-insecure

Allows `gh-ost` to connect to the MySQL servers using encrypted connections, but without verifying the validity of the certificate provided by the server during the connection. Requires `--ssl`.

### ssl-ca

`--ssl-ca=/path/to/ca-cert.pem`: ca certificate file (in PEM format) to use for server certificate verification. If specified, the default system ca cert pool will not be used for verification, only the ca cert provided here. Requires `--ssl`.

### ssl-cert

`--ssl-cert=/path/to/ssl-cert.crt`: SSL public key certificate file (in PEM format).

### ssl-key

`--ssl-key=/path/to/ssl-key.key`: SSL private key file (in PEM format).

### test-on-replica

Issue the migration on a replica; do not modify data on master. Useful for validating, testing and benchmarking. See [`testing-on-replica`](testing-on-replica.md)

### test-on-replica-skip-replica-stop

Default `False`. When `--test-on-replica` is enabled, do not issue commands stop replication (requires `--test-on-replica`).

### throttle-control-replicas

Provide a command delimited list of replicas; `gh-ost` will throttle when any of the given replicas lag beyond [`--max-lag-millis`](#max-lag-millis). The list can be queried and updated dynamically via [interactive commands](interactive-commands.md)

### throttle-http

Provide a HTTP endpoint; `gh-ost` will issue `HEAD` requests on given URL and throttle whenever response status code is not `200`. The URL can be queried and updated dynamically via [interactive commands](interactive-commands.md). Empty URL disables the HTTP check.

### timestamp-old-table

Makes the _old_ table include a timestamp value. The _old_ table is what the original table is renamed to at the end of a successful migration. For example, if the table is `gh_ost_test`, then the _old_ table would normally be `_gh_ost_test_del`. With `--timestamp-old-table` it would be, for example, `_gh_ost_test_20170221103147_del`.

### tungsten

See [`tungsten`](cheatsheet.md#tungsten) on the cheatsheet.
