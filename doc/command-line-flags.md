# Command line flags

A more in-depth discussion of various `gh-ost` command line flags: implementation, implication, use cases.

### aliyun-rds

Add this flag when executing on Aliyun RDS.

### allow-zero-in-date

Allows the user to make schema changes that include a zero date or zero in date (e.g. adding a `datetime default '0000-00-00 00:00:00'` column), even if global `sql_mode` on MySQL has `NO_ZERO_IN_DATE,NO_ZERO_DATE`.

### alter

Mandatory unless using [`--revert`](#revert). The schema change to apply to the migrated table. You can pass just the alter options, for example `--alter="ADD COLUMN created_at timestamp NULL"`, together with [`--database`](#database) and [`--table`](#table). You can also pass a full `ALTER TABLE [database.]table ...` statement; an explicit database or table in `--alter` can be used instead of the corresponding flag.

### azure

Add this flag when executing on Azure Database for MySQL.

### allow-master-master

See [`--assume-master-host`](#assume-master-host).

### allow-nullable-unique-key

Allows `gh-ost` to choose a unique key that contains nullable columns. This is dangerous if the chosen key contains `NULL` values: row identity may be ambiguous and data may be corrupted. Use only when you know the nullable unique key contains no `NULL` values.

### allow-on-master

By default, `gh-ost` would like you to connect to a replica, from where it figures out the master by itself. This wiring is required should your master execute using `binlog_format=STATEMENT`.

If, for some reason, you do not wish `gh-ost` to connect to a replica, you may connect it directly to the master and approve this via `--allow-on-master`.

### allow-setup-metadata-lock-instruments

`--allow-setup-metadata-lock-instruments` allows gh-ost to enable the [`metadata_locks`](https://dev.mysql.com/doc/refman/8.0/en/performance-schema-metadata-locks-table.html) table in `performance_schema`, if it is not already enabled. This is used for a safety check before cut-over.
See also: [`skip-metadata-lock-check`](#skip-metadata-lock-check)

### approve-renamed-columns

When your migration issues a column rename (`change column old_name new_name ...`) `gh-ost` analyzes the statement to try and associate the old column name with new column name. Otherwise, the new structure may also look like some column was dropped and another was added.

`gh-ost` will print out what it thinks the _rename_ implied, but will not issue the migration unless you provide with `--approve-renamed-columns`.

If you think `gh-ost` is mistaken and that there's actually no _rename_ involved, you may pass [`--skip-renamed-columns`](#skip-renamed-columns) instead. This will cause `gh-ost` to disassociate the column values; data will not be copied between those columns.

### ask-pass

Prompt for the MySQL password instead of passing it on the command line via [`--password`](#password).

### assume-master-host

`gh-ost` infers the identity of the master server by crawling up the replication topology. You may explicitly tell `gh-ost` the identity of the master host via `--assume-master-host=the.master.com`. This is useful in:

- _master-master_ topologies (together with [`--allow-master-master`](#allow-master-master)), where `gh-ost` can arbitrarily pick one of the co-masters, and you prefer that it picks a specific one
- _tungsten replicator_ topologies (together with [`--tungsten`](#tungsten)), where `gh-ost` is unable to crawl and detect the master

### assume-rbr

If you happen to _know_ your servers use RBR (Row Based Replication, i.e. `binlog_format=ROW`), you may specify `--assume-rbr`. This skips a verification step where `gh-ost` would issue a `STOP SLAVE; START SLAVE`.
Skipping this step means `gh-ost` would not need the `SUPER` privilege in order to operate.
You may want to use this on Amazon RDS.

### attempt-instant-ddl

MySQL 8.0 supports "instant DDL" for some operations. If an alter statement can be completed with instant DDL, only a metadata change is required internally. Instant operations include:

- Adding a column
- Dropping a column
- Dropping an index
- Extending a varchar column
- Adding a virtual generated column

It is not reliable to parse the `ALTER` statement to determine if it is instant or not. This is because the table might be in an older row format, or have some other incompatibility that is difficult to identify.

`--attempt-instant-ddl` is disabled by default, but the risks of enabling it are relatively minor: `gh-ost` may need to acquire a metadata lock at the start of the operation. This is not a problem for most scenarios, but it could be a problem for users that start the DDL during a period with long running transactions.

`gh-ost` will automatically fallback to the normal DDL process if the attempt to use instant DDL is unsuccessful.

### binlogsyncer-max-reconnect-attempts
`--binlogsyncer-max-reconnect-attempts=0`, the maximum number of attempts to re-establish a broken inspector connection for sync binlog. `0` or `negative number` means infinite retry, default `0`

### check-flag

Checks whether one or more flags exist in the current `gh-ost` binary. This is useful for cross-version scripting. Exits with `0` when all flags specified alongside `--check-flag` exist, non-zero otherwise. Flags that require a value must be given a dummy value.

Example: `gh-ost --check-flag --cut-over-lock-timeout-seconds 1 --nice-ratio 0`

### checkpoint

`--checkpoint` enables periodic checkpoints of the gh-ost's state so that gh-ost can resume a migration from the checkpoint with `--resume`. Checkpoints are written to a separate table named `_${original_table_name}_ghk`. It is recommended to use with `--gtid` for checkpoints.
See also: [`resuming-migrations`](resume.md)

### checkpoint-seconds

`--checkpoint-seconds` specifies the seconds between checkpoints. Default is 300.

### chunk-size

Controls how many rows `gh-ost` copies in each row-copy iteration. The default is `1000`; allowed range is `10` to `100000`.

### parallel-copy

`--parallel-copy` copies the table rows using multiple workers running concurrently. Binlog event application remains single-threaded, so event ordering is preserved. This is experimental and requires `--checkpoint`. Default is disabled.

Parallel copy requires checkpoints because parallel INSERTs can go out of sequential order and leave gaps in target table. gh-ost checkpoints are consistent and do not have gaps, so resuming from a checkpoint will make the target table consistent again.

Consistency is ensured by using 
-INSERT IGNORE/SELECT FOR SHARE for row copy 
-DELETE/INSERT by DML applier when unique key change is detected
Without all of those, consistency of parallel copy cannot be guaranteed (for example, row copy could insert row back after it was deleted).
In existing serial mode, the problem is much easier because DML applier is never parallel with row copy.

### parallel-copy-workers

`--parallel-copy-workers` sets the number of parallel row-copy workers used when `--parallel-copy` is enabled. Has no effect otherwise. Default is 4; maximum is 64. The applier connection pool is sized to `parallel-copy-workers + 4`. Throughput gains tend to plateau past ~16 workers — so prefer the smallest value that meets your needs.

### parallel-copy-max-heartbeatlag-millis

`--parallel-copy-max-heartbeatlag-millis` sets a heartbeat-lag threshold (in milliseconds) used to throttle row-copy workers when `--parallel-copy` is enabled. Requires `--parallel-copy`. Default is `10000` (10 seconds).

gh-ost's heartbeat lag is used as a lightweight proxy for binlog applier lag. When heartbeat lag is below this threshold, all workers copy rows at full concurrency. When it exceeds the threshold, workers pause and yield, giving the binlog applier time to catch up. This preserves the same priority model as serial row-copy, where binlog event application is never starved by row-copy work. If the applier is consistently lagging, workers will spend most of their time paused and throughput approaches the serial baseline; when the table's DML rate is low and the applier is not lagging, all workers run freely and deliver the full benefit of parallelism.

HeartbeatLag is also the primary signal used by the cut-over gate: gh-ost will not attempt the table swap until HeartbeatLag drops below both `--max-lag-millis` and `--cut-over-lock-timeout-seconds`. Using it here as the parallel-copy throttle signal is therefore consistent with the most safety-critical decision point in the migration.

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

This may sometimes lead to migrations bailing out on a very short spike, that, while in itself is impacting production and is worth investigating, isn't reason enough to kill a 10-hour migration.

### critical-load-hibernate-seconds

When `--critical-load-hibernate-seconds` is non-zero (e.g. `--critical-load-hibernate-seconds=300`), `critical-load` does not panic and bail out; instead, `gh-ost` goes into hibernation for the specified duration. It will not read/write anything from/to any server during this time.  Execution then continues upon waking from hibernation.

If `critical-load` is met again, `gh-ost` will repeat this cycle, and never panic and bail out.

### critical-load-interval-millis

When `--critical-load-interval-millis` is specified (e.g. `--critical-load-interval-millis=2500`), `gh-ost` gives a second chance: when it meets `critical-load` threshold, it doesn't bail out. Instead, it starts a timer (in this example: `2.5` seconds) and re-checks `critical-load` when the timer expires. If `critical-load` is met again, `gh-ost` panics and bails out. If not, execution continues.

This is somewhat similar to a Nagios `n`-times test, where `n` in our case is always `2`.

### cut-over

Optional. Default is `atomic`. Accepted values are `atomic`, `default`, and `two-step`; `default` is a legacy alias for `atomic`. `atomic` performs an atomic table swap where pending connections briefly block and either all see the old table or all see the new table. `two-step` uses a non-atomic rename sequence where the original table is renamed away before the ghost table takes its place, leaving a brief interval where the table does not exist. See more discussion in [`cut-over`](cut-over.md)

### cut-over-exponential-backoff

Wait exponentially longer intervals between failed cut-over attempts. The maximum wait interval is controlled by [`--exponential-backoff-max-interval`](#exponential-backoff-max-interval).

### cut-over-lock-timeout-seconds

Default `3`.  Max number of seconds to hold locks on tables while attempting to cut-over (retry attempted when lock exceeds timeout).

### database

The database containing the table to migrate. Required unless the database is specified in a full [`--alter`](#alter) statement.

### debug

Enables debug logging. This is very verbose. See also [`--verbose`](#verbose), [`--quiet`](#quiet), and [`--stack`](#stack).

### default-retries

Default number of retries for various operations before panicking. Default is `60`.

### discard-foreign-keys

**Danger**: this flag will _silently_ discard any foreign keys existing on your table.

At this time (10-2016) `gh-ost` does not support foreign keys on migrated tables (it bails out when it notices a FK on the migrated table). However, it is able to support _dropping_ of foreign keys via this flag. If you're trying to get rid of foreign keys in your environment, this is a useful flag.

See also: [`skip-foreign-key-checks`](#skip-foreign-key-checks)


### dml-batch-size

`gh-ost` reads event from the binary log and applies them onto the _ghost_ table. It does so in batched writes: grouping multiple events to apply in a single transaction. This gives better write throughput as we don't need to sync the transaction log to disk for each event.

The `--dml-batch-size` flag controls the size of the batched write. Allowed values are `1 - 1000`, where `1` means no batching (every event from the binary log is applied onto the _ghost_ table on its own transaction). Default value is `10`.

Why is this behavior configurable? Different workloads have different characteristics. Some workloads have very large writes, such that aggregating even `50` writes into a transaction makes for a significant transaction size. On other workloads write rate is high such that one just can't allow for a hundred more syncs to disk per second. The default value of `10` is a modest compromise that should probably work very well for most workloads. Your mileage may vary.

Noteworthy is that setting `--dml-batch-size` to higher value _does not_ mean `gh-ost` blocks or waits on writes. The batch size is an upper limit on transaction size, not a minimal one. If `gh-ost` doesn't have "enough" events in the pipe, it does not wait on the binary log, it just writes what it already has. This conveniently suggests that if write load is light enough for `gh-ost` to only see a few events in the binary log at a given time, then it is also light enough for `gh-ost` to apply a fraction of the batch size.

### exact-rowcount

A `gh-ost` execution need to copy whatever rows you have in your existing table onto the ghost table. This can and often will be, a large number. Exactly what that number is?
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

### exponential-backoff-max-interval

Maximum number of seconds to wait between attempts when exponential backoff is used. Default is `64` seconds. See also [`--cut-over-exponential-backoff`](#cut-over-exponential-backoff).

### force-named-cut-over

If given, a `cut-over` command must name the migrated table, or else ignored.

### force-named-panic

If given, a `panic` command must name the migrated table, or else ignored.

### force-table-names

Table name prefix to be used on the temporary tables.

### gcp

Add this flag when executing on a 1st generation Google Cloud Platform (GCP).

### gtid

Add this flag to enable support for [MySQL replication GTIDs](https://dev.mysql.com/doc/refman/5.7/en/replication-gtids-concepts.html) for replication positioning. This requires `gtid_mode` and `enforce_gtid_consistency` to be set to `ON`.

### heartbeat-interval-millis

Default 100. See [`subsecond-lag`](subsecond-lag.md) for details.

### help

Prints command-line usage and exits.

### host

MySQL hostname to connect to. Preferably this is a replica, not the master. Default is `127.0.0.1`.

### hooks-hint

Arbitrary message injected into hooks via the `GH_OST_HOOKS_HINT` environment variable.

### hooks-hint-owner

Arbitrary owner name injected into hooks via the `GH_OST_HOOKS_HINT_OWNER` environment variable.

### hooks-hint-token

Arbitrary token injected into hooks via the `GH_OST_HOOKS_HINT_TOKEN` environment variable.

### hooks-path

Directory where hook files are found. Empty by default, which disables hooks. Hook files found on this path and conforming to hook naming conventions are executed.

### hooks-status-interval

Defaults to 60 seconds. Configures how often the `gh-ost-on-status` hook is called, see [`hooks`](hooks.md) for full details on how to use hooks.

### initially-drop-ghost-table

`gh-ost` maintains two tables while migrating: the _ghost_ table (which is synced from your original table and finally replaces it) and a changelog table, which is used internally for bookkeeping. By default, it panics and aborts if it sees those tables upon startup. Provide `--initially-drop-ghost-table` and `--initially-drop-old-table` to let `gh-ost` know it's OK to drop them beforehand.

We think `gh-ost` should not take chances or make assumptions about the user's tables. Dropping tables can be a dangerous, locking operation. We let the user explicitly approve such operations.

### initially-drop-old-table

See [`initially-drop-ghost-table`](#initially-drop-ghost-table)

### initially-drop-socket-file

Default False. Should `gh-ost` forcibly delete an existing socket file. Be careful: this might drop the socket file of a running migration!

### ignore-http-errors

Ignore HTTP connection errors during checks configured with [`--throttle-http`](#throttle-http).

### include-triggers

When true, existing triggers on the original table are created on the new table. See also [`--trigger-suffix`](#trigger-suffix) and [`--remove-trigger-suffix-if-exists`](#remove-trigger-suffix-if-exists).

### master-password

MySQL password for the master when it differs from the password used for the inspected replica. Requires [`--assume-master-host`](#assume-master-host).

### master-user

MySQL user for the master when it differs from the user used for the inspected replica. Requires [`--assume-master-host`](#assume-master-host).

### max-lag-millis

On a replication topology, this is perhaps the most important migration throttling factor: the maximum lag allowed for migration to work. If lag exceeds this value, migration throttles.

When using [Connect to replica, migrate on master](cheatsheet.md#a-connect-to-replica-migrate-on-master), this lag is primarily tested on the very replica `gh-ost` operates on. Lag is measured by checking the heartbeat events injected by `gh-ost` itself on the utility changelog table. That is, to measure this replica's lag, `gh-ost` doesn't need to issue `show slave status` nor have any external heartbeat mechanism.

When [`--throttle-control-replicas`](#throttle-control-replicas) is provided, throttling also considers lag on specified hosts. Lag measurements on listed hosts is done by querying `gh-ost`'s _changelog_ table, where `gh-ost` injects a heartbeat.

When using on master or when `--allow-on-master` is provided, `max-lag-millis` is also considered a threshold for starting the cutover stage of the migration. If the row copy is complete and the heartbeat lag is less than `max-lag-millis` cutover phase of the migration will start. 

See also: [Sub-second replication lag throttling](subsecond-lag.md)

### max-load

List of metrics and threshold values; topping the threshold of any will cause throttler to kick in. See also: [`throttling`](throttle.md#status-thresholds)

### migrate-on-replica

Typically `gh-ost` is used to migrate tables on a master. If you wish to only perform the migration in full on a replica, connect `gh-ost` to said replica and pass `--migrate-on-replica`. `gh-ost` will briefly connect to the master but otherwise will make no changes on the master. Migration will be fully executed on the replica, while making sure to maintain a small replication lag.

### mysql-timeout

Connect, read, and write timeout for MySQL connections, in seconds. The value is applied to the MySQL driver's `timeout`, `readTimeout`, and `writeTimeout` DSN parameters. It is configured on the initial inspector connection and is copied to related gh-ost MySQL connections, including the applier/master connection and throttle-control replica checks. The default `0` uses the driver's default behavior.

### nice-ratio

Makes row-copy operations sleep after each chunk, proportional to the time spent copying that chunk. Range is `0.0` to `100.0`; default `0` is most aggressive. For example, `--nice-ratio=1` sleeps one millisecond for every millisecond spent copying rows.

### ok-to-drop-table

Allows `gh-ost` to drop the old table at the end of a successful migration. This is disabled by default because dropping a table can be a long locking operation.

### old-table

The old table name to use when [`--revert`](#revert) is enabled, for example `_mytable_del`.

### panic-flag-file

When this file is created, `gh-ost` immediately terminates without cleanup.

### panic-on-warnings

When this flag is set, `gh-ost` will panic when SQL warnings indicating data loss are encountered when copying data. This flag helps prevent data loss scenarios with migrations touching unique keys, column collation and types, as well as `NOT NULL` constraints, where `MySQL` will silently drop inserted rows that no longer satisfy the updated constraint (also dependent on the configured `sql_mode`).

While `panic-on-warnings` is currently disabled by defaults, it will default to `true` in a future version of `gh-ost`.

### password

MySQL password. To avoid passing the password on the command line, use [`--ask-pass`](#ask-pass) or [`--conf`](#conf).

### port

MySQL port to connect to. Preferably this is a replica, not the master. Default is `3306`.

### postpone-cut-over-flag-file

Indicate a file name, such that the final [cut-over](cut-over.md) step does not take place as long as the file exists.
When this flag is set, `gh-ost` expects the file to exist on startup, or else tries to create it. `gh-ost` exits with error if the file does not exist and `gh-ost` is unable to create it.
With this flag set, the migration will cut-over upon deletion of the file or upon `cut-over` [interactive command](interactive-commands.md).

### quiet

Quiet mode. Forces logging to errors only, overriding [`--verbose`](#verbose) or [`--debug`](#debug) if they are also provided.

### replica-server-id

Defaults to 99999. If you run multiple migrations then you must provide a different, unique `--replica-server-id` for each `gh-ost` process.
Optionally involve the process ID, for example: `--replica-server-id=$((1000000000+$$))`.

It's on you to choose a number that does not collide with another `gh-ost` or another running replica.
See also: [`concurrent-migrations`](cheatsheet.md#concurrent-migrations) on the cheatsheet.

### remove-trigger-suffix-if-exists

Remove the given suffix from trigger names. Requires [`--include-triggers`](#include-triggers) and [`--trigger-suffix`](#trigger-suffix).

### replication-lag-query

Deprecated. `gh-ost` uses an internal subsecond-resolution lag query instead.

### resume

`--resume` attempts to resume a migration that was previously interrupted from the last checkpoint. The first `gh-ost` invocation must run with `--checkpoint` and have successfully written a checkpoint in order for `--resume` to work.
See also: [`resuming-migrations`](resume.md)

### revert

Attempts to revert a completed migration. Must be used with [`--old-table`](#old-table). `--revert` cannot be used together with [`--resume`](#resume), and ignores migration options such as `--alter`, `--attempt-instant-ddl`, `--include-triggers`, and `--discard-foreign-keys`.

### runtime-metrics-interval

Seconds between Go runtime memory/GC gauge samples. Requires [`--statsd-addr`](#statsd-addr). Default is `10`; `0` disables runtime metrics.

### serve-socket-file

Defaults to an auto-determined and advertised upon startup file. Defines Unix socket file to serve on.

### serve-tcp-port

TCP port for `gh-ost` to serve its interactive interface on. Default is disabled.

### skip-foreign-key-checks

By default `gh-ost` verifies no foreign keys exist on the migrated table. On servers with large number of tables this check can take a long time. If you're absolutely certain no foreign keys exist (table does not reference other table nor is referenced by other tables) and wish to save the check time, provide with `--skip-foreign-key-checks`.

### skip-metadata-lock-check

By default `gh-ost` performs a check before the cut-over to ensure the rename session holds the exclusive metadata lock on the table. In case `performance_schema.metadata_locks` cannot be enabled on your setup, this check can be skipped with `--skip-metadata-lock-check`. 
:warning: Disabling this check involves the small chance of data loss in case a session accesses the ghost table during cut-over. See https://github.com/github/gh-ost/pull/1536 for details.

See also: [`allow-setup-metadata-lock-instruments`](#allow-setup-metadata-lock-instruments)

### skip-port-validation

Skips port validation for MySQL connections.

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

### stack

Adds a stack trace when logging errors.

### statsd-addr

StatsD endpoint, either `host:port` or a Unix socket. Empty disables StatsD.

### statsd-tags

Global StatsD tags applied to every metric. This flag is repeatable and uses `key:value` format. Example: `--statsd-tags 'env:prod' --statsd-tags 'service:gh-ost'`.

### storage-engine
Default is `innodb`, and `rocksdb` support is currently experimental. InnoDB and RocksDB are both transactional engines, supporting both shared and exclusive row locks.

But RocksDB currently lacks a few features support compared to InnoDB:
- Gap Locks
- Foreign Key
- Generated Columns
- Spatial
- Geometry

When `--storage-engine=rocksdb`, `gh-ost` will make some changes necessary (e.g. sets isolation level to `READ_COMMITTED`) to support RocksDB.

### charset
The default charset for the database connection is utf8mb4, utf8, latin1. The ability to specify character set and collation is supported, eg: utf8mb4_general_ci,utf8_general_ci,latin1. 

### switch-to-rbr

Allows `gh-ost` to automatically switch the replica's binary log format to `ROW`, if needed. The format is not switched back automatically.

### table

The table name to migrate, without the database name. Required unless the table is specified in a full [`--alter`](#alter) statement. See also [`--database`](#database).

### test-on-replica

Issue the migration on a replica; do not modify data on master. Useful for validating, testing and benchmarking. See [`testing-on-replica`](testing-on-replica.md)

### test-on-replica-skip-replica-stop

Default `False`. When `--test-on-replica` is enabled, do not issue commands stop replication (requires `--test-on-replica`).

### throttle-additional-flag-file

An additional flag file that pauses operation while it exists. Defaults to `/tmp/gh-ost.throttle` and is useful for throttling multiple `gh-ost` operations at once.

### throttle-flag-file

A flag file that pauses operation while it exists. Prefer a file name specific to the table being altered.

### throttle-query

A custom query issued every second to decide whether the migration should throttle. The query runs on the migrated server and should return `0` for no throttle and a value greater than `0` to throttle. Keep this query lightweight.

### throttle-control-replicas

Provide a command delimited list of replicas; `gh-ost` will throttle when any of the given replicas lag beyond [`--max-lag-millis`](#max-lag-millis). The list can be queried and updated dynamically via [interactive commands](interactive-commands.md)

### throttle-http

Provide an HTTP endpoint; `gh-ost` will issue `HEAD` requests on given URL and throttle whenever response status code is not `200`. The URL can be queried and updated dynamically via [interactive commands](interactive-commands.md). Empty URL disables the HTTP check.

### throttle-http-interval-millis

Defaults to 100. Configures the HTTP throttle check interval in milliseconds.

### throttle-http-timeout-millis

Defaults to 1000 (1 second). Configures the HTTP throttler check timeout in milliseconds.

### timestamp-old-table

Makes the _old_ table include a timestamp value. The _old_ table is what the original table is renamed to at the end of a successful migration. For example, if the table is `gh_ost_test`, then the _old_ table would normally be `_gh_ost_test_del`. With `--timestamp-old-table` it would be, for example, `_gh_ost_test_20170221103147_del`.

### trigger-suffix

Adds a suffix to trigger names, for example `_v2`. Requires [`--include-triggers`](#include-triggers).

### tungsten

See [`tungsten`](cheatsheet.md#tungsten) on the cheatsheet.

### user

MySQL user.

### verbose

Enables info-level logging. See also [`--debug`](#debug) and [`--quiet`](#quiet).

### version

Prints the `gh-ost` version and exits.
