# Throttle

Throughout a migration operation, `gh-ost` is either actively copying and applying data, or is _throttling_.

When _throttled_, `gh-ost` ceases to write row data and ceases to inspect binary log entries. It pauses all writes except for the low-volume changelog status writes and the heartbeat writes.

As compared with trigger-based solutions, when `gh-ost` is throttled, the write load on the master is truly removed.

Typically, throttling is based on replication lag or on master load. At such time, you wish to reduce load from master and from replication by pausing the _ghost_ writes. However, with a trigger based solution this is impossible to achieve: the triggers must remain in place and they continue to generate excess writes while the table is being used.

Since `gh-ost` is not based on triggers, but of reading binary logs, it controls its own writes. Each and every write on the master comes from the `gh-ost` app, which means `gh-ost` is able to reduce writes to a bare minimum when it wishes so.

`gh-ost` supports various means for controlling throttling behavior; it is operations friendly in that it allows the user greater, dynamic control of throttler behavior.

### Throttling parameters and factors

Throttling is controlled via the following explicit and implicit factors:

#### Replication-lag

The recommended way of running `gh-ost` is by connecting it to a replica. It will figure out the master by traversing the topology. It is by design that `gh-ost` is throttle aware: it generates its own _heartbeat_ mechanism; while it is running the migration, it is self-checking the replica to which it is connected for replication lag.

Otherwise you may specify your own list of replica servers you wish it to observe.

- `--throttle-control-replicas`: list of replicas you explicitly wish `gh-ost` to check for replication lag.

  Example: `--throttle-control-replicas=myhost1.com:3306,myhost2.com,myhost3.com:3307`

- `--max-lag-millis`: maximum allowed lag; any controlled replica lagging more than this value will cause throttling to kick in. When all control replicas have smaller lag than indicated, operation resumes.

Note that you may dynamically change both `--max-lag-millis` and the `throttle-control-replicas` list via [interactive commands](interactive-commands.md)

#### Status thresholds

- `--max-load`: list of metrics and threshold values; topping the threshold of any will cause throttler to kick in.

  Example:

  `--max-load='Threads_running=100,Threads_connected=500'`

  Metrics must be valid, numeric [status variables](http://dev.mysql.com/doc/refman/5.6/en/server-status-variables.html)

#### Throttle query

- When provided, the `--throttle-query` is expected to return a scalar integer. A return value `> 0` implies `gh-ost` should throttle. A return value `<= 0` implied `gh-ost` is free to proceed (pending other throttling factors).

An example query could be: `--throttle-query="select hour(now()) between 8 and 17"` which implies throttling auto-starts `8:00am` and migration auto-resumes at `18:00pm`.

#### HTTP Throttle

The `--throttle-http` flag allows for throttling via HTTP. Every 100ms `gh-ost` issues a `HEAD` request to the provided URL. If the response status code is not `200` throttling will kick in until a `200` response status code is returned.

If no URL is provided or the URL provided doesn't contain the scheme then the HTTP check will be disabled. For example `--throttle-http="http://1.2.3.4:6789/throttle"` will enable the HTTP check/throttling, but `--throttle-http="1.2.3.4:6789/throttle"` will not.

The URL can be queried and updated dynamically via [interactive interface](interactive-commands.md).

#### Manual control

In addition to the above, you are able to take control and throttle the operation any time you like.

- `--throttle-flag-file`: when this file exists, throttling kicks in. Just `touch` the file to begin throttling.

- `--throttle-additional-flag-file`: similar to the above. When this file exists, throttling kicks in.

  Default: `/tmp/gh-ost.throttle`

  The reason for having two files has to do with the intent of being able to run multiple migrations concurrently.
  The setup we wish to use is that each migration would have its own, specific `throttle-flag-file`, but all would use the same `throttle-additional-flag-file`. Thus, we are able to throttle specific migrations by touching their specific files, or we are able to throttle all migrations at once, by touching the shared file.

- `throttle` command via [interactive interface](interactive-commands.md).

  Example:

  ```
    echo throttle | nc -U /tmp/gh-ost.test.sample_data_0.sock
    echo no-throttle | nc -U /tmp/gh-ost.test.sample_data_0.sock
  ```

### Throttle precedence

Any single factor in the above that suggests the migration should throttle - causes throttling. That is, once some component decides to throttle, you cannot override it; you cannot force continued execution of the migration.

`gh-ost` collects different throttle-related metrics at different times, independently. It asynchronously reads the collected metrics and checks if they satisfy conditions/thresholds.

The first check to suggest throttling stops the check; the status message will note the reason for throttling as the first satisfied check.

### Throttle status

The throttle status is printed as part of the periodic [status message](understanding-output.md):

```
Copy: 0/2915 0.0%; Applied: 0; Backlog: 0/100; Elapsed: 41s(copy), 41s(total); streamer: mysql-bin.000551:47983; ETA: throttled, flag-file
Copy: 0/2915 0.0%; Applied: 0; Backlog: 0/100; Elapsed: 42s(copy), 42s(total); streamer: mysql-bin.000551:49370; ETA: throttled, commanded by user
```

### How long can you throttle for?

Throttling time is limited by the availability of the binary logs. When throttling begins, `gh-ost` suspends reading the binary logs, and expects to resume reading from same binary log where it paused.

Your availability of binary logs is typically determined by the [expire_logs_days](https://dev.mysql.com/doc/refman/5.6/en/server-system-variables.html#sysvar_expire_logs_days) variable. If you have `expire_logs_days = 10` (or check `select @@global.expire_logs_days`), then you should be able to throttle for up to `10` days.

Having said that, throttling for so long is far fetching, in that the `gh-ost` process itself must be kept alive during that time; and the amount of binary logs to process once it resumes will potentially take days to replay.

It is worth mentioning that some deployments have external scheduled scripts that purge binary logs, regardless of the `expire_logs_days` configuration. Please verify your own deployment configuration.

To clarify, you only need to keep binary logs on the single server `gh-ost` connects to.
