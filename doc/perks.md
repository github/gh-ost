# Perks

Listed below are some operation perks that make the DBA happy.

### Dynamic reconfiguration

You started with a `chunk-size=5000` but you find out it's too much. You want to reduce it. There is no need to kill and restart the migration with a new configuration. You may change the `chunk-size` dynamically.

`gh-ost` listens on a unix socket file, and optionally via `TCP` as well. You may, for example:

```shell
$ echo "chunk-size=250" | nc -U /tmp/gh-ost.test.sample_data_0.sock
```

Likewise, you can change the `max-load` configuration:

```shell
$ echo "max-load=Threads_running=50,threads_connected=1000" | nc -U /tmp/gh-ost.test.sample_data_0.sock
```

The `max-load` format must be: `some_status=<numeric-threshold>[,some_status=<numeric-threshold>...]`.
In case of parsing error the command is ignored.

Read more about [interactive commands](interactive-commands.md)

### What's the status?

You do not have to have access to the `screen` where the migration is issued. You have two ways to get current status:

1. Use [interactive commands](interactive-commands.md). Via unix socket file or via `TCP` you can get current status:

```shell
$ echo status | nc -U /tmp/gh-ost.test.sample_data_0.sock
# Migrating `test`.`sample_data_0`; Ghost table is `test`.`_sample_data_0_gst`
# Migration started at Tue Jun 07 11:45:16 +0200 2016
# chunk-size: 200; max lag: 1500ms; max-load: map[Threads_connected:20]
# Throttle additional flag file: /tmp/gh-ost.throttle
# Serving on unix socket: /tmp/gh-ost.test.sample_data_0.sock
# Serving on TCP port: 10001
Copy: 0/2915 0.0%; Applied: 0; Backlog: 0/100; Elapsed: 40s(copy), 41s(total); streamer: mysql-bin.000550:49942; ETA: throttled, flag-file
```

2. `gh-ost` creates and uses a changelog table for internal bookkeeping. This table has the `_osc` suffix (the tool creates and announces this table upon startup) If you like, you can SQL your status:

```
> select * from _sample_data_0_osc order by id desc limit 1 \G
*************************** 1. row ***************************
         id: 325
last_update: 2016-06-08 15:52:13
       hint: copy iteration 0 at 1465393933
      value: Copy: 0/2915 0.0%; Applied: 0; Backlog: 0/100; Elapsed: 1m35s(copy), 1m35s(total); streamer: mysql-bin.000560:60904; ETA: throttled, flag-file
```

### Postpone the cut-over phase

You begin a migration, and the ETA is for it to complete at 04:00am. Not a good time for you, because you happen to want to have eyes on things as the migration completes (ideally, you shouldn't need to, but life is hard).

Today, DBAs are coordinating the migration start time such that it completes in a convenient hour. `gh-ost` offers an alternative: postpone the final cut-over phase till you're ready.

Execute `gh-ost` with `--postpone-cut-over-flag-file=/path/to/flag.file`. As long as this file exists, `gh-ost` will not take the final cut-over step. It will complete the row copy, and continue to synchronize the tables by continuously applying changes made on the original table onto the ghost table. It can do so on and on and on. When you're finally ready, remove the file and cut-over will take place.

### Sub-second lag throttling

With sub-second replication lag measurements, `gh-ost` is able to keep a fleet of replicas well below `1sec` lag throughout the migration. We encourage you to issue sub-second heartbeats. Read more on [sub-second replication lag throttling](subsecond-lag.md)
