# Interactive commands

`gh-ost` is designed to be operations friendly. To that effect, it allows the user to control its behavior even while it is running.

### Interactive interfaces

`gh-ost` listens on:

- Unix socket file: either provided via `--serve-socket-file` or determined by `gh-ost`, this interface is always up.
  When self-determined, `gh-ost` will advertise the identify of socket file upon start up and throughout the migration.
- TCP: if `--serve-tcp-port` is provided

Both interfaces may serve at the same time. Both respond to simple text command, which makes it easy to interact via shell.

### Known commands

- `help`: shows a brief list of available commands
- `status`: returns a status summary of migration progress and configuration
replication lag on to determine throttling
- `chunk-size=<newsize>`: modify the `chunk-size`; applies on next running copy-iteration
- `max-load=<max-load-thresholds>`: modify the `max-load` config; applies on next running copy-iteration
  The `max-load` format must be: `some_status=<numeric-threshold>[,some_status=<numeric-threshold>...]`. For example: `Threads_running=50,threads_connected=1000`, and you would then write/echo `max-load=Threads_running=50,threads_connected=1000` to the socket.
- `critical-load=<load>`: change critical load setting (exceeding given thresholds causes panic and abort)
- `throttle-query`: change throttle query
- `throttle-control-replicas`: change list of throttle-control replicas, these are replicas `gh-ost` will cehck
- `throttle`: force migration suspend
- `no-throttle`: cancel forced suspension (though other throttling reasons may still apply)
- `unpostpone`: at a time where `gh-ost` is postponing the [cut-over](cut-over.md) phase, instruct `gh-ost` to stop postponing and proceed immediately to cut-over.
- `panic`: immediately panic and abort operation

### Examples

While migration is running:

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

```shell
$ echo "chunk-size=250" | nc -U /tmp/gh-ost.test.sample_data_0.sock
# Migrating `test`.`sample_data_0`; Ghost table is `test`.`_sample_data_0_gst`
# Migration started at Tue Jun 07 11:56:03 +0200 2016
# chunk-size: 250; max lag: 1500ms; max-load: map[Threads_connected:20]
# Throttle additional flag file: /tmp/gh-ost.throttle
# Serving on unix socket: /tmp/gh-ost.test.sample_data_0.sock
# Serving on TCP port: 10001
```

```shell
$ echo throttle | nc -U /tmp/gh-ost.test.sample_data_0.sock

$ echo status | nc -U /tmp/gh-ost.test.sample_data_0.sock
# Migrating `test`.`sample_data_0`; Ghost table is `test`.`_sample_data_0_gst`
# Migration started at Tue Jun 07 11:56:03 +0200 2016
# chunk-size: 250; max lag: 1500ms; max-load: map[Threads_connected:20]
# Throttle additional flag file: /tmp/gh-ost.throttle
# Serving on unix socket: /tmp/gh-ost.test.sample_data_0.sock
# Serving on TCP port: 10001
Copy: 0/2915 0.0%; Applied: 0; Backlog: 0/100; Elapsed: 59s(copy), 59s(total); streamer: mysql-bin.000551:68067; ETA: throttled, commanded by user
```
