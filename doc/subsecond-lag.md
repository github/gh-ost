# Sub-second replication lag throttling

`gh-ost` is able to utilize sub-second replication lag measurements.

At GitHub, small replication lag is crucial, and we like to keep it below `1s` at all times. If you have similar concern, we strongly urge you to proceed to implement sub-second lag throttling.

`gh-ost` will do sub-second throttling when `--max-lag-millis` is smaller than `1000`, i.e. smaller than `1sec`.
Replication lag is measured on:

- The "inspected" server (the server `gh-ost` connects to; replica is desired but not mandatory)
- The `throttle-control-replicas` list

For the inspected server, `gh-ost` uses an internal heartbeat mechanism. It injects heartbeat events onto the utility changelog table, then reads those events in the binary log, and compares times. This measurement is by default and by definition sub-second enabled.

You can explicitly define how frequently will `gh-ost` inject heartbeat events, via `heartbeat-interval-millis`. You should set `heartbeat-interval-millis <= max-lag-millis`. It still works if not, but loses granularity and effect.

On the `throttle-control-replicas`, `gh-ost` only issues SQL queries, and does not attempt to read the binary log stream. Perhaps those other replicas don't have binary logs in the first place.

The standard way of getting replication lag on a replica is to issue `SHOW SLAVE STATUS`, then reading `Seconds_behind_master` value. But that value has a `1sec` granularity.

To be able to throttle on your production replicas fleet when replication lag exceeds a sub-second threshold, you must provide with a `replication-lag-query` that returns a sub-second resolution lag.

As a common example, many use [pt-heartbeat](https://www.percona.com/doc/percona-toolkit/2.2/pt-heartbeat.html) to inject heartbeat events on the master. You would issue something like:

    /usr/bin/pt-heartbeat -- -D your_schema --create-table --update --replace --interval=0.1 --daemonize --pid ...

Note `--interval=0.1` to indicate `10` heartbeats per second.

You would then provide

    gh-ost ... --replication-lag-query="select unix_timestamp(now(6)) - unix_timestamp(ts) as ghost_lag_check from your_schema.heartbeat order by ts desc limit 1"

Our production migrations use sub-second lag throttling and are able to keep our entire fleet of replicas well below `1sec` lag.
