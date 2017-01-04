# Sub-second replication lag throttling

`gh-ost` is able to utilize sub-second replication lag measurements.

At GitHub, small replication lag is crucial, and we like to keep it below `1s` at all times.

`gh-ost` will do sub-second throttling when `--max-lag-millis` is smaller than `1000`, i.e. smaller than `1sec`.
Replication lag is measured on:

- The "inspected" server (the server `gh-ost` connects to; replica is desired but not mandatory)
- The `throttle-control-replicas` list

In both cases, `gh-ost` uses an internal heartbeat mechanism. It injects heartbeat events onto the utility changelog table, then reads those entries on replicas, and compares times. This measurement is on by default and by definition supports sub-second resolution.

You can explicitly define how frequently will `gh-ost` inject heartbeat events, via `heartbeat-interval-millis`. You should set `heartbeat-interval-millis <= max-lag-millis`. It still works if not, but loses granularity and effect.

In earlier versions, the `--throttle-control-replicas` list was subjected to `1` second resolution or to 3rd party heartbeat injections such as `pt-heartbeat`. This is no longer the case. The argument `--replication-lag-query` has been deprecated and is no longer needed.

Our production migrations use sub-second lag throttling and are able to keep our entire fleet of replicas well below `1sec` lag. We use `--heartbeat-interval-millis=100` on our production migrations with a `--max-lag-millis` value of between `300` and `500`.
