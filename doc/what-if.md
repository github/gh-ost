# What if?

Technical questions and answers. This document will be updated as we go

### What if I'm using Statement Based Replication?

You can still migrate tables with `gh-ost`. We do that. What you will need is a replica configured with:

- `log_bin`
- `log_slave_updates`
- `binlog_format=ROW`

Thus, the replica will transform the master's SBR binlogs into RBR binlogs. `gh-ost` is happy to read the binary logs from the replica. [Read more](migrating-with-sbr.md)

### What if gh-ost crashes halfway through, or I kill it?

Unlike trigger-based solutions, there's nothing urgent to clean up in the event `gh-ost` bails out or gets killed. There are the two tables creates by `gh-ost`:

- The _ghost_ table: `_yourtablename_gho`
- The _changelog_ table: `_yourtablename_ghc`

You may instruct `gh-ost` to drop these tables upon startup; or better yet, you drop them.

### What if the cut-over (table switch) is unable to proceed due to locks/timeout?

There is a `lock_wait_timeout` explicitly associated with the cut-over operation. If your table suddenly suffers from a long running query, the cut-over (involving `LOCK` and `RENAME` statements) may be unable to proceed. There's a finite number of retries, and if none of these succeeds, `gh-ost` bails out.

### What if the migration is causing a high load on my master?

This is where `gh-ost` shines. There is no need to kill it as you may be used to with other tools. You can reconfigure `gh-ost` [on the fly](https://github.com/github/gh-ost/blob/master/doc/interactive-commands.md) to be nicer.

You're always able to actively begin [throttling](throttle.md). Just touch the `throttle-file` or `echo throttle` into `gh-ost`. Otherwise, reconfigure your `max-load`, the `nice-ratio`, the `throttle-query` to gain better thresholds that would suit your needs.

### What if my replicas don't use binary logs?

If the master is running Row Based Replication (RBR) - point `gh-ost` to the master, and specify `--allow-on-master`. See [cheatsheets](cheatsheet.md)

If the master is running Statement Based Replication (SBR) - you have no alternative but to reconfigure a replica with:

- `log_bin`
- `log_slave_updates`
- `binlog_format=ROW`
