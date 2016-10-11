# Migrating with Statement Based Replication

Even though `gh-ost` relies on Row Based Replication (RBR), it does not mean you can't keep your Statement Based Replication (SBR).

`gh-ost` is happy to, and actually prefers and suggests to, connect to a replica. On this replica, it is happy to:
- issue the heavyweight `INFORMATION_SCHEMA` queries that make a table structure analysis
- issue a `select count(*) from mydb.mytable`, should `--exact-rowcount` be provided
- connect itself as a fake replica to get the binary log stream

All of the above can be executed on the master, but we're more comfortable that they execute on a replica.

Please note the third item: `gh-ost` connects as a fake replica and pulls the binary logs. This is how `gh-ost` finds the table's changelog: it looks up entries in the binary log.

The magic is that your master can still produce SBR, but if you have a replica with `log-slave-updates`, you can also configure it to have `binlog_format='ROW'`. Such a replica accepts SBR statements from its master, and produces RBR statements onto its binary logs.

`gh-ost` is happy to modify the `binlog_format` on the replica for you:
- If you supply `--switch-to-rbr`, `gh-ost` will convert the binlog format for you, and restart replication to make sure this takes effect.
- If your replica is an intermediate master, i.e. further serves as a master to other replicas, `gh-ost` will not convert the `binlog_format`.
- At any case, `gh-ost` **will not** convert back to `STATEMENT` (SBR). This is because you may be running multiple migrations concurrently. Being able to run concurrent migrations is one of the design goals of this tool. It's your own responsibility to switch back to SBR once all pending migrations are complete.

### Summary

- If you're already using RBR, all is well for you
- If not, convert one of your replicas to `binlog_format='ROW'`, or let `gh-ost` do this for you.
