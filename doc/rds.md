# Amazon RDS

## Limitations

- No `SUPER` privileges.
- `gh-ost` runs should be setup use [`--assume-rbr`][assume_rbr_docs] and use `binlog_format=ROW`.
- Aurora does not allow editing of the `read_only` parameter. While it is defined as `{TrueIfReplica}`, the parameter is non-modifiable field.
- In order to have binlogs enabled, the backup window must be set to greater than 1 day.

## Aurora

#### Replication

In Aurora replication, you have separate reader and writer endpoints however because the cluster shares the underlying storage layer, `gh-ost` will detect it is running on the master. This becomes an issue when you wish to use [migrate/test on replica][migrate_test_on_replica_docs] because you won't be able to use a single cluster in the same way you would with MySQL RDS.

To work around this, you can follow along the [AWS replication between clusters documentation][aws_replication_docs] for Aurora with one small caveat. For the "Create a Snapshot of Your Replication Master" step, the binlog position is not available in the AWS console. You will need to issue the SQL query `SHOW SLAVE STATUS` or `aws rds describe-events` API call to get the correct position.

#### Percona Toolkit

If you use `pt-table-checksum` as a part of your data integrity checks, you might want to check out [this patch][percona_toolkit_patch] which will enable you to run `pt-table-checksum` with the `--no-binlog-format-check` flag and prevent errors like the following:

```
03-24T12:51:06 Failed to /*!50108 SET @@binlog_format := 'STATEMENT'*/: DBD::mysql::db do failed: Access denied; you need (at least one of) the SUPER privilege(s) for this operation [for Statement "/*!50108 SET @@binlog_format := 'STATEMENT'*/"] at pt-table-checksum line 9292.

This tool requires binlog_format=STATEMENT, but the current binlog_format is set to ROW and an error occurred while attempting to change it.  If running MySQL 5.1.29 or newer, setting binlog_format requires the SUPER privilege.  You will need to manually set binlog_format to 'STATEMENT' before running this tool.
```

[assume_rbr_docs]: https://github.com/github/gh-ost/blob/master/doc/command-line-flags.md#assume-rbr
[migrate_test_on_replica_docs]: https://github.com/github/gh-ost/blob/master/doc/cheatsheet.md#c-migratetest-on-replica
[aws_replication_docs]: http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Aurora.Overview.Replication.MySQLReplication.html
[percona_toolkit_patch]: https://github.com/jacobbednarz/percona-toolkit/commit/0271ba6a094da446a5e5bb8d99b5c26f1777f2b9
