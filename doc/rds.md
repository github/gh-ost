**Note:** `gh-ost` supports Amazon RDS. However, GitHub Engineering does not use AWS for production databases and this page is community-driven. If you find a bug in the documentation please [open an issue][new_issue].

# Amazon RDS

## Limitations

- `gh-ost` must be run directly on the master, as RDS replicas cannot have binary logging enabled.

- RDS does not allow `SUPER` user privileges. Set the `gh-ost` user to have these privileges:

| Privilege | Scope |
|--|--|
| ALTER, CREATE, DELETE, DROP, INDEX, INSERT, LOCK TABLES, SELECT, TRIGGER, UPDATE | [schema].* |
| REPLICATION CLIENT, REPLICATION SLAVE | \*.\* |

## Requirements

- `binlog_format=ROW` must be set on the master.
  - `--switch-to-rbr` will not work on RDS as it requires the `SUPER` privilege.
  - MySQL settings are contained in the [DB Parameter Groups](http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithParamGroups.html). To modify `binlog_format` for an instance, you can either:
    - modify the value in the instance's current DB Parameter Group, wait several minutes for the parameter group to be "in-sync", and bounce any persistent connections to the database
    - change the instance's parameter group; this requires a restart of the instance.

    Note that modifying a DB Parameter Group will change the settings for all instances using that parameter group.

- Required `gh-ost` options:
  - `--host` set to the master host
  - [`--allow-on-master`](https://github.com/github/gh-ost/blob/master/doc/command-line-flags.md#allow-on-master)
  - [`--assume-rbr`][assume_rbr_docs]

## Aurora

#### Replication

In Aurora replication, you have separate reader and writer endpoints; however, because the cluster shares the underlying storage layer, `gh-ost` will detect it as running on the master. This becomes an issue when you wish to use [migrate/test on replica][migrate_test_on_replica_docs] because you won't be able to use a single cluster in the same way you would with MySQL RDS.

To work around this, you can follow the [AWS replication between clusters documentation][aws_replication_docs] for Aurora with one small caveat: for the "Create a Snapshot of Your Replication Master" step, the binlog position will not be available in the AWS console. You will need to issue the SQL query `SHOW SLAVE STATUS` or the `aws rds describe-events` API call to get the correct position.

#### Percona Toolkit

If you use `pt-table-checksum` as a part of your data integrity checks, you might want to check out [this patch][percona_toolkit_patch] which will enable you to run `pt-table-checksum` with the `--no-binlog-format-check` flag and prevent errors like the following:

```
03-24T12:51:06 Failed to /*!50108 SET @@binlog_format := 'STATEMENT'*/: DBD::mysql::db do failed: Access denied; you need (at least one of) the SUPER privilege(s) for this operation [for Statement "/*!50108 SET @@binlog_format := 'STATEMENT'*/"] at pt-table-checksum line 9292.

This tool requires binlog_format=STATEMENT, but the current binlog_format is set to ROW and an error occurred while attempting to change it.  If running MySQL 5.1.29 or newer, setting binlog_format requires the SUPER privilege.  You will need to manually set binlog_format to 'STATEMENT' before running this tool.
```

#### Preflight checklist

Before trying to run any `gh-ost` migrations you will want to confirm the following:

- [ ] You have a secondary cluster available that will act as a replica. Rule of thumb here has been a 1 instance per cluster to mimic MySQL-style replication as opposed to Aurora style.
- [ ] The database instance parameters and database cluster parameters are consistent between your master and replicas.
- [ ] Executing `SHOW SLAVE STATUS\G` on your replica cluster displays the correct master host, binlog position, etc.
- [ ] Database backup retention is greater than 1 day (to enable binary logging).
- [ ] You have set up [`hooks`][ghost_hooks] to issue RDS procedures for stopping and starting replication. (see [github/gh-ost#163][ghost_rds_issue_tracking] for examples)

[new_issue]: https://github.com/github/gh-ost/issues/new
[assume_rbr_docs]: https://github.com/github/gh-ost/blob/master/doc/command-line-flags.md#assume-rbr
[migrate_test_on_replica_docs]: https://github.com/github/gh-ost/blob/master/doc/cheatsheet.md#c-migratetest-on-replica
[aws_replication_docs]: http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Aurora.Overview.Replication.MySQLReplication.html
[percona_toolkit_patch]: https://github.com/jacobbednarz/percona-toolkit/commit/0271ba6a094da446a5e5bb8d99b5c26f1777f2b9
[ghost_hooks]: https://github.com/github/gh-ost/blob/master/doc/hooks.md
[ghost_rds_issue_tracking]: https://github.com/github/gh-ost/issues/163
