# Amazon RDS# Amazon RDS

## Limitations

- No `SUPER` privileges. 
- `gh-ost` runs should be setup use [`--assume-rbr`][assume_rbr_docs] and use `binlog_format=ROW`.
- Aurora does not allow editing of the `read_only` parameter. While it is defined as `{TrueIfReplica}`, the parameter is non-modifiable field.

## Aurora

#### Replication

In Aurora replication, you have separate reader and writer endpoints however because the cluster shares the underlying storage layer, `gh-ost` will detect it is running on the master. This becomes an issue when you wish to use [migrate/test on replica][migrate_test_on_replica_docs] because you won't be able to use a single cluster in the same way you would with MySQL RDS. 

To work around this, you can follow along the [AWS replication between clusters documentation][aws_replication_docs] for Aurora with one small caveat. For the "Create a Snapshot of Your Replication Master" step, the binlog position is not available in the AWS console. You will need to issue the SQL query `SHOW SLAVE STATUS` or `aws rds describe-events` API call to get the correct position.

[migrate_test_on_replica_docs]: https://github.com/github/gh-ost/blob/master/doc/cheatsheet.md#c-migratetest-on-replica
[aws_replication_docs]: http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Aurora.Overview.Replication.MySQLReplication.html