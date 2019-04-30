# Requirements and limitations

### Requirements

- You will need to have one server serving Row Based Replication (RBR) format binary logs. Right now `FULL` row image is supported. `MINIMAL` to be supported in the near future. `gh-ost` prefers to work with replicas. You may [still have your master configured with Statement Based Replication](migrating-with-sbr.md) (SBR).

- If you are using a replica, the table must have an identical schema between the master and replica.

- `gh-ost` requires an account with these privileges:

  - `ALTER, CREATE, DELETE, DROP, INDEX, INSERT, LOCK TABLES, SELECT, TRIGGER, UPDATE` on the database (schema) where your migrated table is, or of course on `*.*`
  - either:
    - `SUPER, REPLICATION SLAVE` on `*.*`, or:
    - `REPLICATION CLIENT, REPLICATION SLAVE` on `*.*`

The `SUPER` privilege is required for `STOP SLAVE`, `START SLAVE` operations. These are used on:

- Switching your `binlog_format` to `ROW`, in the case where it is _not_ `ROW` and you explicitly specified `--switch-to-rbr`
  - If your replication is already in RBR (`binlog_format=ROW`) you can specify `--assume-rbr` to avoid the `STOP SLAVE/START SLAVE` operations, hence no need for `SUPER`.

- Running `--test-on-replica`: before the cut-over phase, `gh-ost` stops replication so that you can compare the two tables and satisfy that the migration is sound.

### Limitations

- Foreign key constraints are not supported. They may be supported in the future, to some extent.

- Triggers are not supported. They may be supported in the future.

- MySQL 5.7 `JSON` columns are supported but not as part of `PRIMARY KEY`

- The two _before_ & _after_ tables must share a `PRIMARY KEY` or other `UNIQUE KEY`. This key will be used by `gh-ost` to iterate through the table rows when copying. [Read more](shared-key.md)
  - The migration key must not include columns with NULL values. This means either:
    1. The columns are `NOT NULL`, or
    2. The columns are nullable but don't contain any NULL values.
  - by default, `gh-ost` will not run if the only `UNIQUE KEY` includes nullable columns.
    - You may override this via `--allow-nullable-unique-key` but make sure there are no actual `NULL` values in those columns. Existing NULL values can't guarantee data integrity on the migrated table.

- It is not allowed to migrate a table where another table exists with same name and different upper/lower case.
  - For example, you may not migrate `MyTable` if another table called `MYtable` exists in the same schema.

- Amazon RDS works, but has its own [limitations](rds.md).
- Google Cloud SQL works, `--gcp` flag required.
- Aliyun RDS works, `--aliyun-rds` flag required.

- Multisource is not supported when migrating via replica. It _should_ work (but never tested) when connecting directly to master (`--allow-on-master`)

- Master-master setup is only supported in active-passive setup. Active-active (where table is being written to on both masters concurrently) is unsupported. It may be supported in the future.

- If you have an `enum` field as part of your migration key (typically the `PRIMARY KEY`), migration performance will be degraded and potentially bad. [Read more](https://github.com/github/gh-ost/pull/277#issuecomment-254811520)

- Migrating a `FEDERATED` table is unsupported and is irrelevant to the problem `gh-ost` tackles.

- `ALTER TABLE ... RENAME TO some_other_name` is not supported (and you shouldn't use `gh-ost` for such a trivial operation).
