# Requirements and limitations

### Requirements

- You will need to have one server serving Row Based Replication (RBR) format binary logs. Right now `FULL` row image is supported. `MINIMAL` to be supported in the near future. `gh-ost` prefers to work with replicas. You may [still have your master configured with Statement Based Replication](migrating-with-sbr) (SBR).

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

- Foreign keys not supported. They may be supported in the future, to some extent.
- Triggers are not supported. They may be supported in the future.
- The two _before_ & _after_ tables must share some `UNIQUE KEY`. Such key would be used by `gh-ost` to iterate the table.
  - As an example, if your table has a single `UNIQUE KEY` and no `PRIMARY KEY`, and you wish to replace it with a `PRIMARY KEY`, you will need two migrations: one to add the `PRIMARY KEY` (this migration will use the existing `UNIQUE KEY`), another to drop the now redundant `UNIQUE KEY` (this migration will use the `PRIMARY KEY`).
- The chosen migration key must not include columns with `NULL` values.
  - `gh-ost` will do its best to pick a migration key with non-nullable columns. It will by default refuse a migration where the only possible `UNIQUE KEY` includes nullable-columns. You may override this refusal via `--allow-nullable-unique-key` but **you must** be sure there are no actual `NULL` values in those columns. Such `NULL` values would cause a data integrity problem and potentially a corrupted migration.
- It is not allowed to migrate a table where another table exists with same name and different upper/lower case.
  - For example, you may not migrate `MyTable` if another table called `MYtable` exists in the same schema.
- Amazon RDS and Google Cloud SQL are currently not supported
  - We began working towards removing this limitation. See tracking issue: https://github.com/github/gh-ost/issues/163
- Multisource is not supported when migrating via replica. It _should_ work (but never tested) when connecting directly to master (`--allow-on-master`)
- Master-master setup is only supported in active-passive setup. Active-active (where table is being written to on both masters concurrently) is unsupported. It may be supported in the future.
