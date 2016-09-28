# Testing on replica

`gh-ost`'s design allows for trusted and reliable tests of the migration without compromising production data integrity.

Test on replica if you:
- Are unsure of `gh-ost`, have not gained confidence into its workings
- Just want to experiment with a real migration without affecting production (maybe measure migration time?)
- Wish to observe data change impact

## What testing on replica means

TL;DR `gh-ost` will make all changes on a replica and leave both original and ghost tables for you to compare.

## Issuing a test drive

Apply `--test-on-replica --host=<a.replica>`.
- `gh-ost` would connect to the indicated server
- Will verify this is indeed a replica and not a master
- Will perform _everything_ on this replica. Other then checking who the master is, it will otherwise not touch it.
  - All `INFORMATION_SCHEMA` and `SELECT` queries run on the replica
  - Ghost table is created on the replica
  - Rows are copied onto the ghost table on the replica
  - Binlog events are read from the replica and applied to ghost table on the replica
  - So... everything

`gh-ost` will sync the ghost table with the original table.
- When it is satisfied, it will issue a `STOP SLAVE`, stopping replication
- Will finalize last few statements
- Will swap tables via normal [cut-over](cut-over.md), and immediately revert the swap.
- Will terminate. No table is dropped.

You are now left with the original table **and** the ghost table. When using a trivial `alter` statement, such as `engine-innodb`, both tables _should_ be identical.

You now have the time to verify the tool works correctly. You may checksum the entire table data if you like.
- e.g.
  `mysql -e 'select * from mydb.mytable order by id' | md5sum`
  `mysql -e 'select * from mydb._mytable_gst order by id' | md5sum`
- or of course only select the shared columns before/after the migration
- We use the trivial `engine=innodb` for `alter` when testing. This way the resulting ghost table is identical in structure to the original table (including indexes) and we expect data to be completely identical. We use `md5sum` on the entire dataset to confirm the test result.
- When adding/dropping columns, you will want to use the explicit list of shared columns before/after migration. This list is printed by `gh-ost` at the beginning of the migration.

### Cleanup

It's your job to:
- Drop the ghost table (at your leisure, you should be aware that a `DROP` can be a lengthy operation)
- Start replication back (via `START SLAVE`)

### Examples

Simple:
```shell
$ gh-ost --host=myhost.com --conf=/etc/gh-ost.cnf --database=test --table=sample_table --alter="engine=innodb" --chunk-size=2000 --max-load=Threads_connected=20 --initially-drop-ghost-table --initially-drop-old-table --test-on-replica --verbose --execute
```

Elaborate:
```shell
$ gh-ost --host=myhost.com --conf=/etc/gh-ost.cnf --database=test --table=sample_table --alter="engine=innodb" --chunk-size=2000 --max-load=Threads_connected=20 --switch-to-rbr --initially-drop-ghost-table --initially-drop-old-table --test-on-replica --postpone-cut-over-flag-file=/tmp/ghost-postpone.flag --exact-rowcount --concurrent-rowcount --allow-nullable-unique-key --verbose --execute
```
- Count exact number of rows (makes ETA estimation very good). This goes at the expense of paying the time for issuing a `SELECT COUNT(*)` on your table. We use this lovingly.
- Automatically switch to `RBR` if replica is configured as `SBR`. See also: [migrating with SBR](migrating-with-sbr.md)
- allow iterating on a `UNIQUE KEY` that has `NULL`able columns (at your own risk)

### Further notes

Do not confuse `--test-on-replica` with `--migrate-on-replica`; the latter performs the migration and _keeps it that way_ (does not revert the table swap nor stops replication)

As part of testing on replica, `gh-ost` issues a `STOP SLAVE`. This requires the `SUPER` privilege.
See related discussion on https://github.com/github/gh-ost/issues/162
