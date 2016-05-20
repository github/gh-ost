# Testing on replica

`gh-ost`'s design allows for trusted and reliable tests of the migration without compromising production data integrity.

Test on replica if you:
- Are unsure of `gh-ost`, have not gained confidence into its workings
- Just want to experiment with a real migration without affecting production (maybe measure migration time?)
- Wish to observe data change impact

## What testing on replica means

`gh-ost` will make all changes
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
- When it is satisfied, it will issue a `STOP SLAVE IO_THREAD`, effectively stopping replication
- Will finalize last few statements
- Will terminate. No table swap takes place. No table is dropped.

You are now left with the original table **and** the ghost table. They _should_ be identical.

You now have the time to verify the tool works correctly. You may checksum the entire table data if you like.
- e.g.
  `mysql -e 'select * from mydb.mytable order by id' | md5sum`
  `mysql -e 'select * from mydb._mytable_gst order by id' | md5sum`
- or of course only select the shared columns before/after the migration
- We use the trivial `engine=innodb` for `alter` when testing. This way the resulting ghost table is identical in structure to the original table (including indexes) and we expect data to be completely identical. We use `md5sum` on the entire dataset to confirm the test result.


### Cleanup

It's your job to:
- Drop the ghost table (at your leisure, you should be aware that a `DROP` can be a lengthy operation)
- Start replication back (via `START SLAVE`)
