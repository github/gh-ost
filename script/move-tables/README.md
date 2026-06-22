### Setup

Setup the multi-cluster topology and seed the data. This seeds two tables on the
source — `gh_ost_test` and `gh_ost_test_other` (see
`localtests/move-tables/multi/create.sql`) — into the `test` database.
```bash
script/move-tables/setup
```

Verify data is present in the source cluster.
```bash
script/move-tables/mysql-source-primary -D test -e "SELECT * FROM gh_ost_test; SELECT * FROM gh_ost_test_other;"
```

Verify the empty database is present in the target cluster.
```bash
script/move-tables/mysql-target-primary -D test -e "SHOW TABLES;"
```

### Testing `gh-ost`

Checkout your branch of `github/gh-ost` and build the binaries:
```bash
script/build --cli
```

Run gh-ost to move both tables in a single atomic cutover:
```bash
./script/build --cli; ./bin/gh-ost --move-tables=gh_ost_test,gh_ost_test_other --host=localhost --port=3308 --user root --password opensesame --database=test --target-host=localhost --target-port=3309 --target-user root --target-password opensesame --target-database=test --postpone-cut-over-flag-file=/tmp/ghost-move-tables.postpone.flag --execute --verbose --checkpoint --checkpoint-seconds 10 --initially-drop-socket-file
```

You'll see per-table row-copy progress in the status output, with both tables
advancing concurrently.

Start continuous inserts against the source. This writes to both tables.
```bash
script/move-tables/insert-source-primary-loop
```

Check the target - it should have the initial data from the source and should be receiving the new data.
```bash
script/move-tables/mysql-target-primary -D test -e "SELECT * FROM gh_ost_test; SELECT * FROM gh_ost_test_other;"
```

Remove the cutover flag file.
```bash
rm /tmp/ghost-move-tables.postpone.flag
```

You'll see the continuous inserts will stop because of the table rename. Both
tables are renamed together in a single atomic `RENAME TABLE`.

Check the source - both tables have been renamed to their `_..._del` rollback handles.
```bash
script/move-tables/mysql-source-primary -D test -e "SELECT * FROM _gh_ost_test_del; SELECT * FROM _gh_ost_test_other_del;"
```

Check the target has the same set of data.
```bash
script/move-tables/mysql-target-primary -D test -e "SELECT * FROM gh_ost_test; SELECT * FROM gh_ost_test_other;"
```

### Resetting between runs

Drop and re-seed both source tables (and clean up the target tables + checkpoint
table) so you can run again without a full teardown:
```bash
script/move-tables/reset
```

### Teardown

Remove the docker containers:
```bash
script/move-tables/teardown
```