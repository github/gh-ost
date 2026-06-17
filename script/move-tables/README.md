### Setup

Setup the multi-cluster topology and seed the data
```bash
script/move-tables/setup
```

Verify data is present in the source cluster.
```bash
script/move-tables/mysql-source-primary -D gh_ost_test_db -e "SELECT * FROM gh_ost_test;"
```

Verify the empty database is present in the target cluster.
```bash
script/move-tables/mysql-target-primary -D gh_ost_test_db -e "SHOW TABLES;"
```

### Testing `gh-ost`

Checkout your branch of `github/gh-ost` and build the binaries:
```bash
script/build --cli
```

Run gh-ost to move tables:
```bash
./script/build --cli; ./bin/gh-ost --move-tables=gh_ost_test --host=localhost --port=3307 --user root --password opensesame --database=gh_ost_test_db --target-host=localhost --target-port=3309 --target-user root --target-password opensesame --target-database=gh_ost_test_db --postpone-cut-over-flag-file=/tmp/ghost-move-tables.postpone.flag --execute --verbose --checkpoint --checkpoint-seconds 10
```

Note: replicas in this local topology are configured with `read_only=ON` and
`super_read_only=ON`. If you point `--host` at `mysql-source-replica` (3308),
the cutover `RENAME TABLE` step will fail by design. Use source primary (3307)
as the inspected host when you want cutover to rename on source.

Start continuous inserts against the source.
```bash
script/move-tables/insert-source-primary-loop
```

Check the target - it should have the initial data from the source and should be receiving the new data.
```bash
script/move-tables/mysql-target-primary -D gh_ost_test_db -e "SELECT * FROM gh_ost_test;"
```

Remove the cutover flag file.
```bash
rm /tmp/ghost-move-tables.postpone.flag
```

You'll see the continous inserts will stop because of the table rename.

Check the source - table has been renamed.
```bash
script/move-tables/mysql-source-primary -D gh_ost_test_db -e "SELECT * FROM _gh_ost_test_del;"
```

Check the target has the same set of data.
```bash
script/move-tables/mysql-target-primary -D gh_ost_test_db -e "SELECT * FROM gh_ost_test;"
```