### Setup

Setup the multi-cluster topology and seed the data
```bash
script/move-tables/setup
```

Verify data is present in the source cluster.
```bash
script/move-tables/mysql-source-primary -D test -e "SELECT * FROM gh_ost_test;"
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

Run gh-ost to move tables:
```bash
./script/build --cli; ./bin/gh-ost --move-tables=gh_ost_test --host=localhost --port=3308 --user root --password opensesame --database=test --target-host=localhost --target-port=3309 --target-user root --target-password opensesame --target-database=test --postpone-cut-over-flag-file=/tmp/ghost-move-tables.postpone.flag --execute --verbose --checkpoint --checkpoint-seconds 10 --initially-drop-socket-file
```

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

You'll see the continuous inserts will stop because of the table rename.

Check the source - table has been renamed.
```bash
script/move-tables/mysql-source-primary -D gh_ost_test_db -e "SELECT * FROM _gh_ost_test_del;"
```

Check the target has the same set of data.
```bash
script/move-tables/mysql-target-primary -D gh_ost_test_db -e "SELECT * FROM gh_ost_test;"
```