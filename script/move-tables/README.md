### Setup

Setup the multi-cluster topology and seed the data. This always seeds the same
canonical **three** tables on the source — `gh_ost_test`, `gh_ost_test_other`,
and `gh_ost_test_third` (see `localtests/move-tables/multiple-three/create.sql`) — into
the `test` database. You then choose how many of them to move via `--move-tables`,
so `setup`/`reset`/`teardown` behave identically regardless of which scenario you
run.
```bash
script/move-tables/setup
```

Verify data is present in the source cluster.
```bash
script/move-tables/mysql-source-primary -D test -e "SELECT * FROM gh_ost_test; SELECT * FROM gh_ost_test_other; SELECT * FROM gh_ost_test_third;"
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

Run gh-ost to move tables. Pick **one**, **two**, or **three** tables by changing
the `--move-tables` list — everything else stays the same:
```bash
# one table
./bin/gh-ost --move-tables=gh_ost_test --host=localhost --port=3308 --user root --password opensesame --database=test --target-host=localhost --target-port=3309 --target-user root --target-password opensesame --target-database=test --postpone-cut-over-flag-file=/tmp/ghost-move-tables.postpone.flag --execute --verbose --checkpoint --checkpoint-seconds 10 --initially-drop-socket-file

# two tables
./bin/gh-ost --move-tables=gh_ost_test,gh_ost_test_other ... (same flags)

# three tables
./bin/gh-ost --move-tables=gh_ost_test,gh_ost_test_other,gh_ost_test_third ... (same flags)
```

You'll see per-table row-copy progress in the status output, with all moved
tables advancing concurrently.

Start continuous inserts against the source. No arguments required: it detects
which of the three fixtures exist and writes to all of them.
```bash
script/move-tables/insert-source-primary-loop
```

Check the target - it should have the initial data from the source and should be receiving the new data.
```bash
script/move-tables/mysql-target-primary -D test -e "SELECT * FROM gh_ost_test;"
```

Remove the cutover flag file.
```bash
rm /tmp/ghost-move-tables.postpone.flag
```

The continuous inserts stop because the moved tables are renamed. When you move
multiple tables, they are all renamed together in a single atomic `RENAME TABLE`.

Check the source - each moved table has been renamed to its `_<table>_del`
rollback handle (only the tables you moved are renamed):
```bash
script/move-tables/mysql-source-primary -D test -e "SELECT * FROM _gh_ost_test_del;"
```

Check the target has the same set of data.
```bash
script/move-tables/mysql-target-primary -D test -e "SELECT * FROM gh_ost_test;"
```

### Resetting between runs

Drop and re-seed all three source tables (and clean up the moved target tables +
checkpoint table) so you can run again without a full teardown. It works the same
no matter how many tables you just moved:
```bash
script/move-tables/reset
```

### Teardown

Remove the docker containers:
```bash
script/move-tables/teardown
```

### CI integration tests

The same fixtures back the CI integration tests, run via
`localtests/move-tables-test.sh [filter]`. Each test directory under
`localtests/move-tables/` is self-contained (its own `create.sql` + `tables.txt`,
plus an optional `on_test.sh` for concurrent workload or `test.sh` for a fully
custom scenario):

- `single` — moves 1 table, idle source
- `single-concurrent-writes` — moves 1 table with sustained DML during copy
- `single-with-hooks` — moves 1 table and asserts the hook env vars
- `multiple-two` — moves 2 tables, idle source
- `multiple-three` — moves 3 tables, idle source
- `multiple-three-concurrent-writes` — moves 3 tables with sustained DML on all three
- `atomic-multi-table-cutover` — moves 2 tables while committing cross-table
  transactions up to cutover; asserts the atomic multi-table RENAME leaves no
  orphaned rows across the pair
- `resume-panic-on-row-copy`, `resume-panic-before-drain-complete`,
  `resume-panic-before-on-success-hook` — crash mid-run via a failpoint, then
  `--resume` to completion

Run a single scenario by name, e.g.:
```bash
localtests/move-tables-test.sh multiple-three
```