# Reverting Migrations

`gh-ost` can attempt to revert a previously completed migration if the follow conditions are met:
- The first `gh-ost` process was invoked with `--checkpoint`
- The checkpoint table (name ends with `_ghk`) still exists
- The binlogs from the time of the migration's cut-over still exist on the replica gh-ost is inspecting (specified by `--host`)

To revert, find the name of the "old" table from the original migration e.g. `_mytable_del`. Then invoke `gh-ost` with the same arguments and the flags `--revert` and `--old-table="_mytable_del"`.
gh-ost will read the binlog coordinates of the original cut-over from the checkpoint table and bring the old table up to date. Then it performs another cut-over to complete the reversion.
Note that the checkpoint table (name ends with _ghk) will not be automatically dropped unless `--ok-to-drop-table` is provided.

> [!WARNING]
> It is recommended use `--checkpoint` with `--gtid` enabled so that checkpoint binlog coordinates store GTID sets rather than file positions. In that case, `gh-ost` can revert using a different replica than it originally attached to.

### ❗ Note ❗
Reverting is roughly equivalent to applying the "reverse" migration. _Before attempting to revert you should determine if the reverse migration is possible and does not involve any unacceptable data loss._

For example: if the original migration drops a `NOT NULL` column that has no `DEFAULT` then the reverse migration adds the column. In this case, the reverse migration is impossible if rows were added after the original cut-over and the revert will fail.
Another example: if the original migration modifies a `VARCHAR(32)` column to `VARCHAR(64)`, the reverse migration truncates the `VARCHAR(64)` column to `VARCHAR(32)`. If values were inserted with length > 32 after the cut-over then the revert will fail.


## Example
The migration starts with a `gh-ost` invocation such as:
```shell
gh-ost \
--chunk-size=100 \
--host=replica1.company.com \
--database="mydb" \
--table="mytable" \
--alter="drop key idx1"
--gtid \
--checkpoint \
--checkpoint-seconds=60 \
--execute
```

In this example `gh-ost` writes a cut-over checkpoint to `_mytable_ghk` after the cut-over is successful. The original table is renamed to `_mytable_del`.

Suppose that dropping the index causes problems, the migration can be revert with:
```shell
# revert migration
gh-ost \
--chunk-size=100 \
--host=replica1.company.com \
--database="mydb" \
--table="mytable" \
--old-table="_mytable_del"
--gtid \
--checkpoint \
--checkpoint-seconds=60 \
--revert \
--execute
```

gh-ost then reconnects at the binlog coordinates stored in the cut-over checkpoint and applies DMLs until the old table is up-to-date.
Note that the "reverse" migration is `ADD KEY idx(...)` so there is no potential data loss to consider in this case.
