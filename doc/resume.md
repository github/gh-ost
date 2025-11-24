# Resuming Migrations

`gh-ost` can attempt to resume an interrupted migration from a checkpoint if the following conditions are met:
- The first `gh-ost` process was invoked with `--checkpoint`
- The first `gh-ost` process had at least one successful checkpoint
- The binlogs from the last checkpoint's binlog coordinates still exist on the replica gh-ost is inspecting (specified by `--host`)
- The checkpoint table (name ends with `_ghk`) still exists

To resume, invoke `gh-ost` again with the same arguments with the `--resume` flag.

> [!WARNING]
> It is recommended use `--checkpoint` with `--gtid` enabled so that checkpoint binlog coordinates store GTID sets rather than file positions. In that case, `gh-ost` can resume using a different replica than it originally attached to.

## Example
The migration starts with a `gh-ost` invocation such as:
```shell
gh-ost \
--chunk-size=100 \
--host=replica1.company.com \
--database="mydb" \
--table="mytable" \
--alter="add column mycol varchar(20)"
--gtid \
--checkpoint \
--checkpoint-seconds=60 \
--execute
```

In this example `gh-ost` writes a checkpoint to a table `_mytable_ghk` every 60 seconds. After `gh-ost` is interrupted/killed, the migration can be resumed with:
```shell
# resume migration
gh-ost \
--chunk-size=100
--host=replica1.company.com \
--database="mydb" \
--table="mytable" \
--alter="add column mycol varchar(20)"
--gtid \
--resume \
--execute
```

`gh-ost` then reconnects at the binlog coordinates of the last checkpoint and resumes copying rows at the chunk specified by the checkpoint. The data integrity of the ghost table is preserved because `gh-ost` applies row DMLs and copies row in an idempotent way.
