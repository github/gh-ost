# Resuming Migrations

`gh-ost` can attempt to resume an interrupted migration from a checkpoint if the following conditions are met:
- The first `gh-ost` process was invoked with `--checkpoint`
- The first `gh-ost` process had at least one successful checkpoint
- The binlogs from the last checkpoint's binlog coordinates still exist on the replica gh-ost is inspecting (specified by `--host`)
- The checkpoint table (name ends with `_ghk`) still exists

To resume, invoke `gh-ost` again with the same arguments with the `--resume` flag.

> [!NOTE]
> It is recommended use `--checkpoint` with `--gtid` enabled so that checkpoint binlog coordinates store GTID sets rather than file positions. In that case, `gh-ost` can resume using a different replica than it originally attached to.

## Example
The migration starts with a `gh-ost` invocation such as:
```shell
gh-ost \
--chunk-size=100 \
--host=replica1.company.com \
--database="mydb" \
--table="mytable" \
--alter="add column mycol varchar(20)" \
--gtid \
--checkpoint \
--checkpoint-seconds=60 \
--execute
```

In this example `gh-ost` writes a checkpoint to a table `_mytable_ghk` every 60 seconds. After `gh-ost` is interrupted/killed, the migration can be resumed with:
```shell
# resume migration
gh-ost \
--chunk-size=100 \
--host=replica1.company.com \
--database="mydb" \
--table="mytable" \
--alter="add column mycol varchar(20)" \
--gtid \
--resume \
--execute
```

`gh-ost` then reconnects at the binlog coordinates of the last checkpoint and resumes copying rows at the chunk specified by the checkpoint. The data integrity of the ghost table is preserved because `gh-ost` applies row DMLs and copies row in an idempotent way.

> [!WARNING]
> A successful resume does not prove data completeness. A checkpoint can be written while a source transaction is only partially applied, so `--resume` may skip the rest of that transaction ([#1781](https://github.com/github/gh-ost/issues/1781)). If the previous run may have stopped mid-transaction, verify the affected rows before cut-over.

## Resuming `--move-tables` runs

[`--move-tables`](move-tables.md) enables `--checkpoint` and `--gtid` automatically in the CLI, storing checkpoints in `_gho_<token>_ghk` in the **target** database.

Before rerunning the original command with `--resume --execute`, confirm:

- The previous `gh-ost` process and all hooks have stopped.
- Target writes were never enabled.
- Target tables, the checkpoint table, and required source binlogs remain intact.
- The moved table set, source database, and target host/database are unchanged.

Preflight validates source tables and target cut-over checkpoints before startup hooks or streaming:

- An interrupted row copy resumes copying.
- A pre-drain cut-over resumes draining, then runs `gh-ost-on-success`, without the postpone gate or `gh-ost-on-before-cut-over`. Prepare routing first.
- A completed drain or source/checkpoint mismatch causes resume to fail closed.

See [move-tables recovery](move-tables.md#decide-whether-to-resume) for the decision table and alternatives.
