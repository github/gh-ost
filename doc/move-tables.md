# Moving Tables Across Clusters

`gh-ost`'s `--move-tables` mode copies tables to another cluster without a schema change, replays ongoing source writes, and waits for you to initiate cut-over.

This guide assumes familiarity with normal online schema changes (OSC). See the [design doc](move-tables-design.md) for the architecture and correctness argument, and the [flag reference](command-line-flags.md#move-tables) for individual options.

## What changes from normal OSC?

| Normal OSC | Move-tables |
| --- | --- |
| One table, with a schema change | One or more tables, without a schema change; use `--move-tables` instead of `--table` and `--alter` |
| Rows are copied by `INSERT ... SELECT` on the source primary | Rows are read in chunks from `--host`, preferably a source replica, and written to `--target-host` |
| Writes go to a ghost table on the same cluster | Writes go directly to the same table names on `--target-host`, optionally in a different database |
| Cut-over swaps the original and ghost tables | Cut-over renames the source tables to `_<table>_del`, then drains remaining changes to the target |
| Application connections continue using the same cluster | You must switch application traffic and enable target writes |
| `--test-on-replica` and `--revert` are available | Neither is supported; test on disposable tables and plan recovery before starting |

> [!IMPORTANT]
> **`gh-ost` does not route application traffic, fence application writes, checksum data, or roll back automatically.** Keep target application writes disabled until cut-over completes. Unlike an in-place OSC swap, source table names become unavailable during cut-over and remain unavailable if the move fails after renaming them.

## Before you start

- Use the same database flavor across the source primary, source read host, and target primary: MySQL/Percona 8.0+ or MariaDB 10.5+. On MariaDB < 10.6, a crash during multi-table rename can leave a partial rename.
- Point `--host` at a source replica with binary logging enabled, `binlog_format=ROW`, `binlog_row_image=FULL`, and `log_slave_updates=ON`. Enable GTIDs on MySQL (`gtid_mode=ON`, `enforce_gtid_consistency=ON`); MariaDB records GTIDs when binary logging is enabled.
- The source tables must exist with identical schemas on the source primary and read replica. Only `BASE TABLE` is supported. The usual [key, foreign-key, and trigger limitations](requirements-and-limitations.md#limitations) still apply.
- Point `--target-host` at a writable target primary. The target database must already exist, with none of the moved table names present. Source `_<table>_del` names must also be available for cut-over.
- Keep target application writes disabled, avoid external source DDL during the move, and pause automated cleanup of source `_<table>_del` tables and target checkpoint tables.
- Ensure the machine running `gh-ost` can reach all three roles below. Allow only one move at a time per source primary; the source lock does not coordinate operations on the target.

### Connections and credentials

| Role | Credentials and required access |
| --- | --- |
| Source read host (`--host`) | Source credentials from `--user` / `--password`, `--ask-pass`, or `--conf`. Requires the [standard gh-ost grants](requirements-and-limitations.md#requirements). |
| Source primary | Discovered by traversing the source replication topology, using source credentials by default. It must allow metadata inspection, the advisory lock, cut-over `RENAME TABLE`, GTID capture, and optional deletion-table cleanup. |
| Target primary (`--target-host`) | Defaults to source credentials; override with `--target-user` and `--target-password`. Requires schema inspection and `CREATE`, `DROP`, `SELECT`, `INSERT`, `UPDATE`, and `DELETE` on the target database. |

On MariaDB, ensure source credentials can read replica status; this requires `SLAVE MONITOR` or `SUPER` on current releases.

Use [`--assume-master-host`](command-line-flags.md#assume-master-host) to specify the source primary explicitly; [`--master-user`](command-line-flags.md#master-user) and [`--master-password`](command-line-flags.md#master-password) can then supply different credentials for it. Target-replica lag checks use the target credentials and need access to replication status on those replicas.

For a lab without a source replica, point `--host` at the source primary and add [`--allow-on-source-primary`](command-line-flags.md#allow-on-source-primary), not `--allow-on-master`. This puts the full copy load on the primary; prefer a replica for production.

## Start a move

### 1. Run a noop check

Verify `ROW` / `FULL` binlogging on the source replica, then run without `--execute`:

```shell
gh-ost \
  --move-tables="orders" \
  --host=source-replica.example.com --database="app_db" \
  --user="gh-ost" --ask-pass --assume-rbr \
  --target-host=target-primary.example.com \
  --postpone-cut-over-flag-file=/tmp/gh-ost.move-tables.postpone.flag \
  --verbose
```

`gh-ost` validates connections and schemas, creates the target and `_gho_<token>_ghk` checkpoint tables, then drops them and exits 0. A noop is **not read-only**, and it skips row copy, cut-over, and `gh-ost-on-success`; other hooks may run. A failed noop may leave target tables behind; see [abandoning before the source rename](#abandon-before-the-source-rename).

`--assume-rbr` skips restarting source replication to apply binlog settings; it does not configure them.

### 2. Copy and replay

Rerun the same command with `--execute`, and keep it running. `gh-ost` creates the postpone flag file, copies rows, and replays ongoing source changes to the target.

Copying is done when the status shows `State: postponing cut-over` with every table `complete` (see [status output](understanding-output.md#move-tables-progress)). Changes keep replaying until you start cut-over. To spot-check replay, update a row on the source primary and read it on the target primary. When ready, continue to [cut-over](#cut-over).

### Options

| Flag | Use |
| --- | --- |
| `--move-tables="orders,order_items"` | Move several tables together; cut-over waits for all of them to finish copying. |
| `--target-database` | Write to a different existing database on the target. |
| `--max-load`, `--throttle-query` | Throttle on the **target primary**'s load. |
| `--critical-load` | Abort on the **target primary**'s load. |
| `--throttle-control-replicas`, `--max-lag-millis` | Throttle on **target replica** lag. |
| `--hooks-path` | Integrate the application, for example to switch traffic in `gh-ost-on-success`. Review existing OSC hooks first; see [move-tables hooks](hooks.md#--move-tables-mode). |
| `--checkpoint-seconds` | Checkpoint interval (default `300`); resume needs at least one checkpoint. |

Throttling protects the target only. Monitor source load and replication separately; see [throttling](throttle.md#move-tables-mode).

While cut-over is postponed, you may route reads to the target if the application tolerates stale reads and partially applied transactions. Keep writes on the source.

## Cut-over

### Before releasing the postpone gate

Keep the postpone gate closed until you are ready. Removing its file during copying allows cut-over as soon as copying finishes; `unpostpone` is accepted only while actively postponing cut-over.

1. Verify the source replica is caught up and healthy; `WriterLag: 0` and an empty backlog do not prove this.
2. Wait for `State: postponing cut-over` with all tables marked `complete`, and check that `WriterLag` and `Backlog` are near zero.
3. Ensure gh-ost is not throttled, keep target application writes disabled, and prepare routing for the source outage.
4. If reads will use target replicas, plan to verify they have replayed the target primary's writes. `GH_OST_DRAIN_GTID` is a **source** stream boundary, not the target's replication position.

The drain timeout ([`--cut-over-lock-timeout-seconds`](command-line-flags.md#cut-over-lock-timeout-seconds), default `60s`) is **not an upper bound on application outage**: rename lock waits, hooks, and routing add time, and a failed drain leaves the source tables renamed.

### Release the gate

Delete the flag file, or send `unpostpone` through the [interactive interface](interactive-commands.md):

```shell
rm /tmp/gh-ost.move-tables.postpone.flag
```

`gh-ost` runs `gh-ost-on-before-cut-over`, renames the source tables to `_<table>_del`, drains remaining source events to the target primary, records drain completion, runs `gh-ost-on-success` with `GH_OST_DRAIN_GTID` set, and performs final cleanup.

### Switch application traffic

**Enable target writes and switch traffic in `gh-ost-on-success`, or manually after the `--execute` run exits cleanly.** When switching manually, omit `--ok-to-drop-table` and clean up later so table drops do not extend the outage.

Confirm the source primary has `_orders_del`, the target serves `orders`, and application writes to the target succeed.

### If cut-over does not exit cleanly

- **Drain timeout:** the source tables stay renamed. See [recovery](#cleanup-and-recovery).
- **`move-tables cutover completed but cleanup failed`:** the drain finished, and `gh-ost-on-success` may already have enabled target writes. Do not rerun or resume to get a clean exit; confirm state via [recovery](#cleanup-and-recovery) before finishing any traffic switch.

## Cleanup and recovery

By default, `gh-ost` retains source `_<table>_del` tables and the target `_gho_<token>_ghk` checkpoint table, and logs their `DROP TABLE` commands. [`--ok-to-drop-table`](command-line-flags.md#ok-to-drop-table) drops both after `gh-ost-on-success`; omit it while rollback remains an option.

> [!IMPORTANT]
> **Rollback is unsafe once target writes begin**, even with `_<table>_del` retained: target writes are not replicated back to the source. A non-zero exit does not prove cut-over did not happen, and a log line alone is not grounds to enable writes.

Before any recovery action, **stop the old `gh-ost` process and all hooks**, then inspect both clusters and hook logs. If target-write history is unknown, assess recovery manually rather than resuming or rolling back.

### Inspect source and checkpoint state

On the **source primary** (not a possibly lagging replica), check each moved table and its `_<table>_del` pair:

```sql
SELECT TABLE_NAME, TABLE_TYPE
FROM information_schema.tables
WHERE TABLE_SCHEMA = 'app_db'
  AND TABLE_NAME IN ('orders', '_orders_del');
```

Before rename, only the original name should exist; afterward, only `_<table>_del`. Both, neither, or a mix across tables is inconsistent.

On the **target primary**, check whether the moved tables and `_gho_<token>_ghk` exist; `<token>` is the run ID in the first log line (`Moving tables [...] (run <token>)`). If the checkpoint table exists, read its most significant row; cut-over markers outrank later copy checkpoints:

```sql
SELECT gh_ost_chk_id, gh_ost_chk_table_name, gh_ost_chk_coords,
       gh_ost_move_tables_cutover_started, gh_ost_is_cutover,
       gh_ost_move_tables_drain_gtid
FROM `app_db`.`_gho_<token>_ghk`
ORDER BY gh_ost_is_cutover DESC,
         gh_ost_move_tables_cutover_started DESC,
         gh_ost_chk_id DESC
LIMIT 1;
```

Markers are run-wide values alongside per-table copy state:

- `gh_ost_move_tables_cutover_started=0`, `gh_ost_is_cutover=0`: copy checkpoint. Compare source table state; rename may have happened without a cut-over checkpoint.
- `gh_ost_move_tables_cutover_started=1`, `gh_ost_is_cutover=0`, non-empty `gh_ost_move_tables_drain_gtid`: pre-drain checkpoint.
- `gh_ost_is_cutover=1`: drain completed; hook/routing status unknown.
- No table or rows: no recorded resume state. **Absence of a marker does not prove cut-over never happened**; `--ok-to-drop-table` cleanup can remove the table.

These queries do not replace resume preflight or hook/application checks. **Do not edit markers to force a resume.**

### Decide whether to resume

Use the first matching row. Resume requires usable checkpoint coordinates and retained source binlogs.

| Observed state | Next action |
| --- | --- |
| Target writes were enabled | Do not resume or rename source tables back. Recovery must account for target-side writes. |
| Completed-drain marker set, whether or not the success hook finished | `--resume` refuses. Confirm source/hook/application state, then finish pending traffic switching or assess manual recovery. |
| Rename outcome unknown, partial rename, or source/checkpoint mismatch | Recover manually; do not retry the rename blindly. |
| All source tables renamed; usable pre-drain checkpoint and target tables exist | Resume draining, or roll back manually. Prepare routing first: resume skips the postpone gate and `gh-ost-on-before-cut-over`, then runs `gh-ost-on-success` after draining. |
| Original source names intact; usable copy checkpoint and target tables exist | Resume copying, or abandon the move. |
| No usable checkpoint or required binlogs unavailable | Resume is unavailable. Abandon if original source names are intact; otherwise assess manual recovery. |

Rerun the original command with `--resume --execute`, preserving the table set, source database, and target host/database. Preflight validates source tables and target cut-over checkpoints before startup hooks or streaming. See [resuming migrations](resume.md#resuming---move-tables-runs).

### Abandon before the source rename

If the original source names are intact and target writes were never enabled, route any target reads back to the source. Then drop **only** the target tables this attempt created, including the `_gho_<token>_ghk` checkpoint table (also after a failed noop), never pre-existing ones. A leftover checkpoint table blocks the next attempt for the same table set.

### Roll back after the source rename

**Only if target writes were never enabled:** rename the `_<table>_del` tables back on the source primary, let source replicas catch up, route any target reads back to the source, and drop this attempt's target tables:

```sql
-- Source primary; for multiple tables, rename them all in one statement
RENAME TABLE `app_db`.`_orders_del` TO `app_db`.`orders`;
-- Target primary; substitute the run ID for <token>
DROP TABLE `app_db`.`orders`, `app_db`.`_gho_<token>_ghk`;
```
