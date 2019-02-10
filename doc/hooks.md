# Hooks

`gh-ost` supports _hooks_: external processes which `gh-ost` executes at particular points of interest.

Use cases include:

- You wish to be notified by mail when a migration completes/fails
- You wish to be notified when `gh-ost` postpones cut-over (at your demand), thus ready to complete (at your leisure)
- RDS users who wish to `--test-on-replica`, but who cannot have `gh-ost` issue a `STOP SLAVE`, would use a hook to command RDS to stop replication
- Send a status message to your chatops every hour
- Perform cleanup on the _ghost_ table (drop/rename/nibble) once migration completes
- etc.

`gh-ost` defines certain points of interest (event types), and executes hooks at those points.

Notes:

- You may have more than one hook per event type.
- `gh-ost` will invoke relevant hooks _sequentially_ and _synchronously_
  - thus, you would generally like the hooks to execute as fast as possible, or otherwise issue tasks in the background
- A hook returning with error code will propagate the error in `gh-ost`. Thus, you are able to force `gh-ost` to fail migration on your conditions.
  - Make sure to only return an error code when you do indeed wish to fail the rest of the migration

### Creating hooks

All hooks are expected to reside in a single directory. This directory is indicated by `--hooks-path`. When not provided, no hooks are executed.

`gh-ost` will dynamically search for hooks in said directory. You may add and remove hooks to/from this directory as `gh-ost` makes progress (though likely you don't want to). Hook files are expected to be executable processes.

In an effort to simplify code and to standardize usage, `gh-ost` expects hooks in explicit naming conventions. As an example, the `onStartup` hook expects processes named `gh-ost-on-startup*`. It will match and accept files named:

- `gh-ost-on-startup`
- `gh-ost-on-startup--send-notification-mail`
- `gh-ost-on-startup12345`
- etc.

The full list of supported hooks is best found in code: [hooks.go](https://github.com/github/gh-ost/blob/master/go/logic/hooks.go). Documentation will always be a bit behind. At this time, though, the following are recognized:

- `gh-ost-on-startup`
- `gh-ost-on-validated`
- `gh-ost-on-rowcount-complete`
- `gh-ost-on-before-row-copy`
- `gh-ost-on-status`
- `gh-ost-on-interactive-command`
- `gh-ost-on-row-copy-complete`
- `gh-ost-on-stop-replication`
- `gh-ost-on-start-replication`
- `gh-ost-on-begin-postponed`
- `gh-ost-on-before-cut-over`
- `gh-ost-on-success`
- `gh-ost-on-failure`

### Context

`gh-ost` will set environment variables per hook invocation. Hooks are then able to read those variables, indicating schema name, table name, `alter` statement, migrated host name etc. Some variables are available on all hooks, and some are available on relevant hooks.

The following variables are available on all hooks:

- `GH_OST_DATABASE_NAME`
- `GH_OST_TABLE_NAME`
- `GH_OST_GHOST_TABLE_NAME`
- `GH_OST_OLD_TABLE_NAME` - the name the original table will be renamed to at the end of operation
- `GH_OST_DDL`
- `GH_OST_ELAPSED_SECONDS` - total runtime
- `GH_OST_ELAPSED_COPY_SECONDS` - row-copy time (excluding startup, row-count and postpone time)
- `GH_OST_ESTIMATED_ROWS` - estimated total rows in table
- `GH_OST_COPIED_ROWS` - number of rows copied by `gh-ost`
- `GH_OST_MIGRATED_HOST`
- `GH_OST_INSPECTED_HOST`
- `GH_OST_EXECUTING_HOST`
- `GH_OST_HOOKS_HINT` - copy of `--hooks-hint` value
- `GH_OST_HOOKS_HINT_OWNER` - copy of `--hooks-hint-owner` value
- `GH_OST_HOOKS_HINT_TOKEN` - copy of `--hooks-hint-token` value
- `GH_OST_DRY_RUN` - whether or not the `gh-ost` run is a dry run

The following variable are available on particular hooks:

- `GH_OST_COMMAND` is only available in `gh-ost-on-interactive-command`
- `GH_OST_STATUS` is only available in `gh-ost-on-status`

### Examples

See [sample hooks](https://github.com/github/gh-ost/tree/master/resources/hooks-sample), as `bash` implementation samples.
