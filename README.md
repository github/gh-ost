# gh-ost

#### GitHub's online schema migration for MySQL

`gh-ost` allows for online schema migrations in MySQL which are:
- Triggerless
- Testable
- Pausable
- Operations-friendly

## How?

WORK IN PROGRESS

Please meanwhile refer to the [docs](doc) for more information. No, really, go to the [docs](doc).

- [Why triggerless](doc/why-triggerless.md)
- [Triggerless design](doc/triggerless-design.md)
- [Cut over phase](doc/cut-over.md)
- [Testing on replica](doc/testing-on-replica.md)
- [Throttle](doc/throttle.md)
- [Operational perks](doc/perks.md)
- [Migrating with Statement Based Replication](doc/migrating-with-sbr.md)
- [Understanding output](doc/understanding-output.md)
- [Interactive commands](doc/interactive-commands.md)
- [Command line flags](doc/command-line-flags.md)

## Usage

#### Where to execute

The recommended way of executing `gh-ost` is to have it connect to a _replica_, as opposed to having it connect to the master. `gh-ost` will crawl its way up the replication chain to figure out who the master is.

By connecting to a replica, `gh-ost` sets up a self-throttling mechanism; feels more comfortable in querying `information_schema` tables; and more. Connecting `gh-ost` to a replica is also the trick to make it work even if your master is configured with `statement based replication`, as `gh-ost` is able to manipulate the replica to rewrite logs in `row based replication`. See [Migrating with Statement Based Replication](migrating-with-sbr.md).

The replica would have to use binary logs and be configured with `log_slave_updates`.

It is still OK to connect `gh-ost` directly on master; you will need to confirm this by providing `--allow-on-master`. The master would have to be using `row based replication`.

`gh-ost` itself may be executed from anywhere. It connects via `tcp` and it does not have to be executed from a `MySQL` box. However, do note it generates a lot of traffic, as it connects as a replica and pulls binary log data.

#### Testing on replica

Newcomer? We think you would enjoy building trust with this tool. You can ask `gh-ost` to simulate a migration on a replica -- this will not affect data on master and will not actually do a complete migration. It will operate on a replica, and end up with two tables: the original (untouched), and the migrated. You will have your chance to compare the two and verify the tool works to your satisfaction.

```
gh-ost --conf=.my.cnf --database=mydb --table=mytable --verbose --alter="engine=innodb" --execute --initially-drop-ghost-table --initially-drop-old-table -max-load=Threads_connected=30 --switch-to-rbr --chunk-size=2500 --exact-rowcount --test-on-replica --verbose
```
Please read more on [testing on replica](testing-on-replica.md)

#### Executing on master

```
gh-ost --conf=.my.cnf --database=mydb --table=mytable --verbose --alter="engine=innodb" --execute --initially-drop-ghost-table --initially-drop-old-table -max-load=Threads_connected=30 --switch-to-rbr --chunk-size=2500 --exact-rowcount --verbose
```

Note: "executing on master" does not mean you need to _connect_ to the master. `gh-ost` is happy if you connect to a replica; it then figures out the identity of the master and makes the connection itself.

#### Notable parameters

Run `gh-ost --help` to get full list of parameters. We like the following:

- `--conf=/path/to/my.cnf`: file where credentials are specified. Should be in (or contain) the following format:

  ```
[client]
user=gromit
password=123456
  ```

- `--user`, `--password`: alternatively, supply these as arguments

- `--host`, `--port`: where to connect to. `gh-ost` prefers to connect to a replica, see above.

- `--exact-rowcount`: actually `select count(*)` from your table prior to migration, and heuristically maintain the updating table size while migrating. This makes for quite accurate assumption on progress. When `gh-ost` says it's `99.8%` done, it really there or very closely there.

- `--execute`: without this parameter, migration is a _noop_: testing table creation and validity of migration, but not touching data.

- `--initially-drop-ghost-table`, `--initially-drop-old-table`: `gh-ost` maintains two tables while migrating: the _ghost_ table (which is synced from your original table and finally replaces it) and a changelog table, which is used internally for bookkeeping. By default, it panics and aborts if it sees those tables upon startup. Provide these two params to let `gh-ost` know it's OK to drop them beforehand.

  We think `gh-ost` should not take chances or make assumptions about the user's tables. Dropping tables can be a dangerous, locking operation. We let the user explicitly approve such operations.

- `--test-on-replica`: `gh-ost` can be tested on a replica, without actually modifying master data. We use this for testing, and we suspect new users of this tool would enjoy checking it out, building trust in this tool, before actually applying it on production masters. Read more on [testing on replica](testing-on-replica.md).

## What's in a name?

Originally this was named `gh-osc`: GitHub Online Schema Change, in the likes of [Facebook online schema change](https://www.facebook.com/notes/mysql-at-facebook/online-schema-change-for-mysql/430801045932/) and [pt-online-schema-change](https://www.percona.com/doc/percona-toolkit/2.2/pt-online-schema-change.html).

But then a rare genetic mutation happened, and the `s` transformed into `t`. And that sent us down the path of trying to figure out a new acronym. Right now, `gh-ost` (pronounce: _Ghost_), stands for:
- GitHub Online Schema Translator/Transformer/Transfigurator

## Authors

`gh-ost` is designed, authored, reviewed and tested by the database infrastructure team at GitHub:
- [@jonahberquist](https://github.com/jonahberquist)
- [@ggunson](https://github.com/ggunson)
- [@tomkrouper](https://github.com/tomkrouper)
- [@shlomi-noach](https://github.com/shlomi-noach)
