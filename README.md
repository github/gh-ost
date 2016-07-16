# gh-ost

#### GitHub's online schema migration for MySQL

`gh-ost` allows for online schema migrations in MySQL which are:
- Triggerless
- Testable
- Pausable
- Operations-friendly

![gh-ost logo](doc/images/gh-ost-logo-light-160.png)


## How?

![gh-ost general flow](doc/images/gh-ost-general-flow.png)

WORK IN PROGRESS

Please meanwhile refer to the [docs](doc) for more information. No, really, go to the [docs](doc).

- [Why triggerless](doc/why-triggerless.md)
- [Triggerless design](doc/triggerless-design.md)
- [Throttle](doc/throttle.md)
- [Operational perks](doc/perks.md)
- [Understanding output](doc/understanding-output.md)
- [Interactive commands](doc/interactive-commands.md)
- [Command line flags](doc/command-line-flags.md)
- [Cut over phase](doc/cut-over.md)
- [Testing on replica](doc/testing-on-replica.md)
- [Migrating with Statement Based Replication](doc/migrating-with-sbr.md)
- [What if](doc/what-if.md)
- [Requirements & Limitations](doc/requirements-and-limitations.md)
- [Cheatsheet](doc/cheatsheet.md)

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
gh-ost --conf=.my.cnf --database=mydb --table=mytable --verbose --alter="engine=innodb" --execute --initially-drop-ghost-table --initially-drop-old-table -max-load=Threads_running=30 --switch-to-rbr --chunk-size=2500 --exact-rowcount --test-on-replica --verbose --postpone-cut-over-flag-file=/tmp/ghost.postpone.flag --throttle-flag-file=/tmp/ghost.throttle.flag
```
Please read more on [testing on replica](testing-on-replica.md)

#### Migrating a master table

```
gh-ost --conf=.my.cnf --database=mydb --table=mytable --verbose --alter="engine=innodb" --initially-drop-ghost-table --initially-drop-old-table --max-load=Threads_running=30 --switch-to-rbr --chunk-size=2500 --exact-rowcount --verbose --postpone-cut-over-flag-file=/tmp/ghost.postpone.flag --throttle-flag-file=/tmp/ghost.throttle.flag [--execute]
```

Note: in order to migrate a table on the master you don't need to _connect_ to the master. `gh-ost` is happy (and prefers) if you connect to a replica; it then figures out the identity of the master and makes the connection itself.

## What's in a name?

Originally this was named `gh-osc`: GitHub Online Schema Change, in the likes of [Facebook online schema change](https://www.facebook.com/notes/mysql-at-facebook/online-schema-change-for-mysql/430801045932/) and [pt-online-schema-change](https://www.percona.com/doc/percona-toolkit/2.2/pt-online-schema-change.html).

But then a rare genetic mutation happened, and the `s` transformed into `t`. And that sent us down the path of trying to figure out a new acronym. Right now, `gh-ost` (pronounce: _Ghost_), stands for:
- GitHub Online Schema Transmogrifier/Translator/Transformer/Transfigurator

Pronounce: _ghost_

## Authors

`gh-ost` is designed, authored, reviewed and tested by the database infrastructure team at GitHub:
- [@jonahberquist](https://github.com/jonahberquist)
- [@ggunson](https://github.com/ggunson)
- [@tomkrouper](https://github.com/tomkrouper)
- [@shlomi-noach](https://github.com/shlomi-noach)
