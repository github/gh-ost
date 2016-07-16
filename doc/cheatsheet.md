# Cheatsheet

![operation modes](images/gh-ost-operation-modes.png)


`gh-ost` operates by connecting to potentially multiple servers, as well as imposing itself as a replica in order to streamline binary log events directly from one of those servers. There are various operation modes, which depend on your setup, configuration, and where you want to run the migration.

### a. Connect to replica, migrate on master

This is the mode `gh-ost` expects by default. `gh-ost` will investigate the replica, crawl up to find the topology's master, and will hook onto it as well. Migration will:

- Read and write row-data on master
- Read binary logs events on the replica, apply the changes onto the master
- Investigates table format, columns & keys, count rows on the replica
- Read internal changelog events (such as heartbeat) from the replica
- Cut-over (switch tables) on the master

If your master works with SBR, this is the mode to work with. The replica must be configured with binary logs enabled (`log_bin`, `log_slave_updates`) and should have `binlog_format=ROW` (`gh-ost` can apply the latter for you).

However even with RBR we suggest this is the least master-intrusive operation mode.

```shell
gh-ost \
--max-load=Threads_running=25 \
--critical-load=Threads_running=1000 \
--chunk-size=1000 \
--throttle-control-replicas="myreplica.1.com,myreplica.2.com" \
--max-lag-millis=1500 \
--user="gh-ost" \
--password="123456" \
--host=replica.with.rbr.com \
--database="my_schema" \
--table="my_table" \
--verbose \
--alter="engine=innodb" \
--switch-to-rbr \
--allow-master-master \
--cut-over=default \
--exact-rowcount \
--default-retries=120 \
--panic-flag-file=/tmp/ghost.panic.flag \
--postpone-cut-over-flag-file=/tmp/ghost.postpone.flag \
[--execute]
```

With `--execute`, migration actually copies data and flips tables. Without it this is a `noop` run.


### b. Connect to master

If you don't have replicas, or do not wish to use them, you are still able to operate directly on the master. `gh-ost` will do all operations directly on the master. You may still ask it to be considerate of replication lag.

- Your master must produce binary logs in RBR format.
- You must approve this mode via `--allow-on-master`.

```shell
gh-ost \
--max-load=Threads_running=25 \
--critical-load=Threads_running=1000 \
--chunk-size=1000 \
--throttle-control-replicas="myreplica.1.com,myreplica.2.com" \
--max-lag-millis=1500 \
--user="gh-ost" \
--password="123456" \
--host=master.with.rbr.com \
--allow-on-master \
--database="my_schema" \
--table="my_table" \
--verbose \
--alter="engine=innodb" \
--switch-to-rbr \
--allow-master-master \
--cut-over=default \
--exact-rowcount \
--default-retries=120 \
--panic-flag-file=/tmp/ghost.panic.flag \
--postpone-cut-over-flag-file=/tmp/ghost.postpone.flag \
[--execute]
```

### c. Migrate/test on replica

This will perform a migration on the replica. `gh-ost` will briefly connect to the master but will thereafter perform all operations on the replica without modifying anything on the master.
Throughout the operation, `gh-ost` will throttle such that the replica is up to date.

- `--migrate-on-replica` indicates to `gh-ost` that it must migrate the table directly on the replica. It will perform the cut-over phase even while replication is running.
- `--test-on-replica` indicates the migration is for purpose of testing only. Before cut-over takes place, replication is stopped. Tables are swapped and then swapped back: your original table returns to its original place.
Both tables are left with replication stopped. You may examine the two and compare data.

Test on replica cheatsheet:
```shell
gh-ost \
  --user="gh-ost" \
  --password="123456" \
  --host=replica.with.rbr.com \
  --test-on-replica \
  --database="my_schema" \
  --table="my_table" \
  --verbose \
  --alter="engine=innodb" \
  --initially-drop-ghost-table \
  --initially-drop-old-table \
  --max-load=Threads_running=30 \
  --switch-to-rbr \
  --chunk-size=2500 \
  --cut-over=default \
  --exact-rowcount \
  --serve-socket-file=/tmp/gh-ost.test.sock \
  --panic-flag-file=/tmp/gh-ost.panic.flag \
  --execute
```

### cnf file

You may use a `cnf` file in the following format:

```
[client]
user=gh-ost
password=123456
```

You may then remove `--user=gh-ost --password=123456` and specify `--conf=/path/to/config/file.cnf`
