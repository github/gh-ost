# Cheatsheet

![operation modes](images/gh-ost-operation-modes.png)


`gh-ost` operates by connecting to potentially multiple servers, as well as imposing itself as a replica in order to streamline binary log events directly from one of those servers. There are various operation modes, which depend on your setup, configuration, and where you want to run the migration.

##### a. Connect to replica, migrate on master

This is the mode `gh-ost` expects by default. `gh-ost` will investigate the replica, crawl up to find the topology's master, and will hook onto it as well. Migration will:

- Read and write row-data on master
- Read binary logs events on the replica, apply the changes onto the master
- Investigates table format, columns & keys, count rows on the replica
- Read internal changelog events (such as heartbeat) from the replica
- Cut-over (switch tables) on the master

If your master works with SBR, this is the mode to work with. The replica must be configured with binary logs enabled (`log_bin`, `log_slave_updates`) and should have `binlog_format=ROW` (`gh-ost` can apply the latter for you).

However even with RBR we suggest this is the least master-intrusive operation mode.

##### b. Connect to master

If you don't have replicas, or do not wish to use them, you are still able to operate directly on the master. `gh-ost` will do all operations directly on the master. You may still ask it to be considerate of replication lag.

- Your master must produce binary logs in RBR format.
- You must approve this mode via `--allow-on-master`.

##### c. Migrate/test on replica

This will perform a migration on the replica. `gh-ost` will briefly connect to the master but will thereafter perform all operations on the replica without modifying anything on the master.
Throughout the operation, `gh-ost` will throttle such that the replica is up to date.

- `--migrate-on-replica` indicates to `gh-ost` that it must migrate the table directly on the replica. It will perform the cut-over phase even while replication is running.
- `--test-on-replica` indicates the migration is for purpose of testing only. Before cut-over takes place, replication is stopped. Tables are swapped and then swapped back: your original table returns to its original place.
Both tables are left with replication stopped. You may examine the two and compare data.
