# Understanding gh-ost output

`gh-ost` attempts to be verbose to the point where you really know what it's doing, without completely spamming you.
You can control output levels:
- `--verbose`: common use. Useful output, not tons of it
- `--debug`: everything. Tons of output.

Initial output lines may look like this:
```
2016-05-19 17:57:04 INFO starting gh-ost 0.7.14
2016-05-19 17:57:04 INFO Migrating `mydb`.`mytable`
2016-05-19 17:57:04 INFO connection validated on 127.0.0.1:3306
2016-05-19 17:57:04 INFO User has ALL privileges
2016-05-19 17:57:04 INFO binary logs validated on 127.0.0.1:3306
2016-05-19 17:57:04 INFO Restarting replication on 127.0.0.1:3306 to make sure binlog settings apply to replication thread
2016-05-19 17:57:04 INFO Table found. Engine=InnoDB
2016-05-19 17:57:05 INFO As instructed, I'm issuing a SELECT COUNT(*) on the table. This may take a while
2016-05-19 17:57:11 INFO Exact number of rows via COUNT: 4466810
2016-05-19 17:57:11 INFO --test-on-replica given. Will not execute on master the.master:3306 but rather on replica 127.0.0.1:3306 itself
2016-05-19 17:57:11 INFO Master found to be 127.0.0.1:3306
2016-05-19 17:57:11 INFO connection validated on 127.0.0.1:3306
2016-05-19 17:57:11 INFO Registering replica at 127.0.0.1:3306
2016-05-19 17:57:11 INFO Connecting binlog streamer at mysql-bin.002587:348694066
2016-05-19 17:57:11 INFO connection validated on 127.0.0.1:3306
2016-05-19 17:57:11 INFO rotate to next log name: mysql-bin.002587
2016-05-19 17:57:11 INFO connection validated on 127.0.0.1:3306
2016-05-19 17:57:11 INFO Dropping table `mydb`.`_mytable_gst`
2016-05-19 17:57:11 INFO Table dropped
2016-05-19 17:57:11 INFO Dropping table `mydb`.`_mytable_old`
2016-05-19 17:57:11 INFO Table dropped
2016-05-19 17:57:11 INFO Creating ghost table `mydb`.`_mytable_gst`
2016-05-19 17:57:11 INFO Ghost table created
2016-05-19 17:57:11 INFO Altering ghost table `mydb`.`_mytable_gst`
2016-05-19 17:57:11 INFO Ghost table altered
2016-05-19 17:57:11 INFO Dropping table `mydb`.`_mytable_osc`
2016-05-19 17:57:11 INFO Table dropped
2016-05-19 17:57:11 INFO Creating changelog table `mydb`.`_mytable_osc`
2016-05-19 17:57:11 INFO Changelog table created
2016-05-19 17:57:11 INFO Chosen shared unique key is PRIMARY
2016-05-19 17:57:11 INFO Shared columns are id,name,ref,col4,col5,col6
```
Those are relatively self explanatory. Mostly they indicate that all goes well.

You will be mostly interested in following up on the migration and understanding whether it goes well. Once migration actually begins, you will see output as follows:

```
Copy: 0/752865 0.0%; Applied: 0; Backlog: 0/100; Time: 29s(total), 0s(copy); streamer: mysql-bin.007068:846528615; ETA: N/A
Copy: 0/752865 0.0%; Applied: 0; Backlog: 0/100; Time: 30s(total), 1s(copy); streamer: mysql-bin.007068:846875570; ETA: N/A
Copy: 7300/752865 1.0%; Applied: 0; Backlog: 0/100; Time: 31s(total), 2s(copy); streamer: mysql-bin.007068:855439063; ETA: N/A
Copy: 14100/752865 1.9%; Applied: 0; Backlog: 0/100; Time: 32s(total), 3s(copy); streamer: mysql-bin.007068:864722759; ETA: 2m37s
Copy: 20100/752865 2.7%; Applied: 0; Backlog: 0/100; Time: 33s(total), 4s(copy); streamer: mysql-bin.007068:874346340; ETA: 2m26s
Copy: 27000/752865 3.6%; Applied: 0; Backlog: 0/100; Time: 34s(total), 5s(copy); streamer: mysql-bin.007068:886997306; ETA: 2m14s
...
```
In the above some time was spent on counting table rows. `29s` have elapsed before actual rowcopy began. `gh-ost` will not deliver ETA before `1%` of the copy is complete.

```
Copy: 460900/752865 61.2%; Applied: 0; Backlog: 0/100; Time: 2m35s(total), 2m6s(copy); streamer: mysql-bin.007069:596112173; ETA: 1m19s
Copy: 466600/752865 62.0%; Applied: 0; Backlog: 0/100; Time: 2m40s(total), 2m11s(copy); streamer: mysql-bin.007069:622646704; ETA: throttled, my.replica-01.com:3306 replica-lag=3.000000s
Copy: 478500/752865 63.6%; Applied: 0; Backlog: 0/100; Time: 2m45s(total), 2m16s(copy); streamer: mysql-bin.007069:641258880; ETA: 1m17s
Copy: 496900/752865 66.0%; Applied: 0; Backlog: 0/100; Time: 2m50s(total), 2m21s(copy); streamer: mysql-bin.007069:678956577; ETA: throttled, my.replica-01.com:3306 replica-lag=2.000000s
Copy: 496900/752865 66.0%; Applied: 0; Backlog: 0/100; Time: 2m55s(total), 2m26s(copy); streamer: mysql-bin.007069:681610879; ETA: throttled, max-load Threads_running=26 >= 25
Copy: 528000/752865 70.1%; Applied: 0; Backlog: 0/100; Time: 3m0s(total), 2m31s(copy); streamer: mysql-bin.007069:711177703; ETA: throttled, lag=2.483039s
Copy: 564900/752865 75.0%; Applied: 0; Backlog: 0/100; Time: 3m30s(total), 3m1s(copy); streamer: mysql-bin.007069:795150744; ETA: throttled, lag=3.482914s
Copy: 577200/752865 76.7%; Applied: 0; Backlog: 0/100; Time: 3m39s(total), 3m10s(copy); streamer: mysql-bin.007069:819956052; ETA: 57s
Copy: 589300/752865 78.3%; Applied: 0; Backlog: 0/100; Time: 3m56s(total), 3m27s(copy); streamer: mysql-bin.007069:858738375; ETA: 57s
Copy: 595700/752865 79.1%; Applied: 0; Backlog: 0/100; Time: 3m57s(total), 3m28s(copy); streamer: mysql-bin.007069:860745762; ETA: 54s
```

In the above migration is throttled on occasion.

- A few times because one of the control replicas, specifically `my.replica-01.com:3306`, was lagging
- Once because `max-load` threshold has been reached (`Threads_running=26 >= 25`)
- Once because the migration replica itself (the server into which `gh-ost` connected) lagged.

`gh-ost` will always specify the reason for throttling.

### Progress

- `Copy: 595700/752865 79.1%` indicates the number of existing table rows copied onto the _ghost_ table, out of an estimate of the total row count.
- `Applied: 0` indicates the number of entries processed in the binary log and applied onto the _ghost_ table. In the examples above there was no traffic on the migrated table, hence no rows processed.

A migration on a more intensively used table may look like this:

```
Copy: 30713100/43138319 71.2%; Applied: 381910; Backlog: 0/100; Time: 2h6m30s(total), 2h3m20s(copy); streamer: mysql-bin.006792:1001340307; ETA: 49m53s
Copy: 30852500/43138338 71.5%; Applied: 383365; Backlog: 0/100; Time: 2h7m0s(total), 2h3m50s(copy); streamer: mysql-bin.006792:1050191186; ETA: 49m18s
2016-07-25 03:20:41 INFO rotate to next log name: mysql-bin.006793
2016-07-25 03:20:41 INFO rotate to next log name: mysql-bin.006793
Copy: 30925700/43138360 71.7%; Applied: 384873; Backlog: 0/100; Time: 2h7m30s(total), 2h4m20s(copy); streamer: mysql-bin.006793:9144080; ETA: 49m5s
Copy: 31028800/43138380 71.9%; Applied: 386325; Backlog: 0/100; Time: 2h8m0s(total), 2h4m50s(copy); streamer: mysql-bin.006793:47984430; ETA: 48m43s
Copy: 31165600/43138397 72.2%; Applied: 387787; Backlog: 0/100; Time: 2h8m30s(total), 2h5m20s(copy); streamer: mysql-bin.006793:96139474; ETA: 48m8s
Copy: 31291200/43138418 72.5%; Applied: 389257; Backlog: 7/100; Time: 2h9m0s(total), 2h5m50s(copy); streamer: mysql-bin.006793:141094700; ETA: 47m38s
Copy: 31389700/43138432 72.8%; Applied: 390629; Backlog: 100/100; Time: 2h9m30s(total), 2h6m20s(copy); streamer: mysql-bin.006793:179473435; ETA: throttled, lag=1.548707s
```

Notes:

- `Applied: 381910`: `381910` events in the binary logs presenting changes to the migrated table have been processed and applied on the _ghost_ table since beginning of migration.
- `Backlog: 0/100`: we are performing well on reading the binary log. There's nothing known in the binary log queue that awaits processing.
- `Backlog: 7/100`: while copying rows, a few events have piled up in the binary log _modifying our table_ that we spotted, and still need to apply.
- `Backlog: 100/100`: our buffer of `100` events is full; you may see this during or right after throttling (the binary logs keep filling up with relevant queries that are not being processed), or immediately following a high workload.
  `gh-ost` will always prioritize binlog event processing (backlog) over row-copy; when next possible (throttling completes, in our example), `gh-ost` will drain the queue first, and only then proceed to resume row copy.
  There is nothing wrong with seeing `100/100`; it just indicates we're behind at that point in time.
- `Copy: 31291200/43138418`, `Copy: 31389700/43138432`: this migration executed with `--exact-rowcount`. `gh-ost` continuously heuristically updates the total number of expected row copies as migration proceeds, hence the change from `43138418` to `43138432`
- `streamer: mysql-bin.006793:179473435` tells us which binary log entry is `gh-ost` processing at this time.

### Status hint

In addition, once every `10` minutes, a friendly reminder is printed, in the following form:

```
# Migrating `mydb`.`mytable`; Ghost table is `mydb`.`_mytable_gst`
# Migrating mysql.master-01.com:3306; inspecting mysql.replica-05.com:3306; executing on some.host-17.com
# Migration started at Mon Jul 25 01:13:19 2016
# chunk-size: 500; max lag: 1000ms; max-load: Threads_running=25; critical-load: Threads_running=1000; nice-ratio: 0
# throttle-additional-flag-file: /tmp/gh-ost.throttle.flag.file
# postpone-cut-over-flag-file: /tmp/gh-ost.postpone.flag.file [set]
# panic-flag-file: /tmp/gh-ost.panic.flag.file
# Serving on unix socket: /tmp/gh-ost.mydb.mytable.sock
```

- The above mostly print out the current configuration. Remember you can [dynamically control](interactive-commands.md) most of them.
- `gh-ost` notes that the `postpone-cut-over-flag-file` file actually exists by printing `[set]`
