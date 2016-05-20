# Understading gh-ost output

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
2016-05-19 17:57:11 INFO Droppping table `mydb`.`_mytable_gst`
2016-05-19 17:57:11 INFO Table dropped
2016-05-19 17:57:11 INFO Droppping table `mydb`.`_mytable_old`
2016-05-19 17:57:11 INFO Table dropped
2016-05-19 17:57:11 INFO Creating ghost table `mydb`.`_mytable_gst`
2016-05-19 17:57:11 INFO Ghost table created
2016-05-19 17:57:11 INFO Altering ghost table `mydb`.`_mytable_gst`
2016-05-19 17:57:11 INFO Ghost table altered
2016-05-19 17:57:11 INFO Droppping table `mydb`.`_mytable_osc`
2016-05-19 17:57:11 INFO Table dropped
2016-05-19 17:57:11 INFO Creating changelog table `mydb`.`_mytable_osc`
2016-05-19 17:57:11 INFO Changelog table created
2016-05-19 17:57:11 INFO Chosen shared unique key is PRIMARY
2016-05-19 17:57:11 INFO Shared columns are id,name,ref,col4,col5,col6
```
Those are relatively self explanatory. Mostly they indicate that all goes well.

You will be mostly interested in following up on the migration and understanding whether it goes well. Once migration actually begins, you will see output as follows:

```
Copy: 0/4466810 0.0%; Applied: 0; Backlog: 0/100; Elapsed: 0s(copy), 6s(total); streamer: mysql-bin.002587:348727198; ETA: N/A
Copy: 0/4466810 0.0%; Applied: 0; Backlog: 100/100; Elapsed: 1s(copy), 7s(total); streamer: mysql-bin.002587:349815124; ETA: throttled, replica-lag=83.000000s
Copy: 0/4466810 0.0%; Applied: 0; Backlog: 100/100; Elapsed: 2s(copy), 8s(total); streamer: mysql-bin.002587:349815124; ETA: throttled, replica-lag=79.000000s
Copy: 0/4466810 0.0%; Applied: 0; Backlog: 100/100; Elapsed: 3s(copy), 9s(total); streamer: mysql-bin.002587:349815124; ETA: throttled, replica-lag=74.000000s
Copy: 0/4466810 0.0%; Applied: 0; Backlog: 100/100; Elapsed: 4s(copy), 10s(total); streamer: mysql-bin.002587:349815124; ETA: throttled, replica-lag=69.000000s
Copy: 0/4466810 0.0%; Applied: 0; Backlog: 100/100; Elapsed: 5s(copy), 11s(total); streamer: mysql-bin.002587:349815124; ETA: throttled, replica-lag=65.000000s
...
```
In the above we're mostly interested to see that `ETA: throttled, replica-lag=65.000000s`.

- Migration is throttled, i.e. `gh-ost` finds that the server is too busy, or replication is too far behind, and so it ceases (or does not start) data copy operation.
- It also provides a reason for the throttling. In out case it seems replication is too far behind. `gh-ost` awaits until replication lag is smaller than `--max-lag-millis`.

However another thing catches the eye: `Backlog: 0/100` transitions into `Backlog: 100/100`

- `Backlog` is the binlog events queue. A queue of events read from the binary log which are relevant for the migration. The queue gets emptied as events are applied onto the ghost table. Typically we want to see that queue empty or almost empty. However, due to the fact we're not throttled it makes perfect sense that the queue is full: htrottling means we do not apply events onto the ghost table, hence we do not purge the queue.

```
...
Copy: 0/4466810 0.0%; Applied: 0; Backlog: 100/100; Elapsed: 16s(copy), 22s(total); streamer: mysql-bin.002587:349815124; ETA: throttled, replica-lag=8.000000s
Copy: 0/4466810 0.0%; Applied: 0; Backlog: 100/100; Elapsed: 17s(copy), 23s(total); streamer: mysql-bin.002587:349815124; ETA: throttled, replica-lag=2.000000s
Copy: 0/4466885 0.0%; Applied: 1492; Backlog: 100/100; Elapsed: 18s(copy), 24s(total); streamer: mysql-bin.002587:358722182; ETA: N/A
Copy: 0/4466942 0.0%; Applied: 2966; Backlog: 100/100; Elapsed: 19s(copy), 25s(total); streamer: mysql-bin.002587:367190999; ETA: N/A
Copy: 0/4466993 0.0%; Applied: 4462; Backlog: 1/100; Elapsed: 20s(copy), 26s(total); streamer: mysql-bin.002587:376732190; ETA: N/A
Copy: 12500/4466994 0.3%; Applied: 4496; Backlog: 2/100; Elapsed: 21s(copy), 27s(total); streamer: mysql-bin.002587:381475469; ETA: N/A
Copy: 25000/4466997 0.6%; Applied: 4535; Backlog: 6/100; Elapsed: 22s(copy), 28s(total); streamer: mysql-bin.002587:386747649; ETA: N/A
Copy: 40000/4467001 0.9%; Applied: 4582; Backlog: 3/100; Elapsed: 23s(copy), 29s(total); streamer: mysql-bin.002587:393017028; ETA: N/A
```

In the above, `gh-ost` found replication to be caught up and began operation. We note:
- `Backlog` goes down to `1` or `2` or otherwise smaller numbers. This means we are good with processing the binlog events and applying them onto the ghost table.
- `Applied` is the incrementing number of events we have applied from the binary log onto the ghost table, since the migration began.
- `Copy`: at the beginning the tool estimated `4466810` rows already existing in the table. Initially `0` of them are copied, hence `0/4466810`. But as `gh-ost` makes progress, this number grows:
  - `12500/4466994 0.3%`
  - `25000/4466997 0.6%`
  - `40000/4467001 0.9%`
  - You can also observe that the number of rows changes. This is implied by the flag `--exact-rowcount`, where we try and keep an updated amount of rows were are going to process throughout the migration, even as new rows are added and old rows deleted. This is not an exact number, but turns out to be a pretty good estimate.
- `Elapsed: 23s(copy), 29s(total)`: `total` stands for total time from executing of `gh-ost`. `copy` stands for the time elapsed since `gh-ost` finished making preparations and was good to go with copy.
- `streamer: mysql-bin.002587:393017028` tells us which binary log entry is `gh-ost` processing at this time.
- `ETA`: Estimated Time of Arrival, is still `N/A` since `gh-ost` has not collected enough data to make an estimate.

Some time later, we will have:

```
Copy: 50000/4467001 1.1%; Applied: 4620; Backlog: 6/100; Elapsed: 24s(copy), 30s(total); streamer: mysql-bin.002587:396414283; ETA: 35m20s
Copy: 62500/4467002 1.4%; Applied: 4671; Backlog: 3/100; Elapsed: 25s(copy), 31s(total); streamer: mysql-bin.002587:402582372; ETA: 29m21s
Copy: 75000/4467003 1.7%; Applied: 4703; Backlog: 3/100; Elapsed: 26s(copy), 32s(total); streamer: mysql-bin.002587:407864888; ETA: 25m22s
Copy: 87500/4467004 2.0%; Applied: 4751; Backlog: 6/100; Elapsed: 27s(copy), 33s(total); streamer: mysql-bin.002587:413142992; ETA: 22m31s
Copy: 100000/4467004 2.2%; Applied: 4795; Backlog: 6/100; Elapsed: 28s(copy), 34s(total); streamer: mysql-bin.002587:418380729; ETA: 20m22s
Copy: 112500/4467005 2.5%; Applied: 4835; Backlog: 1/100; Elapsed: 29s(copy), 35s(total); streamer: mysql-bin.002587:423592450; ETA: 18m42s
```
And `gh-ost` progressively provides an ETA.

Status frequency:
- In the first `60` seconds `gh-ost` emits a status entry every `1` second.
- Then, up till `3` miinutes into operation, status shows every `5` seconds.
- It then drops down to once per `30` seconds
- But goes into once-per-`5`-seconds again when it estimates < `3` minutes ETA
- And once per `1` second when it estimates < `1` minute ETA

```
Copy: 602500/4467053 13.5%; Applied: 6770; Backlog: 0/100; Elapsed: 1m14s(copy), 1m20s(total); streamer: mysql-bin.002587:630949369; ETA: 7m54s
Copy: 655000/4467060 14.7%; Applied: 6985; Backlog: 6/100; Elapsed: 1m19s(copy), 1m25s(total); streamer: mysql-bin.002587:652696032; ETA: 7m39s
Copy: 707500/4467066 15.8%; Applied: 7207; Backlog: 0/100; Elapsed: 1m24s(copy), 1m30s(total); streamer: mysql-bin.002587:674577141; ETA: 7m26s
...
Copy: 1975000/4466798 44.2%; Applied: 12919; Backlog: 2/100; Elapsed: 3m24s(copy), 3m30s(total); streamer: mysql-bin.002588:119901391; ETA: 4m17s
Copy: 2285000/4466855 51.2%; Applied: 14234; Backlog: 13/100; Elapsed: 3m54s(copy), 4m0s(total); streamer: mysql-bin.002588:243346615; ETA: 3m43s
...
Copy: 4397500/4467226 98.4%; Applied: 22996; Backlog: 8/100; Elapsed: 7m11s(copy), 7m17s(total); streamer: mysql-bin.002588:1063945589; ETA: 6s
Copy: 4410000/4467227 98.7%; Applied: 23045; Backlog: 5/100; Elapsed: 7m12s(copy), 7m18s(total); streamer: mysql-bin.002588:1068763841; ETA: 5s
Copy: 4420000/4467229 98.9%; Applied: 23086; Backlog: 5/100; Elapsed: 7m13s(copy), 7m19s(total); streamer: mysql-bin.002588:1072751966; ETA: 4s
2016-05-19 18:04:25 INFO rotate to next log name: mysql-bin.002589
2016-05-19 18:04:25 INFO rotate to next log name: mysql-bin.002589
Copy: 4430000/4467231 99.2%; Applied: 23124; Backlog: 3/100; Elapsed: 7m14s(copy), 7m20s(total); streamer: mysql-bin.002589:2944139; ETA: 3s
Copy: 4442500/4467231 99.4%; Applied: 23181; Backlog: 2/100; Elapsed: 7m15s(copy), 7m21s(total); streamer: mysql-bin.002589:8042490; ETA: 2s
Copy: 4452500/4467232 99.7%; Applied: 23235; Backlog: 5/100; Elapsed: 7m16s(copy), 7m22s(total); streamer: mysql-bin.002589:12084190; ETA: 1s
Copy: 4462500/4467235 99.9%; Applied: 23295; Backlog: 8/100; Elapsed: 7m17s(copy), 7m23s(total); streamer: mysql-bin.002589:16174016; ETA: 0s
2016-05-19 18:04:29 INFO Row copy complete
Copy: 4466492/4467235 100.0%; Applied: 23309; Backlog: 0/100; Elapsed: 7m17s(copy), 7m24s(total); streamer: mysql-bin.002589:17255091; ETA: 0s
2016-05-19 18:04:29 INFO Stopping replication
2016-05-19 18:04:29 INFO Replication stopped
2016-05-19 18:04:29 INFO Verifying SQL thread is running
2016-05-19 18:04:29 INFO SQL thread started
2016-05-19 18:04:29 INFO Replication IO thread at mysql-bin.001801:719204179. SQL thread is at mysql-bin.001801:719204179
2016-05-19 18:04:29 INFO Writing changelog state: AllEventsUpToLockProcessed
2016-05-19 18:04:29 INFO Waiting for events up to lock
Copy: 4466492/4467235 100.0%; Applied: 23309; Backlog: 1/100; Elapsed: 7m18s(copy), 7m24s(total); streamer: mysql-bin.002589:17702369; ETA: 0s
2016-05-19 18:04:30 INFO Done waiting for events up to lock
Copy: 4466492/4467235 100.0%; Applied: 23309; Backlog: 0/100; Elapsed: 7m18s(copy), 7m25s(total); streamer: mysql-bin.002589:17703056; ETA: 0s
```
This migration took, till this point, `7m25s`, had applied `23309` events from the binary log and has copied `4466492` rows onto the ghost table.
 
