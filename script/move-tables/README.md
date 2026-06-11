### Setup

Setup the multi-cluster topology and seed the data
```bash
script/move-tables/setup
```

Verify data is present in the source cluster.
```bash
script/move-tables/mysql-source-primary -D gh_ost_test_db -e "SELECT * FROM gh_ost_test;"
```

Verify the empty database is present in the target cluster.
```bash
script/move-tables/mysql-target-primary -D gh_ost_test_db -e "SHOW TABLES;"
```

### Testing `gh-ost`

Checkout your branch of `github/gh-ost` and build the binaries:
```bash
script/build --cli
```

Run gh-ost to move tables:
```bash
./bin/gh-ost --move-tables=gh_ost_test --host=localhost --port=3308 --user root --password opensesame --database=gh_ost_test_db --target-host=localhost --target-port=3309 --target-user root --target-password opensesame --target-database=gh_ost_test_db  --execute --verbose
```

### WIP

Current state based on the current outter-dev loop:


```bash


\u2718 \e[2m\u2388 (\u2205) gh-ost:(move-tables/1.2-skip-ghost-tables)
> rm /tmp/gh-ost.gh_ost_test_db..sock; ./script/build --cli && ./bin/gh-ost --move-tables=gh_ost_test --host=localhost --port=3308 --user root --password opensesame --database=gh_ost_test_db --target-host=localhost --target-port=3309 --target-user root --target-password opensesame --target-database=gh_ost_test_db --execute --verbose
rm: /tmp/gh-ost.gh_ost_test_db..sock: No such file or directory
go version go1.25.9 darwin/arm64 found in : Go Binary: /opt/homebrew/bin/go
++ '[' '!' -L .gopath/src/github.com/github/gh-ost ']'
++ export GOPATH=/Users/chriskirkland/git/src/github.com/github/gh-ost/.gopath:/Users/chriskirkland/git/src/github.com/github/gh-ost/.vendor
++ GOPATH=/Users/chriskirkland/git/src/github.com/github/gh-ost/.gopath:/Users/chriskirkland/git/src/github.com/github/gh-ost/.vendor
+ mkdir -p bin
+ bindir=/Users/chriskirkland/git/src/github.com/github/gh-ost/bin
+ scriptdir=/Users/chriskirkland/git/src/github.com/github/gh-ost/script
++ git rev-parse HEAD
+ version=0508dd782e1871de9dcaa51d3f59e5ba4cd92117
++ git describe --tags --always --dirty
+ describe=v1.1.9-19-g0508dd78-dirty
+ export GOPATH=/Users/chriskirkland/git/src/github.com/github/gh-ost/.gopath
+ GOPATH=/Users/chriskirkland/git/src/github.com/github/gh-ost/.gopath
+ cd .gopath/src/github.com/github/gh-ost
+ go build -o /Users/chriskirkland/git/src/github.com/github/gh-ost/bin/gh-ost -ldflags '-X main.AppVersion=0508dd782e1871de9dcaa51d3f59e5ba4cd92117 -X main.BuildDescribe=v1.1.9-19-g0508dd78-dirty' ./go/cmd/gh-ost/main.go
2026-06-01 16:41:13 INFO starting gh-ost 0508dd782e1871de9dcaa51d3f59e5ba4cd92117 (git commit: unknown)
2026-06-01 16:41:13 INFO Moving tables [gh_ost_test] from `gh_ost_test_db` to `gh_ost_test_db` (localhost)
2026-06-01 16:41:13 INFO inspector connection validated on localhost:3308
2026-06-01 16:41:13 INFO User has SUPER, REPLICATION SLAVE privileges, and has ALL privileges on `gh_ost_test_db`.*
2026-06-01 16:41:13 INFO binary logs validated on localhost:3308
2026-06-01 16:41:13 INFO Restarting replication on localhost:3308 to make sure binlog settings apply to replication thread
2026-06-01 16:41:13 INFO Inspector initiated on 3e162abb4a14:3308, version 8.0.41
2026-06-01 16:41:13 INFO Inspector validating original table
2026-06-01 16:41:13 INFO Table found. Engine=InnoDB
2026-06-01 16:41:13 INFO Estimated number of rows via EXPLAIN: 20
2026-06-01 16:41:13 INFO Inspector validated original table
2026-06-01 16:41:13 INFO Inspector inspected original table
2026-06-01 16:41:13 INFO log_slave_updates validated on localhost:3308
2026-06-01 16:41:13 INFO Inspector validated and initialized
2026-06-01 16:41:13 INFO applier connection validated on localhost:3309
2026-06-01 16:41:13 INFO applier connection validated on localhost:3309
2026-06-01 16:41:13 INFO will use time_zone='SYSTEM' on applier
2026-06-01 16:41:13 INFO applier connection validated on localhost:3309
2026-06-01 16:41:13 INFO Applier initiated on 381ee87dc2c6:3309, version 8.0.41
2026-06-01 16:41:13 INFO Fetching create table statement for `gh_ost_test_db.gh_ost_test`
2026-06-01 16:41:13 INFO Create table statement: CREATE TABLE `gh_ost_test` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `column1` int NOT NULL,
  `column2` smallint unsigned NOT NULL,
  `column3` mediumint unsigned NOT NULL,
  `column4` tinyint unsigned NOT NULL,
  `column5` int NOT NULL,
  `column6` int NOT NULL,
  PRIMARY KEY (`id`),
  KEY `c12_ix` (`column1`,`column2`)
) ENGINE=InnoDB AUTO_INCREMENT=21 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
2026-06-01 16:41:13 INFO Creating target table `gh_ost_test_db`.`gh_ost_test`
2026-06-01 16:41:13 INFO Target table created
2026-06-01 16:41:13 INFO streamer connection validated on localhost:3308
[2026/06/01 16:41:13] [info] binlogsyncer.go:191 create BinlogSyncer with config {ServerID:99999 Flavor:mysql Host:localhost Port:3308 User:root Password: Localhost: Charset: SemiSyncEnabled:false RawModeEnabled:false TLSConfig:<nil> ParseTime:false TimestampStringLocation:UTC UseDecimal:true RecvBufferSize:0 HeartbeatPeriod:0s ReadTimeout:0s MaxReconnectAttempts:0 DisableRetrySync:false VerifyChecksum:false DumpCommandFlag:0 Option:<nil> Logger:0x14000494a20 Dialer:0x100c8c0c0 RowsEventDecodeFunc:<nil> TableMapOptionalMetaDecodeFunc:<nil> DiscardGTIDSet:false EventCacheCount:10240 SynchronousEventHandler:<nil>}
2026-06-01 16:41:13 INFO Connecting binlog streamer at mysql-bin.000003:2987908
[2026/06/01 16:41:13] [info] binlogsyncer.go:443 begin to sync binlog from position (mysql-bin.000003, 2987908)
[2026/06/01 16:41:13] [info] binlogsyncer.go:409 Connected to mysql 8.0.41 server
2026-06-01 16:41:13 INFO Skipping stream of the changelog table []
[2026/06/01 16:41:13] [info] binlogsyncer.go:868 rotate to (mysql-bin.000003, 2987908)
2026-06-01 16:41:13 INFO rotate to next log from mysql-bin.000003:0 to mysql-bin.000003
2026-06-01 16:41:13 INFO Listening on unix socket file: /tmp/gh-ost.gh_ost_test_db..sock
2026-06-01 16:41:13 INFO Adding listener for gh_ost_test_db.gh_ost_test
2026-06-01 16:41:13 INFO Reading migration range according to key: PRIMARY (
		select /* gh-ost `gh_ost_test_db`.`gh_ost_test` */ `id`
		from
			`gh_ost_test_db`.`gh_ost_test`
		force index (PRIMARY)
		order by
			`id` asc
		limit 1)
2026-06-01 16:41:13 INFO Migration min values: [1]
2026-06-01 16:41:13 INFO Migration max values: [20]
2026-06-01 16:41:13 INFO Skipping throttling in move tables mode []
# Migrating `gh_ost_test_db`.`gh_ost_test`; Target table is `gh_ost_test_db`.`gh_ost_test`
# Migrating 381ee87dc2c6:3309; inspecting 3e162abb4a14:3308; executing on Chriss-MBP-2
# Migration started at Mon Jun 01 16:41:13 -0600 2026
# chunk-size: 1000; max-lag-millis: 1500ms; dml-batch-size: 10; max-load: ; critical-load: ; nice-ratio: 0.000000
# throttle-additional-flag-file: /tmp/gh-ost.throttle
# Serving on unix socket: /tmp/gh-ost.gh_ost_test_db..sock
Copy: 0/20 0.0%; Applied: 0; Backlog: 0/1000; Time: 0s(total), 0s(copy); streamer: mysql-bin.000003:0; Lag: 0.00s, HeartbeatLag: 9223372036.85s, State: migrating; ETA: N/A
2026-06-01 16:41:13 INFO Copy: 0/20 0.0%; Applied: 0; Backlog: 0/1000; Time: 0s(total), 0s(copy); streamer: mysql-bin.000003:0; Lag: 0.00s, HeartbeatLag: 9223372036.85s, State: migrating; ETA: N/A []
Copy: 0/20 0.0%; Applied: 0; Backlog: 0/1000; Time: 1s(total), 1s(copy); streamer: mysql-bin.000003:0; Lag: 0.00s, HeartbeatLag: 9223372036.85s, State: migrating; ETA: N/A
2026-06-01 16:41:14 INFO [execWriteFuncs] Processing row copy function []
2026-06-01 16:41:14 INFO Copy: 0/20 0.0%; Applied: 0; Backlog: 0/1000; Time: 1s(total), 1s(copy); streamer: mysql-bin.000003:0; Lag: 0.00s, HeartbeatLag: 9223372036.85s, State: migrating; ETA: N/A []
2026-06-01 16:41:14 INFO ApplyIterationInsertQuery affected 20 rows
2026-06-01 16:41:14 INFO [execWriteFuncs] Processing row copy function []
2026-06-01 16:41:14 INFO [execWriteFuncs] Processing row copy function []
2026-06-01 16:41:14 INFO Row copy complete
2026-06-01 16:41:14 INFO Writing changelog state: Migrated
[2026/06/01 16:41:14] [info] binlogsyncer.go:225 syncer is closing...
2026-06-01 16:41:14 INFO StreamEvents encountered unexpected error: Sync was closed
github.com/go-mysql-org/go-mysql/replication.init
	<autogenerated>:1
runtime.doInit1
	/Users/chriskirkland/git/src/github.com/github/gh-ost/.gopath/pkg/mod/golang.org/toolchain@v0.0.1-go1.25.9.darwin-arm64/src/runtime/proc.go:7670
runtime.doInit
	/Users/chriskirkland/git/src/github.com/github/gh-ost/.gopath/pkg/mod/golang.org/toolchain@v0.0.1-go1.25.9.darwin-arm64/src/runtime/proc.go:7637
runtime.main
	/Users/chriskirkland/git/src/github.com/github/gh-ost/.gopath/pkg/mod/golang.org/toolchain@v0.0.1-go1.25.9.darwin-arm64/src/runtime/proc.go:256
runtime.goexit
	/Users/chriskirkland/git/src/github.com/github/gh-ost/.gopath/pkg/mod/golang.org/toolchain@v0.0.1-go1.25.9.darwin-arm64/src/runtime/asm_arm64.s:1268
[2026/06/01 16:41:14] [info] binlogsyncer.go:988 kill last connection id 405
[2026/06/01 16:41:14] [info] binlogsyncer.go:255 syncer is closed
2026-06-01 16:41:14 INFO Closed streamer connection. err=<nil>
2026-06-01 16:41:14 INFO Done moving tables [gh_ost_test] from `gh_ost_test_db` to `gh_ost_test_db` (localhost)
2026-06-01 16:41:14 INFO Removing socket file: /tmp/gh-ost.gh_ost_test_db..sock
2026-06-01 16:41:14 INFO Tearing down inspector
2026-06-01 16:41:14 INFO Tearing down applier
2026-06-01 16:41:14 INFO Tearing down streamer
# Done

```

:tada: :tada: :tada: :tada:
```bash

\u2714 \e[2m\u2388 (\u2205) gh-ost-tablemove-poc:(chriskirkland/move-tables)
> ./script/move-tables/mysql-target-primary -D "gh_ost_test_db" -e "SELECT * FROM gh_ost_test;"
+----+---------+---------+---------+---------+------------+------------+
| id | column1 | column2 | column3 | column4 | column5    | column6    |
+----+---------+---------+---------+---------+------------+------------+
|  1 |    1001 |     100 |  500000 |      10 | 1700000001 | 1700000002 |
|  2 |    1002 |     200 |  600000 |      20 | 1700000003 | 1700000004 |
|  3 |    1003 |     300 |  700000 |      30 | 1700000005 | 1700000006 |
|  4 |    1004 |     400 |  800000 |      40 | 1700000007 | 1700000008 |
|  5 |    1005 |     500 |  900000 |      50 | 1700000009 | 1700000010 |
|  6 |    1006 |     600 | 1000000 |      60 | 1700000011 | 1700000012 |
|  7 |    1007 |     700 | 1100000 |      70 | 1700000013 | 1700000014 |
|  8 |    1008 |     800 | 1200000 |      80 | 1700000015 | 1700000016 |
|  9 |    1009 |     900 | 1300000 |      90 | 1700000017 | 1700000018 |
| 10 |    1010 |    1000 | 1400000 |     100 | 1700000019 | 1700000020 |
| 11 |    1011 |    1100 | 1500000 |     110 | 1700000021 | 1700000022 |
| 12 |    1012 |    1200 | 1600000 |     120 | 1700000023 | 1700000024 |
| 13 |    1013 |    1300 | 1700000 |     130 | 1700000025 | 1700000026 |
| 14 |    1014 |    1400 | 1800000 |     140 | 1700000027 | 1700000028 |
| 15 |    1015 |    1500 | 1900000 |     150 | 1700000029 | 1700000030 |
| 16 |    1016 |    1600 | 2000000 |     160 | 1700000031 | 1700000032 |
| 17 |    1017 |    1700 | 2100000 |     170 | 1700000033 | 1700000034 |
| 18 |    1018 |    1800 | 2200000 |     180 | 1700000035 | 1700000036 |
| 19 |    1019 |    1900 | 2300000 |     190 | 1700000037 | 1700000038 |
| 20 |    1020 |    2000 | 2400000 |     200 | 1700000039 | 1700000040 |
+----+---------+---------+---------+---------+------------+------------+

```