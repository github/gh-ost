/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/logic"
	"github.com/outbrain/golib/log"
)

var AppVersion string

// acceptSignals registers for OS signals
func acceptSignals(migrationContext *base.MigrationContext) {
	c := make(chan os.Signal, 1)

	signal.Notify(c, syscall.SIGHUP)
	go func() {
		for sig := range c {
			switch sig {
			case syscall.SIGHUP:
				log.Infof("Received SIGHUP. Reloading configuration")
				if err := migrationContext.ReadConfigFile(); err != nil {
					log.Errore(err)
				} else {
					migrationContext.MarkPointOfInterest()
				}
			}
		}
	}()
}

// main is the application's entry point. It will either spawn a CLI or HTTP interfaces.
func main() {
	migrationContext := base.GetMigrationContext()

	flag.StringVar(&migrationContext.InspectorConnectionConfig.Key.Hostname, "host", "127.0.0.1", "MySQL hostname (preferably a replica, not the master)")
	flag.IntVar(&migrationContext.InspectorConnectionConfig.Key.Port, "port", 3306, "MySQL port (preferably a replica, not the master)")
	flag.StringVar(&migrationContext.CliUser, "user", "", "MySQL user")
	flag.StringVar(&migrationContext.CliPassword, "password", "", "MySQL password")
	flag.StringVar(&migrationContext.ConfigFile, "conf", "", "Config file")

	flag.StringVar(&migrationContext.DatabaseName, "database", "", "database name (mandatory)")
	flag.StringVar(&migrationContext.OriginalTableName, "table", "", "table name (mandatory)")
	flag.StringVar(&migrationContext.AlterStatement, "alter", "", "alter statement (mandatory)")
	flag.BoolVar(&migrationContext.CountTableRows, "exact-rowcount", false, "actually count table rows as opposed to estimate them (results in more accurate progress estimation)")
	flag.BoolVar(&migrationContext.AllowedRunningOnMaster, "allow-on-master", false, "allow this migration to run directly on master. Preferably it would run on a replica")
	flag.BoolVar(&migrationContext.AllowedMasterMaster, "allow-master-master", false, "explicitly allow running in a master-master setup")
	flag.BoolVar(&migrationContext.NullableUniqueKeyAllowed, "allow-nullable-unique-key", false, "allow gh-ost to migrate based on a unique key with nullable columns. As long as no NULL values exist, this should be OK. If NULL values exist in chosen key, data may be corrupted. Use at your own risk!")
	flag.BoolVar(&migrationContext.ApproveRenamedColumns, "approve-renamed-columns", false, "in case your `ALTER` statement renames columns, gh-ost will note that and offer its interpretation of the rename. By default gh-ost does not proceed to execute. This flag approves that gh-ost's interpretation si correct")
	flag.BoolVar(&migrationContext.SkipRenamedColumns, "skip-renamed-columns", false, "in case your `ALTER` statement renames columns, gh-ost will note that and offer its interpretation of the rename. By default gh-ost does not proceed to execute. This flag tells gh-ost to skip the renamed columns, i.e. to treat what gh-ost thinks are renamed columns as unrelated columns. NOTE: you may lose column data")

	executeFlag := flag.Bool("execute", false, "actually execute the alter & migrate the table. Default is noop: do some tests and exit")
	flag.BoolVar(&migrationContext.TestOnReplica, "test-on-replica", false, "Have the migration run on a replica, not on the master. At the end of migration replication is stopped, and tables are swapped and immediately swap-revert. Replication remains stopped and you can compare the two tables for building trust")
	flag.BoolVar(&migrationContext.MigrateOnReplica, "migrate-on-replica", false, "Have the migration run on a replica, not on the master. This will do the full migration on the replica including cut-over (as opposed to --test-on-replica)")

	flag.BoolVar(&migrationContext.OkToDropTable, "ok-to-drop-table", false, "Shall the tool drop the old table at end of operation. DROPping tables can be a long locking operation, which is why I'm not doing it by default. I'm an online tool, yes?")
	flag.BoolVar(&migrationContext.InitiallyDropOldTable, "initially-drop-old-table", false, "Drop a possibly existing OLD table (remains from a previous run?) before beginning operation. Default is to panic and abort if such table exists")
	flag.BoolVar(&migrationContext.InitiallyDropGhostTable, "initially-drop-ghost-table", false, "Drop a possibly existing Ghost table (remains from a previous run?) before beginning operation. Default is to panic and abort if such table exists")
	cutOver := flag.String("cut-over", "atomic", "choose cut-over type (default|atomic, two-step)")

	flag.BoolVar(&migrationContext.SwitchToRowBinlogFormat, "switch-to-rbr", false, "let this tool automatically switch binary log format to 'ROW' on the replica, if needed. The format will NOT be switched back. I'm too scared to do that, and wish to protect you if you happen to execute another migration while this one is running")
	flag.BoolVar(&migrationContext.AssumeRBR, "assume-rbr", false, "set to 'true' when you know for certain your server uses 'ROW' binlog_format. gh-ost is unable to tell, event after reading binlog_format, whether the replication process does indeed use 'ROW', and restarts replication to be certain RBR setting is applied. Such operation requires SUPER privileges which you might not have. Setting this flag avoids restarting replication and you can proceed to use gh-ost without SUPER privileges")
	chunkSize := flag.Int64("chunk-size", 1000, "amount of rows to handle in each iteration (allowed range: 100-100,000)")
	defaultRetries := flag.Int64("default-retries", 60, "Default number of retries for various operations before panicking")
	cutOverLockTimeoutSeconds := flag.Int64("cut-over-lock-timeout-seconds", 3, "Max number of seconds to hold locks on tables while attempting to cut-over (retry attempted when lock exceeds timeout)")
	niceRatio := flag.Float64("nice-ratio", 0, "force being 'nice', imply sleep time per chunk time; range: [0.0..100.0]. Example values: 0 is aggressive. 1: for every 1ms spent copying rows, sleep additional 1ms (effectively doubling runtime); 0.7: for every 10ms spend in a rowcopy chunk, spend 7ms sleeping immediately after")

	maxLagMillis := flag.Int64("max-lag-millis", 1500, "replication lag at which to throttle operation")
	replicationLagQuery := flag.String("replication-lag-query", "", "Query that detects replication lag in seconds. Result can be a floating point (by default gh-ost issues SHOW SLAVE STATUS and reads Seconds_behind_master). If you're using pt-heartbeat, query would be something like: SELECT ROUND(UNIX_TIMESTAMP() - MAX(UNIX_TIMESTAMP(ts))) AS delay FROM my_schema.heartbeat")
	throttleControlReplicas := flag.String("throttle-control-replicas", "", "List of replicas on which to check for lag; comma delimited. Example: myhost1.com:3306,myhost2.com,myhost3.com:3307")
	throttleQuery := flag.String("throttle-query", "", "when given, issued (every second) to check if operation should throttle. Expecting to return zero for no-throttle, >0 for throttle. Query is issued on the migrated server. Make sure this query is lightweight")
	flag.StringVar(&migrationContext.ThrottleFlagFile, "throttle-flag-file", "", "operation pauses when this file exists; hint: use a file that is specific to the table being altered")
	flag.StringVar(&migrationContext.ThrottleAdditionalFlagFile, "throttle-additional-flag-file", "/tmp/gh-ost.throttle", "operation pauses when this file exists; hint: keep default, use for throttling multiple gh-ost operations")
	flag.StringVar(&migrationContext.PostponeCutOverFlagFile, "postpone-cut-over-flag-file", "", "while this file exists, migration will postpone the final stage of swapping tables, and will keep on syncing the ghost table. Cut-over/swapping would be ready to perform the moment the file is deleted.")
	flag.StringVar(&migrationContext.PanicFlagFile, "panic-flag-file", "", "when this file is created, gh-ost will immediately terminate, without cleanup")

	flag.BoolVar(&migrationContext.DropServeSocket, "initially-drop-socket-file", false, "Should gh-ost forcibly delete an existing socket file. Be careful: this might drop the socket file of a running migration!")
	flag.StringVar(&migrationContext.ServeSocketFile, "serve-socket-file", "", "Unix socket file to serve on. Default: auto-determined and advertised upon startup")
	flag.Int64Var(&migrationContext.ServeTCPPort, "serve-tcp-port", 0, "TCP port to serve on. Default: disabled")

	maxLoad := flag.String("max-load", "", "Comma delimited status-name=threshold. e.g: 'Threads_running=100,Threads_connected=500'. When status exceeds threshold, app throttles writes")
	criticalLoad := flag.String("critical-load", "", "Comma delimited status-name=threshold, same format as `--max-load`. When status exceeds threshold, app panics and quits")
	quiet := flag.Bool("quiet", false, "quiet")
	verbose := flag.Bool("verbose", false, "verbose")
	debug := flag.Bool("debug", false, "debug mode (very verbose)")
	stack := flag.Bool("stack", false, "add stack trace upon error")
	help := flag.Bool("help", false, "Display usage")
	version := flag.Bool("version", false, "Print version & exit")
	flag.Parse()

	if *help {
		fmt.Fprintf(os.Stderr, "Usage of gh-ost:\n")
		flag.PrintDefaults()
		return
	}
	if *version {
		appVersion := AppVersion
		if appVersion == "" {
			appVersion = "unversioned"
		}
		fmt.Println(appVersion)
		return
	}

	log.SetLevel(log.ERROR)
	if *verbose {
		log.SetLevel(log.INFO)
	}
	if *debug {
		log.SetLevel(log.DEBUG)
	}
	if *stack {
		log.SetPrintStackTrace(*stack)
	}
	if *quiet {
		// Override!!
		log.SetLevel(log.ERROR)
	}

	if migrationContext.DatabaseName == "" {
		log.Fatalf("--database must be provided and database name must not be empty")
	}
	if migrationContext.OriginalTableName == "" {
		log.Fatalf("--table must be provided and table name must not be empty")
	}
	if migrationContext.AlterStatement == "" {
		log.Fatalf("--alter must be provided and statement must not be empty")
	}
	migrationContext.Noop = !(*executeFlag)
	if migrationContext.AllowedRunningOnMaster && migrationContext.TestOnReplica {
		log.Fatalf("--allow-on-master and --test-on-replica are mutually exclusive")
	}
	if migrationContext.AllowedRunningOnMaster && migrationContext.MigrateOnReplica {
		log.Fatalf("--allow-on-master and --migrate-on-replica are mutually exclusive")
	}
	if migrationContext.MigrateOnReplica && migrationContext.TestOnReplica {
		log.Fatalf("--migrate-on-replica and --test-on-replica are mutually exclusive")
	}
	if migrationContext.SwitchToRowBinlogFormat && migrationContext.AssumeRBR {
		log.Fatalf("--switch-to-rbr and --assume-rbr are mutually exclusive")
	}
	switch *cutOver {
	case "atomic", "default", "":
		migrationContext.CutOverType = base.CutOverAtomic
	case "two-step":
		migrationContext.CutOverType = base.CutOverTwoStep
	default:
		log.Fatalf("Unknown cut-over: %s", *cutOver)
	}
	if err := migrationContext.ReadConfigFile(); err != nil {
		log.Fatale(err)
	}
	if err := migrationContext.ReadThrottleControlReplicaKeys(*throttleControlReplicas); err != nil {
		log.Fatale(err)
	}
	if err := migrationContext.ReadMaxLoad(*maxLoad); err != nil {
		log.Fatale(err)
	}
	if err := migrationContext.ReadCriticalLoad(*criticalLoad); err != nil {
		log.Fatale(err)
	}
	if migrationContext.ServeSocketFile == "" {
		migrationContext.ServeSocketFile = fmt.Sprintf("/tmp/gh-ost.%s.%s.sock", migrationContext.DatabaseName, migrationContext.OriginalTableName)
	}
	migrationContext.SetNiceRatio(*niceRatio)
	migrationContext.SetChunkSize(*chunkSize)
	migrationContext.SetMaxLagMillisecondsThrottleThreshold(*maxLagMillis)
	migrationContext.SetReplicationLagQuery(*replicationLagQuery)
	migrationContext.SetThrottleQuery(*throttleQuery)
	migrationContext.SetDefaultNumRetries(*defaultRetries)
	migrationContext.ApplyCredentials()
	if err := migrationContext.SetCutOverLockTimeoutSeconds(*cutOverLockTimeoutSeconds); err != nil {
		log.Errore(err)
	}

	log.Infof("starting gh-ost %+v", AppVersion)
	acceptSignals(migrationContext)

	migrator := logic.NewMigrator()
	err := migrator.Migrate()
	if err != nil {
		log.Fatale(err)
	}
	log.Info("Done")
}
