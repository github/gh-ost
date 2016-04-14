/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-osc/blob/master/LICENSE
*/

package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/github/gh-osc/go/base"
	"github.com/github/gh-osc/go/logic"
	"github.com/outbrain/golib/log"
)

// main is the application's entry point. It will either spawn a CLI or HTTP itnerfaces.
func main() {
	migrationContext := base.GetMigrationContext()

	flag.StringVar(&migrationContext.InspectorConnectionConfig.Key.Hostname, "host", "127.0.0.1", "MySQL hostname (preferably a replica, not the master)")
	flag.IntVar(&migrationContext.InspectorConnectionConfig.Key.Port, "port", 3306, "MySQL port (preferably a replica, not the master)")
	flag.StringVar(&migrationContext.InspectorConnectionConfig.User, "user", "root", "MySQL user")
	flag.StringVar(&migrationContext.InspectorConnectionConfig.Password, "password", "", "MySQL password")

	flag.StringVar(&migrationContext.DatabaseName, "database", "", "database name (mandatory)")
	flag.StringVar(&migrationContext.OriginalTableName, "table", "", "table name (mandatory)")
	flag.StringVar(&migrationContext.AlterStatement, "alter", "", "alter statement (mandatory)")
	flag.BoolVar(&migrationContext.CountTableRows, "exact-rowcount", false, "actually count table rows as opposed to estimate them (results in more accurate progress estimation)")
	flag.BoolVar(&migrationContext.AllowedRunningOnMaster, "allow-on-master", false, "allow this migration to run directly on master. Preferably it would run on a replica")

	executeFlag := flag.Bool("execute", false, "actually execute the alter & migrate the table. Default is noop: do some tests and exit")
	flag.BoolVar(&migrationContext.TestOnReplica, "test-on-replica", false, "Have the migration run on a replica, not on the master. At the end of migration tables are not swapped; gh-osc issues `STOP SLAVE` and you can compare the two tables for building trust")

	flag.Int64Var(&migrationContext.ChunkSize, "chunk-size", 1000, "amount of rows to handle in each iteration (allowed range: 100-100,000)")
	if migrationContext.ChunkSize < 100 {
		migrationContext.ChunkSize = 100
	}
	if migrationContext.ChunkSize > 100000 {
		migrationContext.ChunkSize = 100000
	}
	flag.Int64Var(&migrationContext.MaxLagMillisecondsThrottleThreshold, "max-lag-millis", 1500, "replication lag at which to throttle operation")
	flag.StringVar(&migrationContext.ThrottleFlagFile, "throttle-flag-file", "", "operation pauses when this file exists; hint: use a file that is specific to the table being altered")
	flag.StringVar(&migrationContext.ThrottleAdditionalFlagFile, "throttle-additional-flag-file", "/tmp/gh-osc.throttle", "operation pauses when this file exists; hint: keep default, use for throttling multiple gh-osc operations")
	maxLoad := flag.String("max-load", "", "Comma delimited status-name=threshold. e.g: 'Threads_running=100,Threads_connected=500'")
	quiet := flag.Bool("quiet", false, "quiet")
	verbose := flag.Bool("verbose", false, "verbose")
	debug := flag.Bool("debug", false, "debug mode (very verbose)")
	stack := flag.Bool("stack", false, "add stack trace upon error")
	help := flag.Bool("help", false, "Display usage")
	flag.Parse()

	if *help {
		fmt.Fprintf(os.Stderr, "Usage of gh-osc:\n")
		flag.PrintDefaults()
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
	if err := migrationContext.ReadMaxLoad(*maxLoad); err != nil {
		log.Fatale(err)
	}

	log.Info("starting gh-osc")

	migrator := logic.NewMigrator()
	err := migrator.Migrate()
	if err != nil {
		log.Fatale(err)
	}
	log.Info("Done")
}
