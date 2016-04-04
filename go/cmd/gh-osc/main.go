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
	"github.com/github/gh-osc/go/binlog"
	"github.com/github/gh-osc/go/logic"
	"github.com/outbrain/golib/log"
)

// main is the application's entry point. It will either spawn a CLI or HTTP itnerfaces.
func main() {
	migrationContext := base.GetMigrationContext()

	// mysqlBasedir := flag.String("mysql-basedir", "", "the --basedir config for MySQL (auto-detected if not given)")
	// mysqlDatadir := flag.String("mysql-datadir", "", "the --datadir config for MySQL (auto-detected if not given)")
	internalExperiment := flag.Bool("internal-experiment", false, "issue an internal experiment")
	binlogFile := flag.String("binlog-file", "", "Name of binary log file")

	flag.StringVar(&migrationContext.InspectorConnectionConfig.Key.Hostname, "host", "127.0.0.1", "MySQL hostname (preferably a replica, not the master)")
	flag.IntVar(&migrationContext.InspectorConnectionConfig.Key.Port, "port", 3306, "MySQL port (preferably a replica, not the master)")
	flag.StringVar(&migrationContext.InspectorConnectionConfig.User, "user", "root", "MySQL user")
	flag.StringVar(&migrationContext.InspectorConnectionConfig.Password, "password", "", "MySQL password")

	flag.StringVar(&migrationContext.DatabaseName, "database", "", "database name (mandatory)")
	flag.StringVar(&migrationContext.OriginalTableName, "table", "", "table name (mandatory)")
	flag.StringVar(&migrationContext.AlterStatement, "alter", "", "alter statement (mandatory)")
	flag.BoolVar(&migrationContext.CountTableRows, "exact-rowcount", false, "actually count table rows as opposed to estimate them (results in more accurate progress estimation)")
	flag.BoolVar(&migrationContext.AllowedRunningOnMaster, "allow-on-master", false, "allow this migration to run directly on master. Preferably it would run on a replica")

	flag.IntVar(&migrationContext.ChunkSize, "chunk-size", 1000, "amount of rows to handle in each iteration")

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

	log.Info("starting gh-osc")

	if *internalExperiment {
		log.Debug("starting experiment")
		var binlogReader binlog.BinlogReader
		var err error

		//binlogReader = binlog.NewMySQLBinlogReader(*mysqlBasedir, *mysqlDatadir)
		binlogReader, err = binlog.NewGoMySQLReader(migrationContext.InspectorConnectionConfig)
		if err != nil {
			log.Fatale(err)
		}
		binlogReader.ReadEntries(*binlogFile, 0, 0)
		return
	}
	migrator := logic.NewMigrator()
	err := migrator.Migrate()
	if err != nil {
		log.Fatale(err)
	}
	log.Info("Done")
}
