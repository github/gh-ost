/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-osc/blob/master/LICENSE
*/

package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/github/gh-osc/go/binlog"
	"github.com/github/gh-osc/go/mysql"
	"github.com/outbrain/golib/log"
)

// main is the application's entry point. It will either spawn a CLI or HTTP itnerfaces.
func main() {
	var connectionConfig mysql.ConnectionConfig

	// mysqlBasedir := flag.String("mysql-basedir", "", "the --basedir config for MySQL (auto-detected if not given)")
	// mysqlDatadir := flag.String("mysql-datadir", "", "the --datadir config for MySQL (auto-detected if not given)")
	internalExperiment := flag.Bool("internal-experiment", false, "issue an internal experiment")
	binlogFile := flag.String("binlog-file", "", "Name of binary log file")

	flag.StringVar(&connectionConfig.Hostname, "host", "127.0.0.1", "MySQL hostname (preferably a replica, not the master)")
	flag.IntVar(&connectionConfig.Port, "port", 3306, "MySQL port (preferably a replica, not the master)")
	flag.StringVar(&connectionConfig.User, "user", "root", "MySQL user")
	flag.StringVar(&connectionConfig.Password, "password", "", "MySQL password")

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
	log.Info("starting gh-osc")

	if *internalExperiment {
		log.Debug("starting experiment")
		var binlogReader binlog.BinlogReader
		var err error

		//binlogReader = binlog.NewMySQLBinlogReader(*mysqlBasedir, *mysqlDatadir)
		binlogReader, err = binlog.NewGoMySQLReader(&connectionConfig)
		if err != nil {
			log.Fatale(err)
		}
		binlogReader.ReadEntries(*binlogFile, 0, 0)
	}
}
