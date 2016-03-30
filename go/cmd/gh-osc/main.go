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
	"github.com/outbrain/golib/log"
)

// main is the application's entry point. It will either spawn a CLI or HTTP itnerfaces.
func main() {
	mysqlBasedir := flag.String("mysql-basedir", "", "the --basedir config for MySQL (auto-detected if not given)")
	mysqlDatadir := flag.String("mysql-datadir", "", "the --datadir config for MySQL (auto-detected if not given)")
	internalExperiment := flag.Bool("internal-experiment", false, "issue an internal experiment")
	binlogFile := flag.String("binlog-file", "", "Name of binary log file")
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
		binlogReader := binlog.NewMySQLBinlogReader(*mysqlBasedir, *mysqlDatadir)
		binlogReader.ReadEntries(*binlogFile, 0, 0)
	}
}
