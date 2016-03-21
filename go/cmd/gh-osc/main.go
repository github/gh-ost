package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/outbrain/golib/log"
)

// main is the application's entry point. It will either spawn a CLI or HTTP itnerfaces.
func main() {
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
}
