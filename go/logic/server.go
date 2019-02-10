/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/github/gh-ost/go/base"
	"github.com/outbrain/golib/log"
)

type printStatusFunc func(PrintStatusRule, io.Writer)

// Server listens for requests on a socket file or via TCP
type Server struct {
	migrationContext *base.MigrationContext
	unixListener     net.Listener
	tcpListener      net.Listener
	hooksExecutor    *HooksExecutor
	printStatus      printStatusFunc
}

func NewServer(migrationContext *base.MigrationContext, hooksExecutor *HooksExecutor, printStatus printStatusFunc) *Server {
	return &Server{
		migrationContext: migrationContext,
		hooksExecutor:    hooksExecutor,
		printStatus:      printStatus,
	}
}

func (this *Server) BindSocketFile() (err error) {
	if this.migrationContext.ServeSocketFile == "" {
		return nil
	}
	if this.migrationContext.DropServeSocket && base.FileExists(this.migrationContext.ServeSocketFile) {
		os.Remove(this.migrationContext.ServeSocketFile)
	}
	this.unixListener, err = net.Listen("unix", this.migrationContext.ServeSocketFile)
	if err != nil {
		return err
	}
	log.Infof("Listening on unix socket file: %s", this.migrationContext.ServeSocketFile)
	return nil
}

func (this *Server) RemoveSocketFile() (err error) {
	log.Infof("Removing socket file: %s", this.migrationContext.ServeSocketFile)
	return os.Remove(this.migrationContext.ServeSocketFile)
}

func (this *Server) BindTCPPort() (err error) {
	if this.migrationContext.ServeTCPPort == 0 {
		return nil
	}
	this.tcpListener, err = net.Listen("tcp", fmt.Sprintf(":%d", this.migrationContext.ServeTCPPort))
	if err != nil {
		return err
	}
	log.Infof("Listening on tcp port: %d", this.migrationContext.ServeTCPPort)
	return nil
}

// Serve begins listening & serving on whichever device was configured
func (this *Server) Serve() (err error) {
	go func() {
		for {
			conn, err := this.unixListener.Accept()
			if err != nil {
				log.Errore(err)
			}
			go this.handleConnection(conn)
		}
	}()
	go func() {
		if this.tcpListener == nil {
			return
		}
		for {
			conn, err := this.tcpListener.Accept()
			if err != nil {
				log.Errore(err)
			}
			go this.handleConnection(conn)
		}
	}()

	return nil
}

func (this *Server) handleConnection(conn net.Conn) (err error) {
	if conn != nil {
		defer conn.Close()
	}
	command, _, err := bufio.NewReader(conn).ReadLine()
	if err != nil {
		return err
	}
	return this.onServerCommand(string(command), bufio.NewWriter(conn))
}

// onServerCommand responds to a user's interactive command
func (this *Server) onServerCommand(command string, writer *bufio.Writer) (err error) {
	defer writer.Flush()

	printStatusRule, err := this.applyServerCommand(command, writer)
	if err == nil {
		this.printStatus(printStatusRule, writer)
	} else {
		fmt.Fprintf(writer, "%s\n", err.Error())
	}
	return log.Errore(err)
}

// applyServerCommand parses and executes commands by user
func (this *Server) applyServerCommand(command string, writer *bufio.Writer) (printStatusRule PrintStatusRule, err error) {
	printStatusRule = NoPrintStatusRule

	tokens := strings.SplitN(command, "=", 2)
	command = strings.TrimSpace(tokens[0])
	arg := ""
	if len(tokens) > 1 {
		arg = strings.TrimSpace(tokens[1])
		if unquoted, err := strconv.Unquote(arg); err == nil {
			arg = unquoted
		}
	}
	argIsQuestion := (arg == "?")
	throttleHint := "# Note: you may only throttle for as long as your binary logs are not purged\n"

	if err := this.hooksExecutor.onInteractiveCommand(command); err != nil {
		return NoPrintStatusRule, err
	}

	switch command {
	case "help":
		{
			fmt.Fprintln(writer, `available commands:
status                               # Print a detailed status message
sup                                  # Print a short status message
coordinates													 # Print the currently inspected coordinates
chunk-size=<newsize>                 # Set a new chunk-size
dml-batch-size=<newsize>             # Set a new dml-batch-size
nice-ratio=<ratio>                   # Set a new nice-ratio, immediate sleep after each row-copy operation, float (examples: 0 is aggressive, 0.7 adds 70% runtime, 1.0 doubles runtime, 2.0 triples runtime, ...)
critical-load=<load>                 # Set a new set of max-load thresholds
max-lag-millis=<max-lag>             # Set a new replication lag threshold
replication-lag-query=<query>        # Set a new query that determines replication lag (no quotes)
max-load=<load>                      # Set a new set of max-load thresholds
throttle-query=<query>               # Set a new throttle-query (no quotes)
throttle-http=<URL>                  # Set a new throttle URL
throttle-control-replicas=<replicas> # Set a new comma delimited list of throttle control replicas
throttle                             # Force throttling
no-throttle                          # End forced throttling (other throttling may still apply)
unpostpone                           # Bail out a cut-over postpone; proceed to cut-over
panic                                # panic and quit without cleanup
help                                 # This message
- use '?' (question mark) as argument to get info rather than set. e.g. "max-load=?" will just print out current max-load.
`)
		}
	case "sup":
		return ForcePrintStatusOnlyRule, nil
	case "info", "status":
		return ForcePrintStatusAndHintRule, nil
	case "coordinates":
		{
			if argIsQuestion || arg == "" {
				fmt.Fprintf(writer, "%+v\n", this.migrationContext.GetRecentBinlogCoordinates())
				return NoPrintStatusRule, nil
			}
			return NoPrintStatusRule, fmt.Errorf("coordinates are read-only")
		}
	case "chunk-size":
		{
			if argIsQuestion {
				fmt.Fprintf(writer, "%+v\n", atomic.LoadInt64(&this.migrationContext.ChunkSize))
				return NoPrintStatusRule, nil
			}
			if chunkSize, err := strconv.Atoi(arg); err != nil {
				return NoPrintStatusRule, err
			} else {
				this.migrationContext.SetChunkSize(int64(chunkSize))
				return ForcePrintStatusAndHintRule, nil
			}
		}
	case "dml-batch-size":
		{
			if argIsQuestion {
				fmt.Fprintf(writer, "%+v\n", atomic.LoadInt64(&this.migrationContext.DMLBatchSize))
				return NoPrintStatusRule, nil
			}
			if dmlBatchSize, err := strconv.Atoi(arg); err != nil {
				return NoPrintStatusRule, err
			} else {
				this.migrationContext.SetDMLBatchSize(int64(dmlBatchSize))
				return ForcePrintStatusAndHintRule, nil
			}
		}
	case "max-lag-millis":
		{
			if argIsQuestion {
				fmt.Fprintf(writer, "%+v\n", atomic.LoadInt64(&this.migrationContext.MaxLagMillisecondsThrottleThreshold))
				return NoPrintStatusRule, nil
			}
			if maxLagMillis, err := strconv.Atoi(arg); err != nil {
				return NoPrintStatusRule, err
			} else {
				this.migrationContext.SetMaxLagMillisecondsThrottleThreshold(int64(maxLagMillis))
				return ForcePrintStatusAndHintRule, nil
			}
		}
	case "replication-lag-query":
		{
			return NoPrintStatusRule, fmt.Errorf("replication-lag-query is deprecated. gh-ost uses an internal, subsecond resolution query")
		}
	case "nice-ratio":
		{
			if argIsQuestion {
				fmt.Fprintf(writer, "%+v\n", this.migrationContext.GetNiceRatio())
				return NoPrintStatusRule, nil
			}
			if niceRatio, err := strconv.ParseFloat(arg, 64); err != nil {
				return NoPrintStatusRule, err
			} else {
				this.migrationContext.SetNiceRatio(niceRatio)
				return ForcePrintStatusAndHintRule, nil
			}
		}
	case "max-load":
		{
			if argIsQuestion {
				maxLoad := this.migrationContext.GetMaxLoad()
				fmt.Fprintf(writer, "%s\n", maxLoad.String())
				return NoPrintStatusRule, nil
			}
			if err := this.migrationContext.ReadMaxLoad(arg); err != nil {
				return NoPrintStatusRule, err
			}
			return ForcePrintStatusAndHintRule, nil
		}
	case "critical-load":
		{
			if argIsQuestion {
				criticalLoad := this.migrationContext.GetCriticalLoad()
				fmt.Fprintf(writer, "%s\n", criticalLoad.String())
				return NoPrintStatusRule, nil
			}
			if err := this.migrationContext.ReadCriticalLoad(arg); err != nil {
				return NoPrintStatusRule, err
			}
			return ForcePrintStatusAndHintRule, nil
		}
	case "throttle-query":
		{
			if argIsQuestion {
				fmt.Fprintf(writer, "%+v\n", this.migrationContext.GetThrottleQuery())
				return NoPrintStatusRule, nil
			}
			this.migrationContext.SetThrottleQuery(arg)
			fmt.Fprintf(writer, throttleHint)
			return ForcePrintStatusAndHintRule, nil
		}
	case "throttle-http":
		{
			if argIsQuestion {
				fmt.Fprintf(writer, "%+v\n", this.migrationContext.GetThrottleHTTP())
				return NoPrintStatusRule, nil
			}
			this.migrationContext.SetThrottleHTTP(arg)
			fmt.Fprintf(writer, throttleHint)
			return ForcePrintStatusAndHintRule, nil
		}
	case "throttle-control-replicas":
		{
			if argIsQuestion {
				fmt.Fprintf(writer, "%s\n", this.migrationContext.GetThrottleControlReplicaKeys().ToCommaDelimitedList())
				return NoPrintStatusRule, nil
			}
			if err := this.migrationContext.ReadThrottleControlReplicaKeys(arg); err != nil {
				return NoPrintStatusRule, err
			}
			fmt.Fprintf(writer, "%s\n", this.migrationContext.GetThrottleControlReplicaKeys().ToCommaDelimitedList())
			return ForcePrintStatusAndHintRule, nil
		}
	case "throttle", "pause", "suspend":
		{
			atomic.StoreInt64(&this.migrationContext.ThrottleCommandedByUser, 1)
			fmt.Fprintf(writer, throttleHint)
			return ForcePrintStatusAndHintRule, nil
		}
	case "no-throttle", "unthrottle", "resume", "continue":
		{
			atomic.StoreInt64(&this.migrationContext.ThrottleCommandedByUser, 0)
			return ForcePrintStatusAndHintRule, nil
		}
	case "unpostpone", "no-postpone", "cut-over":
		{
			if arg == "" && this.migrationContext.ForceNamedCutOverCommand {
				err := fmt.Errorf("User commanded 'unpostpone' without specifying table name, but --force-named-cut-over is set")
				return NoPrintStatusRule, err
			}
			if arg != "" && arg != this.migrationContext.OriginalTableName {
				// User explicitly provided table name. This is a courtesy protection mechanism
				err := fmt.Errorf("User commanded 'unpostpone' on %s, but migrated table is %s; ignoring request.", arg, this.migrationContext.OriginalTableName)
				return NoPrintStatusRule, err
			}
			if atomic.LoadInt64(&this.migrationContext.IsPostponingCutOver) > 0 {
				atomic.StoreInt64(&this.migrationContext.UserCommandedUnpostponeFlag, 1)
				fmt.Fprintf(writer, "Unpostponed\n")
				return ForcePrintStatusAndHintRule, nil
			}
			fmt.Fprintf(writer, "You may only invoke this when gh-ost is actively postponing migration. At this time it is not.\n")
			return NoPrintStatusRule, nil
		}
	case "panic":
		{
			err := fmt.Errorf("User commanded 'panic'. I will now panic, without cleanup. PANIC!")
			this.migrationContext.PanicAbort <- err
			return NoPrintStatusRule, err
		}
	default:
		err = fmt.Errorf("Unknown command: %s", command)
		return NoPrintStatusRule, err
	}
	return NoPrintStatusRule, nil
}
