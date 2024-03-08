/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/github/gh-ost/go/base"
)

var (
	ErrCPUProfilingBadOption  = errors.New("unrecognized cpu profiling option")
	ErrCPUProfilingInProgress = errors.New("cpu profiling already in progress")
	defaultCPUProfileDuration = time.Second * 30
)

type printStatusFunc func(PrintStatusRule, io.Writer)

// Server listens for requests on a socket file or via TCP
type Server struct {
	migrationContext *base.MigrationContext
	unixListener     net.Listener
	tcpListener      net.Listener
	hooksExecutor    *HooksExecutor
	printStatus      printStatusFunc
	isCPUProfiling   int64
}

func NewServer(migrationContext *base.MigrationContext, hooksExecutor *HooksExecutor, printStatus printStatusFunc) *Server {
	return &Server{
		migrationContext: migrationContext,
		hooksExecutor:    hooksExecutor,
		printStatus:      printStatus,
	}
}

func (this *Server) runCPUProfile(args string) (io.Reader, error) {
	duration := defaultCPUProfileDuration

	var err error
	var blockProfile, useGzip bool
	if args != "" {
		s := strings.Split(args, ",")
		// a duration string must be the 1st field, if any
		if duration, err = time.ParseDuration(s[0]); err != nil {
			return nil, err
		}
		for _, arg := range s[1:] {
			switch arg {
			case "block", "blocked", "blocking":
				blockProfile = true
			case "gzip":
				useGzip = true
			default:
				return nil, ErrCPUProfilingBadOption
			}
		}
	}

	if atomic.LoadInt64(&this.isCPUProfiling) > 0 {
		return nil, ErrCPUProfilingInProgress
	}
	atomic.StoreInt64(&this.isCPUProfiling, 1)
	defer atomic.StoreInt64(&this.isCPUProfiling, 0)

	var buf bytes.Buffer
	var writer io.Writer = &buf
	if blockProfile {
		runtime.SetBlockProfileRate(1)
		defer runtime.SetBlockProfileRate(0)
	}
	if useGzip {
		writer = gzip.NewWriter(writer)
	}
	if err = pprof.StartCPUProfile(writer); err != nil {
		return nil, err
	}

	time.Sleep(duration)
	pprof.StopCPUProfile()
	this.migrationContext.Log.Infof("Captured %d byte runtime/pprof CPU profile (gzip=%v)", buf.Len(), useGzip)
	return &buf, nil
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
	this.migrationContext.Log.Infof("Listening on unix socket file: %s", this.migrationContext.ServeSocketFile)
	return nil
}

func (this *Server) RemoveSocketFile() (err error) {
	this.migrationContext.Log.Infof("Removing socket file: %s", this.migrationContext.ServeSocketFile)
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
	this.migrationContext.Log.Infof("Listening on tcp port: %d", this.migrationContext.ServeTCPPort)
	return nil
}

// Serve begins listening & serving on whichever device was configured
func (this *Server) Serve() (err error) {
	go func() {
		for {
			conn, err := this.unixListener.Accept()
			if err != nil {
				this.migrationContext.Log.Errore(err)
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
				this.migrationContext.Log.Errore(err)
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
	return this.migrationContext.Log.Errore(err)
}

// applyServerCommand parses and executes commands by user
func (this *Server) applyServerCommand(command string, writer *bufio.Writer) (printStatusRule PrintStatusRule, err error) {
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
	throttleHint := "# Note: you may only throttle for as long as your binary logs are not purged"

	if err := this.hooksExecutor.onInteractiveCommand(command); err != nil {
		return NoPrintStatusRule, err
	}

	switch command {
	case "help":
		{
			fmt.Fprint(writer, `available commands:
status                               # Print a detailed status message
sup                                  # Print a short status message
cpu-profile=<options>                # Print a base64-encoded runtime/pprof CPU profile using a duration, default: 30s. Comma-separated options 'gzip' and/or 'block' (blocked profile) may follow the profile duration
coordinates                          # Print the currently inspected coordinates
applier                              # Print the hostname of the applier
inspector                            # Print the hostname of the inspector
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
	case "cpu-profile":
		cpuProfile, err := this.runCPUProfile(arg)
		if err == nil {
			fmt.Fprint(base64.NewEncoder(base64.StdEncoding, writer), cpuProfile)
		}
		return NoPrintStatusRule, err
	case "coordinates":
		{
			if argIsQuestion || arg == "" {
				fmt.Fprintf(writer, "%+v\n", this.migrationContext.GetRecentBinlogCoordinates())
				return NoPrintStatusRule, nil
			}
			return NoPrintStatusRule, fmt.Errorf("coordinates are read-only")
		}
	case "applier":
		if this.migrationContext.ApplierConnectionConfig != nil && this.migrationContext.ApplierConnectionConfig.ImpliedKey != nil {
			fmt.Fprintf(writer, "Host: %s, Version: %s\n",
				this.migrationContext.ApplierConnectionConfig.ImpliedKey.String(),
				this.migrationContext.ApplierMySQLVersion,
			)
		}
		return NoPrintStatusRule, nil
	case "inspector":
		if this.migrationContext.InspectorConnectionConfig != nil && this.migrationContext.InspectorConnectionConfig.ImpliedKey != nil {
			fmt.Fprintf(writer, "Host: %s, Version: %s\n",
				this.migrationContext.InspectorConnectionConfig.ImpliedKey.String(),
				this.migrationContext.InspectorMySQLVersion,
			)
		}
		return NoPrintStatusRule, nil
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
			fmt.Fprintln(writer, throttleHint)
			return ForcePrintStatusAndHintRule, nil
		}
	case "throttle-http":
		{
			if argIsQuestion {
				fmt.Fprintf(writer, "%+v\n", this.migrationContext.GetThrottleHTTP())
				return NoPrintStatusRule, nil
			}
			this.migrationContext.SetThrottleHTTP(arg)
			fmt.Fprintln(writer, throttleHint)
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
			if arg != "" && arg != this.migrationContext.OriginalTableName {
				// User explicitly provided table name. This is a courtesy protection mechanism
				err := fmt.Errorf("User commanded 'throttle' on %s, but migrated table is %s; ignoring request.", arg, this.migrationContext.OriginalTableName)
				return NoPrintStatusRule, err
			}
			atomic.StoreInt64(&this.migrationContext.ThrottleCommandedByUser, 1)
			fmt.Fprintln(writer, throttleHint)
			return ForcePrintStatusAndHintRule, nil
		}
	case "no-throttle", "unthrottle", "resume", "continue":
		{
			if arg != "" && arg != this.migrationContext.OriginalTableName {
				// User explicitly provided table name. This is a courtesy protection mechanism
				err := fmt.Errorf("User commanded 'no-throttle' on %s, but migrated table is %s; ignoring request.", arg, this.migrationContext.OriginalTableName)
				return NoPrintStatusRule, err
			}
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
			if arg == "" && this.migrationContext.ForceNamedPanicCommand {
				err := fmt.Errorf("User commanded 'panic' without specifying table name, but --force-named-panic is set")
				return NoPrintStatusRule, err
			}
			if arg != "" && arg != this.migrationContext.OriginalTableName {
				// User explicitly provided table name. This is a courtesy protection mechanism
				err := fmt.Errorf("User commanded 'panic' on %s, but migrated table is %s; ignoring request.", arg, this.migrationContext.OriginalTableName)
				return NoPrintStatusRule, err
			}
			err := fmt.Errorf("User commanded 'panic'. The migration will be aborted without cleanup. Please drop the gh-ost tables before trying again.")
			this.migrationContext.PanicAbort <- err
			return NoPrintStatusRule, err
		}
	default:
		err = fmt.Errorf("Unknown command: %s", command)
		return NoPrintStatusRule, err
	}
	return NoPrintStatusRule, nil
}
