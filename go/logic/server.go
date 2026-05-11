/*
   Copyright 2021 GitHub Inc.
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

func (srv *Server) runCPUProfile(args string) (io.Reader, error) {
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

	if atomic.LoadInt64(&srv.isCPUProfiling) > 0 {
		return nil, ErrCPUProfilingInProgress
	}
	atomic.StoreInt64(&srv.isCPUProfiling, 1)
	defer atomic.StoreInt64(&srv.isCPUProfiling, 0)

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
	srv.migrationContext.Log.Infof("Captured %d byte runtime/pprof CPU profile (gzip=%v)", buf.Len(), useGzip)
	return &buf, nil
}

func (srv *Server) createPostponeCutOverFlagFile(filePath string) (err error) {
	if !base.FileExists(filePath) {
		if err := base.TouchFile(filePath); err != nil {
			return fmt.Errorf("failed to create postpone cut-over flag file %s: %w", filePath, err)
		}
		srv.migrationContext.Log.Infof("Created postpone-cut-over-flag-file: %s", filePath)
	}
	return nil
}

func (srv *Server) BindSocketFile() (err error) {
	if srv.migrationContext.ServeSocketFile == "" {
		return nil
	}
	if srv.migrationContext.DropServeSocket && base.FileExists(srv.migrationContext.ServeSocketFile) {
		os.Remove(srv.migrationContext.ServeSocketFile)
	}
	srv.unixListener, err = net.Listen("unix", srv.migrationContext.ServeSocketFile)
	if err != nil {
		return err
	}
	srv.migrationContext.Log.Infof("Listening on unix socket file: %s", srv.migrationContext.ServeSocketFile)
	return nil
}

func (srv *Server) RemoveSocketFile() (err error) {
	srv.migrationContext.Log.Infof("Removing socket file: %s", srv.migrationContext.ServeSocketFile)
	return os.Remove(srv.migrationContext.ServeSocketFile)
}

func (srv *Server) BindTCPPort() (err error) {
	if srv.migrationContext.ServeTCPPort == 0 {
		return nil
	}
	srv.tcpListener, err = net.Listen("tcp", fmt.Sprintf(":%d", srv.migrationContext.ServeTCPPort))
	if err != nil {
		return err
	}
	srv.migrationContext.Log.Infof("Listening on tcp port: %d", srv.migrationContext.ServeTCPPort)
	return nil
}

// Serve begins listening & serving on whichever device was configured
func (srv *Server) Serve() (err error) {
	go func() {
		for {
			conn, err := srv.unixListener.Accept()
			if err != nil {
				srv.migrationContext.Log.Errore(err)
			}
			go srv.handleConnection(conn)
		}
	}()
	go func() {
		if srv.tcpListener == nil {
			return
		}
		for {
			conn, err := srv.tcpListener.Accept()
			if err != nil {
				srv.migrationContext.Log.Errore(err)
			}
			go srv.handleConnection(conn)
		}
	}()

	return nil
}

func (srv *Server) handleConnection(conn net.Conn) (err error) {
	if conn != nil {
		defer conn.Close()
	}
	command, _, err := bufio.NewReader(conn).ReadLine()
	if err != nil {
		return err
	}
	return srv.onServerCommand(string(command), bufio.NewWriter(conn))
}

// onServerCommand responds to a user's interactive command
func (srv *Server) onServerCommand(command string, writer *bufio.Writer) (err error) {
	defer writer.Flush()

	printStatusRule, err := srv.applyServerCommand(command, writer)
	if err == nil {
		srv.printStatus(printStatusRule, writer)
	} else {
		fmt.Fprintf(writer, "%s\n", err.Error())
	}
	return srv.migrationContext.Log.Errore(err)
}

// applyServerCommand parses and executes commands by user
func (srv *Server) applyServerCommand(command string, writer *bufio.Writer) (printStatusRule PrintStatusRule, err error) {
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

	if err := srv.hooksExecutor.onInteractiveCommand(command); err != nil {
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
postpone-cut-over-flag-file=<path>   # Postpone the cut-over phase, writing a cut over flag file to the given path
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
		cpuProfile, err := srv.runCPUProfile(arg)
		if err == nil {
			fmt.Fprint(base64.NewEncoder(base64.StdEncoding, writer), cpuProfile)
		}
		return NoPrintStatusRule, err
	case "coordinates":
		{
			if argIsQuestion || arg == "" {
				fmt.Fprintf(writer, "%+v\n", srv.migrationContext.GetRecentBinlogCoordinates())
				return NoPrintStatusRule, nil
			}
			return NoPrintStatusRule, fmt.Errorf("coordinates are read-only")
		}
	case "applier":
		if srv.migrationContext.ApplierConnectionConfig != nil && srv.migrationContext.ApplierConnectionConfig.ImpliedKey != nil {
			fmt.Fprintf(writer, "Host: %s, Version: %s\n",
				srv.migrationContext.ApplierConnectionConfig.ImpliedKey.String(),
				srv.migrationContext.ApplierMySQLVersion,
			)
		}
		return NoPrintStatusRule, nil
	case "inspector":
		if srv.migrationContext.InspectorConnectionConfig != nil && srv.migrationContext.InspectorConnectionConfig.ImpliedKey != nil {
			fmt.Fprintf(writer, "Host: %s, Version: %s\n",
				srv.migrationContext.InspectorConnectionConfig.ImpliedKey.String(),
				srv.migrationContext.InspectorMySQLVersion,
			)
		}
		return NoPrintStatusRule, nil
	case "chunk-size":
		{
			if argIsQuestion {
				fmt.Fprintf(writer, "%+v\n", atomic.LoadInt64(&srv.migrationContext.ChunkSize))
				return NoPrintStatusRule, nil
			}
			if chunkSize, err := strconv.Atoi(arg); err != nil {
				return NoPrintStatusRule, err
			} else {
				srv.migrationContext.SetChunkSize(int64(chunkSize))
				return ForcePrintStatusAndHintRule, nil
			}
		}
	case "dml-batch-size":
		{
			if argIsQuestion {
				fmt.Fprintf(writer, "%+v\n", atomic.LoadInt64(&srv.migrationContext.DMLBatchSize))
				return NoPrintStatusRule, nil
			}
			if dmlBatchSize, err := strconv.Atoi(arg); err != nil {
				return NoPrintStatusRule, err
			} else {
				srv.migrationContext.SetDMLBatchSize(int64(dmlBatchSize))
				return ForcePrintStatusAndHintRule, nil
			}
		}
	case "max-lag-millis":
		{
			if argIsQuestion {
				fmt.Fprintf(writer, "%+v\n", atomic.LoadInt64(&srv.migrationContext.MaxLagMillisecondsThrottleThreshold))
				return NoPrintStatusRule, nil
			}
			if maxLagMillis, err := strconv.Atoi(arg); err != nil {
				return NoPrintStatusRule, err
			} else {
				srv.migrationContext.SetMaxLagMillisecondsThrottleThreshold(int64(maxLagMillis))
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
				fmt.Fprintf(writer, "%+v\n", srv.migrationContext.GetNiceRatio())
				return NoPrintStatusRule, nil
			}
			if niceRatio, err := strconv.ParseFloat(arg, 64); err != nil {
				return NoPrintStatusRule, err
			} else {
				srv.migrationContext.SetNiceRatio(niceRatio)
				return ForcePrintStatusAndHintRule, nil
			}
		}
	case "max-load":
		{
			if argIsQuestion {
				maxLoad := srv.migrationContext.GetMaxLoad()
				fmt.Fprintf(writer, "%s\n", maxLoad.String())
				return NoPrintStatusRule, nil
			}
			if err := srv.migrationContext.ReadMaxLoad(arg); err != nil {
				return NoPrintStatusRule, err
			}
			return ForcePrintStatusAndHintRule, nil
		}
	case "critical-load":
		{
			if argIsQuestion {
				criticalLoad := srv.migrationContext.GetCriticalLoad()
				fmt.Fprintf(writer, "%s\n", criticalLoad.String())
				return NoPrintStatusRule, nil
			}
			if err := srv.migrationContext.ReadCriticalLoad(arg); err != nil {
				return NoPrintStatusRule, err
			}
			return ForcePrintStatusAndHintRule, nil
		}
	case "throttle-query":
		{
			if argIsQuestion {
				fmt.Fprintf(writer, "%+v\n", srv.migrationContext.GetThrottleQuery())
				return NoPrintStatusRule, nil
			}
			srv.migrationContext.SetThrottleQuery(arg)
			fmt.Fprintln(writer, throttleHint)
			return ForcePrintStatusAndHintRule, nil
		}
	case "throttle-http":
		{
			if argIsQuestion {
				fmt.Fprintf(writer, "%+v\n", srv.migrationContext.GetThrottleHTTP())
				return NoPrintStatusRule, nil
			}
			srv.migrationContext.SetThrottleHTTP(arg)
			fmt.Fprintln(writer, throttleHint)
			return ForcePrintStatusAndHintRule, nil
		}
	case "throttle-control-replicas":
		{
			if argIsQuestion {
				fmt.Fprintf(writer, "%s\n", srv.migrationContext.GetThrottleControlReplicaKeys().ToCommaDelimitedList())
				return NoPrintStatusRule, nil
			}
			if err := srv.migrationContext.ReadThrottleControlReplicaKeys(arg); err != nil {
				return NoPrintStatusRule, err
			}
			fmt.Fprintf(writer, "%s\n", srv.migrationContext.GetThrottleControlReplicaKeys().ToCommaDelimitedList())
			return ForcePrintStatusAndHintRule, nil
		}
	case "throttle", "pause", "suspend":
		{
			if arg != "" && arg != srv.migrationContext.OriginalTableName {
				// User explicitly provided table name. This is a courtesy protection mechanism
				err := fmt.Errorf("user commanded 'throttle' on %s, but migrated table is %s; ignoring request", arg, srv.migrationContext.OriginalTableName)
				return NoPrintStatusRule, err
			}
			atomic.StoreInt64(&srv.migrationContext.ThrottleCommandedByUser, 1)
			fmt.Fprintln(writer, throttleHint)
			return ForcePrintStatusAndHintRule, nil
		}
	case "no-throttle", "unthrottle", "resume", "continue":
		{
			if arg != "" && arg != srv.migrationContext.OriginalTableName {
				// User explicitly provided table name. This is a courtesy protection mechanism
				err := fmt.Errorf("user commanded 'no-throttle' on %s, but migrated table is %s; ignoring request", arg, srv.migrationContext.OriginalTableName)
				return NoPrintStatusRule, err
			}
			atomic.StoreInt64(&srv.migrationContext.ThrottleCommandedByUser, 0)
			return ForcePrintStatusAndHintRule, nil
		}
	case "postpone-cut-over-flag-file":
		{
			if arg == "" {
				err := fmt.Errorf("user commanded 'postpone-cut-over-flag-file' without specifying file path")
				return NoPrintStatusRule, err
			}
			if err := srv.createPostponeCutOverFlagFile(arg); err != nil {
				return NoPrintStatusRule, err
			}
			srv.migrationContext.PostponeCutOverFlagFile = arg
			fmt.Fprintf(writer, "Postponed\n")
			return ForcePrintStatusAndHintRule, nil
		}
	case "unpostpone", "no-postpone", "cut-over":
		{
			if arg == "" && srv.migrationContext.ForceNamedCutOverCommand {
				err := fmt.Errorf("user commanded 'unpostpone' without specifying table name, but --force-named-cut-over is set")
				return NoPrintStatusRule, err
			}
			if arg != "" && arg != srv.migrationContext.OriginalTableName {
				// User explicitly provided table name. This is a courtesy protection mechanism
				err := fmt.Errorf("user commanded 'unpostpone' on %s, but migrated table is %s; ignoring request", arg, srv.migrationContext.OriginalTableName)
				return NoPrintStatusRule, err
			}
			if atomic.LoadInt64(&srv.migrationContext.IsPostponingCutOver) > 0 {
				atomic.StoreInt64(&srv.migrationContext.UserCommandedUnpostponeFlag, 1)
				fmt.Fprintf(writer, "Unpostponed\n")
				return ForcePrintStatusAndHintRule, nil
			}
			fmt.Fprintf(writer, "You may only invoke this when gh-ost is actively postponing migration. At this time it is not\n")
			return NoPrintStatusRule, nil
		}
	case "panic":
		{
			if arg == "" && srv.migrationContext.ForceNamedPanicCommand {
				err := fmt.Errorf("user commanded 'panic' without specifying table name, but --force-named-panic is set")
				return NoPrintStatusRule, err
			}
			if arg != "" && arg != srv.migrationContext.OriginalTableName {
				// User explicitly provided table name. This is a courtesy protection mechanism
				err := fmt.Errorf("user commanded 'panic' on %s, but migrated table is %s; ignoring request", arg, srv.migrationContext.OriginalTableName)
				return NoPrintStatusRule, err
			}
			err := fmt.Errorf("user commanded 'panic'. The migration will be aborted without cleanup. Please drop the gh-ost tables before trying again")
			// Use helper to prevent deadlock if listenOnPanicAbort already exited
			_ = base.SendWithContext(srv.migrationContext.GetContext(), srv.migrationContext.PanicAbort, err)
			return NoPrintStatusRule, err
		}
	default:
		err = fmt.Errorf("unknown command: %s", command)
		return NoPrintStatusRule, err
	}
	return NoPrintStatusRule, nil
}
