/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/binlog"
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"

	"github.com/outbrain/golib/log"
)

type ChangelogState string

const (
	TablesInPlace              ChangelogState = "TablesInPlace"
	AllEventsUpToLockProcessed                = "AllEventsUpToLockProcessed"
)

type tableWriteFunc func() error

const (
	applyEventsQueueBuffer        = 100
	heartbeatIntervalMilliseconds = 1000
)

type PrintStatusRule int

const (
	HeuristicPrintStatusRule PrintStatusRule = iota
	ForcePrintStatusRule                     = iota
	ForcePrintStatusAndHint                  = iota
)

// Migrator is the main schema migration flow manager.
type Migrator struct {
	parser           *sql.Parser
	inspector        *Inspector
	applier          *Applier
	eventsStreamer   *EventsStreamer
	server           *Server
	migrationContext *base.MigrationContext
	hostname         string

	tablesInPlace              chan bool
	rowCopyComplete            chan bool
	allEventsUpToLockProcessed chan bool
	panicAbort                 chan error

	rowCopyCompleteFlag                    int64
	allEventsUpToLockProcessedInjectedFlag int64
	inCutOverCriticalActionFlag            int64
	cleanupImminentFlag                    int64
	userCommandedUnpostponeFlag            int64
	// copyRowsQueue should not be buffered; if buffered some non-damaging but
	//  excessive work happens at the end of the iteration as new copy-jobs arrive befroe realizing the copy is complete
	copyRowsQueue    chan tableWriteFunc
	applyEventsQueue chan tableWriteFunc

	handledChangelogStates map[string]bool
}

func NewMigrator() *Migrator {
	migrator := &Migrator{
		migrationContext:           base.GetMigrationContext(),
		parser:                     sql.NewParser(),
		tablesInPlace:              make(chan bool),
		rowCopyComplete:            make(chan bool),
		allEventsUpToLockProcessed: make(chan bool),
		panicAbort:                 make(chan error),

		allEventsUpToLockProcessedInjectedFlag: 0,

		copyRowsQueue:          make(chan tableWriteFunc),
		applyEventsQueue:       make(chan tableWriteFunc, applyEventsQueueBuffer),
		handledChangelogStates: make(map[string]bool),
	}
	return migrator
}

// acceptSignals registers for OS signals
func (this *Migrator) acceptSignals() {
	c := make(chan os.Signal, 1)

	signal.Notify(c, syscall.SIGHUP)
	go func() {
		for sig := range c {
			switch sig {
			case syscall.SIGHUP:
				log.Debugf("Received SIGHUP. Reloading configuration")
			}
		}
	}()
}

// shouldThrottle performs checks to see whether we should currently be throttling.
// It also checks for critical-load and panic aborts.
func (this *Migrator) shouldThrottle() (result bool, reason string) {
	// Regardless of throttle, we take opportunity to check for panic-abort
	if this.migrationContext.PanicFlagFile != "" {
		if base.FileExists(this.migrationContext.PanicFlagFile) {
			this.panicAbort <- fmt.Errorf("Found panic-file %s. Aborting without cleanup", this.migrationContext.PanicFlagFile)
		}
	}
	criticalLoad := this.migrationContext.GetCriticalLoad()
	for variableName, threshold := range criticalLoad {
		value, err := this.applier.ShowStatusVariable(variableName)
		if err != nil {
			return true, fmt.Sprintf("%s %s", variableName, err)
		}
		if value >= threshold {
			this.panicAbort <- fmt.Errorf("critical-load met: %s=%d, >=%d", variableName, value, threshold)
		}
	}

	// Back to throttle considerations

	// User-based throttle
	if atomic.LoadInt64(&this.migrationContext.ThrottleCommandedByUser) > 0 {
		return true, "commanded by user"
	}
	if this.migrationContext.ThrottleFlagFile != "" {
		if base.FileExists(this.migrationContext.ThrottleFlagFile) {
			// Throttle file defined and exists!
			return true, "flag-file"
		}
	}
	if this.migrationContext.ThrottleAdditionalFlagFile != "" {
		if base.FileExists(this.migrationContext.ThrottleAdditionalFlagFile) {
			// 2nd Throttle file defined and exists!
			return true, "flag-file"
		}
	}
	// Replication lag throttle
	maxLagMillisecondsThrottleThreshold := atomic.LoadInt64(&this.migrationContext.MaxLagMillisecondsThrottleThreshold)
	lag := atomic.LoadInt64(&this.migrationContext.CurrentLag)
	if time.Duration(lag) > time.Duration(maxLagMillisecondsThrottleThreshold)*time.Millisecond {
		return true, fmt.Sprintf("lag=%fs", time.Duration(lag).Seconds())
	}
	checkThrottleControlReplicas := true
	if (this.migrationContext.TestOnReplica || this.migrationContext.MigrateOnReplica) && (atomic.LoadInt64(&this.allEventsUpToLockProcessedInjectedFlag) > 0) {
		checkThrottleControlReplicas = false
	}
	if checkThrottleControlReplicas {
		lagResult := mysql.GetMaxReplicationLag(this.migrationContext.InspectorConnectionConfig, this.migrationContext.GetThrottleControlReplicaKeys(), this.migrationContext.GetReplicationLagQuery())
		if lagResult.Err != nil {
			return true, fmt.Sprintf("%+v %+v", lagResult.Key, lagResult.Err)
		}
		if lagResult.Lag > time.Duration(maxLagMillisecondsThrottleThreshold)*time.Millisecond {
			return true, fmt.Sprintf("%+v replica-lag=%fs", lagResult.Key, lagResult.Lag.Seconds())
		}
	}

	maxLoad := this.migrationContext.GetMaxLoad()
	for variableName, threshold := range maxLoad {
		value, err := this.applier.ShowStatusVariable(variableName)
		if err != nil {
			return true, fmt.Sprintf("%s %s", variableName, err)
		}
		if value >= threshold {
			return true, fmt.Sprintf("max-load %s=%d >= %d", variableName, value, threshold)
		}
	}
	if this.migrationContext.GetThrottleQuery() != "" {
		if res, _ := this.applier.ExecuteThrottleQuery(); res > 0 {
			return true, "throttle-query"
		}
	}

	return false, ""
}

// initiateThrottler initiates the throttle ticker and sets the basic behavior of throttling.
func (this *Migrator) initiateThrottler() error {
	throttlerTick := time.Tick(1 * time.Second)

	throttlerFunction := func() {
		alreadyThrottling, currentReason := this.migrationContext.IsThrottled()
		shouldThrottle, throttleReason := this.shouldThrottle()
		if shouldThrottle && !alreadyThrottling {
			// New throttling
			this.applier.WriteAndLogChangelog("throttle", throttleReason)
		} else if shouldThrottle && alreadyThrottling && (currentReason != throttleReason) {
			// Change of reason
			this.applier.WriteAndLogChangelog("throttle", throttleReason)
		} else if alreadyThrottling && !shouldThrottle {
			// End of throttling
			this.applier.WriteAndLogChangelog("throttle", "done throttling")
		}
		this.migrationContext.SetThrottled(shouldThrottle, throttleReason)
	}
	throttlerFunction()
	for range throttlerTick {
		throttlerFunction()
	}

	return nil
}

// throttle initiates a throttling event, if need be, updates the Context and
// calls callback functions, if any
func (this *Migrator) throttle(onThrottled func()) {
	for {
		// IsThrottled() is non-blocking; the throttling decision making takes place asynchronously.
		// Therefore calling IsThrottled() is cheap
		if shouldThrottle, _ := this.migrationContext.IsThrottled(); !shouldThrottle {
			return
		}
		if onThrottled != nil {
			onThrottled()
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// sleepWhileTrue sleeps indefinitely until the given function returns 'false'
// (or fails with error)
func (this *Migrator) sleepWhileTrue(operation func() (bool, error)) error {
	for {
		shouldSleep, err := operation()
		if err != nil {
			return err
		}
		if !shouldSleep {
			return nil
		}
		time.Sleep(time.Second)
	}
}

// retryOperation attempts up to `count` attempts at running given function,
// exiting as soon as it returns with non-error.
func (this *Migrator) retryOperation(operation func() error, notFatalHint ...bool) (err error) {
	maxRetries := int(this.migrationContext.MaxRetries())
	for i := 0; i < maxRetries; i++ {
		if i != 0 {
			// sleep after previous iteration
			time.Sleep(1 * time.Second)
		}
		err = operation()
		if err == nil {
			return nil
		}
		// there's an error. Let's try again.
	}
	if len(notFatalHint) == 0 {
		this.panicAbort <- err
	}
	return err
}

// executeAndThrottleOnError executes a given function. If it errors, it
// throttles.
func (this *Migrator) executeAndThrottleOnError(operation func() error) (err error) {
	if err := operation(); err != nil {
		this.throttle(nil)
		return err
	}
	return nil
}

// consumeRowCopyComplete blocks on the rowCopyComplete channel once, and then
// consumers and drops any further incoming events that may be left hanging.
func (this *Migrator) consumeRowCopyComplete() {
	<-this.rowCopyComplete
	atomic.StoreInt64(&this.rowCopyCompleteFlag, 1)
	this.migrationContext.MarkRowCopyEndTime()
	go func() {
		for <-this.rowCopyComplete {
		}
	}()
}

func (this *Migrator) canStopStreaming() bool {
	return false
}

// onChangelogStateEvent is called when a binlog event operation on the changelog table is intercepted.
func (this *Migrator) onChangelogStateEvent(dmlEvent *binlog.BinlogDMLEvent) (err error) {
	// Hey, I created the changlog table, I know the type of columns it has!
	if hint := dmlEvent.NewColumnValues.StringColumn(2); hint != "state" {
		return nil
	}
	changelogState := ChangelogState(dmlEvent.NewColumnValues.StringColumn(3))
	switch changelogState {
	case TablesInPlace:
		{
			this.tablesInPlace <- true
		}
	case AllEventsUpToLockProcessed:
		{
			applyEventFunc := func() error {
				this.allEventsUpToLockProcessed <- true
				return nil
			}
			// at this point we know all events up to lock have been read from the streamer,
			// because the streamer works sequentially. So those events are either already handled,
			// or have event functions in applyEventsQueue.
			// So as not to create a potential deadlock, we write this func to applyEventsQueue
			// asynchronously, understanding it doesn't really matter.
			go func() {
				this.applyEventsQueue <- applyEventFunc
			}()
		}
	default:
		{
			return fmt.Errorf("Unknown changelog state: %+v", changelogState)
		}
	}
	log.Debugf("Received state %+v", changelogState)
	return nil
}

// onChangelogHeartbeat is called when a heartbeat event is intercepted
func (this *Migrator) onChangelogHeartbeat(heartbeatValue string) (err error) {
	heartbeatTime, err := time.Parse(time.RFC3339Nano, heartbeatValue)
	if err != nil {
		return log.Errore(err)
	}
	lag := time.Since(heartbeatTime)

	atomic.StoreInt64(&this.migrationContext.CurrentLag, int64(lag))

	return nil
}

// listenOnPanicAbort aborts on abort request
func (this *Migrator) listenOnPanicAbort() {
	err := <-this.panicAbort
	log.Fatale(err)
}

// validateStatement validates the `alter` statement meets criteria.
// At this time this means:
// - column renames are approved
func (this *Migrator) validateStatement() (err error) {
	if this.parser.HasNonTrivialRenames() && !this.migrationContext.SkipRenamedColumns {
		this.migrationContext.ColumnRenameMap = this.parser.GetNonTrivialRenames()
		if !this.migrationContext.ApproveRenamedColumns {
			return fmt.Errorf("Alter statement has column(s) renamed. gh-ost suspects the following renames: %v; but to proceed you must approve via `--approve-renamed-columns` (or you can skip renamed columns via `--skip-renamed-columns`)", this.parser.GetNonTrivialRenames())
		}
		log.Infof("Alter statement has column(s) renamed. gh-ost finds the following renames: %v; --approve-renamed-columns is given and so migration proceeds.", this.parser.GetNonTrivialRenames())
	}
	return nil
}

// Migrate executes the complete migration logic. This is *the* major gh-ost function.
func (this *Migrator) Migrate() (err error) {
	log.Infof("Migrating %s.%s", sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.OriginalTableName))
	this.migrationContext.StartTime = time.Now()
	if this.hostname, err = os.Hostname(); err != nil {
		return err
	}

	go this.listenOnPanicAbort()

	if err := this.parser.ParseAlterStatement(this.migrationContext.AlterStatement); err != nil {
		return err
	}
	if err := this.validateStatement(); err != nil {
		return err
	}
	if err := this.initiateInspector(); err != nil {
		return err
	}
	if err := this.initiateStreaming(); err != nil {
		return err
	}
	if err := this.initiateApplier(); err != nil {
		return err
	}

	log.Debugf("Waiting for tables to be in place")
	<-this.tablesInPlace
	log.Debugf("Tables are in place")
	// Yay! We now know the Ghost and Changelog tables are good to examine!
	// When running on replica, this means the replica has those tables. When running
	// on master this is always true, of course, and yet it also implies this knowledge
	// is in the binlogs.
	if err := this.inspector.InspectOriginalAndGhostTables(); err != nil {
		return err
	}
	if err := this.initiateServer(); err != nil {
		return err
	}
	defer this.server.RemoveSocketFile()

	if this.migrationContext.CountTableRows {
		if this.migrationContext.Noop {
			log.Debugf("Noop operation; not really counting table rows")
		} else if err := this.inspector.CountTableRows(); err != nil {
			return err
		}
	}

	if err := this.addDMLEventsListener(); err != nil {
		return err
	}
	go this.initiateHeartbeatListener()

	if err := this.applier.ReadMigrationRangeValues(); err != nil {
		return err
	}
	go this.initiateThrottler()
	go this.executeWriteFuncs()
	go this.iterateChunks()
	this.migrationContext.MarkRowCopyStartTime()
	go this.initiateStatus()

	log.Debugf("Operating until row copy is complete")
	this.consumeRowCopyComplete()
	log.Infof("Row copy complete")
	this.printStatus(ForcePrintStatusRule)

	if err := this.cutOver(); err != nil {
		return err
	}

	if err := this.finalCleanup(); err != nil {
		return nil
	}
	log.Infof("Done migrating %s.%s", sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.OriginalTableName))
	return nil
}

// cutOver performs the final step of migration, based on migration
// type (on replica? bumpy? safe?)
func (this *Migrator) cutOver() (err error) {
	if this.migrationContext.Noop {
		log.Debugf("Noop operation; not really swapping tables")
		return nil
	}
	this.migrationContext.MarkPointOfInterest()
	this.throttle(func() {
		log.Debugf("throttling before swapping tables")
	})

	this.migrationContext.MarkPointOfInterest()
	this.sleepWhileTrue(
		func() (bool, error) {
			if this.migrationContext.PostponeCutOverFlagFile == "" {
				return false, nil
			}
			if atomic.LoadInt64(&this.userCommandedUnpostponeFlag) > 0 {
				return false, nil
			}
			if base.FileExists(this.migrationContext.PostponeCutOverFlagFile) {
				// Throttle file defined and exists!
				atomic.StoreInt64(&this.migrationContext.IsPostponingCutOver, 1)
				//log.Debugf("Postponing final table swap as flag file exists: %+v", this.migrationContext.PostponeCutOverFlagFile)
				return true, nil
			}
			return false, nil
		},
	)
	atomic.StoreInt64(&this.migrationContext.IsPostponingCutOver, 0)
	this.migrationContext.MarkPointOfInterest()

	if this.migrationContext.TestOnReplica {
		// With `--test-on-replica` we stop replication thread, and then proceed to use
		// the same cut-over phase as the master would use. That means we take locks
		// and swap the tables.
		// The difference is that we will later swap the tables back.
		log.Debugf("testing on replica. Stopping replication IO thread")
		if err := this.retryOperation(this.applier.StopReplication); err != nil {
			return err
		}
		// We're merly testing, we don't want to keep this state. Rollback the renames as possible
		defer this.applier.RenameTablesRollback()
		// We further proceed to do the cutover by normal means; the 'defer' above will rollback the swap
	}
	if this.migrationContext.CutOverType == base.CutOverAtomic {
		// Atomic solution: we use low timeout and multiple attempts. But for
		// each failed attempt, we throttle until replication lag is back to normal
		err := this.retryOperation(
			func() error {
				return this.executeAndThrottleOnError(this.atomicCutOver)
			},
		)
		return err
	}
	if this.migrationContext.CutOverType == base.CutOverTwoStep {
		err := this.retryOperation(
			func() error {
				return this.executeAndThrottleOnError(this.cutOverTwoStep)
			},
		)
		return err
	}
	return log.Fatalf("Unknown cut-over type: %d; should never get here!", this.migrationContext.CutOverType)
}

// Inject the "AllEventsUpToLockProcessed" state hint, wait for it to appear in the binary logs,
// make sure the queue is drained.
func (this *Migrator) waitForEventsUpToLock() (err error) {
	this.migrationContext.MarkPointOfInterest()
	waitForEventsUpToLockStartTime := time.Now()

	log.Infof("Writing changelog state: %+v", AllEventsUpToLockProcessed)
	if _, err := this.applier.WriteChangelogState(string(AllEventsUpToLockProcessed)); err != nil {
		return err
	}
	log.Infof("Waiting for events up to lock")
	atomic.StoreInt64(&this.allEventsUpToLockProcessedInjectedFlag, 1)
	<-this.allEventsUpToLockProcessed
	waitForEventsUpToLockDuration := time.Since(waitForEventsUpToLockStartTime)

	log.Infof("Done waiting for events up to lock; duration=%+v", waitForEventsUpToLockDuration)
	this.printStatus(ForcePrintStatusAndHint)

	return nil
}

// cutOverTwoStep will lock down the original table, execute
// what's left of last DML entries, and **non-atomically** swap original->old, then new->original.
// There is a point in time where the "original" table does not exist and queries are non-blocked
// and failing.
func (this *Migrator) cutOverTwoStep() (err error) {
	atomic.StoreInt64(&this.inCutOverCriticalActionFlag, 1)
	defer atomic.StoreInt64(&this.inCutOverCriticalActionFlag, 0)
	atomic.StoreInt64(&this.allEventsUpToLockProcessedInjectedFlag, 0)

	if err := this.retryOperation(this.applier.LockOriginalTable); err != nil {
		return err
	}

	if err := this.retryOperation(this.waitForEventsUpToLock); err != nil {
		return err
	}
	if err := this.retryOperation(this.applier.SwapTablesQuickAndBumpy); err != nil {
		return err
	}
	if err := this.retryOperation(this.applier.UnlockTables); err != nil {
		return err
	}

	lockAndRenameDuration := this.migrationContext.RenameTablesEndTime.Sub(this.migrationContext.LockTablesStartTime)
	renameDuration := this.migrationContext.RenameTablesEndTime.Sub(this.migrationContext.RenameTablesStartTime)
	log.Debugf("Lock & rename duration: %s (rename only: %s). During this time, queries on %s were locked or failing", lockAndRenameDuration, renameDuration, sql.EscapeName(this.migrationContext.OriginalTableName))
	return nil
}

// atomicCutOver
func (this *Migrator) atomicCutOver() (err error) {
	atomic.StoreInt64(&this.inCutOverCriticalActionFlag, 1)
	defer atomic.StoreInt64(&this.inCutOverCriticalActionFlag, 0)

	defer func() {
		this.applier.DropAtomicCutOverSentryTableIfExists()
	}()

	atomic.StoreInt64(&this.allEventsUpToLockProcessedInjectedFlag, 0)

	lockOriginalSessionIdChan := make(chan int64, 2)
	tableLocked := make(chan error, 2)
	okToUnlockTable := make(chan bool, 3)
	tableUnlocked := make(chan error, 2)
	go func() {
		if err := this.applier.AtomicCutOverMagicLock(lockOriginalSessionIdChan, tableLocked, okToUnlockTable, tableUnlocked); err != nil {
			log.Errore(err)
		}
	}()
	if err := <-tableLocked; err != nil {
		return log.Errore(err)
	}
	lockOriginalSessionId := <-lockOriginalSessionIdChan
	log.Infof("Session locking original & magic tables is %+v", lockOriginalSessionId)
	// At this point we know the original table is locked.
	// We know any newly incoming DML on original table is blocked.
	this.waitForEventsUpToLock()

	// Step 2
	// We now attempt an atomic RENAME on original & ghost tables, and expect it to block.
	this.migrationContext.RenameTablesStartTime = time.Now()

	var tableRenameKnownToHaveFailed int64
	renameSessionIdChan := make(chan int64, 2)
	tablesRenamed := make(chan error, 2)
	go func() {
		if err := this.applier.AtomicCutoverRename(renameSessionIdChan, tablesRenamed); err != nil {
			// Abort! Release the lock
			atomic.StoreInt64(&tableRenameKnownToHaveFailed, 1)
			okToUnlockTable <- true
		}
	}()
	renameSessionId := <-renameSessionIdChan
	log.Infof("Session renaming tables is %+v", renameSessionId)

	waitForRename := func() error {
		if atomic.LoadInt64(&tableRenameKnownToHaveFailed) == 1 {
			// We return `nil` here so as to avoid the `retry`. The RENAME has failed,
			// it won't show up in PROCESSLIST, no point in waiting
			return nil
		}
		return this.applier.ExpectProcess(renameSessionId, "metadata lock", "rename")
	}
	// Wait for the RENAME to appear in PROCESSLIST
	if err := this.retryOperation(waitForRename, true); err != nil {
		// Abort! Release the lock
		okToUnlockTable <- true
		return err
	}
	if atomic.LoadInt64(&tableRenameKnownToHaveFailed) == 0 {
		log.Infof("Found atomic RENAME to be blocking, as expected. Double checking the lock is still in place (though I don't strictly have to)")
	}
	if err := this.applier.ExpectUsedLock(lockOriginalSessionId); err != nil {
		// Abort operation. Just make sure to drop the magic table.
		return log.Errore(err)
	}
	log.Infof("Connection holding lock on original table still exists")

	// Now that we've found the RENAME blocking, AND the locking connection still alive,
	// we know it is safe to proceed to release the lock

	okToUnlockTable <- true
	// BAM! magic table dropped, original table lock is released
	// -> RENAME released -> queries on original are unblocked.
	if err := <-tableUnlocked; err != nil {
		return log.Errore(err)
	}
	if err := <-tablesRenamed; err != nil {
		return log.Errore(err)
	}
	this.migrationContext.RenameTablesEndTime = time.Now()

	// ooh nice! We're actually truly and thankfully done
	lockAndRenameDuration := this.migrationContext.RenameTablesEndTime.Sub(this.migrationContext.LockTablesStartTime)
	log.Infof("Lock & rename duration: %s. During this time, queries on %s were blocked", lockAndRenameDuration, sql.EscapeName(this.migrationContext.OriginalTableName))
	return nil
}

// onServerCommand responds to a user's interactive command
func (this *Migrator) onServerCommand(command string, writer *bufio.Writer) (err error) {
	defer writer.Flush()

	tokens := strings.SplitN(command, "=", 2)
	command = strings.TrimSpace(tokens[0])
	arg := ""
	if len(tokens) > 1 {
		arg = strings.TrimSpace(tokens[1])
	}

	throttleHint := "# Note: you may only throttle for as long as your binary logs are not purged\n"

	switch command {
	case "help":
		{
			fmt.Fprintln(writer, `available commands:
status                               # Print a status message
chunk-size=<newsize>                 # Set a new chunk-size
nice-ratio=<ratio>                   # Set a new nice-ratio, immediate sleep after each row-copy operation, float (examples: 0 is agrressive, 0.7 adds 70% runtime, 1.0 doubles runtime, 2.0 triples runtime, ...)
critical-load=<load>                 # Set a new set of max-load thresholds
max-lag-millis=<max-lag>             # Set a new replication lag threshold
replication-lag-query=<query>        # Set a new query that determines replication lag (no quotes)
max-load=<load>                      # Set a new set of max-load thresholds
throttle-query=<query>               # Set a new throttle-query (no quotes)
throttle-control-replicas=<replicas> # Set a new comma delimited list of throttle control replicas
throttle                             # Force throttling
no-throttle                          # End forced throttling (other throttling may still apply)
unpostpone                           # Bail out a cut-over postpone; proceed to cut-over
panic                                # panic and quit without cleanup
help                                 # This message
`)
		}
	case "info", "status":
		this.printStatus(ForcePrintStatusAndHint, writer)
	case "chunk-size":
		{
			if chunkSize, err := strconv.Atoi(arg); err != nil {
				fmt.Fprintf(writer, "%s\n", err.Error())
				return log.Errore(err)
			} else {
				this.migrationContext.SetChunkSize(int64(chunkSize))
				this.printStatus(ForcePrintStatusAndHint, writer)
			}
		}
	case "max-lag-millis":
		{
			if maxLagMillis, err := strconv.Atoi(arg); err != nil {
				fmt.Fprintf(writer, "%s\n", err.Error())
				return log.Errore(err)
			} else {
				this.migrationContext.SetMaxLagMillisecondsThrottleThreshold(int64(maxLagMillis))
				this.printStatus(ForcePrintStatusAndHint, writer)
			}
		}
	case "replication-lag-query":
		{
			this.migrationContext.SetReplicationLagQuery(arg)
			this.printStatus(ForcePrintStatusAndHint, writer)
		}
	case "nice-ratio":
		{
			if niceRatio, err := strconv.ParseFloat(arg, 64); err != nil {
				fmt.Fprintf(writer, "%s\n", err.Error())
				return log.Errore(err)
			} else {
				this.migrationContext.SetNiceRatio(niceRatio)
				this.printStatus(ForcePrintStatusAndHint, writer)
			}
		}
	case "max-load":
		{
			if err := this.migrationContext.ReadMaxLoad(arg); err != nil {
				fmt.Fprintf(writer, "%s\n", err.Error())
				return log.Errore(err)
			}
			this.printStatus(ForcePrintStatusAndHint, writer)
		}
	case "critical-load":
		{
			if err := this.migrationContext.ReadCriticalLoad(arg); err != nil {
				fmt.Fprintf(writer, "%s\n", err.Error())
				return log.Errore(err)
			}
			this.printStatus(ForcePrintStatusAndHint, writer)
		}
	case "throttle-query":
		{
			this.migrationContext.SetThrottleQuery(arg)
			fmt.Fprintf(writer, throttleHint)
			this.printStatus(ForcePrintStatusAndHint, writer)
		}
	case "throttle-control-replicas":
		{
			if err := this.migrationContext.ReadThrottleControlReplicaKeys(arg); err != nil {
				fmt.Fprintf(writer, "%s\n", err.Error())
				return log.Errore(err)
			}
			fmt.Fprintf(writer, "%s\n", this.migrationContext.GetThrottleControlReplicaKeys().ToCommaDelimitedList())
			this.printStatus(ForcePrintStatusAndHint, writer)
		}
	case "throttle", "pause", "suspend":
		{
			atomic.StoreInt64(&this.migrationContext.ThrottleCommandedByUser, 1)
			fmt.Fprintf(writer, throttleHint)
			this.printStatus(ForcePrintStatusAndHint, writer)
		}
	case "no-throttle", "unthrottle", "resume", "continue":
		{
			atomic.StoreInt64(&this.migrationContext.ThrottleCommandedByUser, 0)
		}
	case "unpostpone", "no-postpone", "cut-over":
		{
			if atomic.LoadInt64(&this.migrationContext.IsPostponingCutOver) > 0 {
				atomic.StoreInt64(&this.userCommandedUnpostponeFlag, 1)
				fmt.Fprintf(writer, "Unpostponed\n")
			} else {
				fmt.Fprintf(writer, "You may only invoke this when gh-ost is actively postponing migration. At this time it is not.\n")
			}
		}
	case "panic":
		{
			err := fmt.Errorf("User commanded 'panic'. I will now panic, without cleanup. PANIC!")
			fmt.Fprintf(writer, "%s\n", err.Error())
			this.panicAbort <- err
		}
	default:
		err = fmt.Errorf("Unknown command: %s", command)
		fmt.Fprintf(writer, "%s\n", err.Error())
		return err
	}
	return nil
}

// initiateServer begins listening on unix socket/tcp for incoming interactive commands
func (this *Migrator) initiateServer() (err error) {
	this.server = NewServer(this.onServerCommand)
	if err := this.server.BindSocketFile(); err != nil {
		return err
	}
	if err := this.server.BindTCPPort(); err != nil {
		return err
	}

	go this.server.Serve()
	return nil
}

// initiateInspector connects, validates and inspects the "inspector" server.
// The "inspector" server is typically a replica; it is where we issue some
// queries such as:
// - table row count
// - schema validation
// - heartbeat
// When `--allow-on-master` is supplied, the inspector is actually the master.
func (this *Migrator) initiateInspector() (err error) {
	this.inspector = NewInspector()
	if err := this.inspector.InitDBConnections(); err != nil {
		return err
	}
	if err := this.inspector.ValidateOriginalTable(); err != nil {
		return err
	}
	if err := this.inspector.InspectOriginalTable(); err != nil {
		return err
	}
	// So far so good, table is accessible and valid.
	// Let's get master connection config
	if this.migrationContext.ApplierConnectionConfig, err = this.inspector.getMasterConnectionConfig(); err != nil {
		return err
	}
	if this.migrationContext.TestOnReplica || this.migrationContext.MigrateOnReplica {
		if this.migrationContext.InspectorIsAlsoApplier() {
			return fmt.Errorf("Instructed to --test-on-replica or --migrate-on-replica, but the server we connect to doesn't seem to be a replica")
		}
		log.Infof("--test-on-replica or --migrate-on-replica given. Will not execute on master %+v but rather on replica %+v itself",
			*this.migrationContext.ApplierConnectionConfig.ImpliedKey, *this.migrationContext.InspectorConnectionConfig.ImpliedKey,
		)
		this.migrationContext.ApplierConnectionConfig = this.migrationContext.InspectorConnectionConfig.Duplicate()
		if this.migrationContext.GetThrottleControlReplicaKeys().Len() == 0 {
			this.migrationContext.AddThrottleControlReplicaKey(this.migrationContext.InspectorConnectionConfig.Key)
		}
	} else if this.migrationContext.InspectorIsAlsoApplier() && !this.migrationContext.AllowedRunningOnMaster {
		return fmt.Errorf("It seems like this migration attempt to run directly on master. Preferably it would be executed on a replica (and this reduces load from the master). To proceed please provide --allow-on-master")
	}
	if err := this.inspector.validateLogSlaveUpdates(); err != nil {
		return err
	}

	log.Infof("Master found to be %+v", *this.migrationContext.ApplierConnectionConfig.ImpliedKey)
	return nil
}

// initiateStatus sets and activates the printStatus() ticker
func (this *Migrator) initiateStatus() error {
	this.printStatus(ForcePrintStatusAndHint)
	statusTick := time.Tick(1 * time.Second)
	for range statusTick {
		go this.printStatus(HeuristicPrintStatusRule)
	}

	return nil
}

// printMigrationStatusHint prints a detailed configuration dump, that is useful
// to keep in mind; such as the name of migrated table, throttle params etc.
// This gets printed at beginning and end of migration, every 10 minutes throughout
// migration, and as reponse to the "status" interactive command.
func (this *Migrator) printMigrationStatusHint(writers ...io.Writer) {
	w := io.MultiWriter(writers...)
	fmt.Fprintln(w, fmt.Sprintf("# Migrating %s.%s; Ghost table is %s.%s",
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.OriginalTableName),
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetGhostTableName()),
	))
	fmt.Fprintln(w, fmt.Sprintf("# Migrating %+v; inspecting %+v; executing on %+v",
		*this.applier.connectionConfig.ImpliedKey,
		*this.inspector.connectionConfig.ImpliedKey,
		this.hostname,
	))
	fmt.Fprintln(w, fmt.Sprintf("# Migration started at %+v",
		this.migrationContext.StartTime.Format(time.RubyDate),
	))
	maxLoad := this.migrationContext.GetMaxLoad()
	criticalLoad := this.migrationContext.GetCriticalLoad()
	fmt.Fprintln(w, fmt.Sprintf("# chunk-size: %+v; max-lag-millis: %+vms; max-load: %s; critical-load: %s; nice-ratio: %f",
		atomic.LoadInt64(&this.migrationContext.ChunkSize),
		atomic.LoadInt64(&this.migrationContext.MaxLagMillisecondsThrottleThreshold),
		maxLoad.String(),
		criticalLoad.String(),
		this.migrationContext.GetNiceRatio(),
	))
	if replicationLagQuery := this.migrationContext.GetReplicationLagQuery(); replicationLagQuery != "" {
		fmt.Fprintln(w, fmt.Sprintf("# replication-lag-query: %+v",
			replicationLagQuery,
		))
	}
	if this.migrationContext.ThrottleFlagFile != "" {
		setIndicator := ""
		if base.FileExists(this.migrationContext.ThrottleFlagFile) {
			setIndicator = "[set]"
		}
		fmt.Fprintln(w, fmt.Sprintf("# throttle-flag-file: %+v %+v",
			this.migrationContext.ThrottleFlagFile, setIndicator,
		))
	}
	if this.migrationContext.ThrottleAdditionalFlagFile != "" {
		setIndicator := ""
		if base.FileExists(this.migrationContext.ThrottleAdditionalFlagFile) {
			setIndicator = "[set]"
		}
		fmt.Fprintln(w, fmt.Sprintf("# throttle-additional-flag-file: %+v %+v",
			this.migrationContext.ThrottleAdditionalFlagFile, setIndicator,
		))
	}
	if throttleQuery := this.migrationContext.GetThrottleQuery(); throttleQuery != "" {
		fmt.Fprintln(w, fmt.Sprintf("# throttle-query: %+v",
			throttleQuery,
		))
	}
	if this.migrationContext.PostponeCutOverFlagFile != "" {
		setIndicator := ""
		if base.FileExists(this.migrationContext.PostponeCutOverFlagFile) {
			setIndicator = "[set]"
		}
		fmt.Fprintln(w, fmt.Sprintf("# postpone-cut-over-flag-file: %+v %+v",
			this.migrationContext.PostponeCutOverFlagFile, setIndicator,
		))
	}
	if this.migrationContext.PanicFlagFile != "" {
		fmt.Fprintln(w, fmt.Sprintf("# panic-flag-file: %+v",
			this.migrationContext.PanicFlagFile,
		))
	}
	fmt.Fprintln(w, fmt.Sprintf("# Serving on unix socket: %+v",
		this.migrationContext.ServeSocketFile,
	))
	if this.migrationContext.ServeTCPPort != 0 {
		fmt.Fprintln(w, fmt.Sprintf("# Serving on TCP port: %+v", this.migrationContext.ServeTCPPort))
	}
}

// printStatus prints the prgoress status, and optionally additionally detailed
// dump of configuration.
// `rule` indicates the type of output expected.
// By default the status is written to standard output, but other writers can
// be used as well.
func (this *Migrator) printStatus(rule PrintStatusRule, writers ...io.Writer) {
	writers = append(writers, os.Stdout)

	elapsedTime := this.migrationContext.ElapsedTime()
	elapsedSeconds := int64(elapsedTime.Seconds())
	totalRowsCopied := this.migrationContext.GetTotalRowsCopied()
	rowsEstimate := atomic.LoadInt64(&this.migrationContext.RowsEstimate)
	var progressPct float64
	if rowsEstimate > 0 {
		progressPct = 100.0 * float64(totalRowsCopied) / float64(rowsEstimate)
	}

	// Before status, let's see if we should print a nice reminder for what exactly we're doing here.
	shouldPrintMigrationStatusHint := (elapsedSeconds%600 == 0)
	if rule == ForcePrintStatusAndHint {
		shouldPrintMigrationStatusHint = true
	}
	if shouldPrintMigrationStatusHint {
		this.printMigrationStatusHint(writers...)
	}

	var etaSeconds float64 = math.MaxFloat64
	eta := "N/A"
	if atomic.LoadInt64(&this.migrationContext.CountingRowsFlag) > 0 {
		eta = "counting rows"
	} else if atomic.LoadInt64(&this.migrationContext.IsPostponingCutOver) > 0 {
		eta = "postponing cut-over"
	} else if isThrottled, throttleReason := this.migrationContext.IsThrottled(); isThrottled {
		eta = fmt.Sprintf("throttled, %s", throttleReason)
	} else if progressPct > 100.0 {
		eta = "Due"
	} else if progressPct >= 1.0 {
		elapsedRowCopySeconds := this.migrationContext.ElapsedRowCopyTime().Seconds()
		totalExpectedSeconds := elapsedRowCopySeconds * float64(rowsEstimate) / float64(totalRowsCopied)
		etaSeconds = totalExpectedSeconds - elapsedRowCopySeconds
		if etaSeconds >= 0 {
			etaDuration := time.Duration(etaSeconds) * time.Second
			eta = base.PrettifyDurationOutput(etaDuration)
		} else {
			eta = "Due"
		}
	}

	shouldPrintStatus := false
	if elapsedSeconds <= 60 {
		shouldPrintStatus = true
	} else if etaSeconds <= 60 {
		shouldPrintStatus = true
	} else if etaSeconds <= 180 {
		shouldPrintStatus = (elapsedSeconds%5 == 0)
	} else if elapsedSeconds <= 180 {
		shouldPrintStatus = (elapsedSeconds%5 == 0)
	} else if this.migrationContext.TimeSincePointOfInterest().Seconds() <= 60 {
		shouldPrintStatus = (elapsedSeconds%5 == 0)
	} else {
		shouldPrintStatus = (elapsedSeconds%30 == 0)
	}
	if rule == ForcePrintStatusRule || rule == ForcePrintStatusAndHint {
		shouldPrintStatus = true
	}
	if !shouldPrintStatus {
		return
	}

	currentBinlogCoordinates := *this.eventsStreamer.GetCurrentBinlogCoordinates()

	status := fmt.Sprintf("Copy: %d/%d %.1f%%; Applied: %d; Backlog: %d/%d; Time: %+v(total), %+v(copy); streamer: %+v; ETA: %s",
		totalRowsCopied, rowsEstimate, progressPct,
		atomic.LoadInt64(&this.migrationContext.TotalDMLEventsApplied),
		len(this.applyEventsQueue), cap(this.applyEventsQueue),
		base.PrettifyDurationOutput(elapsedTime), base.PrettifyDurationOutput(this.migrationContext.ElapsedRowCopyTime()),
		currentBinlogCoordinates,
		eta,
	)
	this.applier.WriteChangelog(
		fmt.Sprintf("copy iteration %d at %d", this.migrationContext.GetIteration(), time.Now().Unix()),
		status,
	)
	w := io.MultiWriter(writers...)
	fmt.Fprintln(w, status)
}

// initiateHeartbeatListener listens for heartbeat events. gh-ost implements its own
// heartbeat mechanism, whether your DB has or hasn't an existing heartbeat solution.
// Heartbeat is supplied via the changelog table
func (this *Migrator) initiateHeartbeatListener() {
	ticker := time.Tick((heartbeatIntervalMilliseconds * time.Millisecond) / 2)
	for range ticker {
		go func() error {
			if atomic.LoadInt64(&this.cleanupImminentFlag) > 0 {
				return nil
			}
			changelogState, err := this.inspector.readChangelogState()
			if err != nil {
				return log.Errore(err)
			}
			for hint, value := range changelogState {
				switch hint {
				case "heartbeat":
					{
						this.onChangelogHeartbeat(value)
					}
				}
			}
			return nil
		}()
	}
}

// initiateStreaming begins treaming of binary log events and registers listeners for such events
func (this *Migrator) initiateStreaming() error {
	this.eventsStreamer = NewEventsStreamer()
	if err := this.eventsStreamer.InitDBConnections(); err != nil {
		return err
	}
	this.eventsStreamer.AddListener(
		false,
		this.migrationContext.DatabaseName,
		this.migrationContext.GetChangelogTableName(),
		func(dmlEvent *binlog.BinlogDMLEvent) error {
			return this.onChangelogStateEvent(dmlEvent)
		},
	)

	go func() {
		log.Debugf("Beginning streaming")
		err := this.eventsStreamer.StreamEvents(this.canStopStreaming)
		if err != nil {
			this.panicAbort <- err
		}
		log.Debugf("Done streaming")
	}()
	return nil
}

// addDMLEventsListener begins listening for binlog events on the original table,
// and creates & enqueues a write task per such event.
func (this *Migrator) addDMLEventsListener() error {
	err := this.eventsStreamer.AddListener(
		false,
		this.migrationContext.DatabaseName,
		this.migrationContext.OriginalTableName,
		func(dmlEvent *binlog.BinlogDMLEvent) error {
			// Create a task to apply the DML event; this will be execute by executeWriteFuncs()
			applyEventFunc := func() error {
				return this.applier.ApplyDMLEventQuery(dmlEvent)
			}
			this.applyEventsQueue <- applyEventFunc
			return nil
		},
	)
	return err
}

func (this *Migrator) initiateApplier() error {
	this.applier = NewApplier()
	if err := this.applier.InitDBConnections(); err != nil {
		return err
	}
	if err := this.applier.ValidateOrDropExistingTables(); err != nil {
		return err
	}
	if err := this.applier.CreateChangelogTable(); err != nil {
		log.Errorf("Unable to create changelog table, see further error details. Perhaps a previous migration failed without dropping the table? OR is there a running migration? Bailing out")
		return err
	}
	if err := this.applier.CreateGhostTable(); err != nil {
		log.Errorf("Unable to create ghost table, see further error details. Perhaps a previous migration failed without dropping the table? Bailing out")
		return err
	}
	if err := this.applier.AlterGhost(); err != nil {
		log.Errorf("Unable to ALTER ghost table, see further error details. Bailing out")
		return err
	}

	this.applier.WriteChangelogState(string(TablesInPlace))
	go this.applier.InitiateHeartbeat(heartbeatIntervalMilliseconds)
	return nil
}

// iterateChunks iterates the existing table rows, and generates a copy task of
// a chunk of rows onto the ghost table.
func (this *Migrator) iterateChunks() error {
	terminateRowIteration := func(err error) error {
		this.rowCopyComplete <- true
		return log.Errore(err)
	}
	if this.migrationContext.Noop {
		log.Debugf("Noop operation; not really copying data")
		return terminateRowIteration(nil)
	}
	if this.migrationContext.MigrationRangeMinValues == nil {
		log.Debugf("No rows found in table. Rowcopy will be implicitly empty")
		return terminateRowIteration(nil)
	}
	// Iterate per chunk:
	for {
		if atomic.LoadInt64(&this.rowCopyCompleteFlag) == 1 {
			// Done
			return nil
		}
		copyRowsFunc := func() error {
			hasFurtherRange, err := this.applier.CalculateNextIterationRangeEndValues()
			if err != nil {
				return terminateRowIteration(err)
			}
			if !hasFurtherRange {
				return terminateRowIteration(nil)
			}
			// Copy task:
			applyCopyRowsFunc := func() error {
				_, rowsAffected, _, err := this.applier.ApplyIterationInsertQuery()
				if err != nil {
					return terminateRowIteration(err)
				}
				atomic.AddInt64(&this.migrationContext.TotalRowsCopied, rowsAffected)
				atomic.AddInt64(&this.migrationContext.Iteration, 1)
				return nil
			}
			return this.retryOperation(applyCopyRowsFunc)
		}
		// Enqueue copy operation; to be executed by executeWriteFuncs()
		this.copyRowsQueue <- copyRowsFunc
	}
	return nil
}

// executeWriteFuncs writes data via applier: both the rowcopy and the events backlog.
// This is where the ghost table gets the data. The function fills the data single-threaded.
// Both event backlog and rowcopy events are polled; the backlog events have precedence.
func (this *Migrator) executeWriteFuncs() error {
	if this.migrationContext.Noop {
		log.Debugf("Noop operation; not really executing write funcs")
		return nil
	}
	for {
		if atomic.LoadInt64(&this.inCutOverCriticalActionFlag) == 0 {
			// we don't throttle when cutting over. We _do_ throttle:
			// - during copy phase
			// - just before cut-over
			// - in between cut-over retries
			this.throttle(nil)
			// When cutting over, we need to be aggressive. Cut-over holds table locks.
			// We need to release those asap.
		}
		// We give higher priority to event processing, then secondary priority to
		// rowcopy
		select {
		case applyEventFunc := <-this.applyEventsQueue:
			{
				if err := this.retryOperation(applyEventFunc); err != nil {
					return log.Errore(err)
				}
			}
		default:
			{
				select {
				case copyRowsFunc := <-this.copyRowsQueue:
					{
						copyRowsStartTime := time.Now()
						// Retries are handled within the copyRowsFunc
						if err := copyRowsFunc(); err != nil {
							return log.Errore(err)
						}
						if niceRatio := this.migrationContext.GetNiceRatio(); niceRatio > 0 {
							copyRowsDuration := time.Since(copyRowsStartTime)
							sleepTimeNanosecondFloat64 := niceRatio * float64(copyRowsDuration.Nanoseconds())
							sleepTime := time.Duration(time.Duration(int64(sleepTimeNanosecondFloat64)) * time.Nanosecond)
							time.Sleep(sleepTime)
						}
					}
				default:
					{
						// Hmmmmm... nothing in the queue; no events, but also no row copy.
						// This is possible upon load. Let's just sleep it over.
						log.Debugf("Getting nothing in the write queue. Sleeping...")
						time.Sleep(time.Second)
					}
				}
			}
		}
	}
	return nil
}

// finalCleanup takes actions at very end of migration, dropping tables etc.
func (this *Migrator) finalCleanup() error {
	atomic.StoreInt64(&this.cleanupImminentFlag, 1)

	if this.migrationContext.Noop {
		if createTableStatement, err := this.inspector.showCreateTable(this.migrationContext.GetGhostTableName()); err == nil {
			log.Infof("New table structure follows")
			fmt.Println(createTableStatement)
		} else {
			log.Errore(err)
		}
	}

	if err := this.retryOperation(this.applier.DropChangelogTable); err != nil {
		return err
	}
	if this.migrationContext.OkToDropTable && !this.migrationContext.TestOnReplica {
		if err := this.retryOperation(this.applier.DropOldTable); err != nil {
			return err
		}
	} else {
		if !this.migrationContext.Noop {
			log.Infof("Am not dropping old table because I want this operation to be as live as possible. If you insist I should do it, please add `--ok-to-drop-table` next time. But I prefer you do not. To drop the old table, issue:")
			log.Infof("-- drop table %s.%s", sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.GetOldTableName()))
		}
	}
	if this.migrationContext.Noop {
		if err := this.retryOperation(this.applier.DropGhostTable); err != nil {
			return err
		}
	}

	return nil
}
