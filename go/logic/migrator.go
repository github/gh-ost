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

// Migrator is the main schema migration flow manager.
type Migrator struct {
	inspector        *Inspector
	applier          *Applier
	eventsStreamer   *EventsStreamer
	server           *Server
	migrationContext *base.MigrationContext

	tablesInPlace              chan bool
	rowCopyComplete            chan bool
	allEventsUpToLockProcessed chan bool
	panicAbort                 chan error

	rowCopyCompleteFlag                    int64
	allEventsUpToLockProcessedInjectedFlag int64
	cleanupImminentFlag                    int64
	// copyRowsQueue should not be buffered; if buffered some non-damaging but
	//  excessive work happens at the end of the iteration as new copy-jobs arrive befroe realizing the copy is complete
	copyRowsQueue    chan tableWriteFunc
	applyEventsQueue chan tableWriteFunc

	handledChangelogStates map[string]bool
}

func NewMigrator() *Migrator {
	migrator := &Migrator{
		migrationContext:           base.GetMigrationContext(),
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

func (this *Migrator) shouldThrottle() (result bool, reason string) {
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
	lag := atomic.LoadInt64(&this.migrationContext.CurrentLag)
	if time.Duration(lag) > time.Duration(this.migrationContext.MaxLagMillisecondsThrottleThreshold)*time.Millisecond {
		return true, fmt.Sprintf("lag=%fs", time.Duration(lag).Seconds())
	}
	if this.migrationContext.TestOnReplica && (atomic.LoadInt64(&this.allEventsUpToLockProcessedInjectedFlag) == 0) {
		replicationLag, err := mysql.GetMaxReplicationLag(this.migrationContext.InspectorConnectionConfig, this.migrationContext.ThrottleControlReplicaKeys, this.migrationContext.ReplictionLagQuery)
		if err != nil {
			return true, err.Error()
		}
		if replicationLag > time.Duration(this.migrationContext.MaxLagMillisecondsThrottleThreshold)*time.Millisecond {
			return true, fmt.Sprintf("replica-lag=%fs", replicationLag.Seconds())
		}
	}

	maxLoad := this.migrationContext.GetMaxLoad()
	for variableName, threshold := range maxLoad {
		value, err := this.applier.ShowStatusVariable(variableName)
		if err != nil {
			return true, fmt.Sprintf("%s %s", variableName, err)
		}
		if value > threshold {
			return true, fmt.Sprintf("%s=%d", variableName, value)
		}
	}

	return false, ""
}

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
		if shouldThrottle, _ := this.migrationContext.IsThrottled(); !shouldThrottle {
			return
		}
		if onThrottled != nil {
			onThrottled()
		}
		time.Sleep(time.Second)
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
func (this *Migrator) retryOperation(operation func() error) (err error) {
	maxRetries := this.migrationContext.MaxRetries()
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
	this.panicAbort <- err
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
	go func() {
		for <-this.rowCopyComplete {
		}
	}()
}

func (this *Migrator) canStopStreaming() bool {
	return false
}

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

func (this *Migrator) onChangelogHeartbeat(heartbeatValue string) (err error) {
	heartbeatTime, err := time.Parse(time.RFC3339Nano, heartbeatValue)
	if err != nil {
		return log.Errore(err)
	}
	lag := time.Now().Sub(heartbeatTime)

	atomic.StoreInt64(&this.migrationContext.CurrentLag, int64(lag))

	return nil
}

//
func (this *Migrator) listenOnPanicAbort() {
	err := <-this.panicAbort
	log.Fatale(err)
}

func (this *Migrator) Migrate() (err error) {
	log.Infof("Migrating %s.%s", sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.OriginalTableName))
	this.migrationContext.StartTime = time.Now()

	go this.listenOnPanicAbort()
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
	if this.migrationContext.CountTableRows {
		if this.migrationContext.Noop {
			log.Debugf("Noop operation; not really counting table rows")
		} else if err := this.inspector.CountTableRows(); err != nil {
			return err
		}
	}

	if err := this.initiateServer(); err != nil {
		return err
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
	this.migrationContext.RowCopyStartTime = time.Now()
	go this.initiateStatus()

	log.Debugf("Operating until row copy is complete")
	this.consumeRowCopyComplete()
	log.Infof("Row copy complete")
	this.printStatus()

	if err := this.stopWritesAndCompleteMigration(); err != nil {
		return err
	}

	if err := this.finalCleanup(); err != nil {
		return nil
	}
	log.Infof("Done migrating %s.%s", sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.OriginalTableName))
	return nil
}

// stopWritesAndCompleteMigration performs the final step of migration, based on migration
// type (on replica? bumpy? safe?)
func (this *Migrator) stopWritesAndCompleteMigration() (err error) {
	if this.migrationContext.Noop {
		log.Debugf("Noop operation; not really swapping tables")
		return nil
	}
	this.throttle(func() {
		log.Debugf("throttling before swapping tables")
	})

	this.sleepWhileTrue(
		func() (bool, error) {
			if this.migrationContext.PostponeCutOverFlagFile == "" {
				return false, nil
			}
			if base.FileExists(this.migrationContext.PostponeCutOverFlagFile) {
				// Throttle file defined and exists!
				atomic.StoreInt64(&this.migrationContext.IsPostponingCutOver, 1)
				log.Debugf("Postponing final table swap as flag file exists: %+v", this.migrationContext.PostponeCutOverFlagFile)
				return true, nil
			}
			return false, nil
		},
	)
	atomic.StoreInt64(&this.migrationContext.IsPostponingCutOver, 0)

	if this.migrationContext.TestOnReplica {
		// With `--test-on-replica` we stop replication thread, and then proceed to use
		// the same cut-over phase as the master would use. That means we take locks
		// and swap the tables.
		// The difference is that we will later swap the tables back.
		log.Debugf("testing on replica. Stopping replication IO thread")
		if err := this.retryOperation(this.applier.StopSlaveNicely); err != nil {
			return err
		}
		// We're merly testing, we don't want to keep this state. Rollback the renames as possible
		defer this.applier.RenameTablesRollback()
	}
	if this.migrationContext.CutOverType == base.CutOverSafe {
		// Lock-based solution: we use low timeout and multiple attempts. But for
		// each failed attempt, we throttle until replication lag is back to normal
		err := this.retryOperation(
			func() error {
				return this.executeAndThrottleOnError(this.safeCutOver)
			},
		)
		return err
	}
	if this.migrationContext.CutOverType == base.CutOverTwoStep {
		err := this.retryOperation(this.cutOverTwoStep)
		return err
	}
	return nil
}

// Inject the "AllEventsUpToLockProcessed" state hint, wait for it to appear in the binary logs,
// make sure the queue is drained.
func (this *Migrator) waitForEventsUpToLock() (err error) {
	waitForEventsUpToLockStartTime := time.Now()

	log.Infof("Writing changelog state: %+v", AllEventsUpToLockProcessed)
	if _, err := this.applier.WriteChangelogState(string(AllEventsUpToLockProcessed)); err != nil {
		return err
	}
	log.Infof("Waiting for events up to lock")
	atomic.StoreInt64(&this.allEventsUpToLockProcessedInjectedFlag, 1)
	<-this.allEventsUpToLockProcessed
	waitForEventsUpToLockDuration := time.Now().Sub(waitForEventsUpToLockStartTime)

	log.Infof("Done waiting for events up to lock; duration=%+v", waitForEventsUpToLockDuration)
	this.printStatus()

	return nil
}

// cutOverTwoStep will lock down the original table, execute
// what's left of last DML entries, and **non-atomically** swap original->old, then new->original.
// There is a point in time where the "original" table does not exist and queries are non-blocked
// and failing.
func (this *Migrator) cutOverTwoStep() (err error) {
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

// cutOverSafe performs a safe cut over, where normally (no failure) the original table
// is being locked until swapped, hence DML queries being locked and unaware of the cut-over.
// In the worst case, there will ba a minor outage, where the original table would not exist.
func (this *Migrator) safeCutOver() (err error) {
	okToUnlockTable := make(chan bool, 2)
	originalTableRenamed := make(chan error, 1)
	defer func() {
		// The following is to make sure we unlock the table no-matter-what!
		// There's enough buffer in the channel to support a redundant write here.
		okToUnlockTable <- true
		// We need to make sure we wait for the original-rename, successful or not,
		// so as to be able to rollback in case the ghost-rename fails.
		<-originalTableRenamed

		// Rollback operation
		if !this.applier.tableExists(this.migrationContext.OriginalTableName) {
			log.Infof("Cannot find %s, rolling back", this.migrationContext.OriginalTableName)
			err := this.applier.RenameTable(this.migrationContext.GetOldTableName(), this.migrationContext.OriginalTableName)
			log.Errore(err)
		}
	}()
	lockOriginalSessionIdChan := make(chan int64, 1)
	tableLocked := make(chan error, 1)
	tableUnlocked := make(chan error, 1)
	go func() {
		if err := this.applier.LockOriginalTableAndWait(lockOriginalSessionIdChan, tableLocked, okToUnlockTable, tableUnlocked); err != nil {
			log.Errore(err)
		}
	}()
	if err := <-tableLocked; err != nil {
		return log.Errore(err)
	}
	lockOriginalSessionId := <-lockOriginalSessionIdChan
	log.Infof("Session locking original table is %+v", lockOriginalSessionId)
	// At this point we know the table is locked.
	// We know any newly incoming DML on original table is blocked.
	this.waitForEventsUpToLock()

	// Step 2
	// We now attempt a RENAME on the original table, and expect it to block
	renameOriginalSessionIdChan := make(chan int64, 1)
	this.migrationContext.RenameTablesStartTime = time.Now()
	go func() {
		this.applier.RenameOriginalTable(renameOriginalSessionIdChan, originalTableRenamed)
	}()
	renameOriginalSessionId := <-renameOriginalSessionIdChan
	log.Infof("Session renaming original table is %+v", renameOriginalSessionId)

	if err := this.retryOperation(
		func() error {
			return this.applier.ExpectProcess(renameOriginalSessionId, "metadata lock", "rename")
		}); err != nil {
		return err
	}
	log.Infof("Found RENAME on original table to be blocking, as expected. Double checking original is still being locked")
	if err := this.applier.ExpectUsedLock(lockOriginalSessionId); err != nil {
		// Abort operation; but make sure to unlock table!
		return log.Errore(err)
	}
	log.Infof("Connection holding lock on original table still exists")

	// Now that we've found the RENAME blocking, AND the locking connection still alive,
	// we know it is safe to proceed to renaming ghost table.

	// Step 3
	// We now attempt a RENAME on the ghost table, and expect it to block
	renameGhostSessionIdChan := make(chan int64, 1)
	ghostTableRenamed := make(chan error, 1)
	go func() {
		this.applier.RenameGhostTable(renameGhostSessionIdChan, ghostTableRenamed)
	}()
	renameGhostSessionId := <-renameGhostSessionIdChan
	log.Infof("Session renaming ghost table is %+v", renameGhostSessionId)

	if err := this.retryOperation(
		func() error {
			return this.applier.ExpectProcess(renameGhostSessionId, "metadata lock", "rename")
		}); err != nil {
		return err
	}
	log.Infof("Found RENAME on ghost table to be blocking, as expected. Will next release lock on original table")

	// Step 4
	okToUnlockTable <- true
	// BAM! original table lock is released, RENAME original->old released,
	// RENAME ghost->original is released, queries on original are unblocked.
	// (that is, assuming all went well)
	if err := <-tableUnlocked; err != nil {
		return log.Errore(err)
	}
	if err := <-ghostTableRenamed; err != nil {
		return log.Errore(err)
	}
	this.migrationContext.RenameTablesEndTime = time.Now()

	// ooh nice! We're actually truly and thankfully done
	lockAndRenameDuration := this.migrationContext.RenameTablesEndTime.Sub(this.migrationContext.LockTablesStartTime)
	log.Infof("Lock & rename duration: %s. During this time, queries on %s were blocked", lockAndRenameDuration, sql.EscapeName(this.migrationContext.OriginalTableName))
	return nil
}

// stopWritesAndCompleteMigrationOnReplica will stop replication IO thread, apply
// what DML events are left, and that's it.
// This only applies in --test-on-replica. It leaves replication stopped, with both tables
// in sync. There is no table swap.
func (this *Migrator) stopWritesAndCompleteMigrationOnReplica() (err error) {
	log.Debugf("testing on replica. Instead of LOCK tables I will STOP SLAVE")
	if err := this.retryOperation(this.applier.StopSlaveNicely); err != nil {
		return err
	}

	this.waitForEventsUpToLock()

	this.printMigrationStatusHint()
	log.Info("Table duplicated with new schema. Am not touching the original table. Replication is stopped. You may now compare the two tables to gain trust into this tool's operation")
	return nil
}

func (this *Migrator) onServerCommand(command string, writer *bufio.Writer) (err error) {
	defer writer.Flush()

	tokens := strings.SplitN(command, "=", 2)
	command = strings.TrimSpace(tokens[0])
	arg := ""
	if len(tokens) > 1 {
		arg = strings.TrimSpace(tokens[1])
	}
	switch command {
	case "help":
		{
			fmt.Fprintln(writer, `available commands:
  status               # Print a status message
  chunk-size=<newsize> # Set a new chunk-size
  throttle             # Force throttling
  no-throttle          # End forced throttling (other throttling may still apply)
  help                 # This message
`)
		}
	case "info", "status":
		this.printMigrationStatusHint(writer)
		this.printStatus(writer)
	case "chunk-size":
		{
			if chunkSize, err := strconv.Atoi(arg); err != nil {
				fmt.Fprintf(writer, "%s\n", err.Error())
				return log.Errore(err)
			} else {
				this.migrationContext.SetChunkSize(int64(chunkSize))
				this.printMigrationStatusHint(writer)
			}
		}
	case "max-load":
		{
			if err := this.migrationContext.ReadMaxLoad(arg); err != nil {
				fmt.Fprintf(writer, "%s\n", err.Error())
				return log.Errore(err)
			}
			this.printMigrationStatusHint(writer)
		}
	case "throttle", "pause", "suspend":
		{
			atomic.StoreInt64(&this.migrationContext.ThrottleCommandedByUser, 1)
		}
	case "no-throttle", "unthrottle", "resume", "continue":
		{
			atomic.StoreInt64(&this.migrationContext.ThrottleCommandedByUser, 0)
		}
	default:
		err = fmt.Errorf("Unknown command: %s", command)
		fmt.Fprintf(writer, "%s\n", err.Error())
		return err
	}
	return nil
}

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
	if this.migrationContext.TestOnReplica {
		if this.migrationContext.InspectorIsAlsoApplier() {
			return fmt.Errorf("Instructed to --test-on-replica, but the server we connect to doesn't seem to be a replica")
		}
		log.Infof("--test-on-replica given. Will not execute on master %+v but rather on replica %+v itself",
			this.migrationContext.ApplierConnectionConfig.Key, this.migrationContext.InspectorConnectionConfig.Key,
		)
		this.migrationContext.ApplierConnectionConfig = this.migrationContext.InspectorConnectionConfig.Duplicate()
		if this.migrationContext.ThrottleControlReplicaKeys.Len() == 0 {
			this.migrationContext.ThrottleControlReplicaKeys.AddKey(this.migrationContext.InspectorConnectionConfig.Key)
		}
	} else if this.migrationContext.InspectorIsAlsoApplier() && !this.migrationContext.AllowedRunningOnMaster {
		return fmt.Errorf("It seems like this migration attempt to run directly on master. Preferably it would be executed on a replica (and this reduces load from the master). To proceed please provide --allow-on-master")
	}

	log.Infof("Master found to be %+v", this.migrationContext.ApplierConnectionConfig.Key)
	return nil
}

func (this *Migrator) initiateStatus() error {
	this.printStatus()
	statusTick := time.Tick(1 * time.Second)
	for range statusTick {
		go this.printStatus()
	}

	return nil
}

func (this *Migrator) printMigrationStatusHint(writers ...io.Writer) {
	writers = append(writers, os.Stdout)
	w := io.MultiWriter(writers...)
	fmt.Fprintln(w, fmt.Sprintf("# Migrating %s.%s; Ghost table is %s.%s",
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.OriginalTableName),
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetGhostTableName()),
	))
	fmt.Fprintln(w, fmt.Sprintf("# Migration started at %+v",
		this.migrationContext.StartTime.Format(time.RubyDate),
	))
	maxLoad := this.migrationContext.GetMaxLoad()
	fmt.Fprintln(w, fmt.Sprintf("# chunk-size: %+v; max lag: %+vms; max-load: %+v",
		atomic.LoadInt64(&this.migrationContext.ChunkSize),
		atomic.LoadInt64(&this.migrationContext.MaxLagMillisecondsThrottleThreshold),
		maxLoad,
	))
	if this.migrationContext.ThrottleFlagFile != "" {
		fmt.Fprintln(w, fmt.Sprintf("# Throttle flag file: %+v",
			this.migrationContext.ThrottleFlagFile,
		))
	}
	if this.migrationContext.ThrottleAdditionalFlagFile != "" {
		fmt.Fprintln(w, fmt.Sprintf("# Throttle additional flag file: %+v",
			this.migrationContext.ThrottleAdditionalFlagFile,
		))
	}
	fmt.Fprintln(w, fmt.Sprintf("# Serving on unix socket: %+v",
		this.migrationContext.ServeSocketFile,
	))
	if this.migrationContext.ServeTCPPort != 0 {
		fmt.Fprintln(w, fmt.Sprintf("# Serving on TCP port: %+v", this.migrationContext.ServeTCPPort))
	}
}

func (this *Migrator) printStatus(writers ...io.Writer) {
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
	if shouldPrintMigrationStatusHint {
		this.printMigrationStatusHint()
	}

	var etaSeconds float64 = math.MaxFloat64
	eta := "N/A"
	if atomic.LoadInt64(&this.migrationContext.IsPostponingCutOver) > 0 {
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
	if !shouldPrintStatus {
		return
	}

	currentBinlogCoordinates := *this.eventsStreamer.GetCurrentBinlogCoordinates()

	status := fmt.Sprintf("Copy: %d/%d %.1f%%; Applied: %d; Backlog: %d/%d; Elapsed: %+v(copy), %+v(total); streamer: %+v; ETA: %s",
		totalRowsCopied, rowsEstimate, progressPct,
		atomic.LoadInt64(&this.migrationContext.TotalDMLEventsApplied),
		len(this.applyEventsQueue), cap(this.applyEventsQueue),
		base.PrettifyDurationOutput(this.migrationContext.ElapsedRowCopyTime()), base.PrettifyDurationOutput(elapsedTime),
		currentBinlogCoordinates,
		eta,
	)
	this.applier.WriteChangelog(
		fmt.Sprintf("copy iteration %d at %d", this.migrationContext.GetIteration(), time.Now().Unix()),
		status,
	)
	writers = append(writers, os.Stdout)
	w := io.MultiWriter(writers...)
	fmt.Fprintln(w, status)
}

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

// addDMLEventsListener
func (this *Migrator) addDMLEventsListener() error {
	err := this.eventsStreamer.AddListener(
		false,
		this.migrationContext.DatabaseName,
		this.migrationContext.OriginalTableName,
		func(dmlEvent *binlog.BinlogDMLEvent) error {
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
		this.copyRowsQueue <- copyRowsFunc
	}
	return nil
}

func (this *Migrator) executeWriteFuncs() error {
	if this.migrationContext.Noop {
		log.Debugf("Noop operation; not really executing write funcs")
		return nil
	}
	for {
		this.throttle(nil)
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
						// Retries are handled within the copyRowsFunc
						if err := copyRowsFunc(); err != nil {
							return log.Errore(err)
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
	if err := this.retryOperation(this.applier.DropChangelogTable); err != nil {
		return err
	}
	if this.migrationContext.OkToDropTable && !this.migrationContext.TestOnReplica {
		dropTableFunc := func() error {
			return this.applier.dropTable(this.migrationContext.GetOldTableName())
		}
		if err := this.retryOperation(dropTableFunc); err != nil {
			return err
		}
	}

	return nil
}
