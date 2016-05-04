/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-osc/blob/master/LICENSE
*/

package logic

import (
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/github/gh-osc/go/base"
	"github.com/github/gh-osc/go/binlog"
	"github.com/github/gh-osc/go/mysql"
	"github.com/github/gh-osc/go/sql"

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
	migrationContext *base.MigrationContext

	tablesInPlace              chan bool
	rowCopyComplete            chan bool
	allEventsUpToLockProcessed chan bool
	voluntaryLockAcquired      chan bool
	panicAbort                 chan error

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
		voluntaryLockAcquired:      make(chan bool, 1),
		panicAbort:                 make(chan error),

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
	if this.migrationContext.ThrottleFlagFile != "" {
		if _, err := os.Stat(this.migrationContext.ThrottleFlagFile); err == nil {
			// Throttle file defined and exists!
			return true, "flag-file"
		}
	}
	if this.migrationContext.ThrottleAdditionalFlagFile != "" {
		if _, err := os.Stat(this.migrationContext.ThrottleAdditionalFlagFile); err == nil {
			// 2nd Throttle file defined and exists!
			return true, "flag-file"
		}
	}
	// Replication lag throttle
	lag := atomic.LoadInt64(&this.migrationContext.CurrentLag)
	if time.Duration(lag) > time.Duration(this.migrationContext.MaxLagMillisecondsThrottleThreshold)*time.Millisecond {
		return true, fmt.Sprintf("lag=%fs", time.Duration(lag).Seconds())
	}
	if this.migrationContext.TestOnReplica {
		replicationLag, err := mysql.GetMaxReplicationLag(this.migrationContext.InspectorConnectionConfig, this.migrationContext.ThrottleControlReplicaKeys, this.migrationContext.ReplictionLagQuery)
		if err != nil {
			return true, err.Error()
		}
		if replicationLag > time.Duration(this.migrationContext.MaxLagMillisecondsThrottleThreshold)*time.Millisecond {
			return true, fmt.Sprintf("replica-lag=%fs", replicationLag.Seconds())
		}
	}

	for variableName, threshold := range this.migrationContext.MaxLoad {
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
			this.allEventsUpToLockProcessed <- true
		}
	default:
		{
			return fmt.Errorf("Unknown changelog state: %+v", changelogState)
		}
	}
	log.Debugf("Received state %+v", changelogState)
	return nil
}

func (this *Migrator) onChangelogState(stateValue string) (err error) {
	log.Fatalf("I shouldn't be here")
	if this.handledChangelogStates[stateValue] {
		return nil
	}
	this.handledChangelogStates[stateValue] = true

	changelogState := ChangelogState(stateValue)
	switch changelogState {
	case TablesInPlace:
		{
			this.tablesInPlace <- true
		}
	case AllEventsUpToLockProcessed:
		{
			this.allEventsUpToLockProcessed <- true
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
	<-this.rowCopyComplete
	log.Debugf("Row copy complete")
	this.printStatus()

	if err := this.stopWritesAndCompleteMigration(); err != nil {
		return err
	}

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

	if this.migrationContext.TestOnReplica {
		return this.stopWritesAndCompleteMigrationOnReplica()
	}
	// Running on master
	if this.migrationContext.QuickAndBumpySwapTables {
		return this.stopWritesAndCompleteMigrationOnMasterQuickAndBumpy()
	}
	// Lock-based solution: we use low timeout and multiple attempts. But for
	// each failed attempt, we throttle until replication lag is back to normal
	if err := this.retryOperation(
		func() error {
			return this.executeAndThrottleOnError(this.stopWritesAndCompleteMigrationOnMasterViaLock)
		}); err != nil {
		return err
	}
	if err := this.dropOldTableIfRequired(); err != nil {
		return err
	}

	return
}

func (this *Migrator) dropOldTableIfRequired() (err error) {
	if !this.migrationContext.OkToDropTable {
		return nil
	}
	dropTableFunc := func() error {
		return this.applier.dropTable(this.migrationContext.GetOldTableName())
	}
	if err := this.retryOperation(dropTableFunc); err != nil {
		return err
	}
	return nil
}

// stopWritesAndCompleteMigrationOnMasterQuickAndBumpy will lock down the original table, execute
// what's left of last DML entries, and **non-atomically** swap original->old, then new->original.
// There is a point in time where the "original" table does not exist and queries are non-blocked
// and failing.
func (this *Migrator) stopWritesAndCompleteMigrationOnMasterQuickAndBumpy() (err error) {
	if err := this.retryOperation(this.applier.LockTables); err != nil {
		return err
	}

	this.applier.WriteChangelogState(string(AllEventsUpToLockProcessed))
	log.Debugf("Waiting for events up to lock")
	<-this.allEventsUpToLockProcessed
	log.Debugf("Done waiting for events up to lock")

	if err := this.retryOperation(this.applier.SwapTablesQuickAndBumpy); err != nil {
		return err
	}
	if err := this.retryOperation(this.applier.UnlockTables); err != nil {
		return err
	}
	if err := this.dropOldTableIfRequired(); err != nil {
		return err
	}

	lockAndRenameDuration := this.migrationContext.RenameTablesEndTime.Sub(this.migrationContext.LockTablesStartTime)
	renameDuration := this.migrationContext.RenameTablesEndTime.Sub(this.migrationContext.RenameTablesStartTime)
	log.Debugf("Lock & rename duration: %s (rename only: %s). During this time, queries on %s were locked or failing", lockAndRenameDuration, renameDuration, sql.EscapeName(this.migrationContext.OriginalTableName))
	return nil
}

// stopWritesAndCompleteMigrationOnMasterViaLock will lock down the original table, execute
// what's left of last DML entries, and atomically swap & unlock (original->old && new->original)
func (this *Migrator) stopWritesAndCompleteMigrationOnMasterViaLock() (err error) {
	lockGrabbed := make(chan error, 1)
	okToReleaseLock := make(chan bool, 1)
	swapResult := make(chan error, 1)
	go func() {
		if err := this.applier.GrabVoluntaryLock(lockGrabbed, okToReleaseLock); err != nil {
			log.Errore(err)
		}
	}()
	if err := <-lockGrabbed; err != nil {
		return log.Errore(err)
	}
	blockingQuerySessionIdChan := make(chan int64, 1)
	go func() {
		this.applier.IssueBlockingQueryOnVoluntaryLock(blockingQuerySessionIdChan)
	}()
	blockingQuerySessionId := <-blockingQuerySessionIdChan
	log.Infof("Intentional blocking query connection id is %+v", blockingQuerySessionId)

	if err := this.retryOperation(
		func() error {
			return this.applier.ExpectProcess(blockingQuerySessionId, "User lock", this.migrationContext.GetVoluntaryLockName())
		}); err != nil {
		return err
	}
	log.Infof("Found blocking query to be executing")
	swapSessionIdChan := make(chan int64, 1)
	go func() {
		swapResult <- this.applier.SwapTablesAtomic(swapSessionIdChan)
	}()

	swapSessionId := <-swapSessionIdChan
	log.Infof("RENAME connection id is %+v", swapSessionId)
	if err := this.retryOperation(
		func() error {
			return this.applier.ExpectProcess(swapSessionId, "metadata lock", "rename")
		}); err != nil {
		return err
	}
	log.Infof("Found RENAME to be executing")

	// OK, at this time we know any newly incoming DML on original table is blocked.
	this.applier.WriteChangelogState(string(AllEventsUpToLockProcessed))
	log.Debugf("Waiting for events up to lock")
	<-this.allEventsUpToLockProcessed
	log.Debugf("Done waiting for events up to lock")

	okToReleaseLock <- true
	// BAM: voluntary lock is released, blocking query is released, rename is released.
	// We now check RENAME result. We have lock_wait_timeout. We put it on purpose, to avoid
	// locking the tables for too long. If lock time exceeds said timeout, the RENAME fails
	// and returns a non-nil error, in which case tables have not been swapped, and we are
	// not really done. We are, however, good to go for more retries.
	if err := <-swapResult; err != nil {
		// Bummer. We shall rest a while and try again
		return err
	}
	// ooh nice! We're actually truly and thankfully done
	lockAndRenameDuration := this.migrationContext.RenameTablesEndTime.Sub(this.migrationContext.LockTablesStartTime)
	renameDuration := this.migrationContext.RenameTablesEndTime.Sub(this.migrationContext.RenameTablesStartTime)
	log.Debugf("Lock & rename duration: %s. Of this, rename time was %s. During rename time, queries on %s were blocked", lockAndRenameDuration, renameDuration, sql.EscapeName(this.migrationContext.OriginalTableName))
	return nil
}

// stopWritesAndCompleteMigrationOnReplica will stop replication IO thread, apply
// what DML events are left, and that's it.
// This only applies in --test-on-replica. It leaves replication stopped, with both tables
// in sync. There is no table swap.
func (this *Migrator) stopWritesAndCompleteMigrationOnReplica() (err error) {
	log.Debugf("testing on replica. Instead of LOCK tables I will STOP SLAVE")
	if err := this.retryOperation(this.applier.StopSlaveIOThread); err != nil {
		return err
	}

	this.applier.WriteChangelogState(string(AllEventsUpToLockProcessed))
	log.Debugf("Waiting for events up to lock")
	<-this.allEventsUpToLockProcessed
	log.Debugf("Done waiting for events up to lock")

	log.Info("Table duplicated with new schema. Am not touching the original table. Replication is stopped. You may now compare the two tables to gain trust into this tool's operation")
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

func (this *Migrator) printStatus() {
	elapsedTime := this.migrationContext.ElapsedTime()
	elapsedSeconds := int64(elapsedTime.Seconds())
	totalRowsCopied := this.migrationContext.GetTotalRowsCopied()
	rowsEstimate := atomic.LoadInt64(&this.migrationContext.RowsEstimate)
	progressPct := 100.0 * float64(totalRowsCopied) / float64(rowsEstimate)

	shouldPrintStatus := false
	if elapsedSeconds <= 60 {
		shouldPrintStatus = true
	} else if progressPct >= 99.0 {
		shouldPrintStatus = true
	} else if progressPct >= 95.0 {
		shouldPrintStatus = (elapsedSeconds%5 == 0)
	} else if elapsedSeconds <= 120 {
		shouldPrintStatus = (elapsedSeconds%5 == 0)
	} else {
		shouldPrintStatus = (elapsedSeconds%30 == 0)
	}
	if !shouldPrintStatus {
		return
	}

	eta := "N/A"
	if isThrottled, throttleReason := this.migrationContext.IsThrottled(); isThrottled {
		eta = fmt.Sprintf("throttled, %s", throttleReason)
	}
	status := fmt.Sprintf("Copy: %d/%d %.1f%%; Applied: %d; Backlog: %d/%d; Elapsed: %+v(copy), %+v(total); ETA: %s",
		totalRowsCopied, rowsEstimate, progressPct,
		atomic.LoadInt64(&this.migrationContext.TotalDMLEventsApplied),
		len(this.applyEventsQueue), cap(this.applyEventsQueue),
		base.PrettifyDurationOutput(this.migrationContext.ElapsedRowCopyTime()), base.PrettifyDurationOutput(elapsedTime),
		eta,
	)
	this.applier.WriteChangelog(
		fmt.Sprintf("copy iteration %d at %d", this.migrationContext.GetIteration(), time.Now().Unix()),
		status,
	)
	fmt.Println(status)
}

func (this *Migrator) initiateHeartbeatListener() {
	ticker := time.Tick((heartbeatIntervalMilliseconds * time.Millisecond) / 2)
	for range ticker {
		go func() error {
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
	this.eventsStreamer.AddListener(
		true,
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

	go func() {
		log.Debugf("Beginning streaming")
		this.eventsStreamer.StreamEvents(func() bool { return this.canStopStreaming() })
	}()
	return nil
}

func (this *Migrator) initiateApplier() error {
	this.applier = NewApplier()
	if err := this.applier.InitDBConnections(); err != nil {
		return err
	}
	if err := this.applier.ValidateOrDropExistingTables(); err != nil {
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
	if err := this.applier.CreateChangelogTable(); err != nil {
		log.Errorf("Unable to create changelog table, see further error details. Perhaps a previous migration failed without dropping the table? OR is there a running migration? Bailing out")
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
		copyRowsFunc := func() error {
			hasFurtherRange, err := this.applier.CalculateNextIterationRangeEndValues()
			if err != nil {
				return terminateRowIteration(err)
			}
			if !hasFurtherRange {
				return terminateRowIteration(nil)
			}
			_, rowsAffected, _, err := this.applier.ApplyIterationInsertQuery()
			if err != nil {
				return terminateRowIteration(err)
			}
			atomic.AddInt64(&this.migrationContext.TotalRowsCopied, rowsAffected)
			atomic.AddInt64(&this.migrationContext.Iteration, 1)
			return nil
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
						if err := this.retryOperation(copyRowsFunc); err != nil {
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
