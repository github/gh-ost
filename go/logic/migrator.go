/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/binlog"
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"
)

var (
	ErrMigratorUnsupportedRenameAlter = errors.New("ALTER statement seems to RENAME the table. This is not supported, and you should run your RENAME outside gh-ost.")
)

type ChangelogState string

const (
	AllEventsUpToLockProcessed ChangelogState = "AllEventsUpToLockProcessed"
	GhostTableMigrated         ChangelogState = "GhostTableMigrated"
	Migrated                   ChangelogState = "Migrated"
	ReadMigrationRangeValues   ChangelogState = "ReadMigrationRangeValues"
)

func ReadChangelogState(s string) ChangelogState {
	return ChangelogState(strings.Split(s, ":")[0])
}

type tableWriteFunc func() error

type applyEventStruct struct {
	writeFunc *tableWriteFunc
	dmlEvent  *binlog.BinlogDMLEvent
}

func newApplyEventStructByFunc(writeFunc *tableWriteFunc) *applyEventStruct {
	result := &applyEventStruct{writeFunc: writeFunc}
	return result
}

func newApplyEventStructByDML(dmlEvent *binlog.BinlogDMLEvent) *applyEventStruct {
	result := &applyEventStruct{dmlEvent: dmlEvent}
	return result
}

type PrintStatusRule int

const (
	NoPrintStatusRule           PrintStatusRule = iota
	HeuristicPrintStatusRule                    = iota
	ForcePrintStatusRule                        = iota
	ForcePrintStatusOnlyRule                    = iota
	ForcePrintStatusAndHintRule                 = iota
)

// Migrator is the main schema migration flow manager.
type Migrator struct {
	appVersion       string
	parser           *sql.AlterTableParser
	inspector        *Inspector
	applier          *Applier
	eventsStreamer   *EventsStreamer
	server           *Server
	throttler        *Throttler
	hooksExecutor    *HooksExecutor
	migrationContext *base.MigrationContext

	firstThrottlingCollected   chan bool
	ghostTableMigrated         chan bool
	rowCopyComplete            chan error
	allEventsUpToLockProcessed chan string

	rowCopyCompleteFlag int64
	// copyRowsQueue should not be buffered; if buffered some non-damaging but
	//  excessive work happens at the end of the iteration as new copy-jobs arrive before realizing the copy is complete
	copyRowsQueue    chan tableWriteFunc
	applyEventsQueue chan *applyEventStruct

	handledChangelogStates map[string]bool

	finishedMigrating int64
}

func NewMigrator(context *base.MigrationContext, appVersion string) *Migrator {
	migrator := &Migrator{
		appVersion:                 appVersion,
		hooksExecutor:              NewHooksExecutor(context),
		migrationContext:           context,
		parser:                     sql.NewAlterTableParser(),
		ghostTableMigrated:         make(chan bool),
		firstThrottlingCollected:   make(chan bool, 3),
		rowCopyComplete:            make(chan error),
		allEventsUpToLockProcessed: make(chan string),

		copyRowsQueue:          make(chan tableWriteFunc),
		applyEventsQueue:       make(chan *applyEventStruct, base.MaxEventsBatchSize),
		handledChangelogStates: make(map[string]bool),
		finishedMigrating:      0,
	}
	return migrator
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
		this.migrationContext.PanicAbort <- err
	}
	return err
}

// `retryOperationWithExponentialBackoff` attempts running given function, waiting 2^(n-1)
// seconds between each attempt, where `n` is the running number of attempts. Exits
// as soon as the function returns with non-error, or as soon as `MaxRetries`
// attempts are reached. Wait intervals between attempts obey a maximum of
// `ExponentialBackoffMaxInterval`.
func (this *Migrator) retryOperationWithExponentialBackoff(operation func() error, notFatalHint ...bool) (err error) {
	var interval int64
	maxRetries := int(this.migrationContext.MaxRetries())
	maxInterval := this.migrationContext.ExponentialBackoffMaxInterval
	for i := 0; i < maxRetries; i++ {
		newInterval := int64(math.Exp2(float64(i - 1)))
		if newInterval <= maxInterval {
			interval = newInterval
		}
		if i != 0 {
			time.Sleep(time.Duration(interval) * time.Second)
		}
		err = operation()
		if err == nil {
			return nil
		}
	}
	if len(notFatalHint) == 0 {
		this.migrationContext.PanicAbort <- err
	}
	return err
}

// consumeRowCopyComplete blocks on the rowCopyComplete channel once, and then
// consumes and drops any further incoming events that may be left hanging.
func (this *Migrator) consumeRowCopyComplete() {
	if err := <-this.rowCopyComplete; err != nil {
		this.migrationContext.PanicAbort <- err
	}
	atomic.StoreInt64(&this.rowCopyCompleteFlag, 1)
	this.migrationContext.MarkRowCopyEndTime()
	go func() {
		for err := range this.rowCopyComplete {
			if err != nil {
				this.migrationContext.PanicAbort <- err
			}
		}
	}()
}

func (this *Migrator) canStopStreaming() bool {
	return atomic.LoadInt64(&this.migrationContext.CutOverCompleteFlag) != 0
}

// onChangelogEvent is called when a binlog event operation on the changelog table is intercepted.
func (this *Migrator) onChangelogEvent(dmlEvent *binlog.BinlogDMLEvent) (err error) {
	// Hey, I created the changelog table, I know the type of columns it has!
	switch hint := dmlEvent.NewColumnValues.StringColumn(2); hint {
	case "state":
		return this.onChangelogStateEvent(dmlEvent)
	case "heartbeat":
		return this.onChangelogHeartbeatEvent(dmlEvent)
	default:
		return nil
	}
}

func (this *Migrator) onChangelogStateEvent(dmlEvent *binlog.BinlogDMLEvent) (err error) {
	changelogStateString := dmlEvent.NewColumnValues.StringColumn(3)
	changelogState := ReadChangelogState(changelogStateString)
	this.migrationContext.Log.Infof("Intercepted changelog state %s", changelogState)
	switch changelogState {
	case Migrated, ReadMigrationRangeValues:
		// no-op event
	case GhostTableMigrated:
		this.ghostTableMigrated <- true
	case AllEventsUpToLockProcessed:
		var applyEventFunc tableWriteFunc = func() error {
			this.allEventsUpToLockProcessed <- changelogStateString
			return nil
		}
		// at this point we know all events up to lock have been read from the streamer,
		// because the streamer works sequentially. So those events are either already handled,
		// or have event functions in applyEventsQueue.
		// So as not to create a potential deadlock, we write this func to applyEventsQueue
		// asynchronously, understanding it doesn't really matter.
		go func() {
			this.applyEventsQueue <- newApplyEventStructByFunc(&applyEventFunc)
		}()
	default:
		return fmt.Errorf("Unknown changelog state: %+v", changelogState)
	}
	this.migrationContext.Log.Infof("Handled changelog state %s", changelogState)
	return nil
}

func (this *Migrator) onChangelogHeartbeatEvent(dmlEvent *binlog.BinlogDMLEvent) (err error) {
	changelogHeartbeatString := dmlEvent.NewColumnValues.StringColumn(3)

	heartbeatTime, err := time.Parse(time.RFC3339Nano, changelogHeartbeatString)
	if err != nil {
		return this.migrationContext.Log.Errore(err)
	} else {
		this.migrationContext.SetLastHeartbeatOnChangelogTime(heartbeatTime)
		return nil
	}
}

// listenOnPanicAbort aborts on abort request
func (this *Migrator) listenOnPanicAbort() {
	err := <-this.migrationContext.PanicAbort
	this.migrationContext.Log.Fatale(err)
}

// validateAlterStatement validates the `alter` statement meets criteria.
// At this time this means:
// - column renames are approved
// - no table rename allowed
func (this *Migrator) validateAlterStatement() (err error) {
	if this.parser.IsRenameTable() {
		return ErrMigratorUnsupportedRenameAlter
	}
	if this.parser.HasNonTrivialRenames() && !this.migrationContext.SkipRenamedColumns {
		this.migrationContext.ColumnRenameMap = this.parser.GetNonTrivialRenames()
		if !this.migrationContext.ApproveRenamedColumns {
			return fmt.Errorf("gh-ost believes the ALTER statement renames columns, as follows: %v; as precaution, you are asked to confirm gh-ost is correct, and provide with `--approve-renamed-columns`, and we're all happy. Or you can skip renamed columns via `--skip-renamed-columns`, in which case column data may be lost", this.parser.GetNonTrivialRenames())
		}
		this.migrationContext.Log.Infof("Alter statement has column(s) renamed. gh-ost finds the following renames: %v; --approve-renamed-columns is given and so migration proceeds.", this.parser.GetNonTrivialRenames())
	}
	this.migrationContext.DroppedColumnsMap = this.parser.DroppedColumnsMap()
	return nil
}

func (this *Migrator) countTableRows() (err error) {
	if !this.migrationContext.CountTableRows {
		// Not counting; we stay with an estimate
		return nil
	}
	if this.migrationContext.Noop {
		this.migrationContext.Log.Debugf("Noop operation; not really counting table rows")
		return nil
	}

	countRowsFunc := func(ctx context.Context) error {
		if err := this.inspector.CountTableRows(ctx); err != nil {
			return err
		}
		if err := this.hooksExecutor.onRowCountComplete(); err != nil {
			return err
		}
		return nil
	}

	if this.migrationContext.ConcurrentCountTableRows {
		// store a cancel func so we can stop this query before a cut over
		rowCountContext, rowCountCancel := context.WithCancel(context.Background())
		this.migrationContext.SetCountTableRowsCancelFunc(rowCountCancel)

		this.migrationContext.Log.Infof("As instructed, counting rows in the background; meanwhile I will use an estimated count, and will update it later on")
		go countRowsFunc(rowCountContext)

		// and we ignore errors, because this turns to be a background job
		return nil
	}
	return countRowsFunc(context.Background())
}

func (this *Migrator) createFlagFiles() (err error) {
	if this.migrationContext.PostponeCutOverFlagFile != "" {
		if !base.FileExists(this.migrationContext.PostponeCutOverFlagFile) {
			if err := base.TouchFile(this.migrationContext.PostponeCutOverFlagFile); err != nil {
				return this.migrationContext.Log.Errorf("--postpone-cut-over-flag-file indicated by gh-ost is unable to create said file: %s", err.Error())
			}
			this.migrationContext.Log.Infof("Created postpone-cut-over-flag-file: %s", this.migrationContext.PostponeCutOverFlagFile)
		}
	}
	return nil
}

// Migrate executes the complete migration logic. This is *the* major gh-ost function.
func (this *Migrator) Migrate() (err error) {
	this.migrationContext.Log.Infof("Migrating %s.%s", sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.OriginalTableName))
	this.migrationContext.StartTime = time.Now()
	if this.migrationContext.Hostname, err = os.Hostname(); err != nil {
		return err
	}

	go this.listenOnPanicAbort()

	if err := this.hooksExecutor.onStartup(); err != nil {
		return err
	}
	if err := this.parser.ParseAlterStatement(this.migrationContext.AlterStatement); err != nil {
		return err
	}
	if err := this.validateAlterStatement(); err != nil {
		return err
	}

	// After this point, we'll need to teardown anything that's been started
	//   so we don't leave things hanging around
	defer this.teardown()

	if err := this.initiateInspector(); err != nil {
		return err
	}
	if err := this.initiateStreaming(); err != nil {
		return err
	}
	if err := this.initiateApplier(); err != nil {
		return err
	}
	if err := this.createFlagFiles(); err != nil {
		return err
	}
	// In MySQL 8.0 (and possibly earlier) some DDL statements can be applied instantly.
	// Attempt to do this if AttemptInstantDDL is set.
	if this.migrationContext.AttemptInstantDDL {
		if this.migrationContext.Noop {
			this.migrationContext.Log.Debugf("Noop operation; not really attempting instant DDL")
		} else {
			this.migrationContext.Log.Infof("Attempting to execute alter with ALGORITHM=INSTANT")
			if err := this.applier.AttemptInstantDDL(); err == nil {
				if err := this.hooksExecutor.onSuccess(); err != nil {
					return err
				}
				this.migrationContext.Log.Infof("Success! table %s.%s migrated instantly", sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.OriginalTableName))
				return nil
			} else {
				this.migrationContext.Log.Infof("ALGORITHM=INSTANT not supported for this operation, proceeding with original algorithm: %s", err)
			}
		}
	}

	initialLag, _ := this.inspector.getReplicationLag()
	this.migrationContext.Log.Infof("Waiting for ghost table to be migrated. Current lag is %+v", initialLag)
	<-this.ghostTableMigrated
	this.migrationContext.Log.Debugf("ghost table migrated")
	// Yay! We now know the Ghost and Changelog tables are good to examine!
	// When running on replica, this means the replica has those tables. When running
	// on master this is always true, of course, and yet it also implies this knowledge
	// is in the binlogs.
	if err := this.inspector.inspectOriginalAndGhostTables(); err != nil {
		return err
	}
	// We can prepare some of the queries on the applier
	if err := this.applier.prepareQueries(); err != nil {
		return err
	}
	// Validation complete! We're good to execute this migration
	if err := this.hooksExecutor.onValidated(); err != nil {
		return err
	}

	if err := this.initiateServer(); err != nil {
		return err
	}
	defer this.server.RemoveSocketFile()

	if err := this.countTableRows(); err != nil {
		return err
	}
	if err := this.addDMLEventsListener(); err != nil {
		return err
	}
	if err := this.applier.ReadMigrationRangeValues(); err != nil {
		return err
	}

	this.initiateThrottler()

	if err := this.hooksExecutor.onBeforeRowCopy(); err != nil {
		return err
	}
	go this.executeWriteFuncs()
	go this.iterateChunks()
	this.migrationContext.MarkRowCopyStartTime()
	go this.initiateStatus()

	this.migrationContext.Log.Debugf("Operating until row copy is complete")
	this.consumeRowCopyComplete()
	this.migrationContext.Log.Infof("Row copy complete")
	if err := this.hooksExecutor.onRowCopyComplete(); err != nil {
		return err
	}
	this.printStatus(ForcePrintStatusRule)

	if this.migrationContext.IsCountingTableRows() {
		this.migrationContext.Log.Info("stopping query for exact row count, because that can accidentally lock out the cut over")
		this.migrationContext.CancelTableRowsCount()
	}
	if err := this.hooksExecutor.onBeforeCutOver(); err != nil {
		return err
	}
	var retrier func(func() error, ...bool) error
	if this.migrationContext.CutOverExponentialBackoff {
		retrier = this.retryOperationWithExponentialBackoff
	} else {
		retrier = this.retryOperation
	}
	if err := retrier(this.cutOver); err != nil {
		return err
	}
	atomic.StoreInt64(&this.migrationContext.CutOverCompleteFlag, 1)

	if err := this.finalCleanup(); err != nil {
		return nil
	}
	if err := this.hooksExecutor.onSuccess(); err != nil {
		return err
	}
	this.migrationContext.Log.Infof("Done migrating %s.%s", sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.OriginalTableName))
	return nil
}

// ExecOnFailureHook executes the onFailure hook, and this method is provided as the only external
// hook access point
func (this *Migrator) ExecOnFailureHook() (err error) {
	return this.hooksExecutor.onFailure()
}

func (this *Migrator) handleCutOverResult(cutOverError error) (err error) {
	if this.migrationContext.TestOnReplica {
		// We're merely testing, we don't want to keep this state. Rollback the renames as possible
		this.applier.RenameTablesRollback()
	}
	if cutOverError == nil {
		return nil
	}
	// Only on error:

	if this.migrationContext.TestOnReplica {
		// With `--test-on-replica` we stop replication thread, and then proceed to use
		// the same cut-over phase as the master would use. That means we take locks
		// and swap the tables.
		// The difference is that we will later swap the tables back.
		if err := this.hooksExecutor.onStartReplication(); err != nil {
			return this.migrationContext.Log.Errore(err)
		}
		if this.migrationContext.TestOnReplicaSkipReplicaStop {
			this.migrationContext.Log.Warningf("--test-on-replica-skip-replica-stop enabled, we are not starting replication.")
		} else {
			this.migrationContext.Log.Debugf("testing on replica. Starting replication IO thread after cut-over failure")
			if err := this.retryOperation(this.applier.StartReplication); err != nil {
				return this.migrationContext.Log.Errore(err)
			}
		}
	}
	return nil
}

// cutOver performs the final step of migration, based on migration
// type (on replica? atomic? safe?)
func (this *Migrator) cutOver() (err error) {
	if this.migrationContext.Noop {
		this.migrationContext.Log.Debugf("Noop operation; not really swapping tables")
		return nil
	}
	this.migrationContext.MarkPointOfInterest()
	this.throttler.throttle(func() {
		this.migrationContext.Log.Debugf("throttling before swapping tables")
	})

	this.migrationContext.MarkPointOfInterest()
	this.migrationContext.Log.Debugf("checking for cut-over postpone")
	this.sleepWhileTrue(
		func() (bool, error) {
			heartbeatLag := this.migrationContext.TimeSinceLastHeartbeatOnChangelog()
			maxLagMillisecondsThrottle := time.Duration(atomic.LoadInt64(&this.migrationContext.MaxLagMillisecondsThrottleThreshold)) * time.Millisecond
			cutOverLockTimeout := time.Duration(this.migrationContext.CutOverLockTimeoutSeconds) * time.Second
			if heartbeatLag > maxLagMillisecondsThrottle || heartbeatLag > cutOverLockTimeout {
				this.migrationContext.Log.Debugf("current HeartbeatLag (%.2fs) is too high, it needs to be less than both --max-lag-millis (%.2fs) and --cut-over-lock-timeout-seconds (%.2fs) to continue", heartbeatLag.Seconds(), maxLagMillisecondsThrottle.Seconds(), cutOverLockTimeout.Seconds())
				return true, nil
			}
			if this.migrationContext.PostponeCutOverFlagFile == "" {
				return false, nil
			}
			if atomic.LoadInt64(&this.migrationContext.UserCommandedUnpostponeFlag) > 0 {
				atomic.StoreInt64(&this.migrationContext.UserCommandedUnpostponeFlag, 0)
				return false, nil
			}
			if base.FileExists(this.migrationContext.PostponeCutOverFlagFile) {
				// Postpone file defined and exists!
				if atomic.LoadInt64(&this.migrationContext.IsPostponingCutOver) == 0 {
					if err := this.hooksExecutor.onBeginPostponed(); err != nil {
						return true, err
					}
				}
				atomic.StoreInt64(&this.migrationContext.IsPostponingCutOver, 1)
				return true, nil
			}
			return false, nil
		},
	)
	atomic.StoreInt64(&this.migrationContext.IsPostponingCutOver, 0)
	this.migrationContext.MarkPointOfInterest()
	this.migrationContext.Log.Debugf("checking for cut-over postpone: complete")

	if this.migrationContext.TestOnReplica {
		// With `--test-on-replica` we stop replication thread, and then proceed to use
		// the same cut-over phase as the master would use. That means we take locks
		// and swap the tables.
		// The difference is that we will later swap the tables back.
		if err := this.hooksExecutor.onStopReplication(); err != nil {
			return err
		}
		if this.migrationContext.TestOnReplicaSkipReplicaStop {
			this.migrationContext.Log.Warningf("--test-on-replica-skip-replica-stop enabled, we are not stopping replication.")
		} else {
			this.migrationContext.Log.Debugf("testing on replica. Stopping replication IO thread")
			if err := this.retryOperation(this.applier.StopReplication); err != nil {
				return err
			}
		}
	}

	switch this.migrationContext.CutOverType {
	case base.CutOverAtomic:
		// Atomic solution: we use low timeout and multiple attempts. But for
		// each failed attempt, we throttle until replication lag is back to normal
		err = this.atomicCutOver()
	case base.CutOverTwoStep:
		err = this.cutOverTwoStep()
	default:
		return this.migrationContext.Log.Fatalf("Unknown cut-over type: %d; should never get here!", this.migrationContext.CutOverType)
	}
	this.handleCutOverResult(err)
	return err
}

// Inject the "AllEventsUpToLockProcessed" state hint, wait for it to appear in the binary logs,
// make sure the queue is drained.
func (this *Migrator) waitForEventsUpToLock() (err error) {
	timeout := time.NewTimer(time.Second * time.Duration(this.migrationContext.CutOverLockTimeoutSeconds))

	this.migrationContext.MarkPointOfInterest()
	waitForEventsUpToLockStartTime := time.Now()

	allEventsUpToLockProcessedChallenge := fmt.Sprintf("%s:%d", string(AllEventsUpToLockProcessed), waitForEventsUpToLockStartTime.UnixNano())
	this.migrationContext.Log.Infof("Writing changelog state: %+v", allEventsUpToLockProcessedChallenge)
	if _, err := this.applier.WriteChangelogState(allEventsUpToLockProcessedChallenge); err != nil {
		return err
	}
	this.migrationContext.Log.Infof("Waiting for events up to lock")
	atomic.StoreInt64(&this.migrationContext.AllEventsUpToLockProcessedInjectedFlag, 1)
	for found := false; !found; {
		select {
		case <-timeout.C:
			{
				return this.migrationContext.Log.Errorf("Timeout while waiting for events up to lock")
			}
		case state := <-this.allEventsUpToLockProcessed:
			{
				if state == allEventsUpToLockProcessedChallenge {
					this.migrationContext.Log.Infof("Waiting for events up to lock: got %s", state)
					found = true
				} else {
					this.migrationContext.Log.Infof("Waiting for events up to lock: skipping %s", state)
				}
			}
		}
	}
	waitForEventsUpToLockDuration := time.Since(waitForEventsUpToLockStartTime)

	this.migrationContext.Log.Infof("Done waiting for events up to lock; duration=%+v", waitForEventsUpToLockDuration)
	this.printStatus(ForcePrintStatusAndHintRule)

	return nil
}

// cutOverTwoStep will lock down the original table, execute
// what's left of last DML entries, and **non-atomically** swap original->old, then new->original.
// There is a point in time where the "original" table does not exist and queries are non-blocked
// and failing.
func (this *Migrator) cutOverTwoStep() (err error) {
	atomic.StoreInt64(&this.migrationContext.InCutOverCriticalSectionFlag, 1)
	defer atomic.StoreInt64(&this.migrationContext.InCutOverCriticalSectionFlag, 0)
	atomic.StoreInt64(&this.migrationContext.AllEventsUpToLockProcessedInjectedFlag, 0)

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
	this.migrationContext.Log.Debugf("Lock & rename duration: %s (rename only: %s). During this time, queries on %s were locked or failing", lockAndRenameDuration, renameDuration, sql.EscapeName(this.migrationContext.OriginalTableName))
	return nil
}

// atomicCutOver
func (this *Migrator) atomicCutOver() (err error) {
	atomic.StoreInt64(&this.migrationContext.InCutOverCriticalSectionFlag, 1)
	defer atomic.StoreInt64(&this.migrationContext.InCutOverCriticalSectionFlag, 0)

	okToUnlockTable := make(chan bool, 4)
	defer func() {
		okToUnlockTable <- true
	}()

	atomic.StoreInt64(&this.migrationContext.AllEventsUpToLockProcessedInjectedFlag, 0)

	lockOriginalSessionIdChan := make(chan int64, 2)
	tableLocked := make(chan error, 2)
	tableUnlocked := make(chan error, 2)
	go func() {
		if err := this.applier.AtomicCutOverMagicLock(lockOriginalSessionIdChan, tableLocked, okToUnlockTable, tableUnlocked); err != nil {
			this.migrationContext.Log.Errore(err)
		}
	}()
	if err := <-tableLocked; err != nil {
		return this.migrationContext.Log.Errore(err)
	}
	lockOriginalSessionId := <-lockOriginalSessionIdChan
	this.migrationContext.Log.Infof("Session locking original & magic tables is %+v", lockOriginalSessionId)
	// At this point we know the original table is locked.
	// We know any newly incoming DML on original table is blocked.
	if err := this.waitForEventsUpToLock(); err != nil {
		return this.migrationContext.Log.Errore(err)
	}

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
	this.migrationContext.Log.Infof("Session renaming tables is %+v", renameSessionId)

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
		this.migrationContext.Log.Infof("Found atomic RENAME to be blocking, as expected. Double checking the lock is still in place (though I don't strictly have to)")
	}
	if err := this.applier.ExpectUsedLock(lockOriginalSessionId); err != nil {
		// Abort operation. Just make sure to drop the magic table.
		return this.migrationContext.Log.Errore(err)
	}
	this.migrationContext.Log.Infof("Connection holding lock on original table still exists")

	// Now that we've found the RENAME blocking, AND the locking connection still alive,
	// we know it is safe to proceed to release the lock

	okToUnlockTable <- true
	// BAM! magic table dropped, original table lock is released
	// -> RENAME released -> queries on original are unblocked.
	if err := <-tableUnlocked; err != nil {
		return this.migrationContext.Log.Errore(err)
	}
	if err := <-tablesRenamed; err != nil {
		return this.migrationContext.Log.Errore(err)
	}
	this.migrationContext.RenameTablesEndTime = time.Now()

	// ooh nice! We're actually truly and thankfully done
	lockAndRenameDuration := this.migrationContext.RenameTablesEndTime.Sub(this.migrationContext.LockTablesStartTime)
	this.migrationContext.Log.Infof("Lock & rename duration: %s. During this time, queries on %s were blocked", lockAndRenameDuration, sql.EscapeName(this.migrationContext.OriginalTableName))
	return nil
}

// initiateServer begins listening on unix socket/tcp for incoming interactive commands
func (this *Migrator) initiateServer() (err error) {
	var f printStatusFunc = func(rule PrintStatusRule, writer io.Writer) {
		this.printStatus(rule, writer)
	}
	this.server = NewServer(this.migrationContext, this.hooksExecutor, f)
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
	this.inspector = NewInspector(this.migrationContext)
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
	if this.migrationContext.AssumeMasterHostname == "" {
		// No forced master host; detect master
		if this.migrationContext.ApplierConnectionConfig, err = this.inspector.getMasterConnectionConfig(); err != nil {
			return err
		}
		this.migrationContext.Log.Infof("Master found to be %+v", *this.migrationContext.ApplierConnectionConfig.ImpliedKey)
	} else {
		// Forced master host.
		key, err := mysql.ParseInstanceKey(this.migrationContext.AssumeMasterHostname)
		if err != nil {
			return err
		}
		this.migrationContext.ApplierConnectionConfig = this.migrationContext.InspectorConnectionConfig.DuplicateCredentials(*key)
		if this.migrationContext.CliMasterUser != "" {
			this.migrationContext.ApplierConnectionConfig.User = this.migrationContext.CliMasterUser
		}
		if this.migrationContext.CliMasterPassword != "" {
			this.migrationContext.ApplierConnectionConfig.Password = this.migrationContext.CliMasterPassword
		}
		this.migrationContext.Log.Infof("Master forced to be %+v", *this.migrationContext.ApplierConnectionConfig.ImpliedKey)
	}
	// validate configs
	if this.migrationContext.TestOnReplica || this.migrationContext.MigrateOnReplica {
		if this.migrationContext.InspectorIsAlsoApplier() {
			return fmt.Errorf("Instructed to --test-on-replica or --migrate-on-replica, but the server we connect to doesn't seem to be a replica")
		}
		this.migrationContext.Log.Infof("--test-on-replica or --migrate-on-replica given. Will not execute on master %+v but rather on replica %+v itself",
			*this.migrationContext.ApplierConnectionConfig.ImpliedKey, *this.migrationContext.InspectorConnectionConfig.ImpliedKey,
		)
		this.migrationContext.ApplierConnectionConfig = this.migrationContext.InspectorConnectionConfig.Duplicate()
		if this.migrationContext.GetThrottleControlReplicaKeys().Len() == 0 {
			this.migrationContext.AddThrottleControlReplicaKey(this.migrationContext.InspectorConnectionConfig.Key)
		}
	} else if this.migrationContext.InspectorIsAlsoApplier() && !this.migrationContext.AllowedRunningOnMaster {
		return fmt.Errorf("It seems like this migration attempt to run directly on master. Preferably it would be executed on a replica (and this reduces load from the master). To proceed please provide --allow-on-master. Inspector config=%+v, applier config=%+v", this.migrationContext.InspectorConnectionConfig, this.migrationContext.ApplierConnectionConfig)
	}
	if err := this.inspector.validateLogSlaveUpdates(); err != nil {
		return err
	}

	return nil
}

// initiateStatus sets and activates the printStatus() ticker
func (this *Migrator) initiateStatus() {
	this.printStatus(ForcePrintStatusAndHintRule)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var previousCount int64
	for range ticker.C {
		if atomic.LoadInt64(&this.finishedMigrating) > 0 {
			return
		}
		go this.printStatus(HeuristicPrintStatusRule)
		totalCopied := atomic.LoadInt64(&this.migrationContext.TotalRowsCopied)
		if previousCount > 0 {
			copiedThisLoop := totalCopied - previousCount
			atomic.StoreInt64(&this.migrationContext.EtaRowsPerSecond, copiedThisLoop)
		}
		previousCount = totalCopied
	}
}

// printMigrationStatusHint prints a detailed configuration dump, that is useful
// to keep in mind; such as the name of migrated table, throttle params etc.
// This gets printed at beginning and end of migration, every 10 minutes throughout
// migration, and as response to the "status" interactive command.
func (this *Migrator) printMigrationStatusHint(writers ...io.Writer) {
	w := io.MultiWriter(writers...)
	fmt.Fprintf(w, "# Migrating %s.%s; Ghost table is %s.%s\n",
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.OriginalTableName),
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetGhostTableName()),
	)
	fmt.Fprintf(w, "# Migrating %+v; inspecting %+v; executing on %+v\n",
		*this.applier.connectionConfig.ImpliedKey,
		*this.inspector.connectionConfig.ImpliedKey,
		this.migrationContext.Hostname,
	)
	fmt.Fprintf(w, "# Migration started at %+v\n",
		this.migrationContext.StartTime.Format(time.RubyDate),
	)
	maxLoad := this.migrationContext.GetMaxLoad()
	criticalLoad := this.migrationContext.GetCriticalLoad()
	fmt.Fprintf(w, "# chunk-size: %+v; max-lag-millis: %+vms; dml-batch-size: %+v; max-load: %s; critical-load: %s; nice-ratio: %f\n",
		atomic.LoadInt64(&this.migrationContext.ChunkSize),
		atomic.LoadInt64(&this.migrationContext.MaxLagMillisecondsThrottleThreshold),
		atomic.LoadInt64(&this.migrationContext.DMLBatchSize),
		maxLoad.String(),
		criticalLoad.String(),
		this.migrationContext.GetNiceRatio(),
	)
	if this.migrationContext.ThrottleFlagFile != "" {
		setIndicator := ""
		if base.FileExists(this.migrationContext.ThrottleFlagFile) {
			setIndicator = "[set]"
		}
		fmt.Fprintf(w, "# throttle-flag-file: %+v %+v\n",
			this.migrationContext.ThrottleFlagFile, setIndicator,
		)
	}
	if this.migrationContext.ThrottleAdditionalFlagFile != "" {
		setIndicator := ""
		if base.FileExists(this.migrationContext.ThrottleAdditionalFlagFile) {
			setIndicator = "[set]"
		}
		fmt.Fprintf(w, "# throttle-additional-flag-file: %+v %+v\n",
			this.migrationContext.ThrottleAdditionalFlagFile, setIndicator,
		)
	}
	if throttleQuery := this.migrationContext.GetThrottleQuery(); throttleQuery != "" {
		fmt.Fprintf(w, "# throttle-query: %+v\n",
			throttleQuery,
		)
	}
	if throttleControlReplicaKeys := this.migrationContext.GetThrottleControlReplicaKeys(); throttleControlReplicaKeys.Len() > 0 {
		fmt.Fprintf(w, "# throttle-control-replicas count: %+v\n",
			throttleControlReplicaKeys.Len(),
		)
	}

	if this.migrationContext.PostponeCutOverFlagFile != "" {
		setIndicator := ""
		if base.FileExists(this.migrationContext.PostponeCutOverFlagFile) {
			setIndicator = "[set]"
		}
		fmt.Fprintf(w, "# postpone-cut-over-flag-file: %+v %+v\n",
			this.migrationContext.PostponeCutOverFlagFile, setIndicator,
		)
	}
	if this.migrationContext.PanicFlagFile != "" {
		fmt.Fprintf(w, "# panic-flag-file: %+v\n",
			this.migrationContext.PanicFlagFile,
		)
	}
	fmt.Fprintf(w, "# Serving on unix socket: %+v\n",
		this.migrationContext.ServeSocketFile,
	)
	if this.migrationContext.ServeTCPPort != 0 {
		fmt.Fprintf(w, "# Serving on TCP port: %+v\n", this.migrationContext.ServeTCPPort)
	}
}

// getProgressPercent returns an estimate of migration progess as a percent.
func (this *Migrator) getProgressPercent(rowsEstimate int64) (progressPct float64) {
	progressPct = 100.0
	if rowsEstimate > 0 {
		progressPct *= float64(this.migrationContext.GetTotalRowsCopied()) / float64(rowsEstimate)
	}
	return progressPct
}

// getMigrationETA returns the estimated duration of the migration
func (this *Migrator) getMigrationETA(rowsEstimate int64) (eta string, duration time.Duration) {
	duration = time.Duration(base.ETAUnknown)
	progressPct := this.getProgressPercent(rowsEstimate)
	if progressPct >= 100.0 {
		duration = 0
	} else if progressPct >= 0.1 {
		totalRowsCopied := this.migrationContext.GetTotalRowsCopied()
		etaRowsPerSecond := atomic.LoadInt64(&this.migrationContext.EtaRowsPerSecond)
		var etaSeconds float64
		// If there is data available on our current row-copies-per-second rate, use it.
		// Otherwise we can fallback to the total elapsed time and extrapolate.
		// This is going to be less accurate on a longer copy as the insert rate
		// will tend to slow down.
		if etaRowsPerSecond > 0 {
			remainingRows := float64(rowsEstimate) - float64(totalRowsCopied)
			etaSeconds = remainingRows / float64(etaRowsPerSecond)
		} else {
			elapsedRowCopySeconds := this.migrationContext.ElapsedRowCopyTime().Seconds()
			totalExpectedSeconds := elapsedRowCopySeconds * float64(rowsEstimate) / float64(totalRowsCopied)
			etaSeconds = totalExpectedSeconds - elapsedRowCopySeconds
		}
		if etaSeconds >= 0 {
			duration = time.Duration(etaSeconds) * time.Second
		} else {
			duration = 0
		}
	}

	switch duration {
	case 0:
		eta = "due"
	case time.Duration(base.ETAUnknown):
		eta = "N/A"
	default:
		eta = base.PrettifyDurationOutput(duration)
	}

	return eta, duration
}

// getMigrationStateAndETA returns the state and eta of the migration.
func (this *Migrator) getMigrationStateAndETA(rowsEstimate int64) (state, eta string, etaDuration time.Duration) {
	eta, etaDuration = this.getMigrationETA(rowsEstimate)
	state = "migrating"
	if atomic.LoadInt64(&this.migrationContext.CountingRowsFlag) > 0 && !this.migrationContext.ConcurrentCountTableRows {
		state = "counting rows"
	} else if atomic.LoadInt64(&this.migrationContext.IsPostponingCutOver) > 0 {
		eta = "due"
		state = "postponing cut-over"
	} else if isThrottled, throttleReason, _ := this.migrationContext.IsThrottled(); isThrottled {
		state = fmt.Sprintf("throttled, %s", throttleReason)
	}
	return state, eta, etaDuration
}

// shouldPrintStatus returns true when the migrator is due to print status info.
func (this *Migrator) shouldPrintStatus(rule PrintStatusRule, elapsedSeconds int64, etaDuration time.Duration) (shouldPrint bool) {
	if rule != HeuristicPrintStatusRule {
		return true
	}

	etaSeconds := etaDuration.Seconds()
	if elapsedSeconds <= 60 {
		shouldPrint = true
	} else if etaSeconds <= 60 {
		shouldPrint = true
	} else if etaSeconds <= 180 {
		shouldPrint = (elapsedSeconds%5 == 0)
	} else if elapsedSeconds <= 180 {
		shouldPrint = (elapsedSeconds%5 == 0)
	} else if this.migrationContext.TimeSincePointOfInterest().Seconds() <= 60 {
		shouldPrint = (elapsedSeconds%5 == 0)
	} else {
		shouldPrint = (elapsedSeconds%30 == 0)
	}

	return shouldPrint
}

// shouldPrintMigrationStatus returns true when the migrator is due to print the migration status hint
func (this *Migrator) shouldPrintMigrationStatusHint(rule PrintStatusRule, elapsedSeconds int64) (shouldPrint bool) {
	if elapsedSeconds%600 == 0 {
		shouldPrint = true
	} else if rule == ForcePrintStatusAndHintRule {
		shouldPrint = true
	}
	return shouldPrint
}

// printStatus prints the progress status, and optionally additionally detailed
// dump of configuration.
// `rule` indicates the type of output expected.
// By default the status is written to standard output, but other writers can
// be used as well.
func (this *Migrator) printStatus(rule PrintStatusRule, writers ...io.Writer) {
	if rule == NoPrintStatusRule {
		return
	}
	writers = append(writers, os.Stdout)

	elapsedTime := this.migrationContext.ElapsedTime()
	elapsedSeconds := int64(elapsedTime.Seconds())
	totalRowsCopied := this.migrationContext.GetTotalRowsCopied()
	rowsEstimate := atomic.LoadInt64(&this.migrationContext.RowsEstimate) + atomic.LoadInt64(&this.migrationContext.RowsDeltaEstimate)
	if atomic.LoadInt64(&this.rowCopyCompleteFlag) == 1 {
		// Done copying rows. The totalRowsCopied value is the de-facto number of rows,
		// and there is no further need to keep updating the value.
		rowsEstimate = totalRowsCopied
	}

	// we take the opportunity to update migration context with progressPct
	progressPct := this.getProgressPercent(rowsEstimate)
	this.migrationContext.SetProgressPct(progressPct)

	// Before status, let's see if we should print a nice reminder for what exactly we're doing here.
	if this.shouldPrintMigrationStatusHint(rule, elapsedSeconds) {
		this.printMigrationStatusHint(writers...)
	}

	// Get state + ETA
	state, eta, etaDuration := this.getMigrationStateAndETA(rowsEstimate)
	this.migrationContext.SetETADuration(etaDuration)

	if !this.shouldPrintStatus(rule, elapsedSeconds, etaDuration) {
		return
	}

	currentBinlogCoordinates := *this.eventsStreamer.GetCurrentBinlogCoordinates()

	status := fmt.Sprintf("Copy: %d/%d %.1f%%; Applied: %d; Backlog: %d/%d; Time: %+v(total), %+v(copy); streamer: %+v; Lag: %.2fs, HeartbeatLag: %.2fs, State: %s; ETA: %s",
		totalRowsCopied, rowsEstimate, progressPct,
		atomic.LoadInt64(&this.migrationContext.TotalDMLEventsApplied),
		len(this.applyEventsQueue), cap(this.applyEventsQueue),
		base.PrettifyDurationOutput(elapsedTime), base.PrettifyDurationOutput(this.migrationContext.ElapsedRowCopyTime()),
		currentBinlogCoordinates,
		this.migrationContext.GetCurrentLagDuration().Seconds(),
		this.migrationContext.TimeSinceLastHeartbeatOnChangelog().Seconds(),
		state,
		eta,
	)
	this.applier.WriteChangelog(
		fmt.Sprintf("copy iteration %d at %d", this.migrationContext.GetIteration(), time.Now().Unix()),
		state,
	)
	w := io.MultiWriter(writers...)
	fmt.Fprintln(w, status)

	// This "hack" is required here because the underlying logging library
	// github.com/outbrain/golib/log provides two functions Info and Infof; but the arguments of
	// both these functions are eventually redirected to the same function, which internally calls
	// fmt.Sprintf. So, the argument of every function called on the DefaultLogger object
	// migrationContext.Log will eventually pass through fmt.Sprintf, and thus the '%' character
	// needs to be escaped.
	this.migrationContext.Log.Info(strings.Replace(status, "%", "%%", 1))

	hooksStatusIntervalSec := this.migrationContext.HooksStatusIntervalSec
	if hooksStatusIntervalSec > 0 && elapsedSeconds%hooksStatusIntervalSec == 0 {
		this.hooksExecutor.onStatus(status)
	}
}

// initiateStreaming begins streaming of binary log events and registers listeners for such events
func (this *Migrator) initiateStreaming() error {
	this.eventsStreamer = NewEventsStreamer(this.migrationContext)
	if err := this.eventsStreamer.InitDBConnections(); err != nil {
		return err
	}
	this.eventsStreamer.AddListener(
		false,
		this.migrationContext.DatabaseName,
		this.migrationContext.GetChangelogTableName(),
		func(dmlEvent *binlog.BinlogDMLEvent) error {
			return this.onChangelogEvent(dmlEvent)
		},
	)

	go func() {
		this.migrationContext.Log.Debugf("Beginning streaming")
		err := this.eventsStreamer.StreamEvents(this.canStopStreaming)
		if err != nil {
			this.migrationContext.PanicAbort <- err
		}
		this.migrationContext.Log.Debugf("Done streaming")
	}()

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if atomic.LoadInt64(&this.finishedMigrating) > 0 {
				return
			}
			this.migrationContext.SetRecentBinlogCoordinates(*this.eventsStreamer.GetCurrentBinlogCoordinates())
		}
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
			this.applyEventsQueue <- newApplyEventStructByDML(dmlEvent)
			return nil
		},
	)
	return err
}

// initiateThrottler kicks in the throttling collection and the throttling checks.
func (this *Migrator) initiateThrottler() {
	this.throttler = NewThrottler(this.migrationContext, this.applier, this.inspector, this.appVersion)

	go this.throttler.initiateThrottlerCollection(this.firstThrottlingCollected)
	this.migrationContext.Log.Infof("Waiting for first throttle metrics to be collected")
	<-this.firstThrottlingCollected // replication lag
	<-this.firstThrottlingCollected // HTTP status
	<-this.firstThrottlingCollected // other, general metrics
	this.migrationContext.Log.Infof("First throttle metrics collected")
	go this.throttler.initiateThrottlerChecks()
}

func (this *Migrator) initiateApplier() error {
	this.applier = NewApplier(this.migrationContext)
	if err := this.applier.InitDBConnections(); err != nil {
		return err
	}
	if err := this.applier.ValidateOrDropExistingTables(); err != nil {
		return err
	}
	if err := this.applier.CreateChangelogTable(); err != nil {
		this.migrationContext.Log.Errorf("Unable to create changelog table, see further error details. Perhaps a previous migration failed without dropping the table? OR is there a running migration? Bailing out")
		return err
	}
	if err := this.applier.CreateGhostTable(); err != nil {
		this.migrationContext.Log.Errorf("Unable to create ghost table, see further error details. Perhaps a previous migration failed without dropping the table? Bailing out")
		return err
	}

	if err := this.applier.AlterGhost(); err != nil {
		this.migrationContext.Log.Errorf("Unable to ALTER ghost table, see further error details. Bailing out")
		return err
	}

	if this.migrationContext.OriginalTableAutoIncrement > 0 && !this.parser.IsAutoIncrementDefined() {
		// Original table has AUTO_INCREMENT value and the -alter statement does not indicate any override,
		// so we should copy AUTO_INCREMENT value onto our ghost table.
		if err := this.applier.AlterGhostAutoIncrement(); err != nil {
			this.migrationContext.Log.Errorf("Unable to ALTER ghost table AUTO_INCREMENT value, see further error details. Bailing out")
			return err
		}
	}
	this.applier.WriteChangelogState(string(GhostTableMigrated))
	go this.applier.InitiateHeartbeat()
	return nil
}

// iterateChunks iterates the existing table rows, and generates a copy task of
// a chunk of rows onto the ghost table.
func (this *Migrator) iterateChunks() error {
	terminateRowIteration := func(err error) error {
		this.rowCopyComplete <- err
		return this.migrationContext.Log.Errore(err)
	}
	if this.migrationContext.Noop {
		this.migrationContext.Log.Debugf("Noop operation; not really copying data")
		return terminateRowIteration(nil)
	}
	if this.migrationContext.MigrationRangeMinValues == nil {
		this.migrationContext.Log.Debugf("No rows found in table. Rowcopy will be implicitly empty")
		return terminateRowIteration(nil)
	}

	var hasNoFurtherRangeFlag int64
	// Iterate per chunk:
	for {
		if atomic.LoadInt64(&this.rowCopyCompleteFlag) == 1 || atomic.LoadInt64(&hasNoFurtherRangeFlag) == 1 {
			// Done
			// There's another such check down the line
			return nil
		}
		copyRowsFunc := func() error {
			if atomic.LoadInt64(&this.rowCopyCompleteFlag) == 1 || atomic.LoadInt64(&hasNoFurtherRangeFlag) == 1 {
				// Done.
				// There's another such check down the line
				return nil
			}

			// When hasFurtherRange is false, original table might be write locked and CalculateNextIterationRangeEndValues would hangs forever

			hasFurtherRange := false
			if err := this.retryOperation(func() (e error) {
				hasFurtherRange, e = this.applier.CalculateNextIterationRangeEndValues()
				return e
			}); err != nil {
				return terminateRowIteration(err)
			}
			if !hasFurtherRange {
				atomic.StoreInt64(&hasNoFurtherRangeFlag, 1)
				return terminateRowIteration(nil)
			}
			// Copy task:
			applyCopyRowsFunc := func() error {
				if atomic.LoadInt64(&this.rowCopyCompleteFlag) == 1 {
					// No need for more writes.
					// This is the de-facto place where we avoid writing in the event of completed cut-over.
					// There could _still_ be a race condition, but that's as close as we can get.
					// What about the race condition? Well, there's actually no data integrity issue.
					// when rowCopyCompleteFlag==1 that means **guaranteed** all necessary rows have been copied.
					// But some are still then collected at the binary log, and these are the ones we're trying to
					// not apply here. If the race condition wins over us, then we just attempt to apply onto the
					// _ghost_ table, which no longer exists. So, bothering error messages and all, but no damage.
					return nil
				}
				_, rowsAffected, _, err := this.applier.ApplyIterationInsertQuery()
				if err != nil {
					return err // wrapping call will retry
				}
				atomic.AddInt64(&this.migrationContext.TotalRowsCopied, rowsAffected)
				atomic.AddInt64(&this.migrationContext.Iteration, 1)
				return nil
			}
			if err := this.retryOperation(applyCopyRowsFunc); err != nil {
				return terminateRowIteration(err)
			}
			return nil
		}
		// Enqueue copy operation; to be executed by executeWriteFuncs()
		this.copyRowsQueue <- copyRowsFunc
	}
}

func (this *Migrator) onApplyEventStruct(eventStruct *applyEventStruct) error {
	handleNonDMLEventStruct := func(eventStruct *applyEventStruct) error {
		if eventStruct.writeFunc != nil {
			if err := this.retryOperation(*eventStruct.writeFunc); err != nil {
				return this.migrationContext.Log.Errore(err)
			}
		}
		return nil
	}
	if eventStruct.dmlEvent == nil {
		return handleNonDMLEventStruct(eventStruct)
	}
	if eventStruct.dmlEvent != nil {
		dmlEvents := [](*binlog.BinlogDMLEvent){}
		dmlEvents = append(dmlEvents, eventStruct.dmlEvent)
		var nonDmlStructToApply *applyEventStruct

		availableEvents := len(this.applyEventsQueue)
		batchSize := int(atomic.LoadInt64(&this.migrationContext.DMLBatchSize))
		if availableEvents > batchSize-1 {
			// The "- 1" is because we already consumed one event: the original event that led to this function getting called.
			// So, if DMLBatchSize==1 we wish to not process any further events
			availableEvents = batchSize - 1
		}
		for i := 0; i < availableEvents; i++ {
			additionalStruct := <-this.applyEventsQueue
			if additionalStruct.dmlEvent == nil {
				// Not a DML. We don't group this, and we don't batch any further
				nonDmlStructToApply = additionalStruct
				break
			}
			dmlEvents = append(dmlEvents, additionalStruct.dmlEvent)
		}
		// Create a task to apply the DML event; this will be execute by executeWriteFuncs()
		var applyEventFunc tableWriteFunc = func() error {
			return this.applier.ApplyDMLEventQueries(dmlEvents)
		}
		if err := this.retryOperation(applyEventFunc); err != nil {
			return this.migrationContext.Log.Errore(err)
		}
		if nonDmlStructToApply != nil {
			// We pulled DML events from the queue, and then we hit a non-DML event. Wait!
			// We need to handle it!
			if err := handleNonDMLEventStruct(nonDmlStructToApply); err != nil {
				return this.migrationContext.Log.Errore(err)
			}
		}
	}
	return nil
}

// executeWriteFuncs writes data via applier: both the rowcopy and the events backlog.
// This is where the ghost table gets the data. The function fills the data single-threaded.
// Both event backlog and rowcopy events are polled; the backlog events have precedence.
func (this *Migrator) executeWriteFuncs() error {
	if this.migrationContext.Noop {
		this.migrationContext.Log.Debugf("Noop operation; not really executing write funcs")
		return nil
	}
	for {
		if atomic.LoadInt64(&this.finishedMigrating) > 0 {
			return nil
		}

		this.throttler.throttle(nil)

		// We give higher priority to event processing, then secondary priority to
		// rowcopy
		select {
		case eventStruct := <-this.applyEventsQueue:
			{
				if err := this.onApplyEventStruct(eventStruct); err != nil {
					return err
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
							return this.migrationContext.Log.Errore(err)
						}
						if niceRatio := this.migrationContext.GetNiceRatio(); niceRatio > 0 {
							copyRowsDuration := time.Since(copyRowsStartTime)
							sleepTimeNanosecondFloat64 := niceRatio * float64(copyRowsDuration.Nanoseconds())
							sleepTime := time.Duration(int64(sleepTimeNanosecondFloat64)) * time.Nanosecond
							time.Sleep(sleepTime)
						}
					}
				default:
					{
						// Hmmmmm... nothing in the queue; no events, but also no row copy.
						// This is possible upon load. Let's just sleep it over.
						this.migrationContext.Log.Debugf("Getting nothing in the write queue. Sleeping...")
						time.Sleep(time.Second)
					}
				}
			}
		}
	}
}

// finalCleanup takes actions at very end of migration, dropping tables etc.
func (this *Migrator) finalCleanup() error {
	atomic.StoreInt64(&this.migrationContext.CleanupImminentFlag, 1)

	this.migrationContext.Log.Infof("Writing changelog state: %+v", Migrated)
	if _, err := this.applier.WriteChangelogState(string(Migrated)); err != nil {
		return err
	}

	if this.migrationContext.Noop {
		if createTableStatement, err := this.inspector.showCreateTable(this.migrationContext.GetGhostTableName()); err == nil {
			this.migrationContext.Log.Infof("New table structure follows")
			fmt.Println(createTableStatement)
		} else {
			this.migrationContext.Log.Errore(err)
		}
	}
	if err := this.eventsStreamer.Close(); err != nil {
		this.migrationContext.Log.Errore(err)
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
			this.migrationContext.Log.Infof("Am not dropping old table because I want this operation to be as live as possible. If you insist I should do it, please add `--ok-to-drop-table` next time. But I prefer you do not. To drop the old table, issue:")
			this.migrationContext.Log.Infof("-- drop table %s.%s", sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.GetOldTableName()))
		}
	}
	if this.migrationContext.Noop {
		if err := this.retryOperation(this.applier.DropGhostTable); err != nil {
			return err
		}
	}

	return nil
}

func (this *Migrator) teardown() {
	atomic.StoreInt64(&this.finishedMigrating, 1)

	if this.inspector != nil {
		this.migrationContext.Log.Infof("Tearing down inspector")
		this.inspector.Teardown()
	}

	if this.applier != nil {
		this.migrationContext.Log.Infof("Tearing down applier")
		this.applier.Teardown()
	}

	if this.eventsStreamer != nil {
		this.migrationContext.Log.Infof("Tearing down streamer")
		this.eventsStreamer.Teardown()
	}

	if this.throttler != nil {
		this.migrationContext.Log.Infof("Tearing down throttler")
		this.throttler.Teardown()
	}
}
