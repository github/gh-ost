/*
   Copyright 2025 GitHub Inc.
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
	ErrMigratorUnsupportedRenameAlter = errors.New("alter statement seems to RENAME the table. This is not supported, and you should run your RENAME outside gh-ost")
	ErrMigrationNotAllowedOnMaster    = errors.New("it seems like this migration attempt to run directly on master. Preferably it would be executed on a replica (this reduces load from the master). To proceed please provide --allow-on-master")
	RetrySleepFn                      = time.Sleep
	checkpointTimeout                 = 2 * time.Second
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

type lockProcessedStruct struct {
	state  string
	coords mysql.BinlogCoordinates
}

type applyEventStruct struct {
	writeFunc *tableWriteFunc
	dmlEvent  *binlog.BinlogDMLEvent
	coords    mysql.BinlogCoordinates
}

func newApplyEventStructByFunc(writeFunc *tableWriteFunc) *applyEventStruct {
	result := &applyEventStruct{writeFunc: writeFunc}
	return result
}

func newApplyEventStructByDML(dmlEntry *binlog.BinlogEntry) *applyEventStruct {
	result := &applyEventStruct{dmlEvent: dmlEntry.DmlEvent, coords: dmlEntry.Coordinates}
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
	allEventsUpToLockProcessed chan *lockProcessedStruct
	lastLockProcessed          *lockProcessedStruct

	rowCopyCompleteFlag int64
	// copyRowsQueue should not be buffered; if buffered some non-damaging but
	//  excessive work happens at the end of the iteration as new copy-jobs arrive before realizing the copy is complete
	copyRowsQueue    chan tableWriteFunc
	applyEventsQueue chan *applyEventStruct

	finishedMigrating int64
}

func NewMigrator(context *base.MigrationContext, appVersion string) *Migrator {
	migrator := &Migrator{
		appVersion:               appVersion,
		hooksExecutor:            NewHooksExecutor(context),
		migrationContext:         context,
		parser:                   sql.NewAlterTableParser(),
		ghostTableMigrated:       make(chan bool),
		firstThrottlingCollected: make(chan bool, 3),
		rowCopyComplete:          make(chan error),
		// Buffered with capacity 1; the send uses overwrite-oldest semantics
		// to prevent both deadlock (see https://github.com/github/gh-ost/pull/1637)
		// and OOM when MaxRetries() is extremely large.
		allEventsUpToLockProcessed: make(chan *lockProcessedStruct, 1),

		copyRowsQueue:     make(chan tableWriteFunc),
		applyEventsQueue:  make(chan *applyEventStruct, base.MaxEventsBatchSize),
		finishedMigrating: 0,
	}
	return migrator
}

// sleepWhileTrue sleeps indefinitely until the given function returns 'false'
// (or fails with error)
func (mgtr *Migrator) sleepWhileTrue(operation func() (bool, error)) error {
	for {
		// Check for abort before continuing
		if err := mgtr.checkAbort(); err != nil {
			return err
		}
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

func (mgtr *Migrator) retryBatchCopyWithHooks(operation func() error, notFatalHint ...bool) (err error) {
	wrappedOperation := func() error {
		if err := operation(); err != nil {
			mgtr.hooksExecutor.onBatchCopyRetry(err.Error())
			return err
		}
		return nil
	}

	return mgtr.retryOperation(wrappedOperation, notFatalHint...)
}

// retryOperation attempts up to `count` attempts at running given function,
// exiting as soon as it returns with non-error.
func (mgtr *Migrator) retryOperation(operation func() error, notFatalHint ...bool) (err error) {
	maxRetries := int(mgtr.migrationContext.MaxRetries())
	for i := 0; i < maxRetries; i++ {
		if i != 0 {
			// sleep after previous iteration
			RetrySleepFn(1 * time.Second)
		}
		// Check for abort/context cancellation before each retry
		if abortErr := mgtr.checkAbort(); abortErr != nil {
			return abortErr
		}
		err = operation()
		if err == nil {
			return nil
		}
		// Check if this is an unrecoverable error (data consistency issues won't resolve on retry)
		if strings.Contains(err.Error(), "warnings detected") {
			if len(notFatalHint) == 0 {
				_ = base.SendWithContext(mgtr.migrationContext.GetContext(), mgtr.migrationContext.PanicAbort, err)
			}
			return err
		}
		// there's an error. Let's try again.
	}
	if len(notFatalHint) == 0 {
		// Use helper to prevent deadlock if listenOnPanicAbort already exited
		_ = base.SendWithContext(mgtr.migrationContext.GetContext(), mgtr.migrationContext.PanicAbort, err)
	}
	return err
}

// `retryOperationWithExponentialBackoff` attempts running given function, waiting 2^(n-1)
// seconds between each attempt, where `n` is the running number of attempts. Exits
// as soon as the function returns with non-error, or as soon as `MaxRetries`
// attempts are reached. Wait intervals between attempts obey a maximum of
// `ExponentialBackoffMaxInterval`.
func (mgtr *Migrator) retryOperationWithExponentialBackoff(operation func() error, notFatalHint ...bool) (err error) {
	maxRetries := int(mgtr.migrationContext.MaxRetries())
	maxInterval := mgtr.migrationContext.ExponentialBackoffMaxInterval
	for i := 0; i < maxRetries; i++ {
		interval := math.Min(
			float64(maxInterval),
			math.Max(1, math.Exp2(float64(i-1))),
		)

		if i != 0 {
			RetrySleepFn(time.Duration(interval) * time.Second)
		}
		// Check for abort/context cancellation before each retry
		if abortErr := mgtr.checkAbort(); abortErr != nil {
			return abortErr
		}
		err = operation()
		if err == nil {
			return nil
		}
		// Check if this is an unrecoverable error (data consistency issues won't resolve on retry)
		if strings.Contains(err.Error(), "warnings detected") {
			if len(notFatalHint) == 0 {
				_ = base.SendWithContext(mgtr.migrationContext.GetContext(), mgtr.migrationContext.PanicAbort, err)
			}
			return err
		}
	}
	if len(notFatalHint) == 0 {
		// Use helper to prevent deadlock if listenOnPanicAbort already exited
		_ = base.SendWithContext(mgtr.migrationContext.GetContext(), mgtr.migrationContext.PanicAbort, err)
	}
	return err
}

// consumeRowCopyComplete blocks on the rowCopyComplete channel once, and then
// consumes and drops any further incoming events that may be left hanging.
func (mgtr *Migrator) consumeRowCopyComplete() {
	if err := <-mgtr.rowCopyComplete; err != nil {
		// Abort synchronously to ensure checkAbort() sees the error immediately
		mgtr.abort(err)
		// Don't mark row copy as complete if there was an error
		return
	}
	atomic.StoreInt64(&mgtr.rowCopyCompleteFlag, 1)
	mgtr.migrationContext.MarkRowCopyEndTime()
	go func() {
		for err := range mgtr.rowCopyComplete {
			if err != nil {
				// Abort synchronously to ensure the error is stored immediately
				mgtr.abort(err)
				return
			}
		}
	}()
}

func (mgtr *Migrator) canStopStreaming() bool {
	return atomic.LoadInt64(&mgtr.migrationContext.CutOverCompleteFlag) != 0
}

// onChangelogEvent is called when a binlog event operation on the changelog table is intercepted.
func (mgtr *Migrator) onChangelogEvent(dmlEntry *binlog.BinlogEntry) (err error) {
	// Hey, I created the changelog table, I know the type of columns it has!
	switch hint := dmlEntry.DmlEvent.NewColumnValues.StringColumn(2); hint {
	case "state":
		return mgtr.onChangelogStateEvent(dmlEntry)
	case "heartbeat":
		return mgtr.onChangelogHeartbeatEvent(dmlEntry)
	default:
		return nil
	}
}

func (mgtr *Migrator) onChangelogStateEvent(dmlEntry *binlog.BinlogEntry) (err error) {
	changelogStateString := dmlEntry.DmlEvent.NewColumnValues.StringColumn(3)
	changelogState := ReadChangelogState(changelogStateString)
	mgtr.migrationContext.Log.Infof("Intercepted changelog state %s", changelogState)
	switch changelogState {
	case Migrated, ReadMigrationRangeValues:
		// no-op event
	case GhostTableMigrated:
		// Use helper to prevent deadlock if migration aborts before receiver is ready
		_ = base.SendWithContext(mgtr.migrationContext.GetContext(), mgtr.ghostTableMigrated, true)
	case AllEventsUpToLockProcessed:
		lps := &lockProcessedStruct{
			state:  changelogStateString,
			coords: dmlEntry.Coordinates.Clone(),
		}
		var applyEventFunc tableWriteFunc = func() error {
			// Non-blocking send with overwrite-oldest semantics: if the buffer is
			// full (receiver timed out on a previous attempt), drain the stale
			// message first so the current sentinel is always delivered. This
			// prevents both goroutine leaks (the original PR #1637 issue) and OOM
			// when MaxRetries() is very large.
			select {
			case mgtr.allEventsUpToLockProcessed <- lps:
			default:
				// Buffer full — drain the stale value, then send the current one.
				select {
				case <-mgtr.allEventsUpToLockProcessed:
				default:
				}
				select {
				case mgtr.allEventsUpToLockProcessed <- lps:
				default:
					// Concurrent drain by another goroutine or receiver; the current
					// value is no longer needed since a newer sentinel will follow.
				}
			}
			return nil
		}
		// at this point we know all events up to lock have been read from the streamer,
		// because the streamer works sequentially. So those events are either already handled,
		// or have event functions in applyEventsQueue.
		// So as not to create a potential deadlock, we write this func to applyEventsQueue
		// asynchronously, understanding it doesn't really matter.
		go func() {
			// Use helper to prevent deadlock if buffer fills and executeWriteFuncs exits
			_ = base.SendWithContext(mgtr.migrationContext.GetContext(), mgtr.applyEventsQueue, newApplyEventStructByFunc(&applyEventFunc))
		}()
	default:
		return fmt.Errorf("unknown changelog state: %+v", changelogState)
	}
	mgtr.migrationContext.Log.Infof("Handled changelog state %s", changelogState)
	return nil
}

func (mgtr *Migrator) onChangelogHeartbeatEvent(dmlEntry *binlog.BinlogEntry) (err error) {
	changelogHeartbeatString := dmlEntry.DmlEvent.NewColumnValues.StringColumn(3)

	heartbeatTime, err := time.Parse(time.RFC3339Nano, changelogHeartbeatString)
	if err != nil {
		return mgtr.migrationContext.Log.Errore(err)
	} else {
		mgtr.migrationContext.SetLastHeartbeatOnChangelogTime(heartbeatTime)
		mgtr.applier.CurrentCoordinatesMutex.Lock()
		mgtr.applier.CurrentCoordinates = dmlEntry.Coordinates
		mgtr.applier.CurrentCoordinatesMutex.Unlock()
		return nil
	}
}

// abort stores the error, cancels the context, and logs the abort.
// This is the common abort logic used by both listenOnPanicAbort and
// consumeRowCopyComplete to ensure consistent error handling.
func (mgtr *Migrator) abort(err error) {
	// Store the error for Migrate() to return
	mgtr.migrationContext.SetAbortError(err)

	// Cancel the context to signal all goroutines to stop
	mgtr.migrationContext.CancelContext()

	// Log the error (but don't panic or exit)
	mgtr.migrationContext.Log.Errorf("migration aborted: %v", err)
}

// listenOnPanicAbort listens for fatal errors and initiates graceful shutdown
func (mgtr *Migrator) listenOnPanicAbort() {
	err := <-mgtr.migrationContext.PanicAbort
	mgtr.abort(err)
}

// validateAlterStatement validates the `alter` statement meets criteria.
// At this time this means:
// - column renames are approved
// - no table rename allowed
func (mgtr *Migrator) validateAlterStatement() (err error) {
	if mgtr.parser.IsRenameTable() {
		return ErrMigratorUnsupportedRenameAlter
	}
	if mgtr.parser.HasNonTrivialRenames() && !mgtr.migrationContext.SkipRenamedColumns {
		mgtr.migrationContext.ColumnRenameMap = mgtr.parser.GetNonTrivialRenames()
		if !mgtr.migrationContext.ApproveRenamedColumns {
			return fmt.Errorf("gh-ost believes the ALTER statement renames columns, as follows: %v; as precaution, you are asked to confirm gh-ost is correct, and provide with `--approve-renamed-columns`, and we're all happy. Or you can skip renamed columns via `--skip-renamed-columns`, in which case column data may be lost", mgtr.parser.GetNonTrivialRenames())
		}
		mgtr.migrationContext.Log.Infof("alter statement has column(s) renamed. gh-ost finds the following renames: %v; --approve-renamed-columns is given and so migration proceeds.", mgtr.parser.GetNonTrivialRenames())
	}
	mgtr.migrationContext.DroppedColumnsMap = mgtr.parser.DroppedColumnsMap()
	return nil
}

func (mgtr *Migrator) countTableRows() (err error) {
	if !mgtr.migrationContext.CountTableRows {
		// Not counting; we stay with an estimate
		return nil
	}
	if mgtr.migrationContext.Noop {
		mgtr.migrationContext.Log.Debugf("Noop operation; not really counting table rows")
		return nil
	}

	countRowsFunc := func(ctx context.Context) error {
		if err := mgtr.inspector.CountTableRows(ctx); err != nil {
			return err
		}
		if err := mgtr.hooksExecutor.onRowCountComplete(); err != nil {
			return err
		}
		return nil
	}

	if mgtr.migrationContext.ConcurrentCountTableRows {
		// store a cancel func so we can stop this query before a cut over
		rowCountContext, rowCountCancel := context.WithCancel(context.Background())
		mgtr.migrationContext.SetCountTableRowsCancelFunc(rowCountCancel)

		mgtr.migrationContext.Log.Infof("As instructed, counting rows in the background; meanwhile I will use an estimated count, and will update it later on")
		go countRowsFunc(rowCountContext)

		// and we ignore errors, because this turns to be a background job
		return nil
	}
	return countRowsFunc(context.Background())
}

func (mgtr *Migrator) createFlagFiles() (err error) {
	if mgtr.migrationContext.PostponeCutOverFlagFile != "" {
		if !base.FileExists(mgtr.migrationContext.PostponeCutOverFlagFile) {
			if err := base.TouchFile(mgtr.migrationContext.PostponeCutOverFlagFile); err != nil {
				return mgtr.migrationContext.Log.Errorf("--postpone-cut-over-flag-file indicated by gh-ost is unable to create said file: %s", err.Error())
			}
			mgtr.migrationContext.Log.Infof("Created postpone-cut-over-flag-file: %s", mgtr.migrationContext.PostponeCutOverFlagFile)
		}
	}
	return nil
}

// checkAbort returns abort error if migration was aborted
func (mgtr *Migrator) checkAbort() error {
	if abortErr := mgtr.migrationContext.GetAbortError(); abortErr != nil {
		return abortErr
	}

	ctx := mgtr.migrationContext.GetContext()
	if ctx != nil {
		select {
		case <-ctx.Done():
			// Context cancelled but no abort error stored yet
			if abortErr := mgtr.migrationContext.GetAbortError(); abortErr != nil {
				return abortErr
			}
			return ctx.Err()
		default:
			// Not cancelled
		}
	}
	return nil
}

// Migrate executes the complete migration logic. This is *the* major gh-ost function.
func (mgtr *Migrator) Migrate() (err error) {
	mgtr.migrationContext.Log.Infof("Migrating %s.%s", sql.EscapeName(mgtr.migrationContext.DatabaseName), sql.EscapeName(mgtr.migrationContext.OriginalTableName))
	mgtr.migrationContext.StartTime = time.Now()

	// Ensure context is cancelled on exit (cleanup)
	defer mgtr.migrationContext.CancelContext()

	if mgtr.migrationContext.Hostname, err = os.Hostname(); err != nil {
		return err
	}

	go mgtr.listenOnPanicAbort()

	if err := mgtr.hooksExecutor.onStartup(); err != nil {
		return err
	}
	if err := mgtr.parser.ParseAlterStatement(mgtr.migrationContext.AlterStatement); err != nil {
		return err
	}
	if err := mgtr.validateAlterStatement(); err != nil {
		return err
	}

	// After this point, we'll need to teardown anything that's been started
	//   so we don't leave things hanging around
	defer mgtr.teardown()

	if err := mgtr.initiateInspector(); err != nil {
		return err
	}
	if err := mgtr.checkAbort(); err != nil {
		return err
	}
	// If we are resuming, we will initiateStreaming later when we know
	// the binlog coordinates to resume streaming from.
	// If not resuming, the streamer must be initiated before the applier,
	// so that the "GhostTableMigrated" event gets processed.
	if !mgtr.migrationContext.Resume {
		if err := mgtr.initiateStreaming(); err != nil {
			return err
		}
		if err := mgtr.checkAbort(); err != nil {
			return err
		}
	}
	if err := mgtr.initiateApplier(); err != nil {
		return err
	}
	if err := mgtr.checkAbort(); err != nil {
		return err
	}
	if err := mgtr.createFlagFiles(); err != nil {
		return err
	}
	// In MySQL 8.0 (and possibly earlier) some DDL statements can be applied instantly.
	// Attempt to do this if AttemptInstantDDL is set.
	if mgtr.migrationContext.AttemptInstantDDL {
		if mgtr.migrationContext.Noop {
			mgtr.migrationContext.Log.Debugf("Noop operation; not really attempting instant DDL")
		} else {
			mgtr.migrationContext.Log.Infof("Attempting to execute alter with ALGORITHM=INSTANT")
			if err := mgtr.applier.AttemptInstantDDL(); err == nil {
				if err := mgtr.finalCleanup(); err != nil {
					return nil
				}
				if err := mgtr.hooksExecutor.onSuccess(true); err != nil {
					return err
				}
				mgtr.migrationContext.Log.Infof("Success! table %s.%s migrated instantly", sql.EscapeName(mgtr.migrationContext.DatabaseName), sql.EscapeName(mgtr.migrationContext.OriginalTableName))
				return nil
			} else {
				mgtr.migrationContext.Log.Infof("ALGORITHM=INSTANT not supported for this operation, proceeding with original algorithm: %s", err)
			}
		}
	}

	initialLag, _ := mgtr.inspector.getReplicationLag()
	if !mgtr.migrationContext.Resume {
		mgtr.migrationContext.Log.Infof("Waiting for ghost table to be migrated. Current lag is %+v", initialLag)
		<-mgtr.ghostTableMigrated
		mgtr.migrationContext.Log.Debugf("ghost table migrated")
	}
	// Yay! We now know the Ghost and Changelog tables are good to examine!
	// When running on replica, this means the replica has those tables. When running
	// on master this is always true, of course, and yet it also implies this knowledge
	// is in the binlogs.
	if err := mgtr.inspector.inspectOriginalAndGhostTables(); err != nil {
		return err
	}

	// We can prepare some of the queries on the applier
	if err := mgtr.applier.prepareQueries(); err != nil {
		return err
	}

	// inspectOriginalAndGhostTables must be called before creating checkpoint table.
	if mgtr.migrationContext.Checkpoint && !mgtr.migrationContext.Resume {
		if err := mgtr.applier.CreateCheckpointTable(); err != nil {
			mgtr.migrationContext.Log.Errorf("unable to create checkpoint table, see further error details")
		}
	}

	if mgtr.migrationContext.Resume {
		lastCheckpoint, err := mgtr.applier.ReadLastCheckpoint()
		if err != nil {
			return mgtr.migrationContext.Log.Errorf("no checkpoint found, unable to resume: %+v", err)
		}
		mgtr.migrationContext.Log.Infof("Resuming from checkpoint coords=%+v range_min=%+v range_max=%+v iteration=%d",
			lastCheckpoint.LastTrxCoords, lastCheckpoint.IterationRangeMin.String(), lastCheckpoint.IterationRangeMax.String(), lastCheckpoint.Iteration)

		mgtr.migrationContext.MigrationIterationRangeMinValues = lastCheckpoint.IterationRangeMin
		mgtr.migrationContext.MigrationIterationRangeMaxValues = lastCheckpoint.IterationRangeMax
		mgtr.migrationContext.Iteration = lastCheckpoint.Iteration
		mgtr.migrationContext.TotalRowsCopied = lastCheckpoint.RowsCopied
		mgtr.migrationContext.TotalDMLEventsApplied = lastCheckpoint.DMLApplied
		mgtr.migrationContext.InitialStreamerCoords = lastCheckpoint.LastTrxCoords
		if err := mgtr.initiateStreaming(); err != nil {
			return err
		}
	}

	// Validation complete! We're good to execute this migration
	if err := mgtr.hooksExecutor.onValidated(); err != nil {
		return err
	}

	if err := mgtr.initiateServer(); err != nil {
		return err
	}
	defer mgtr.server.RemoveSocketFile()

	if err := mgtr.countTableRows(); err != nil {
		return err
	}
	if err := mgtr.addDMLEventsListener(); err != nil {
		return err
	}
	if err := mgtr.applier.ReadMigrationRangeValues(); err != nil {
		return err
	}

	mgtr.initiateThrottler()

	if err := mgtr.hooksExecutor.onBeforeRowCopy(); err != nil {
		return err
	}
	go func() {
		if err := mgtr.executeWriteFuncs(); err != nil {
			// Send error to PanicAbort to trigger abort
			_ = base.SendWithContext(mgtr.migrationContext.GetContext(), mgtr.migrationContext.PanicAbort, err)
		}
	}()
	go mgtr.iterateChunks()
	mgtr.migrationContext.MarkRowCopyStartTime()
	go mgtr.initiateStatus()
	if mgtr.migrationContext.Checkpoint {
		go mgtr.checkpointLoop()
	}

	mgtr.migrationContext.Log.Debugf("Operating until row copy is complete")
	mgtr.consumeRowCopyComplete()
	mgtr.migrationContext.Log.Infof("Row copy complete")
	// Check if row copy was aborted due to error
	if err := mgtr.checkAbort(); err != nil {
		return err
	}
	if err := mgtr.hooksExecutor.onRowCopyComplete(); err != nil {
		return err
	}
	mgtr.printStatus(ForcePrintStatusRule)

	if mgtr.migrationContext.IsCountingTableRows() {
		mgtr.migrationContext.Log.Info("stopping query for exact row count, because that can accidentally lock out the cut over")
		mgtr.migrationContext.CancelTableRowsCount()
	}
	if err := mgtr.hooksExecutor.onBeforeCutOver(); err != nil {
		return err
	}
	var retrier func(func() error, ...bool) error
	if mgtr.migrationContext.CutOverExponentialBackoff {
		retrier = mgtr.retryOperationWithExponentialBackoff
	} else {
		retrier = mgtr.retryOperation
	}
	if err := retrier(mgtr.cutOver); err != nil {
		return err
	}
	atomic.StoreInt64(&mgtr.migrationContext.CutOverCompleteFlag, 1)

	if mgtr.migrationContext.Checkpoint && !mgtr.migrationContext.Noop {
		cutoverChk, err := mgtr.CheckpointAfterCutOver()
		if err != nil {
			mgtr.migrationContext.Log.Warningf("failed to checkpoint after cutover: %+v", err)
		} else {
			mgtr.migrationContext.Log.Infof("checkpoint success after cutover at coords=%+v", cutoverChk.LastTrxCoords.DisplayString())
		}
	}

	if err := mgtr.finalCleanup(); err != nil {
		return nil
	}
	if err := mgtr.hooksExecutor.onSuccess(false); err != nil {
		return err
	}
	mgtr.migrationContext.Log.Infof("Done migrating %s.%s", sql.EscapeName(mgtr.migrationContext.DatabaseName), sql.EscapeName(mgtr.migrationContext.OriginalTableName))
	// Final check for abort before declaring success
	if err := mgtr.checkAbort(); err != nil {
		return err
	}
	return nil
}

// Revert reverts a migration that previously completed by applying all DML events that happened
// after the original cutover, then doing another cutover to swap the tables back.
// The steps are similar to Migrate(), but without row copying.
func (mgtr *Migrator) Revert() error {
	mgtr.migrationContext.Log.Infof("Reverting %s.%s from %s.%s",
		sql.EscapeName(mgtr.migrationContext.DatabaseName), sql.EscapeName(mgtr.migrationContext.OriginalTableName),
		sql.EscapeName(mgtr.migrationContext.DatabaseName), sql.EscapeName(mgtr.migrationContext.OldTableName))
	mgtr.migrationContext.StartTime = time.Now()

	// Ensure context is cancelled on exit (cleanup)
	defer mgtr.migrationContext.CancelContext()

	var err error
	if mgtr.migrationContext.Hostname, err = os.Hostname(); err != nil {
		return err
	}

	go mgtr.listenOnPanicAbort()

	if err := mgtr.hooksExecutor.onStartup(); err != nil {
		return err
	}
	if err := mgtr.validateAlterStatement(); err != nil {
		return err
	}
	defer mgtr.teardown()

	if err := mgtr.initiateInspector(); err != nil {
		return err
	}
	if err := mgtr.checkAbort(); err != nil {
		return err
	}
	if err := mgtr.initiateApplier(); err != nil {
		return err
	}
	if err := mgtr.checkAbort(); err != nil {
		return err
	}
	if err := mgtr.createFlagFiles(); err != nil {
		return err
	}
	if err := mgtr.inspector.inspectOriginalAndGhostTables(); err != nil {
		return err
	}
	if err := mgtr.applier.prepareQueries(); err != nil {
		return err
	}

	lastCheckpoint, err := mgtr.applier.ReadLastCheckpoint()
	if err != nil {
		return mgtr.migrationContext.Log.Errorf("no checkpoint found, unable to revert: %+v", err)
	}
	if !lastCheckpoint.IsCutover {
		return mgtr.migrationContext.Log.Errorf("last checkpoint is not after cutover, unable to revert: coords=%+v time=%+v", lastCheckpoint.LastTrxCoords, lastCheckpoint.Timestamp)
	}
	mgtr.migrationContext.InitialStreamerCoords = lastCheckpoint.LastTrxCoords
	mgtr.migrationContext.TotalRowsCopied = lastCheckpoint.RowsCopied
	mgtr.migrationContext.MigrationIterationRangeMinValues = lastCheckpoint.IterationRangeMin
	mgtr.migrationContext.MigrationIterationRangeMaxValues = lastCheckpoint.IterationRangeMax
	if err := mgtr.initiateStreaming(); err != nil {
		return err
	}
	if err := mgtr.checkAbort(); err != nil {
		return err
	}
	if err := mgtr.hooksExecutor.onValidated(); err != nil {
		return err
	}
	if err := mgtr.initiateServer(); err != nil {
		return err
	}
	defer mgtr.server.RemoveSocketFile()
	if err := mgtr.addDMLEventsListener(); err != nil {
		return err
	}

	mgtr.initiateThrottler()
	go mgtr.initiateStatus()
	go func() {
		if err := mgtr.executeDMLWriteFuncs(); err != nil {
			// Send error to PanicAbort to trigger abort
			_ = base.SendWithContext(mgtr.migrationContext.GetContext(), mgtr.migrationContext.PanicAbort, err)
		}
	}()

	mgtr.printStatus(ForcePrintStatusRule)
	var retrier func(func() error, ...bool) error
	if mgtr.migrationContext.CutOverExponentialBackoff {
		retrier = mgtr.retryOperationWithExponentialBackoff
	} else {
		retrier = mgtr.retryOperation
	}
	if err := mgtr.hooksExecutor.onBeforeCutOver(); err != nil {
		return err
	}
	if err := retrier(mgtr.cutOver); err != nil {
		return err
	}
	atomic.StoreInt64(&mgtr.migrationContext.CutOverCompleteFlag, 1)
	if err := mgtr.finalCleanup(); err != nil {
		return nil
	}
	if err := mgtr.hooksExecutor.onSuccess(false); err != nil {
		return err
	}
	mgtr.migrationContext.Log.Infof("Done reverting %s.%s", sql.EscapeName(mgtr.migrationContext.DatabaseName), sql.EscapeName(mgtr.migrationContext.OriginalTableName))
	return nil
}

// ExecOnFailureHook executes the onFailure hook, and this method is provided as the only external
// hook access point
func (mgtr *Migrator) ExecOnFailureHook() (err error) {
	return mgtr.hooksExecutor.onFailure()
}

func (mgtr *Migrator) handleCutOverResult(cutOverError error) (err error) {
	if mgtr.migrationContext.TestOnReplica {
		// We're merely testing, we don't want to keep this state. Rollback the renames as possible
		mgtr.applier.RenameTablesRollback()
	}
	if cutOverError == nil {
		return nil
	}
	// Only on error:

	if mgtr.migrationContext.TestOnReplica {
		// With `--test-on-replica` we stop replication thread, and then proceed to use
		// the same cut-over phase as the master would use. That means we take locks
		// and swap the tables.
		// The difference is that we will later swap the tables back.
		if err := mgtr.hooksExecutor.onStartReplication(); err != nil {
			return mgtr.migrationContext.Log.Errore(err)
		}
		if mgtr.migrationContext.TestOnReplicaSkipReplicaStop {
			mgtr.migrationContext.Log.Warningf("--test-on-replica-skip-replica-stop enabled, we are not starting replication.")
		} else {
			mgtr.migrationContext.Log.Debugf("testing on replica. Starting replication IO thread after cut-over failure")
			if err := mgtr.retryOperation(mgtr.applier.StartReplication); err != nil {
				return mgtr.migrationContext.Log.Errore(err)
			}
		}
	}
	return nil
}

// cutOver performs the final step of migration, based on migration
// type (on replica? atomic? safe?)
func (mgtr *Migrator) cutOver() (err error) {
	if mgtr.migrationContext.Noop {
		mgtr.migrationContext.Log.Debugf("Noop operation; not really swapping tables")
		return nil
	}
	mgtr.migrationContext.MarkPointOfInterest()
	mgtr.throttler.throttle(func() {
		mgtr.migrationContext.Log.Debugf("throttling before swapping tables")
	})

	mgtr.migrationContext.MarkPointOfInterest()
	mgtr.migrationContext.Log.Debugf("checking for cut-over postpone")
	if err := mgtr.sleepWhileTrue(
		func() (bool, error) {
			heartbeatLag := mgtr.migrationContext.TimeSinceLastHeartbeatOnChangelog()
			maxLagMillisecondsThrottle := time.Duration(atomic.LoadInt64(&mgtr.migrationContext.MaxLagMillisecondsThrottleThreshold)) * time.Millisecond
			cutOverLockTimeout := time.Duration(mgtr.migrationContext.CutOverLockTimeoutSeconds) * time.Second
			if heartbeatLag > maxLagMillisecondsThrottle || heartbeatLag > cutOverLockTimeout {
				mgtr.migrationContext.Log.Debugf("current HeartbeatLag (%.2fs) is too high, it needs to be less than both --max-lag-millis (%.2fs) and --cut-over-lock-timeout-seconds (%.2fs) to continue", heartbeatLag.Seconds(), maxLagMillisecondsThrottle.Seconds(), cutOverLockTimeout.Seconds())
				return true, nil
			}
			if mgtr.migrationContext.PostponeCutOverFlagFile == "" {
				return false, nil
			}
			if atomic.LoadInt64(&mgtr.migrationContext.UserCommandedUnpostponeFlag) > 0 {
				atomic.StoreInt64(&mgtr.migrationContext.UserCommandedUnpostponeFlag, 0)
				return false, nil
			}
			if base.FileExists(mgtr.migrationContext.PostponeCutOverFlagFile) {
				// Postpone file defined and exists!
				if atomic.LoadInt64(&mgtr.migrationContext.IsPostponingCutOver) == 0 {
					if err := mgtr.hooksExecutor.onBeginPostponed(); err != nil {
						return true, err
					}
				}
				atomic.StoreInt64(&mgtr.migrationContext.IsPostponingCutOver, 1)
				return true, nil
			}
			return false, nil
		},
	); err != nil {
		return err
	}
	atomic.StoreInt64(&mgtr.migrationContext.IsPostponingCutOver, 0)
	mgtr.migrationContext.MarkPointOfInterest()
	mgtr.migrationContext.Log.Debugf("checking for cut-over postpone: complete")

	if mgtr.migrationContext.TestOnReplica {
		// With `--test-on-replica` we stop replication thread, and then proceed to use
		// the same cut-over phase as the master would use. That means we take locks
		// and swap the tables.
		// The difference is that we will later swap the tables back.
		if err := mgtr.hooksExecutor.onStopReplication(); err != nil {
			return err
		}
		if mgtr.migrationContext.TestOnReplicaSkipReplicaStop {
			mgtr.migrationContext.Log.Warningf("--test-on-replica-skip-replica-stop enabled, we are not stopping replication.")
		} else {
			mgtr.migrationContext.Log.Debugf("testing on replica. Stopping replication IO thread")
			if err := mgtr.retryOperation(mgtr.applier.StopReplication); err != nil {
				return err
			}
		}
	}

	switch mgtr.migrationContext.CutOverType {
	case base.CutOverAtomic:
		// Atomic solution: we use low timeout and multiple attempts. But for
		// each failed attempt, we throttle until replication lag is back to normal
		err = mgtr.atomicCutOver()
	case base.CutOverTwoStep:
		err = mgtr.cutOverTwoStep()
	default:
		return mgtr.migrationContext.Log.Fatalf("Unknown cut-over type: %d; should never get here!", mgtr.migrationContext.CutOverType)
	}
	mgtr.handleCutOverResult(err)
	return err
}

// Inject the "AllEventsUpToLockProcessed" state hint, wait for it to appear in the binary logs,
// make sure the queue is drained.
func (mgtr *Migrator) waitForEventsUpToLock() error {
	timeout := time.NewTimer(time.Second * time.Duration(mgtr.migrationContext.CutOverLockTimeoutSeconds))

	mgtr.migrationContext.MarkPointOfInterest()
	waitForEventsUpToLockStartTime := time.Now()

	allEventsUpToLockProcessedChallenge := fmt.Sprintf("%s:%d", string(AllEventsUpToLockProcessed), waitForEventsUpToLockStartTime.UnixNano())
	mgtr.migrationContext.Log.Infof("Writing changelog state: %+v", allEventsUpToLockProcessedChallenge)
	if _, err := mgtr.applier.WriteChangelogState(allEventsUpToLockProcessedChallenge); err != nil {
		return err
	}
	mgtr.migrationContext.Log.Infof("Waiting for events up to lock")
	atomic.StoreInt64(&mgtr.migrationContext.AllEventsUpToLockProcessedInjectedFlag, 1)
	var lockProcessed *lockProcessedStruct
	for found := false; !found; {
		select {
		case <-timeout.C:
			{
				return mgtr.migrationContext.Log.Errorf("timeout while waiting for events up to lock")
			}
		case lockProcessed = <-mgtr.allEventsUpToLockProcessed:
			{
				if lockProcessed.state == allEventsUpToLockProcessedChallenge {
					mgtr.migrationContext.Log.Infof("Waiting for events up to lock: got %s", lockProcessed.state)
					found = true
					mgtr.lastLockProcessed = lockProcessed
				} else {
					mgtr.migrationContext.Log.Infof("Waiting for events up to lock: skipping %s", lockProcessed.state)
				}
			}
		}
	}
	waitForEventsUpToLockDuration := time.Since(waitForEventsUpToLockStartTime)

	mgtr.migrationContext.Log.Infof("Done waiting for events up to lock; duration=%+v", waitForEventsUpToLockDuration)
	mgtr.printStatus(ForcePrintStatusAndHintRule)

	return nil
}

// cutOverTwoStep will lock down the original table, execute
// what's left of last DML entries, and **non-atomically** swap original->old, then new->original.
// There is a point in time where the "original" table does not exist and queries are non-blocked
// and failing.
func (mgtr *Migrator) cutOverTwoStep() (err error) {
	atomic.StoreInt64(&mgtr.migrationContext.InCutOverCriticalSectionFlag, 1)
	defer atomic.StoreInt64(&mgtr.migrationContext.InCutOverCriticalSectionFlag, 0)
	atomic.StoreInt64(&mgtr.migrationContext.AllEventsUpToLockProcessedInjectedFlag, 0)

	if err := mgtr.retryOperation(mgtr.applier.LockOriginalTable); err != nil {
		return err
	}

	if err := mgtr.retryOperation(mgtr.waitForEventsUpToLock); err != nil {
		return err
	}
	// If we need to create triggers we need to do it here (only create part)
	if mgtr.migrationContext.IncludeTriggers && len(mgtr.migrationContext.Triggers) > 0 {
		if err := mgtr.retryOperation(mgtr.applier.CreateTriggersOnGhost); err != nil {
			return err
		}
	}
	if err := mgtr.retryOperation(mgtr.applier.SwapTablesQuickAndBumpy); err != nil {
		return err
	}
	if err := mgtr.retryOperation(mgtr.applier.UnlockTables); err != nil {
		return err
	}

	lockAndRenameDuration := mgtr.migrationContext.RenameTablesEndTime.Sub(mgtr.migrationContext.LockTablesStartTime)
	renameDuration := mgtr.migrationContext.RenameTablesEndTime.Sub(mgtr.migrationContext.RenameTablesStartTime)
	mgtr.migrationContext.Log.Debugf("Lock & rename duration: %s (rename only: %s). During mgtr time, queries on %s were locked or failing", lockAndRenameDuration, renameDuration, sql.EscapeName(mgtr.migrationContext.OriginalTableName))
	return nil
}

// atomicCutOver
func (mgtr *Migrator) atomicCutOver() (err error) {
	atomic.StoreInt64(&mgtr.migrationContext.InCutOverCriticalSectionFlag, 1)
	defer atomic.StoreInt64(&mgtr.migrationContext.InCutOverCriticalSectionFlag, 0)

	okToUnlockTable := make(chan bool, 4)
	defer func() {
		okToUnlockTable <- true
	}()

	atomic.StoreInt64(&mgtr.migrationContext.AllEventsUpToLockProcessedInjectedFlag, 0)

	lockOriginalSessionIdChan := make(chan int64, 2)
	tableLocked := make(chan error, 2)
	tableUnlocked := make(chan error, 2)
	var renameLockSessionId int64
	go func() {
		if err := mgtr.applier.AtomicCutOverMagicLock(lockOriginalSessionIdChan, tableLocked, okToUnlockTable, tableUnlocked, &renameLockSessionId); err != nil {
			mgtr.migrationContext.Log.Errore(err)
		}
	}()
	if err := <-tableLocked; err != nil {
		return mgtr.migrationContext.Log.Errore(err)
	}
	lockOriginalSessionId := <-lockOriginalSessionIdChan
	mgtr.migrationContext.Log.Infof("Session locking original & magic tables is %+v", lockOriginalSessionId)
	// At this point we know the original table is locked.
	// We know any newly incoming DML on original table is blocked.
	if err := mgtr.waitForEventsUpToLock(); err != nil {
		return mgtr.migrationContext.Log.Errore(err)
	}

	// If we need to create triggers we need to do it here (only create part)
	if mgtr.migrationContext.IncludeTriggers && len(mgtr.migrationContext.Triggers) > 0 {
		if err := mgtr.applier.CreateTriggersOnGhost(); err != nil {
			return mgtr.migrationContext.Log.Errore(err)
		}
	}

	// Step 2
	// We now attempt an atomic RENAME on original & ghost tables, and expect it to block.
	mgtr.migrationContext.RenameTablesStartTime = time.Now()

	var tableRenameKnownToHaveFailed int64
	renameSessionIdChan := make(chan int64, 2)
	tablesRenamed := make(chan error, 2)
	go func() {
		if err := mgtr.applier.AtomicCutoverRename(renameSessionIdChan, tablesRenamed); err != nil {
			// Abort! Release the lock
			atomic.StoreInt64(&tableRenameKnownToHaveFailed, 1)
			okToUnlockTable <- true
		}
	}()
	renameSessionId := <-renameSessionIdChan
	mgtr.migrationContext.Log.Infof("Session renaming tables is %+v", renameSessionId)

	waitForRename := func() error {
		if atomic.LoadInt64(&tableRenameKnownToHaveFailed) == 1 {
			// We return `nil` here so as to avoid the `retry`. The RENAME has failed,
			// it won't show up in PROCESSLIST, no point in waiting
			return nil
		}
		return mgtr.applier.ExpectProcess(renameSessionId, "metadata lock", "rename")
	}
	// Wait for the RENAME to appear in PROCESSLIST
	if err := mgtr.retryOperation(waitForRename, true); err != nil {
		// Abort! Release the lock
		okToUnlockTable <- true
		return err
	}
	if atomic.LoadInt64(&tableRenameKnownToHaveFailed) == 0 {
		mgtr.migrationContext.Log.Infof("Found atomic RENAME to be blocking, as expected. Double checking the lock is still in place (though I don't strictly have to)")
	}
	if err := mgtr.applier.ExpectUsedLock(lockOriginalSessionId); err != nil {
		// Abort operation. Just make sure to drop the magic table.
		return mgtr.migrationContext.Log.Errore(err)
	}
	mgtr.migrationContext.Log.Infof("Connection holding lock on original table still exists")

	// Now that we've found the RENAME blocking, AND the locking connection still alive,
	// we know it is safe to proceed to release the lock

	renameLockSessionId = renameSessionId
	okToUnlockTable <- true
	// BAM! magic table dropped, original table lock is released
	// -> RENAME released -> queries on original are unblocked.
	if err := <-tableUnlocked; err != nil {
		return mgtr.migrationContext.Log.Errore(err)
	}
	if err := <-tablesRenamed; err != nil {
		return mgtr.migrationContext.Log.Errore(err)
	}
	mgtr.migrationContext.RenameTablesEndTime = time.Now()

	// ooh nice! We're actually truly and thankfully done
	lockAndRenameDuration := mgtr.migrationContext.RenameTablesEndTime.Sub(mgtr.migrationContext.LockTablesStartTime)
	mgtr.migrationContext.Log.Infof("Lock & rename duration: %s. During mgtr time, queries on %s were blocked", lockAndRenameDuration, sql.EscapeName(mgtr.migrationContext.OriginalTableName))
	return nil
}

// initiateServer begins listening on unix socket/tcp for incoming interactive commands
func (mgtr *Migrator) initiateServer() (err error) {
	var f printStatusFunc = func(rule PrintStatusRule, writer io.Writer) {
		mgtr.printStatus(rule, writer)
	}
	mgtr.server = NewServer(mgtr.migrationContext, mgtr.hooksExecutor, f)
	if err := mgtr.server.BindSocketFile(); err != nil {
		return err
	}
	if err := mgtr.server.BindTCPPort(); err != nil {
		return err
	}

	go mgtr.server.Serve()
	return nil
}

// initiateInspector connects, validates and inspects the "inspector" server.
// The "inspector" server is typically a replica; it is where we issue some
// queries such as:
// - table row count
// - schema validation
// - heartbeat
// When `--allow-on-master` is supplied, the inspector is actually the master.
func (mgtr *Migrator) initiateInspector() (err error) {
	mgtr.inspector = NewInspector(mgtr.migrationContext)
	if err := mgtr.inspector.InitDBConnections(); err != nil {
		return err
	}
	if err := mgtr.inspector.ValidateOriginalTable(); err != nil {
		return err
	}
	if err := mgtr.inspector.InspectOriginalTable(); err != nil {
		return err
	}
	// So far so good, table is accessible and valid.
	// Let's get master connection config
	if mgtr.migrationContext.AssumeMasterHostname == "" {
		// No forced master host; detect master
		if mgtr.migrationContext.ApplierConnectionConfig, err = mgtr.inspector.getMasterConnectionConfig(); err != nil {
			return err
		}
		mgtr.migrationContext.Log.Infof("Master found to be %+v", *mgtr.migrationContext.ApplierConnectionConfig.ImpliedKey)
	} else {
		// Forced master host.
		key, err := mysql.ParseInstanceKey(mgtr.migrationContext.AssumeMasterHostname)
		if err != nil {
			return err
		}
		mgtr.migrationContext.ApplierConnectionConfig = mgtr.migrationContext.InspectorConnectionConfig.DuplicateCredentials(*key)
		if mgtr.migrationContext.CliMasterUser != "" {
			mgtr.migrationContext.ApplierConnectionConfig.User = mgtr.migrationContext.CliMasterUser
		}
		if mgtr.migrationContext.CliMasterPassword != "" {
			mgtr.migrationContext.ApplierConnectionConfig.Password = mgtr.migrationContext.CliMasterPassword
		}
		if err := mgtr.migrationContext.ApplierConnectionConfig.RegisterTLSConfig(); err != nil {
			return err
		}
		mgtr.migrationContext.Log.Infof("Master forced to be %+v", *mgtr.migrationContext.ApplierConnectionConfig.ImpliedKey)
	}
	// validate configs
	if mgtr.migrationContext.TestOnReplica || mgtr.migrationContext.MigrateOnReplica {
		if mgtr.migrationContext.InspectorIsAlsoApplier() {
			return fmt.Errorf("instructed to --test-on-replica or --migrate-on-replica, but the server we connect to doesn't seem to be a replica")
		}
		mgtr.migrationContext.Log.Infof("--test-on-replica or --migrate-on-replica given. Will not execute on master %+v but rather on replica %+v itself",
			*mgtr.migrationContext.ApplierConnectionConfig.ImpliedKey, *mgtr.migrationContext.InspectorConnectionConfig.ImpliedKey,
		)
		mgtr.migrationContext.ApplierConnectionConfig = mgtr.migrationContext.InspectorConnectionConfig.Duplicate()
		if mgtr.migrationContext.GetThrottleControlReplicaKeys().Len() == 0 {
			mgtr.migrationContext.AddThrottleControlReplicaKey(mgtr.migrationContext.InspectorConnectionConfig.Key)
		}
	} else if mgtr.migrationContext.InspectorIsAlsoApplier() && !mgtr.migrationContext.AllowedRunningOnMaster {
		return ErrMigrationNotAllowedOnMaster
	}
	if err := mgtr.inspector.validateLogSlaveUpdates(); err != nil {
		return err
	}

	return nil
}

// initiateStatus sets and activates the printStatus() ticker
func (mgtr *Migrator) initiateStatus() {
	mgtr.printStatus(ForcePrintStatusAndHintRule)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var previousCount int64
	for range ticker.C {
		if atomic.LoadInt64(&mgtr.finishedMigrating) > 0 {
			return
		}
		go mgtr.printStatus(HeuristicPrintStatusRule)
		totalCopied := atomic.LoadInt64(&mgtr.migrationContext.TotalRowsCopied)
		if previousCount > 0 {
			copiedThisLoop := totalCopied - previousCount
			atomic.StoreInt64(&mgtr.migrationContext.EtaRowsPerSecond, copiedThisLoop)
		}
		previousCount = totalCopied
	}
}

// printMigrationStatusHint prints a detailed configuration dump, that is useful
// to keep in mind; such as the name of migrated table, throttle params etc.
// This gets printed at beginning and end of migration, every 10 minutes throughout
// migration, and as response to the "status" interactive command.
func (mgtr *Migrator) printMigrationStatusHint(writers ...io.Writer) {
	w := io.MultiWriter(writers...)
	fmt.Fprintf(w, "# Migrating %s.%s; Ghost table is %s.%s\n",
		sql.EscapeName(mgtr.migrationContext.DatabaseName),
		sql.EscapeName(mgtr.migrationContext.OriginalTableName),
		sql.EscapeName(mgtr.migrationContext.DatabaseName),
		sql.EscapeName(mgtr.migrationContext.GetGhostTableName()),
	)
	fmt.Fprintf(w, "# Migrating %+v; inspecting %+v; executing on %+v\n",
		*mgtr.applier.connectionConfig.ImpliedKey,
		*mgtr.inspector.connectionConfig.ImpliedKey,
		mgtr.migrationContext.Hostname,
	)
	fmt.Fprintf(w, "# Migration started at %+v\n",
		mgtr.migrationContext.StartTime.Format(time.RubyDate),
	)
	maxLoad := mgtr.migrationContext.GetMaxLoad()
	criticalLoad := mgtr.migrationContext.GetCriticalLoad()
	fmt.Fprintf(w, "# chunk-size: %+v; max-lag-millis: %+vms; dml-batch-size: %+v; max-load: %s; critical-load: %s; nice-ratio: %f\n",
		atomic.LoadInt64(&mgtr.migrationContext.ChunkSize),
		atomic.LoadInt64(&mgtr.migrationContext.MaxLagMillisecondsThrottleThreshold),
		atomic.LoadInt64(&mgtr.migrationContext.DMLBatchSize),
		maxLoad.String(),
		criticalLoad.String(),
		mgtr.migrationContext.GetNiceRatio(),
	)
	if mgtr.migrationContext.ThrottleFlagFile != "" {
		setIndicator := ""
		if base.FileExists(mgtr.migrationContext.ThrottleFlagFile) {
			setIndicator = "[set]"
		}
		fmt.Fprintf(w, "# throttle-flag-file: %+v %+v\n",
			mgtr.migrationContext.ThrottleFlagFile, setIndicator,
		)
	}
	if mgtr.migrationContext.ThrottleAdditionalFlagFile != "" {
		setIndicator := ""
		if base.FileExists(mgtr.migrationContext.ThrottleAdditionalFlagFile) {
			setIndicator = "[set]"
		}
		fmt.Fprintf(w, "# throttle-additional-flag-file: %+v %+v\n",
			mgtr.migrationContext.ThrottleAdditionalFlagFile, setIndicator,
		)
	}
	if throttleQuery := mgtr.migrationContext.GetThrottleQuery(); throttleQuery != "" {
		fmt.Fprintf(w, "# throttle-query: %+v\n",
			throttleQuery,
		)
	}
	if throttleControlReplicaKeys := mgtr.migrationContext.GetThrottleControlReplicaKeys(); throttleControlReplicaKeys.Len() > 0 {
		fmt.Fprintf(w, "# throttle-control-replicas count: %+v\n",
			throttleControlReplicaKeys.Len(),
		)
	}

	if mgtr.migrationContext.PostponeCutOverFlagFile != "" {
		setIndicator := ""
		if base.FileExists(mgtr.migrationContext.PostponeCutOverFlagFile) {
			setIndicator = "[set]"
		}
		fmt.Fprintf(w, "# postpone-cut-over-flag-file: %+v %+v\n",
			mgtr.migrationContext.PostponeCutOverFlagFile, setIndicator,
		)
	}
	if mgtr.migrationContext.PanicFlagFile != "" {
		fmt.Fprintf(w, "# panic-flag-file: %+v\n",
			mgtr.migrationContext.PanicFlagFile,
		)
	}
	fmt.Fprintf(w, "# Serving on unix socket: %+v\n",
		mgtr.migrationContext.ServeSocketFile,
	)
	if mgtr.migrationContext.ServeTCPPort != 0 {
		fmt.Fprintf(w, "# Serving on TCP port: %+v\n", mgtr.migrationContext.ServeTCPPort)
	}
}

// getProgressPercent returns an estimate of migration progess as a percent.
func (mgtr *Migrator) getProgressPercent(rowsEstimate int64) (progressPct float64) {
	progressPct = 100.0
	if rowsEstimate > 0 {
		progressPct *= float64(mgtr.migrationContext.GetTotalRowsCopied()) / float64(rowsEstimate)
	}
	return progressPct
}

// getMigrationETA returns the estimated duration of the migration
func (mgtr *Migrator) getMigrationETA(rowsEstimate int64) (eta string, duration time.Duration) {
	duration = time.Duration(base.ETAUnknown)
	progressPct := mgtr.getProgressPercent(rowsEstimate)
	if progressPct >= 100.0 {
		duration = 0
	} else if progressPct >= 0.1 {
		totalRowsCopied := mgtr.migrationContext.GetTotalRowsCopied()
		etaRowsPerSecond := atomic.LoadInt64(&mgtr.migrationContext.EtaRowsPerSecond)
		var etaSeconds float64
		// If there is data available on our current row-copies-per-second rate, use it.
		// Otherwise we can fallback to the total elapsed time and extrapolate.
		// This is going to be less accurate on a longer copy as the insert rate
		// will tend to slow down.
		if etaRowsPerSecond > 0 {
			remainingRows := float64(rowsEstimate) - float64(totalRowsCopied)
			etaSeconds = remainingRows / float64(etaRowsPerSecond)
		} else {
			elapsedRowCopySeconds := mgtr.migrationContext.ElapsedRowCopyTime().Seconds()
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
func (mgtr *Migrator) getMigrationStateAndETA(rowsEstimate int64) (state, eta string, etaDuration time.Duration) {
	eta, etaDuration = mgtr.getMigrationETA(rowsEstimate)
	state = "migrating"
	if atomic.LoadInt64(&mgtr.migrationContext.CountingRowsFlag) > 0 && !mgtr.migrationContext.ConcurrentCountTableRows {
		state = "counting rows"
	} else if atomic.LoadInt64(&mgtr.migrationContext.IsPostponingCutOver) > 0 {
		eta = "due"
		state = "postponing cut-over"
	} else if isThrottled, throttleReason, _ := mgtr.migrationContext.IsThrottled(); isThrottled {
		state = fmt.Sprintf("throttled, %s", throttleReason)
	}
	return state, eta, etaDuration
}

// shouldPrintStatus returns true when the migrator is due to print status info.
func (mgtr *Migrator) shouldPrintStatus(rule PrintStatusRule, elapsedSeconds int64, etaDuration time.Duration) (shouldPrint bool) {
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
	} else if mgtr.migrationContext.TimeSincePointOfInterest().Seconds() <= 60 {
		shouldPrint = (elapsedSeconds%5 == 0)
	} else {
		shouldPrint = (elapsedSeconds%30 == 0)
	}

	return shouldPrint
}

// shouldPrintMigrationStatus returns true when the migrator is due to print the migration status hint
func (mgtr *Migrator) shouldPrintMigrationStatusHint(rule PrintStatusRule, elapsedSeconds int64) (shouldPrint bool) {
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
func (mgtr *Migrator) printStatus(rule PrintStatusRule, writers ...io.Writer) {
	if rule == NoPrintStatusRule {
		return
	}
	writers = append(writers, os.Stdout)

	elapsedTime := mgtr.migrationContext.ElapsedTime()
	elapsedSeconds := int64(elapsedTime.Seconds())
	totalRowsCopied := mgtr.migrationContext.GetTotalRowsCopied()
	rowsEstimate := atomic.LoadInt64(&mgtr.migrationContext.RowsEstimate) + atomic.LoadInt64(&mgtr.migrationContext.RowsDeltaEstimate)
	if atomic.LoadInt64(&mgtr.rowCopyCompleteFlag) == 1 {
		// Done copying rows. The totalRowsCopied value is the de-facto number of rows,
		// and there is no further need to keep updating the value.
		rowsEstimate = totalRowsCopied
	}

	// we take the opportunity to update migration context with progressPct
	progressPct := mgtr.getProgressPercent(rowsEstimate)
	mgtr.migrationContext.SetProgressPct(progressPct)

	// Before status, let's see if we should print a nice reminder for what exactly we're doing here.
	if mgtr.shouldPrintMigrationStatusHint(rule, elapsedSeconds) {
		mgtr.printMigrationStatusHint(writers...)
	}

	// Get state + ETA
	state, eta, etaDuration := mgtr.getMigrationStateAndETA(rowsEstimate)
	mgtr.migrationContext.SetETADuration(etaDuration)

	if !mgtr.shouldPrintStatus(rule, elapsedSeconds, etaDuration) {
		return
	}

	currentBinlogCoordinates := mgtr.eventsStreamer.GetCurrentBinlogCoordinates()

	status := fmt.Sprintf("Copy: %d/%d %.1f%%; Applied: %d; Backlog: %d/%d; Time: %+v(total), %+v(copy); streamer: %+v; Lag: %.2fs, HeartbeatLag: %.2fs, State: %s; ETA: %s",
		totalRowsCopied, rowsEstimate, progressPct,
		atomic.LoadInt64(&mgtr.migrationContext.TotalDMLEventsApplied),
		len(mgtr.applyEventsQueue), cap(mgtr.applyEventsQueue),
		base.PrettifyDurationOutput(elapsedTime), base.PrettifyDurationOutput(mgtr.migrationContext.ElapsedRowCopyTime()),
		currentBinlogCoordinates.DisplayString(),
		mgtr.migrationContext.GetCurrentLagDuration().Seconds(),
		mgtr.migrationContext.TimeSinceLastHeartbeatOnChangelog().Seconds(),
		state,
		eta,
	)
	mgtr.applier.WriteChangelog(
		fmt.Sprintf("copy iteration %d at %d", mgtr.migrationContext.GetIteration(), time.Now().Unix()),
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
	mgtr.migrationContext.Log.Info(strings.Replace(status, "%", "%%", 1))

	hooksStatusIntervalSec := mgtr.migrationContext.HooksStatusIntervalSec
	if hooksStatusIntervalSec > 0 && elapsedSeconds%hooksStatusIntervalSec == 0 {
		mgtr.hooksExecutor.onStatus(status)
	}
}

// initiateStreaming begins streaming of binary log events and registers listeners for such events
func (mgtr *Migrator) initiateStreaming() error {
	mgtr.eventsStreamer = NewEventsStreamer(mgtr.migrationContext)
	if err := mgtr.eventsStreamer.InitDBConnections(); err != nil {
		return err
	}
	mgtr.eventsStreamer.AddListener(
		false,
		mgtr.migrationContext.DatabaseName,
		mgtr.migrationContext.GetChangelogTableName(),
		func(dmlEntry *binlog.BinlogEntry) error {
			return mgtr.onChangelogEvent(dmlEntry)
		},
	)

	go func() {
		mgtr.migrationContext.Log.Debugf("Beginning streaming")
		err := mgtr.eventsStreamer.StreamEvents(mgtr.canStopStreaming)
		if err != nil {
			// Use helper to prevent deadlock if listenOnPanicAbort already exited
			_ = base.SendWithContext(mgtr.migrationContext.GetContext(), mgtr.migrationContext.PanicAbort, err)
		}
		mgtr.migrationContext.Log.Debugf("Done streaming")
	}()

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if atomic.LoadInt64(&mgtr.finishedMigrating) > 0 {
				return
			}
			mgtr.migrationContext.SetRecentBinlogCoordinates(mgtr.eventsStreamer.GetCurrentBinlogCoordinates())
		}
	}()
	return nil
}

// addDMLEventsListener begins listening for binlog events on the original table,
// and creates & enqueues a write task per such event.
func (mgtr *Migrator) addDMLEventsListener() error {
	err := mgtr.eventsStreamer.AddListener(
		false,
		mgtr.migrationContext.DatabaseName,
		mgtr.migrationContext.OriginalTableName,
		func(dmlEntry *binlog.BinlogEntry) error {
			// Use helper to prevent deadlock if buffer fills and executeWriteFuncs exits
			// This is critical because this callback blocks the event streamer
			return base.SendWithContext(mgtr.migrationContext.GetContext(), mgtr.applyEventsQueue, newApplyEventStructByDML(dmlEntry))
		},
	)
	return err
}

// initiateThrottler kicks in the throttling collection and the throttling checks.
func (mgtr *Migrator) initiateThrottler() {
	mgtr.throttler = NewThrottler(mgtr.migrationContext, mgtr.applier, mgtr.inspector, mgtr.appVersion)

	go mgtr.throttler.initiateThrottlerCollection(mgtr.firstThrottlingCollected)
	mgtr.migrationContext.Log.Infof("Waiting for first throttle metrics to be collected")
	<-mgtr.firstThrottlingCollected // replication lag
	<-mgtr.firstThrottlingCollected // HTTP status
	<-mgtr.firstThrottlingCollected // other, general metrics
	mgtr.migrationContext.Log.Infof("First throttle metrics collected")
	go mgtr.throttler.initiateThrottlerChecks()
}

func (mgtr *Migrator) initiateApplier() error {
	mgtr.applier = NewApplier(mgtr.migrationContext)
	if err := mgtr.applier.InitDBConnections(); err != nil {
		return err
	}
	if mgtr.migrationContext.Revert {
		if err := mgtr.applier.CreateChangelogTable(); err != nil {
			mgtr.migrationContext.Log.Errorf("unable to create changelog table, see further error details. Perhaps a previous migration failed without dropping the table? OR is there a running migration? Bailing out")
			return err
		}
	} else if !mgtr.migrationContext.Resume {
		if err := mgtr.applier.ValidateOrDropExistingTables(); err != nil {
			return err
		}
		if err := mgtr.applier.CreateChangelogTable(); err != nil {
			mgtr.migrationContext.Log.Errorf("unable to create changelog table, see further error details. Perhaps a previous migration failed without dropping the table? OR is there a running migration? Bailing out")
			return err
		}
		if err := mgtr.applier.CreateGhostTable(); err != nil {
			mgtr.migrationContext.Log.Errorf("unable to create ghost table, see further error details. Perhaps a previous migration failed without dropping the table? Bailing out")
			return err
		}
		if err := mgtr.applier.AlterGhost(); err != nil {
			mgtr.migrationContext.Log.Errorf("unable to ALTER ghost table, see further error details. Bailing out")
			return err
		}

		if mgtr.migrationContext.OriginalTableAutoIncrement > 0 && !mgtr.parser.IsAutoIncrementDefined() {
			// Original table has AUTO_INCREMENT value and the -alter statement does not indicate any override,
			// so we should copy AUTO_INCREMENT value onto our ghost table.
			if err := mgtr.applier.AlterGhostAutoIncrement(); err != nil {
				mgtr.migrationContext.Log.Errorf("unable to ALTER ghost table AUTO_INCREMENT value, see further error details. Bailing out")
				return err
			}
		}
		if _, err := mgtr.applier.WriteChangelogState(string(GhostTableMigrated)); err != nil {
			return err
		}
	}

	// ensure performance_schema.metadata_locks is available.
	if err := mgtr.applier.StateMetadataLockInstrument(); err != nil {
		mgtr.migrationContext.Log.Warning("unable to enable metadata lock instrument, see further error details")
	}
	if !mgtr.migrationContext.IsOpenMetadataLockInstruments {
		if !mgtr.migrationContext.SkipMetadataLockCheck {
			return mgtr.migrationContext.Log.Errorf("bailing out because metadata lock instrument not enabled. Use --skip-metadata-lock-check if you wish to proceed without. See https://github.com/github/gh-ost/pull/1536 for details")
		}
		mgtr.migrationContext.Log.Warning("proceeding without metadata lock check. There is a small chance of data loss if another session accesses the ghost table during cut-over. See https://github.com/github/gh-ost/pull/1536 for details")
	}

	go mgtr.applier.InitiateHeartbeat()
	return nil
}

// iterateChunks iterates the existing table rows, and generates a copy task of
// a chunk of rows onto the ghost table.
func (mgtr *Migrator) iterateChunks() error {
	terminateRowIteration := func(err error) error {
		_ = base.SendWithContext(mgtr.migrationContext.GetContext(), mgtr.rowCopyComplete, err)
		return mgtr.migrationContext.Log.Errore(err)
	}
	if mgtr.migrationContext.Noop {
		mgtr.migrationContext.Log.Debugf("Noop operation; not really copying data")
		return terminateRowIteration(nil)
	}
	if mgtr.migrationContext.MigrationRangeMinValues == nil {
		mgtr.migrationContext.Log.Debugf("No rows found in table. Rowcopy will be implicitly empty")
		return terminateRowIteration(nil)
	}

	var hasNoFurtherRangeFlag int64
	// Iterate per chunk:
	for {
		if err := mgtr.checkAbort(); err != nil {
			return terminateRowIteration(err)
		}
		if atomic.LoadInt64(&mgtr.rowCopyCompleteFlag) == 1 || atomic.LoadInt64(&hasNoFurtherRangeFlag) == 1 {
			// Done
			// There's another such check down the line
			return nil
		}
		copyRowsFunc := func() error {
			mgtr.migrationContext.SetNextIterationRangeMinValues()
			// Copy task:
			applyCopyRowsFunc := func() error {
				if atomic.LoadInt64(&mgtr.rowCopyCompleteFlag) == 1 || atomic.LoadInt64(&hasNoFurtherRangeFlag) == 1 {
					// Done.
					// There's another such check down the line
					return nil
				}

				// When hasFurtherRange is false, original table might be write locked and CalculateNextIterationRangeEndValues would hangs forever
				hasFurtherRange, err := mgtr.applier.CalculateNextIterationRangeEndValues()
				if err != nil {
					return err // wrapping call will retry
				}
				if !hasFurtherRange {
					atomic.StoreInt64(&hasNoFurtherRangeFlag, 1)
					return terminateRowIteration(nil)
				}
				if atomic.LoadInt64(&mgtr.rowCopyCompleteFlag) == 1 {
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
				_, rowsAffected, _, err := mgtr.applier.ApplyIterationInsertQuery()
				if err != nil {
					return err // wrapping call will retry
				}

				if mgtr.migrationContext.PanicOnWarnings {
					if len(mgtr.migrationContext.MigrationLastInsertSQLWarnings) > 0 {
						for _, warning := range mgtr.migrationContext.MigrationLastInsertSQLWarnings {
							mgtr.migrationContext.Log.Infof("ApplyIterationInsertQuery has SQL warnings! %s", warning)
						}
						joinedWarnings := strings.Join(mgtr.migrationContext.MigrationLastInsertSQLWarnings, "; ")
						return terminateRowIteration(fmt.Errorf("ApplyIterationInsertQuery failed because of SQL warnings: [%s]", joinedWarnings))
					}
				}

				atomic.AddInt64(&mgtr.migrationContext.TotalRowsCopied, rowsAffected)
				atomic.AddInt64(&mgtr.migrationContext.Iteration, 1)
				return nil
			}
			if err := mgtr.retryBatchCopyWithHooks(applyCopyRowsFunc); err != nil {
				return terminateRowIteration(err)
			}

			// record last successfully copied range
			mgtr.applier.LastIterationRangeMutex.Lock()
			if mgtr.migrationContext.MigrationIterationRangeMinValues != nil && mgtr.migrationContext.MigrationIterationRangeMaxValues != nil {
				mgtr.applier.LastIterationRangeMinValues = mgtr.migrationContext.MigrationIterationRangeMinValues.Clone()
				mgtr.applier.LastIterationRangeMaxValues = mgtr.migrationContext.MigrationIterationRangeMaxValues.Clone()
			}
			mgtr.applier.LastIterationRangeMutex.Unlock()

			return nil
		}
		// Enqueue copy operation; to be executed by executeWriteFuncs()
		// Use helper to prevent deadlock if executeWriteFuncs exits
		if err := base.SendWithContext(mgtr.migrationContext.GetContext(), mgtr.copyRowsQueue, copyRowsFunc); err != nil {
			// Context cancelled, check for abort and exit
			if abortErr := mgtr.checkAbort(); abortErr != nil {
				return terminateRowIteration(abortErr)
			}
			return terminateRowIteration(err)
		}
	}
}

func (mgtr *Migrator) onApplyEventStruct(eventStruct *applyEventStruct) error {
	handleNonDMLEventStruct := func(eventStruct *applyEventStruct) error {
		if eventStruct.writeFunc != nil {
			if err := mgtr.retryOperation(*eventStruct.writeFunc); err != nil {
				return mgtr.migrationContext.Log.Errore(err)
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

		availableEvents := len(mgtr.applyEventsQueue)
		batchSize := int(atomic.LoadInt64(&mgtr.migrationContext.DMLBatchSize))
		if availableEvents > batchSize-1 {
			// The "- 1" is because we already consumed one event: the original event that led to this function getting called.
			// So, if DMLBatchSize==1 we wish to not process any further events
			availableEvents = batchSize - 1
		}
		for i := 0; i < availableEvents; i++ {
			additionalStruct := <-mgtr.applyEventsQueue
			if additionalStruct.dmlEvent == nil {
				// Not a DML. We don't group this, and we don't batch any further
				nonDmlStructToApply = additionalStruct
				break
			}
			dmlEvents = append(dmlEvents, additionalStruct.dmlEvent)
		}
		// Create a task to apply the DML event; this will be execute by executeWriteFuncs()
		var applyEventFunc tableWriteFunc = func() error {
			return mgtr.applier.ApplyDMLEventQueries(dmlEvents)
		}
		if err := mgtr.retryOperation(applyEventFunc); err != nil {
			return mgtr.migrationContext.Log.Errore(err)
		}
		// update applier coordinates
		mgtr.applier.CurrentCoordinatesMutex.Lock()
		mgtr.applier.CurrentCoordinates = eventStruct.coords
		mgtr.applier.CurrentCoordinatesMutex.Unlock()

		if nonDmlStructToApply != nil {
			// We pulled DML events from the queue, and then we hit a non-DML event. Wait!
			// We need to handle it!
			if err := handleNonDMLEventStruct(nonDmlStructToApply); err != nil {
				return mgtr.migrationContext.Log.Errore(err)
			}
		}
	}
	return nil
}

// Checkpoint attempts to write a checkpoint of the Migrator's current state.
// It gets the binlog coordinates of the last received trx and waits until the
// applier reaches that trx. At that point it's safe to resume from these coordinates.
func (mgtr *Migrator) Checkpoint(ctx context.Context) (*Checkpoint, error) {
	coords := mgtr.eventsStreamer.GetCurrentBinlogCoordinates()
	mgtr.applier.LastIterationRangeMutex.Lock()
	if mgtr.applier.LastIterationRangeMaxValues == nil || mgtr.applier.LastIterationRangeMinValues == nil {
		mgtr.applier.LastIterationRangeMutex.Unlock()
		return nil, errors.New("iteration range is empty, not checkpointing")
	}
	chk := &Checkpoint{
		Iteration:         mgtr.migrationContext.GetIteration(),
		IterationRangeMin: mgtr.applier.LastIterationRangeMinValues.Clone(),
		IterationRangeMax: mgtr.applier.LastIterationRangeMaxValues.Clone(),
		LastTrxCoords:     coords,
		RowsCopied:        atomic.LoadInt64(&mgtr.migrationContext.TotalRowsCopied),
		DMLApplied:        atomic.LoadInt64(&mgtr.migrationContext.TotalDMLEventsApplied),
	}
	mgtr.applier.LastIterationRangeMutex.Unlock()

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		mgtr.applier.CurrentCoordinatesMutex.Lock()
		if coords.SmallerThanOrEquals(mgtr.applier.CurrentCoordinates) {
			id, err := mgtr.applier.WriteCheckpoint(chk)
			chk.Id = id
			mgtr.applier.CurrentCoordinatesMutex.Unlock()
			return chk, err
		}
		mgtr.applier.CurrentCoordinatesMutex.Unlock()
		time.Sleep(500 * time.Millisecond)
	}
}

// CheckpointAfterCutOver writes a final checkpoint after the cutover completes successfully.
func (mgtr *Migrator) CheckpointAfterCutOver() (*Checkpoint, error) {
	if mgtr.lastLockProcessed == nil || mgtr.lastLockProcessed.coords.IsEmpty() {
		return nil, mgtr.migrationContext.Log.Errorf("lastLockProcessed coords are empty")
	}

	chk := &Checkpoint{
		IsCutover:         true,
		LastTrxCoords:     mgtr.lastLockProcessed.coords,
		IterationRangeMin: sql.NewColumnValues(mgtr.migrationContext.UniqueKey.Len()),
		IterationRangeMax: sql.NewColumnValues(mgtr.migrationContext.UniqueKey.Len()),
		Iteration:         mgtr.migrationContext.GetIteration(),
		RowsCopied:        atomic.LoadInt64(&mgtr.migrationContext.TotalRowsCopied),
		DMLApplied:        atomic.LoadInt64(&mgtr.migrationContext.TotalDMLEventsApplied),
	}
	mgtr.applier.LastIterationRangeMutex.Lock()
	if mgtr.applier.LastIterationRangeMinValues != nil {
		chk.IterationRangeMin = mgtr.applier.LastIterationRangeMinValues.Clone()
	}
	if mgtr.applier.LastIterationRangeMaxValues != nil {
		chk.IterationRangeMax = mgtr.applier.LastIterationRangeMaxValues.Clone()
	}
	mgtr.applier.LastIterationRangeMutex.Unlock()

	id, err := mgtr.applier.WriteCheckpoint(chk)
	chk.Id = id
	return chk, err
}

func (mgtr *Migrator) checkpointLoop() {
	if mgtr.migrationContext.Noop {
		mgtr.migrationContext.Log.Debugf("Noop operation; not really checkpointing")
		return
	}
	checkpointInterval := time.Duration(mgtr.migrationContext.CheckpointIntervalSeconds) * time.Second
	ticker := time.NewTicker(checkpointInterval)
	for t := range ticker.C {
		if atomic.LoadInt64(&mgtr.finishedMigrating) > 0 || atomic.LoadInt64(&mgtr.migrationContext.CutOverCompleteFlag) > 0 {
			return
		}
		if atomic.LoadInt64(&mgtr.migrationContext.InCutOverCriticalSectionFlag) > 0 {
			continue
		}
		mgtr.migrationContext.Log.Infof("starting checkpoint at %+v", t)
		ctx, cancel := context.WithTimeout(context.Background(), checkpointTimeout)
		chk, err := mgtr.Checkpoint(ctx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				mgtr.migrationContext.Log.Errorf("checkpoint attempt timed out after %+v", checkpointTimeout)
			} else {
				mgtr.migrationContext.Log.Errorf("error attempting checkpoint: %+v", err)
			}
		} else {
			mgtr.migrationContext.Log.Infof("checkpoint success at coords=%+v range_min=%+v range_max=%+v iteration=%d",
				chk.LastTrxCoords.DisplayString(), chk.IterationRangeMin.String(), chk.IterationRangeMax.String(), chk.Iteration)
		}
		cancel()
	}
}

// executeWriteFuncs writes data via applier: both the rowcopy and the events backlog.
// This is where the ghost table gets the data. The function fills the data single-threaded.
// Both event backlog and rowcopy events are polled; the backlog events have precedence.
func (mgtr *Migrator) executeWriteFuncs() error {
	if mgtr.migrationContext.Noop {
		mgtr.migrationContext.Log.Debugf("Noop operation; not really executing write funcs")
		return nil
	}
	for {
		if err := mgtr.checkAbort(); err != nil {
			return err
		}
		if atomic.LoadInt64(&mgtr.finishedMigrating) > 0 {
			return nil
		}

		mgtr.throttler.throttle(nil)

		// We give higher priority to event processing, then secondary priority to
		// rowcopy
		select {
		case eventStruct := <-mgtr.applyEventsQueue:
			{
				if err := mgtr.onApplyEventStruct(eventStruct); err != nil {
					return err
				}
			}
		default:
			{
				select {
				case copyRowsFunc := <-mgtr.copyRowsQueue:
					{
						copyRowsStartTime := time.Now()
						// Retries are handled within the copyRowsFunc
						if err := copyRowsFunc(); err != nil {
							return mgtr.migrationContext.Log.Errore(err)
						}
						if niceRatio := mgtr.migrationContext.GetNiceRatio(); niceRatio > 0 {
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
						mgtr.migrationContext.Log.Debugf("Getting nothing in the write queue. Sleeping...")
						time.Sleep(time.Second)
					}
				}
			}
		}
	}
}

func (mgtr *Migrator) executeDMLWriteFuncs() error {
	if mgtr.migrationContext.Noop {
		mgtr.migrationContext.Log.Debugf("Noop operation; not really executing DML write funcs")
		return nil
	}
	for {
		if atomic.LoadInt64(&mgtr.finishedMigrating) > 0 {
			return nil
		}

		mgtr.throttler.throttle(nil)

		select {
		case eventStruct := <-mgtr.applyEventsQueue:
			{
				if err := mgtr.onApplyEventStruct(eventStruct); err != nil {
					return err
				}
			}
		case <-time.After(time.Second):
			continue
		}
	}
}

// finalCleanup takes actions at very end of migration, dropping tables etc.
func (mgtr *Migrator) finalCleanup() error {
	atomic.StoreInt64(&mgtr.migrationContext.CleanupImminentFlag, 1)

	mgtr.migrationContext.Log.Infof("Writing changelog state: %+v", Migrated)
	if _, err := mgtr.applier.WriteChangelogState(string(Migrated)); err != nil {
		return err
	}

	if mgtr.migrationContext.Noop {
		if createTableStatement, err := mgtr.inspector.showCreateTable(mgtr.migrationContext.GetGhostTableName()); err == nil {
			mgtr.migrationContext.Log.Infof("New table structure follows")
			fmt.Println(createTableStatement)
		} else {
			mgtr.migrationContext.Log.Errore(err)
		}
	}
	if err := mgtr.eventsStreamer.Close(); err != nil {
		mgtr.migrationContext.Log.Errore(err)
	}

	if err := mgtr.retryOperation(mgtr.applier.DropChangelogTable); err != nil {
		return err
	}
	if mgtr.migrationContext.OkToDropTable && !mgtr.migrationContext.TestOnReplica {
		if err := mgtr.retryOperation(mgtr.applier.DropOldTable); err != nil {
			return err
		}
		if err := mgtr.retryOperation(mgtr.applier.DropCheckpointTable); err != nil {
			return err
		}
	} else if !mgtr.migrationContext.Noop {
		mgtr.migrationContext.Log.Infof("Am not dropping old table because I want this operation to be as live as possible. If you insist I should do it, please add `--ok-to-drop-table` next time. But I prefer you do not. To drop the old table, issue:")
		mgtr.migrationContext.Log.Infof("-- drop table %s.%s", sql.EscapeName(mgtr.migrationContext.DatabaseName), sql.EscapeName(mgtr.migrationContext.GetOldTableName()))
		if mgtr.migrationContext.Checkpoint {
			mgtr.migrationContext.Log.Infof("Am not dropping checkpoint table without `--ok-to-drop-table`. To drop the checkpoint table, issue:")
			mgtr.migrationContext.Log.Infof("-- drop table %s.%s", sql.EscapeName(mgtr.migrationContext.DatabaseName), sql.EscapeName(mgtr.migrationContext.GetCheckpointTableName()))
		}
	}
	if mgtr.migrationContext.Noop {
		if err := mgtr.retryOperation(mgtr.applier.DropGhostTable); err != nil {
			return err
		}
	}

	return nil
}

func (mgtr *Migrator) teardown() {
	atomic.StoreInt64(&mgtr.finishedMigrating, 1)

	if mgtr.inspector != nil {
		mgtr.migrationContext.Log.Infof("Tearing down inspector")
		mgtr.inspector.Teardown()
	}

	if mgtr.applier != nil {
		mgtr.migrationContext.Log.Infof("Tearing down applier")
		mgtr.applier.Teardown()
	}

	if mgtr.eventsStreamer != nil {
		mgtr.migrationContext.Log.Infof("Tearing down streamer")
		mgtr.eventsStreamer.Teardown()
	}

	if mgtr.throttler != nil {
		mgtr.migrationContext.Log.Infof("Tearing down throttler")
		mgtr.throttler.Teardown()
	}
}
