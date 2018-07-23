/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
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

	"github.com/fatih/color"
	"github.com/outbrain/golib/log"
	"sync"
)

type ChangelogState string

const (
	GhostTableMigrated         ChangelogState = "GhostTableMigrated"
	AllEventsUpToLockProcessed                = "AllEventsUpToLockProcessed"
)

func GetRowFormat(total int64, origin bool) string {
	if origin {
		return "Copy: %d/%d %5.1f%%; Applied: %d; Backlog: %d/%d; Time: %+v(total), %+v(copy); streamer: %+v; State: %s; ETA: %s"
	} else {
		digitNum := int(math.Log(float64(total))/math.Log(10) + 1)
		currentFormat := color.GreenString(fmt.Sprintf("%%%dd", digitNum))
		progress := color.GreenString(" %5.1f%%")
		return "Copy: " + currentFormat + "/" + color.RedString("%d") + progress + "; Applied: %d; Backlog: %d/%d; Time: " +
			color.CyanString("%+v") + "(total), %+v(copy); streamer: " + color.BlueString("%+v") + "; State: %s; ETA: " + color.CyanString("%s")
	}
}

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
	parser           *sql.Parser
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

	tableRWLock         sync.Mutex
	rowCopyCompleteFlag int64

	// copyRowsQueue should not be buffered; if buffered some non-damaging but
	//  excessive work happens at the end of the iteration as new copy-jobs arrive befroe realizing the copy is complete
	copyRowsQueue    chan tableWriteFunc
	applyEventsQueue chan *applyEventStruct

	handledChangelogStates map[string]bool

	finishedMigrating int64
}

func NewMigrator(context *base.MigrationContext) *Migrator {
	migrator := &Migrator{
		migrationContext:           context,
		parser:                     sql.NewParser(),
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

// initiateHooksExecutor
func (this *Migrator) initiateHooksExecutor() (err error) {
	this.hooksExecutor = NewHooksExecutor(this.migrationContext)
	if err := this.hooksExecutor.initHooks(); err != nil {
		return err
	}
	return nil
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
			// 如果遇到异常，最好等待一段时间，否则retry也是失败
			time.Sleep(1 * time.Second)
		}
		err = operation()
		if err == nil {
			return nil
		}
		// there's an error. Let's try again.
	}

	// 直接报错，中断执行
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

// executeAndThrottleOnError executes a given function. If it errors, it
// throttles.
func (this *Migrator) executeAndThrottleOnError(operation func() error) (err error) {
	if err := operation(); err != nil {
		this.throttler.throttle(nil)
		return err
	}
	return nil
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

// onChangelogStateEvent is called when a binlog event operation on the changelog table is intercepted.
func (this *Migrator) onChangelogStateEvent(dmlEvent *binlog.BinlogDMLEvent) (err error) {

	// 如何处理binlog的变化呢?
	// Hey, I created the changlog table, I know the type of columns it has!

	// 只关注state变化的Event
	if hint := dmlEvent.NewColumnValues.StringColumn(2); hint != "state" {
		return nil
	}
	changelogStateString := dmlEvent.NewColumnValues.StringColumn(3)
	changelogState := ReadChangelogState(changelogStateString)
	log.Infof("Intercepted changelog state %s", changelogState)

	switch changelogState {
	case GhostTableMigrated:
		{
			this.ghostTableMigrated <- true
		}
	case AllEventsUpToLockProcessed:
		{
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
		}
	default:
		{
			return fmt.Errorf("Unknown changelog state: %+v", changelogState)
		}
	}
	log.Infof("Handled changelog state %s", changelogState)
	return nil
}

// listenOnPanicAbort aborts on abort request
func (this *Migrator) listenOnPanicAbort() {
	err := <-this.migrationContext.PanicAbort
	log.Infof("PanicAbort: %v", err)
	log.Fatale(err)
}

// validateStatement validates the `alter` statement meets criteria.
// At this time this means:
// - column renames are approved
// - no table rename allowed
func (this *Migrator) validateStatement() (err error) {
	if this.parser.IsRenameTable() {
		return fmt.Errorf("ALTER statement seems to RENAME the table. This is not supported, and you should run your RENAME outside gh-ost.")
	}
	if this.parser.HasNonTrivialRenames() && !this.migrationContext.SkipRenamedColumns {
		this.migrationContext.ColumnRenameMap = this.parser.GetNonTrivialRenames()
		// 一般情况下，数据库不要做rename字段；因为已有的代码可能会继续修改被rename的字段，造成大量的错误
		if !this.migrationContext.ApproveRenamedColumns {
			return fmt.Errorf("gh-ost believes the ALTER statement renames columns, as follows: %v; as precaution, you are asked to confirm gh-ost is correct, and provide with `--approve-renamed-columns`, and we're all happy. Or you can skip renamed columns via `--skip-renamed-columns`, in which case column data may be lost", this.parser.GetNonTrivialRenames())
		}
		log.Infof("Alter statement has column(s) renamed. gh-ost finds the following renames: %v; --approve-renamed-columns is given and so migration proceeds.", this.parser.GetNonTrivialRenames())
	}
	this.migrationContext.DroppedColumnsMap = this.parser.DroppedColumnsMap()
	return nil
}

//
// 如何计算行数呢?
// 如果不精确技术，可能估算不准确; 其他没有多大的影响
//
func (this *Migrator) countTableRows() (err error) {
	// 1. CountTableRows 是否计算精确函数
	if !this.migrationContext.CountTableRows {
		// Not counting; we stay with an estimate
		return nil
	}
	if this.migrationContext.Noop {
		log.Debugf("Noop operation; not really counting table rows")
		return nil
	}

	countRowsFunc := func() error {
		// 目前的这种模式比较适合在从库上执行: select count(*), 主库上设置有max-execution-time, 容易失败
		if err := this.inspector.CountTableRows(); err != nil {
			return err
		}
		if err := this.hooksExecutor.onRowCountComplete(); err != nil {
			return err
		}
		return nil
	}

	// 2. 计算方式: 并行计算或者直接计算
	if this.migrationContext.ConcurrentCountTableRows {
		log.Infof("As instructed, counting rows in the background; meanwhile I will use an estimated count, and will update it later on")
		go countRowsFunc()
		// and we ignore errors, because this turns to be a background job
		return nil
	} else {
		return countRowsFunc()
	}
}

func (this *Migrator) createFlagFiles() (err error) {
	if this.migrationContext.PostponeCutOverFlagFile != "" {
		if !base.FileExists(this.migrationContext.PostponeCutOverFlagFile) {
			if err := base.TouchFile(this.migrationContext.PostponeCutOverFlagFile); err != nil {
				return log.Errorf("--postpone-cut-over-flag-file indicated by gh-ost is unable to create said file: %s", err.Error())
			}
			log.Infof("Created postpone-cut-over-flag-file: %s", this.migrationContext.PostponeCutOverFlagFile)
		}
	}
	return nil
}

// Migrate executes the complete migration logic. This is *the* major gh-ost function.
func (this *Migrator) Migrate() (err error) {
	log.Infof("Migrating %s.%s", sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.OriginalTableName))
	this.migrationContext.StartTime = time.Now()
	if this.migrationContext.Hostname, err = os.Hostname(); err != nil {
		return err
	}

	// 执行过程中，如果出错，就直接报错，然后退出
	go this.listenOnPanicAbort()

	if err := this.initiateHooksExecutor(); err != nil {
		return err
	}
	if err := this.hooksExecutor.onStartup(); err != nil {
		return err
	}

	// 1. Parse要执行的语句
	if err := this.parser.ParseAlterStatement(this.migrationContext.AlterStatement); err != nil {
		return err
	}
	// 2. 验证? 怎么做呢?
	// 一般情况下，数据库不要做rename字段；因为已有的代码可能会继续修改被rename的字段，造成大量的错误
	if err := this.validateStatement(); err != nil {
		return err
	}

	// After this point, we'll need to teardown anything that's been started
	//   so we don't leave things hanging around
	defer this.teardown()
	// 3. 监控
	if err := this.initiateInspector(); err != nil {
		return err
	}

	// 4.
	if err := this.initiateStreaming(); err != nil {
		return err
	}
	if err := this.initiateApplier(); err != nil {
		return err
	}
	if err := this.createFlagFiles(); err != nil {
		return err
	}

	initialLag, _ := this.inspector.getReplicationLag()
	log.Infof("Waiting for ghost table to be migrated. Current lag is %+v", initialLag)
	<-this.ghostTableMigrated
	log.Debugf("ghost table migrated")
	// Yay! We now know the Ghost and Changelog tables are good to examine!
	// When running on replica, this means the replica has those tables. When running
	// on master this is always true, of course, and yet it also implies this knowledge
	// is in the binlogs.
	if err := this.inspector.inspectOriginalAndGhostTables(); err != nil {
		return err
	}
	// Validation complete! We're good to execute this migration
	if err := this.hooksExecutor.onValidated(); err != nil {
		return err
	}

	// 创建Server/删除socket file
	if err := this.initiateServer(); err != nil {
		return err
	}
	defer this.server.RemoveSocketFile()

	//
	// 准备migrate!!!!!
	//
	if err := this.countTableRows(); err != nil {
		return err
	}

	if err := this.addDMLEventsListener(); err != nil {
		return err
	}

	// 获取key的range
	if err := this.applier.ReadMigrationRangeValues(); err != nil {
		return err
	}
	if err := this.initiateThrottler(); err != nil {
		return err
	}
	if err := this.hooksExecutor.onBeforeRowCopy(); err != nil {
		return err
	}

	// 开始执行拷贝操作
	// 1. binlog的同步
	go this.executeWriteFuncs()
	// 2. 批量操作
	go this.iterateChunks()

	this.migrationContext.MarkRowCopyStartTime()
	go this.initiateStatus()

	log.Debugf("Operating until row copy is complete")

	// 等待迁移完毕
	this.consumeRowCopyComplete()
	this.migrationContext.RowCopyComplete.Store(true)

	log.Infof(color.MagentaString("=== Row copy complete, Next: CutOver ==="))
	if err := this.hooksExecutor.onRowCopyComplete(); err != nil {
		return err
	}
	this.printStatus(ForcePrintStatusRule)

	// 执行完毕了，然后如何cutOver呢?
	if err := this.hooksExecutor.onBeforeCutOver(); err != nil {
		return err
	}
	var retrier func(func() error, ...bool) error
	if this.migrationContext.CutOverExponentialBackoff {
		retrier = this.retryOperationWithExponentialBackoff
	} else {
		retrier = this.retryOperation
	}
	// 迁移完毕之后进行cutOver
	if err := retrier(this.cutOver); err != nil {
		return err
	}
	atomic.StoreInt64(&this.migrationContext.CutOverCompleteFlag, 1)

	// teardown和finalCleanup可能有一些不同步的问题
	if err := this.finalCleanup(); err != nil {
		return nil
	}

	if err := this.hooksExecutor.onSuccess(); err != nil {
		return err
	}
	log.Infof(color.MagentaString("=== Done migrating %s.%s ===^-^ ^-^"), sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.OriginalTableName))
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
		// TODO: 基于partition的调整，暂时不支持Rollback
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
			return log.Errore(err)
		}
		if this.migrationContext.TestOnReplicaSkipReplicaStop {
			log.Warningf("--test-on-replica-skip-replica-stop enabled, we are not starting replication.")
		} else {
			log.Debugf("testing on replica. Starting replication IO thread after cut-over failure")
			if err := this.retryOperation(this.applier.StartReplication); err != nil {
				return log.Errore(err)
			}
		}
	}
	return nil
}

// cutOver performs the final step of migration, based on migration
// type (on replica? atomic? safe?)
func (this *Migrator) cutOver() (err error) {
	if this.migrationContext.Noop {
		log.Debugf("Noop operation; not really swapping tables")
		return nil
	}
	this.migrationContext.MarkPointOfInterest()
	this.throttler.throttle(func() {
		log.Debugf("throttling before swapping tables")
	})

	this.migrationContext.MarkPointOfInterest()
	log.Debugf("checking for cut-over postpone")
	this.sleepWhileTrue(
		func() (bool, error) {
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
	log.Debugf("checking for cut-over postpone: complete")

	if this.migrationContext.TestOnReplica {
		// With `--test-on-replica` we stop replication thread, and then proceed to use
		// the same cut-over phase as the master would use. That means we take locks
		// and swap the tables.
		// The difference is that we will later swap the tables back.
		if err := this.hooksExecutor.onStopReplication(); err != nil {
			return err
		}
		if this.migrationContext.TestOnReplicaSkipReplicaStop {
			log.Warningf("--test-on-replica-skip-replica-stop enabled, we are not stopping replication.")
		} else {
			log.Debugf("testing on replica. Stopping replication IO thread")
			if err := this.retryOperation(this.applier.StopReplication); err != nil {
				return err
			}
		}
	}

	// 优先考虑：一步就CutOver
	if this.migrationContext.Partition == nil && this.migrationContext.CutOverType == base.CutOverAtomic {
		// Atomic solution: we use low timeout and multiple attempts. But for
		// each failed attempt, we throttle until replication lag is back to normal
		err := this.atomicCutOver()
		this.handleCutOverResult(err)
		return err
	}

	// 如果支持Partition，或者两步Cut
	if this.migrationContext.Partition != nil || this.migrationContext.CutOverType == base.CutOverTwoStep {
		err := this.cutOverTwoStep()
		this.handleCutOverResult(err)
		return err
	}
	return log.Fatalf("Unknown cut-over type: %d; should never get here!", this.migrationContext.CutOverType)
}

// Inject the "AllEventsUpToLockProcessed" state hint, wait for it to appear in the binary logs,
// make sure the queue is drained.
func (this *Migrator) waitForEventsUpToLock() (err error) {
	// log.Infof(color.CyanString("Enter waitForEventsUpToLock..."))
	timeout := time.NewTimer(time.Second * time.Duration(this.migrationContext.CutOverLockTimeoutSeconds))

	this.migrationContext.MarkPointOfInterest()
	waitForEventsUpToLockStartTime := time.Now()

	// 写入hint
	allEventsUpToLockProcessedChallenge := fmt.Sprintf("%s:%d", string(AllEventsUpToLockProcessed), waitForEventsUpToLockStartTime.UnixNano())

	//log.Infof("A: Writing changelog state: %+v", allEventsUpToLockProcessedChallenge)
	if _, err := this.applier.WriteChangelogState(allEventsUpToLockProcessedChallenge); err != nil {
		//log.Errorf("WriteChangelogState error: %v", err)
		return err
	}
	//log.Infof("B: Waiting for events up to lock")
	atomic.StoreInt64(&this.migrationContext.AllEventsUpToLockProcessedInjectedFlag, 1)

	for found := false; !found; {
		select {
		case <-timeout.C:
			{
				return log.Errorf("C: Timeout while waiting for events up to lock")
			}
		case state := <-this.allEventsUpToLockProcessed:
			{
				log.Infof("state: %s vs. %s", state, allEventsUpToLockProcessedChallenge)
				// 通过binlog收到消息，表明lock之前所有的events都收到了
				if state == allEventsUpToLockProcessedChallenge {
					log.Infof("C: Waiting for events up to lock: got %s", state)
					found = true
				} else {
					log.Infof("C: Waiting for events up to lock: skipping %s", state)
				}
			}
		}
	}
	waitForEventsUpToLockDuration := time.Since(waitForEventsUpToLockStartTime)

	log.Infof("D: Done waiting for events up to lock; duration=%+v", waitForEventsUpToLockDuration)
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

	// 开始执行Table的Lock操作
	this.tableRWLock.Lock()
	defer this.tableRWLock.Unlock()

	// 1. Original Table锁定，不让写入数据
	//    剩余的binlog，会迅速写入ghost文件中；由于优先处理binlog，且有throttle控制，因此当rows被拷贝完毕时，binlog剩余的也不多
	if err := this.retryOperation(this.applier.LockOriginalTable); err != nil {
		return err
	}

	// 2. 确保lock之前所有的binlog已经收到，并且processed
	if err := this.retryOperation(this.waitForEventsUpToLock); err != nil {
		return err
	}

	// 3. 交换table
	if err := this.retryOperation(this.applier.SwapTablesQuickAndBumpy); err != nil {
		return err
	}

	// 4. 当前Session释放locks
	if err := this.retryOperation(this.applier.UnlockTables); err != nil {
		return err
	}

	// 5. 打印执行情况
	lockAndRenameDuration := this.migrationContext.RenameTablesEndTime.Sub(this.migrationContext.LockTablesStartTime)
	renameDuration := this.migrationContext.RenameTablesEndTime.Sub(this.migrationContext.RenameTablesStartTime)
	log.Debugf("Lock & rename duration: %s (rename only: %s). During this time, queries on %s were locked or failing", lockAndRenameDuration, renameDuration, sql.EscapeName(this.migrationContext.OriginalTableName))
	return nil
}

// atomicCutOver
func (this *Migrator) atomicCutOver() (err error) {
	atomic.StoreInt64(&this.migrationContext.InCutOverCriticalSectionFlag, 1)
	defer atomic.StoreInt64(&this.migrationContext.InCutOverCriticalSectionFlag, 0)

	this.tableRWLock.Lock()
	defer this.tableRWLock.Unlock()

	okToUnlockTable := make(chan bool, 4)
	defer func() {
		okToUnlockTable <- true
		this.applier.DropAtomicCutOverSentryTableIfExists()
	}()

	atomic.StoreInt64(&this.migrationContext.AllEventsUpToLockProcessedInjectedFlag, 0)

	lockOriginalSessionIdChan := make(chan int64, 2)

	// 为什么要异步执行 AtomicCutOverMagicLock
	tableLocked := make(chan error, 2)
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
	if err := this.waitForEventsUpToLock(); err != nil {
		return log.Errore(err)
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

// initiateServer begins listening on unix socket/tcp for incoming interactive commands
func (this *Migrator) initiateServer() (err error) {
	var f printStatusFunc = func(rule PrintStatusRule, writer io.Writer) {
		this.printStatus(rule, writer)
	}

	this.server = NewServer(this.migrationContext, this.hooksExecutor, f)

	// 绑定socket
	if err := this.server.BindSocketFile(); err != nil {
		return err
	}

	// 绑定tcp
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
		log.Infof("Master found to be %+v", *this.migrationContext.ApplierConnectionConfig.ImpliedKey)
	} else {
		// Forced master host.
		key, err := mysql.ParseRawInstanceKeyLoose(this.migrationContext.AssumeMasterHostname)
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
		log.Infof("Master forced to be %+v", *this.migrationContext.ApplierConnectionConfig.ImpliedKey)
	}
	// validate configs
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
		return fmt.Errorf("It seems like this migration attempt to run directly on master. Preferably it would be executed on a replica (and this reduces load from the master). To proceed please provide "+color.GreenString("--allow-on-master")+". Inspector config=%+v, applier config=%+v", this.migrationContext.InspectorConnectionConfig, this.migrationContext.ApplierConnectionConfig)
	}
	if err := this.inspector.validateLogSlaveUpdates(); err != nil {
		return err
	}

	return nil
}

// initiateStatus sets and activates the printStatus() ticker
func (this *Migrator) initiateStatus() error {
	this.printStatus(ForcePrintStatusAndHintRule)
	statusTick := time.Tick(1 * time.Second)
	for range statusTick {
		if atomic.LoadInt64(&this.finishedMigrating) > 0 {
			return nil
		}

		// 如果清理工作完成，则直接退出
		if this.migrationContext != nil && atomic.LoadInt64(&this.migrationContext.CleanupImminentFlag) > 0 {
			return nil
		}

		go this.printStatus(HeuristicPrintStatusRule)
	}

	return nil
}

// printMigrationStatusHint prints a detailed configuration dump, that is useful
// to keep in mind; such as the name of migrated table, throttle params etc.
// This gets printed at beginning and end of migration, every 10 minutes throughout
// migration, and as response to the "status" interactive command.
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
		this.migrationContext.Hostname,
	))
	fmt.Fprintln(w, fmt.Sprintf("# Migration started at %+v",
		this.migrationContext.StartTime.Format(time.RubyDate),
	))
	maxLoad := this.migrationContext.GetMaxLoad()
	criticalLoad := this.migrationContext.GetCriticalLoad()

	fmt.Fprintln(w, fmt.Sprintf(color.MagentaString("# chunk-size: %+v; max-lag-millis: %+vms; dml-batch-size: %+v; max-load: %s; critical-load: %s; nice-ratio: %f"),
		atomic.LoadInt64(&this.migrationContext.ChunkSize),
		atomic.LoadInt64(&this.migrationContext.MaxLagMillisecondsThrottleThreshold),
		atomic.LoadInt64(&this.migrationContext.DMLBatchSize),
		maxLoad.String(),
		criticalLoad.String(),
		this.migrationContext.GetNiceRatio(),
	))
	if this.migrationContext.ThrottleFlagFile != "" {
		setIndicator := ""
		if base.FileExists(this.migrationContext.ThrottleFlagFile) {
			setIndicator = "[set]"
		}
		fmt.Fprintln(w, fmt.Sprintf(color.MagentaString("# throttle-flag-file:")+" %+v %+v",
			this.migrationContext.ThrottleFlagFile, setIndicator,
		))
	}
	if this.migrationContext.ThrottleAdditionalFlagFile != "" {
		setIndicator := ""
		if base.FileExists(this.migrationContext.ThrottleAdditionalFlagFile) {
			setIndicator = "[set]"
		}
		fmt.Fprintln(w, fmt.Sprintf(color.MagentaString("# throttle-additional-flag-file:")+" %+v %+v",
			this.migrationContext.ThrottleAdditionalFlagFile, setIndicator,
		))
	}
	if throttleQuery := this.migrationContext.GetThrottleQuery(); throttleQuery != "" {
		fmt.Fprintln(w, fmt.Sprintf("# throttle-query: %+v",
			throttleQuery,
		))
	}
	if throttleControlReplicaKeys := this.migrationContext.GetThrottleControlReplicaKeys(); throttleControlReplicaKeys.Len() > 0 {
		fmt.Fprintln(w, fmt.Sprintf("# throttle-control-replicas count: %+v",
			throttleControlReplicaKeys.Len(),
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
	fmt.Fprintln(w, fmt.Sprintf(color.MagentaString("# Serving on unix socket:")+" %+v",
		this.migrationContext.ServeSocketFile,
	))
	if this.migrationContext.ServeTCPPort != 0 {
		fmt.Fprintln(w, fmt.Sprintf(color.MagentaString("# Serving on TCP port:")+" %+v", this.migrationContext.ServeTCPPort))
	}
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
	var progressPct float64
	if rowsEstimate == 0 {
		progressPct = 100.0
	} else {
		progressPct = 100.0 * float64(totalRowsCopied) / float64(rowsEstimate)
	}
	// Before status, let's see if we should print a nice reminder for what exactly we're doing here.
	shouldPrintMigrationStatusHint := (elapsedSeconds%600 == 0)
	if rule == ForcePrintStatusAndHintRule {
		shouldPrintMigrationStatusHint = true
	}
	if rule == ForcePrintStatusOnlyRule {
		shouldPrintMigrationStatusHint = false
	}
	if shouldPrintMigrationStatusHint {
		this.printMigrationStatusHint(writers...)
	}

	var etaSeconds float64 = math.MaxFloat64
	eta := "N/A"
	if progressPct >= 100.0 {
		eta = "due"
	} else if progressPct >= 1.0 {
		elapsedRowCopySeconds := this.migrationContext.ElapsedRowCopyTime().Seconds()
		totalExpectedSeconds := elapsedRowCopySeconds * float64(rowsEstimate) / float64(totalRowsCopied)
		etaSeconds = totalExpectedSeconds - elapsedRowCopySeconds
		if etaSeconds >= 0 {
			etaDuration := time.Duration(etaSeconds) * time.Second
			eta = base.PrettifyDurationOutput(etaDuration)
		} else {
			eta = "due"
		}
	}

	state := "migrating"
	if atomic.LoadInt64(&this.migrationContext.CountingRowsFlag) > 0 && !this.migrationContext.ConcurrentCountTableRows {
		state = "counting rows"
	} else if atomic.LoadInt64(&this.migrationContext.IsPostponingCutOver) > 0 {
		eta = "due"
		state = "postponing cut-over"
	} else if isThrottled, throttleReason, _ := this.migrationContext.IsThrottled(); isThrottled {
		state = fmt.Sprintf("throttled, %s", throttleReason)
	}

	shouldPrintStatus := false
	if rule == HeuristicPrintStatusRule {
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
	} else {
		// Not heuristic
		shouldPrintStatus = true
	}
	if !shouldPrintStatus {
		return
	}

	currentBinlogCoordinates := *this.eventsStreamer.GetCurrentBinlogCoordinates()

	// 结束之后就不再汇报情况
	if !this.migrationContext.RowCopyComplete.Load().(bool) {
		// "Copy: %d/%d %.1f%%; Applied: %d; Backlog: %d/%d; Time: %+v(total), %+v(copy); streamer: %+v; State: %s; ETA: %s"
		// 保存到数据库
		format := GetRowFormat(rowsEstimate, true)
		status := fmt.Sprintf(format,
			totalRowsCopied, rowsEstimate, progressPct,
			atomic.LoadInt64(&this.migrationContext.TotalDMLEventsApplied),
			len(this.applyEventsQueue), cap(this.applyEventsQueue),
			base.PrettifyDurationOutput(elapsedTime), base.PrettifyDurationOutput(this.migrationContext.ElapsedRowCopyTime()),
			currentBinlogCoordinates,
			state,
			eta,
		)
		this.applier.WriteChangelog(
			fmt.Sprintf("copy iteration %d at %d", this.migrationContext.GetIteration(), time.Now().Unix()),
			status,
		)

		// 打印Log
		format = GetRowFormat(rowsEstimate, false)
		status = fmt.Sprintf(format,
			totalRowsCopied, rowsEstimate, progressPct,
			atomic.LoadInt64(&this.migrationContext.TotalDMLEventsApplied),
			len(this.applyEventsQueue), cap(this.applyEventsQueue),
			base.PrettifyDurationOutput(elapsedTime), base.PrettifyDurationOutput(this.migrationContext.ElapsedRowCopyTime()),
			currentBinlogCoordinates,
			state,
			eta,
		)
		if !this.migrationContext.Noop {
			w := io.MultiWriter(writers...)
			fmt.Fprintln(w, status)
		}

		if elapsedSeconds%60 == 0 {
			this.hooksExecutor.onStatus(status)
		}
	}
}

// initiateStreaming begins streaming of binary log events and registers listeners for such events
func (this *Migrator) initiateStreaming() error {
	// 关注一下: NewEventsStreamer
	this.eventsStreamer = NewEventsStreamer(this.migrationContext)
	if err := this.eventsStreamer.InitDBConnections(); err != nil {
		return err
	}
	this.eventsStreamer.AddListener(
		false, // 同步处理binlog的变化
		this.migrationContext.DatabaseName,
		// gt-ost 应用场景: 一次只改一个table, 因此不需要关注其他的tables的变化
		this.migrationContext.GetChangelogTableName(),
		func(dmlEvent *binlog.BinlogDMLEvent) error {
			// 监听state变化
			return this.onChangelogStateEvent(dmlEvent)
		},
	)

	// 如何开始streaming呢?
	go func() {
		log.Debugf("Beginning streaming")
		err := this.eventsStreamer.StreamEvents(this.canStopStreaming)
		// 出错，就直接PanicAbort
		if err != nil {
			this.migrationContext.PanicAbort <- err
		}
		log.Debugf("Done streaming")
	}()

	go func() {
		ticker := time.Tick(1 * time.Second)
		for range ticker {
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
			// log.Infof(color.RedString("addDMLEventsListener: %v"), dmlEvent)
			this.applyEventsQueue <- newApplyEventStructByDML(dmlEvent)
			return nil
		},
	)
	return err
}

// initiateThrottler kicks in the throttling collection and the throttling checks.
func (this *Migrator) initiateThrottler() error {
	this.throttler = NewThrottler(this.migrationContext, this.applier, this.inspector)

	go this.throttler.initiateThrottlerCollection(this.firstThrottlingCollected)
	log.Infof("Waiting for first throttle metrics to be collected")
	<-this.firstThrottlingCollected // replication lag
	<-this.firstThrottlingCollected // HTTP status
	<-this.firstThrottlingCollected // other, general metrics
	log.Infof("First throttle metrics collected")
	go this.throttler.initiateThrottlerChecks()

	return nil
}

func (this *Migrator) initiateApplier() error {
	this.applier = NewApplier(this.migrationContext)
	if err := this.applier.InitDBConnections(); err != nil {
		return err
	}
	if err := this.applier.ValidateOrDropExistingTables(); err != nil {
		return err
	}

	// Changelog 和 Ghost的关系?
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

	// Ghost表准备好了
	this.applier.WriteChangelogState(string(GhostTableMigrated))

	// 定时写入Heartbeat
	go this.applier.InitiateHeartbeat()
	return nil
}

type TAtomBool struct{ flag int32 }

func (b *TAtomBool) Set(value bool) {
	var i int32 = 0
	if value {
		i = 1
	}
	atomic.StoreInt32(&(b.flag), int32(i))
}
func (b *TAtomBool) Get() bool {
	if atomic.LoadInt32(&(b.flag)) != 0 {
		return true
	}
	return false
}

// iterateChunks iterates the existing table rows, and generates a copy task of
// a chunk of rows onto the ghost table.
func (this *Migrator) iterateChunks() error {
	// Iterate per chunk:
	rowRangeComplete := &TAtomBool{}

	terminateRowIteration := func(err error) error {
		rowRangeComplete.Set(true)
		this.rowCopyComplete <- err
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
			// There's another such check down the line
			return nil
		}
		copyRowsFunc := func() error {
			//defer func() {
			//	log.Infof(color.CyanString("copyRowsFunc over"))
			//}()
			if atomic.LoadInt64(&this.rowCopyCompleteFlag) == 1 || rowRangeComplete.Get() {
				// Done.
				// There's another such check down the line
				return nil
			}

			// 计算当前iteration的range
			// 需要注意所的问题：
			hasFurtherRange, err := this.applier.CalculateNextIterationRangeEndValues()
			if err != nil {
				return terminateRowIteration(err)
			}
			if !hasFurtherRange {
				return terminateRowIteration(nil)
			}
			// Copy task:
			applyCopyRowsFunc := func() error {
				if atomic.LoadInt64(&this.rowCopyCompleteFlag) == 1 || rowRangeComplete.Get() {
					// No need for more writes.
					// This is the de-facto place where we avoid writing in the event of completed cut-over.
					// There could _still_ be a race condition, but that's as close as we can get.
					// What about the race condition? Well, there's actually no data integrity issue.
					// when rowCopyCompleteFlag==1 that means **guaranteed** all necessary rows have been copied.
					// But some are still then collected at the binary log, and these are the ones we're trying to
					// not apply here. If the race condition wins over us, then we just attempt to apply onto the
					// _ghost_ table, which no longer exists. So, bothering error messages and all, but no damage.
					//log.Infof("rowCopyCompleteFlag: 1")
					return nil
				}

				_, rowsAffected, _, err := this.applier.ApplyIterationInsertQuery()
				if err != nil {
					return terminateRowIteration(err)
				}

				// 更改统计数据
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
	return nil
}

func (this *Migrator) onApplyEventStruct(eventStruct *applyEventStruct) error {
	//log.Infof(color.MagentaString("onApplyEventStruct"))

	// 如何执行一个DML Event呢?
	handleNonDMLEventStruct := func(eventStruct *applyEventStruct) error {
		if eventStruct.writeFunc != nil {
			if err := this.retryOperation(*eventStruct.writeFunc); err != nil {
				return log.Errore(err)
			}
		}
		return nil
	}
	if eventStruct.dmlEvent == nil {
		return handleNonDMLEventStruct(eventStruct)
	}
	if eventStruct.dmlEvent != nil {
		// 拷贝一份数据出来?
		dmlEvents := [](*binlog.BinlogDMLEvent){}

		// 先消费一个Event
		dmlEvents = append(dmlEvents, eventStruct.dmlEvent)
		var nonDmlStructToApply *applyEventStruct

		availableEvents := len(this.applyEventsQueue)
		batchSize := int(atomic.LoadInt64(&this.migrationContext.DMLBatchSize))
		if availableEvents > batchSize-1 {
			// The "- 1" is because we already consumed one event: the original event that led to this function getting called.
			// So, if DMLBatchSize==1 we wish to not process any further events
			availableEvents = batchSize - 1
		}

		// 是否考虑批量执行(把剩下可以执行的命令尽可能多地执行)
		for i := 0; i < availableEvents; i++ {
			//log.Infof("QueueLen: %d", len(this.applyEventsQueue))
			additionalStruct := <-this.applyEventsQueue
			if additionalStruct.dmlEvent == nil {
				// Not a DML. We don't group this, and we don't batch any further
				// 非DML, 那可能是什么情况呢?
				nonDmlStructToApply = additionalStruct
				break
			}
			dmlEvents = append(dmlEvents, additionalStruct.dmlEvent)
		}

		// Create a task to apply the DML event; this will be execute by executeWriteFuncs()
		// 如何实现一个RetryWrapper呢?
		// 函数闭包，消除被Retry函数的特异性
		//
		var applyEventFunc tableWriteFunc = func() error {
			return this.applier.ApplyDMLEventQueries(dmlEvents)
		}
		if err := this.retryOperation(applyEventFunc); err != nil {
			return log.Errore(err)
		}

		if nonDmlStructToApply != nil {
			// We pulled DML events from the queue, and then we hit a non-DML event. Wait!
			// We need to handle it!
			if err := handleNonDMLEventStruct(nonDmlStructToApply); err != nil {
				return log.Errore(err)
			}
		}
	}
	return nil
}

// executeWriteFuncs writes data via applier: both the rowcopy and the events backlog.
// This is where the ghost table gets the data. The function fills the data single-threaded.
// Both event backlog and rowcopy events are polled; the backlog events have precedence.
func (this *Migrator) executeWriteFuncs() error {
	defer func() {
		log.Infof(color.GreenString("executeWriteFuncs over..."))
	}()
	// 如果是dry-run，则直接返回
	if this.migrationContext.Noop {
		log.Debugf("Noop operation; not really executing write funcs")
		return nil
	}
	for {
		if atomic.LoadInt64(&this.finishedMigrating) > 0 {
			// log.Infof(color.RedString("finishedMigrating...."))
			return nil
		}

		// log.Infof(color.GreenString("before throttle"))
		this.throttler.throttle(nil)

		// We give higher priority to event processing, then secondary priority to
		// rowcopy
		// 如何处理select/case的优先级问题
		// log.Infof(color.RedString("executeWriteFuncs normal loop, queue: %d, %p"), len(this.applyEventsQueue), this.applyEventsQueue)
		select {
		case eventStruct := <-this.applyEventsQueue:
			{
				// 有新的Event, 则立马Apply
				//log.Infof(color.RedString("consume applyEventsQueue ...."))
				if err := this.onApplyEventStruct(eventStruct); err != nil {
					log.Infof(color.RedString("onApplyEventStruct error: %v"), err)
					return err
				}
			}
		default:
			{
				select {
				case copyRowsFunc := <-this.copyRowsQueue:
					{
						// 否则执行Rows Copy的任务?
						copyRowsStartTime := time.Now()
						//log.Infof(color.GreenString("copyRowsFunc begin"))
						// Retries are handled within the copyRowsFunc
						if err := copyRowsFunc(); err != nil {

							//log.Infof(color.GreenString("copyRowsFunc error: %v"), err)

							return log.Errore(err)
						}

						//log.Infof(color.GreenString("copyRowsFunc finished"))
						if niceRatio := this.migrationContext.GetNiceRatio(); niceRatio > 0 {
							copyRowsDuration := time.Since(copyRowsStartTime)
							sleepTimeNanosecondFloat64 := niceRatio * float64(copyRowsDuration.Nanoseconds())
							sleepTime := time.Duration(time.Duration(int64(sleepTimeNanosecondFloat64)) * time.Nanosecond)
							//log.Infof(color.RedString("sleepTime：%.3fs"), sleepTime)
							time.Sleep(sleepTime)
						}
					}
				default:
					{
						// Hmmmmm... nothing in the queue; no events, but also no row copy.
						// This is possible upon load. Let's just sleep it over.
						log.Infof(color.RedString("Getting nothing in the write queue. Sleeping..."))
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
	atomic.StoreInt64(&this.migrationContext.CleanupImminentFlag, 1)

	if this.migrationContext.Noop {
		if createTableStatement, err := this.inspector.showCreateTable(this.migrationContext.GetGhostTableName()); err == nil {
			log.Infof("New table structure follows")
			log.Infof(createTableStatement)
		} else {
			log.Errore(err)
		}
	}
	if err := this.eventsStreamer.Close(); err != nil {
		log.Errore(err)
	}

	// 这里做清理工作之后，可能会出现一些goroutine协调的问题：例如其他goroutine继续写changelog
	if err := this.retryOperation(this.applier.DropChangelogTable); err != nil {
		return err
	}
	if this.migrationContext.OkToDropTable && !this.migrationContext.TestOnReplica {
		if err := this.retryOperation(this.applier.DropOldTable); err != nil {
			return err
		}
	} else {
		if !this.migrationContext.Noop {
			log.Infof("Am not dropping old table because I want this operation to be as live as possible. If you insist I should do it, please add " + color.MagentaString("`--ok-to-drop-table`") + " next time. But I prefer you do not. To drop the old table, issue:")
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

func (this *Migrator) teardown() {
	atomic.StoreInt64(&this.finishedMigrating, 1)

	if this.inspector != nil {
		log.Infof("Tearing down inspector")
		this.inspector.Teardown()
	}

	if this.applier != nil {
		log.Infof("Tearing down applier")
		this.applier.Teardown()
	}

	if this.eventsStreamer != nil {
		log.Infof("Tearing down streamer")
		this.eventsStreamer.Teardown()
	}

	if this.throttler != nil {
		log.Infof("Tearing down throttler")
		this.throttler.Teardown()
	}
}
