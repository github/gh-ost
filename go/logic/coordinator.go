package logic

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"errors"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/binlog"
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"
	"github.com/go-mysql-org/go-mysql/replication"
	drivermysql "github.com/go-sql-driver/mysql"
)

type Coordinator struct {
	migrationContext *base.MigrationContext

	binlogReader *binlog.GoMySQLReader

	onChangelogEvent func(dmlEvent *binlog.BinlogDMLEvent) error

	applier *Applier

	throttler *Throttler

	// Atomic counter for number of active workers (not in workerQueue)
	busyWorkers atomic.Int64

	// Mutex to protect the fields below
	mu sync.Mutex

	// list of workers
	workers []*Worker

	// The low water mark. We maintain that all transactions with
	// sequence number <= lowWaterMark have been completed.
	lowWaterMark int64

	// This is a map of completed jobs by their sequence numbers.
	// This is used when updating the low water mark.
	// It records the binlog coordinates of the completed transaction.
	completedJobs map[int64]struct{}

	// These are the jobs that are waiting for a previous job to complete.
	// They are indexed by the sequence number of the job they are waiting for.
	waitingJobs map[int64][]chan struct{}

	events chan *replication.BinlogEvent

	workerQueue chan *Worker

	// fatalErr stores the first fatal error from any worker goroutine.
	fatalErr   error
	fatalErrMu sync.Mutex
	// failedCh is closed on the first fatal worker error; all blocking
	// coordinator and worker operations select on this to unblock.
	failedCh chan struct{}

	finishedMigrating atomic.Bool
}

// Worker takes jobs from the Coordinator and applies the job's DML events.
type Worker struct {
	id          int
	coordinator *Coordinator
	eventQueue  chan *replication.BinlogEvent

	executedJobs     atomic.Int64
	dmlEventsApplied atomic.Int64
	waitTimeNs       atomic.Int64
	busyTimeNs       atomic.Int64
}

type stats struct {
	dmlRate float64
	trxRate float64

	// Number of DML events applied
	dmlEventsApplied int64

	// Number of transactions processed
	executedJobs int64

	// Time spent applying DML events
	busyTime time.Duration

	// Time spent waiting on transaction dependecies
	// or waiting on events to arrive in queue.
	waitTime time.Duration
}

// isRetryableError returns true for MySQL errors that are safe to retry
// (deadlock and lock wait timeout).
func isRetryableError(err error) bool {
	var mysqlErr *drivermysql.MySQLError
	if errors.As(err, &mysqlErr) {
		switch mysqlErr.Number {
		case 1213, 1205: // deadlock, lock wait timeout
			return true
		}
	}
	return false
}

// setFatalError records the first fatal error and closes failedCh so
// all blocking operations in the coordinator and workers unblock.
func (c *Coordinator) setFatalError(err error) {
	c.fatalErrMu.Lock()
	defer c.fatalErrMu.Unlock()
	if c.fatalErr == nil {
		c.fatalErr = err
		close(c.failedCh)
	}
}

// getFatalError returns the first fatal error, or nil.
func (c *Coordinator) getFatalError() error {
	c.fatalErrMu.Lock()
	defer c.fatalErrMu.Unlock()
	return c.fatalErr
}

func (w *Worker) ProcessEvents() error {
	databaseName := w.coordinator.migrationContext.DatabaseName
	originalTableName := w.coordinator.migrationContext.OriginalTableName
	changelogTableName := w.coordinator.migrationContext.GetChangelogTableName()

	for {
		if w.coordinator.finishedMigrating.Load() {
			return nil
		}

		// Wait for first event (GTID), interruptible by fatal error
		waitStart := time.Now()
		var ev *replication.BinlogEvent
		select {
		case ev = <-w.eventQueue:
		case <-w.coordinator.failedCh:
			return fmt.Errorf("aborting: %w", w.coordinator.getFatalError())
		}
		w.waitTimeNs.Add(time.Since(waitStart).Nanoseconds())

		// Verify this is a GTID Event
		gtidEvent, ok := ev.Event.(*replication.GTIDEvent)
		if !ok {
			w.coordinator.migrationContext.Log.Debugf("Received unexpected event: %v\n", ev)
		}

		// Dependency wait is done by the coordinator before dispatch
		// (coordinator-side scheduling, matching MySQL applier semantics).

		// Process the transaction
		var changelogEvent *binlog.BinlogDMLEvent
		var txErr error
		dmlEvents := make([]*binlog.BinlogDMLEvent, 0, int(atomic.LoadInt64(&w.coordinator.migrationContext.DMLBatchSize)))
	events:
		for {
			// wait for next event in the transaction
			waitStart := time.Now()
			var ev *replication.BinlogEvent
			select {
			case ev = <-w.eventQueue:
			case <-w.coordinator.failedCh:
				w.coordinator.busyWorkers.Add(-1)
				return fmt.Errorf("aborting: %w", w.coordinator.getFatalError())
			}
			w.waitTimeNs.Add(time.Since(waitStart).Nanoseconds())

			if ev == nil {
				break events
			}

			switch binlogEvent := ev.Event.(type) {
			case *replication.RowsEvent:
				dml := binlog.ToEventDML(ev.Header.EventType.String())
				if dml == binlog.NotDML {
					w.coordinator.busyWorkers.Add(-1)
					return fmt.Errorf("unknown DML type: %s", ev.Header.EventType.String())
				}

				if !strings.EqualFold(databaseName, string(binlogEvent.Table.Schema)) {
					continue
				}

				if !strings.EqualFold(originalTableName, string(binlogEvent.Table.Table)) && !strings.EqualFold(changelogTableName, string(binlogEvent.Table.Table)) {
					continue
				}

				for i, row := range binlogEvent.Rows {
					if dml == binlog.UpdateDML && i%2 == 1 {
						// An update has two rows (WHERE+SET)
						// We do both at the same time
						continue
					}
					dmlEvent := binlog.NewBinlogDMLEvent(
						string(binlogEvent.Table.Schema),
						string(binlogEvent.Table.Table),
						dml,
					)
					switch dml {
					case binlog.InsertDML:
						{
							dmlEvent.NewColumnValues = sql.ToColumnValues(row)
						}
					case binlog.UpdateDML:
						{
							dmlEvent.WhereColumnValues = sql.ToColumnValues(row)
							dmlEvent.NewColumnValues = sql.ToColumnValues(binlogEvent.Rows[i+1])
						}
					case binlog.DeleteDML:
						{
							dmlEvent.WhereColumnValues = sql.ToColumnValues(row)
						}
					}

					if strings.EqualFold(changelogTableName, string(binlogEvent.Table.Table)) {
						changelogEvent = dmlEvent
					} else {
						dmlEvents = append(dmlEvents, dmlEvent)

						if len(dmlEvents) == cap(dmlEvents) {
							if err := w.applyDMLEvents(dmlEvents); err != nil {
								txErr = err
								break events
							}
							dmlEvents = dmlEvents[:0]
						}
					}
				}
			case *replication.XIDEvent:
				if len(dmlEvents) > 0 {
					if err := w.applyDMLEvents(dmlEvents); err != nil {
						txErr = err
						break events
					}
				}

				w.executedJobs.Add(1)
				break events
			}
		}

		if txErr != nil {
			// Fatal: DML failed after retries. Decrement busyWorkers
			// since we won't reach the normal cleanup path below.
			w.coordinator.busyWorkers.Add(-1)
			return txErr
		}

		w.coordinator.MarkTransactionCompleted(gtidEvent.SequenceNumber, int64(ev.Header.LogPos), int64(ev.Header.EventSize))

		// Did we see a changelog event?
		// Handle it now
		if changelogEvent != nil {
			// wait for all transactions before this point
			clWaitCh := w.coordinator.WaitForTransaction(gtidEvent.SequenceNumber - 1)
			if clWaitCh != nil {
				waitStart := time.Now()
				select {
				case <-clWaitCh:
				case <-w.coordinator.failedCh:
					w.coordinator.busyWorkers.Add(-1)
					return fmt.Errorf("aborting: %w", w.coordinator.getFatalError())
				}
				w.waitTimeNs.Add(time.Since(waitStart).Nanoseconds())
			}
			w.coordinator.HandleChangeLogEvent(changelogEvent)
		}

		w.coordinator.workerQueue <- w
		w.coordinator.busyWorkers.Add(-1)
	}
}

func (w *Worker) applyDMLEvents(dmlEvents []*binlog.BinlogDMLEvent) error {
	if w.coordinator.throttler != nil {
		w.coordinator.throttler.throttle(nil)
	}
	// Deadlocks between parallel workers are expected due to InnoDB gap locks
	// on secondary indexes. Use a generous retry limit with jittered backoff
	// to handle contention between workers.
	const maxDeadlockRetries = 100
	var err error
	for attempt := 0; attempt < maxDeadlockRetries; attempt++ {
		if attempt > 0 {
			// Jittered exponential backoff: base * 2^min(attempt,7) + random jitter
			base := time.Duration(10) * time.Millisecond
			backoff := base * (1 << min(attempt, 7))
			jitter := time.Duration(rand.Int63n(int64(backoff)))
			time.Sleep(backoff + jitter)
		}
		busyStart := time.Now()
		err = w.coordinator.applier.ApplyDMLEventQueries(dmlEvents)
		w.busyTimeNs.Add(time.Since(busyStart).Nanoseconds())
		if err == nil {
			w.dmlEventsApplied.Add(int64(len(dmlEvents)))
			return nil
		}
		if !isRetryableError(err) {
			return err
		}
		if attempt > 0 && attempt%10 == 0 {
			w.coordinator.migrationContext.Log.Infof("Worker %d: DML batch retry attempt %d after deadlock", w.id, attempt)
		}
	}
	return fmt.Errorf("DML batch failed after %d deadlock retries: %w", maxDeadlockRetries, err)
}

func NewCoordinator(migrationContext *base.MigrationContext, applier *Applier, throttler *Throttler, onChangelogEvent func(dmlEvent *binlog.BinlogDMLEvent) error) *Coordinator {
	return &Coordinator{
		migrationContext: migrationContext,

		onChangelogEvent: onChangelogEvent,

		throttler: throttler,

		binlogReader: binlog.NewGoMySQLReader(migrationContext),

		lowWaterMark:  -1,
		completedJobs: make(map[int64]struct{}),
		waitingJobs:   make(map[int64][]chan struct{}),

		events:   make(chan *replication.BinlogEvent, 1000),
		failedCh: make(chan struct{}),
	}
}

func (c *Coordinator) StartStreaming(ctx context.Context, coords mysql.BinlogCoordinates, canStopStreaming func() bool) error {
	err := c.binlogReader.ConnectBinlogStreamer(coords)
	if err != nil {
		return err
	}
	defer c.binlogReader.Close()

	var retries int64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if canStopStreaming() {
			return nil
		}
		if err := c.binlogReader.StreamEvents(ctx, canStopStreaming, c.events); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}

			c.migrationContext.Log.Infof("StreamEvents encountered unexpected error: %+v", err)
			c.migrationContext.MarkPointOfInterest()

			if retries >= c.migrationContext.MaxRetries() {
				return fmt.Errorf("%d successive failures in streamer reconnect at coordinates %+v", retries, coords)
			}
			c.migrationContext.Log.Infof("Reconnecting... Will resume at %+v", coords)

			// We reconnect from the event that was last emitted to the stream.
			// This ensures we don't miss any events, and we don't process any events twice.
			// Processing events twice messes up the transaction tracking and
			// will cause data corruption.
			coords := c.binlogReader.GetCurrentBinlogCoordinates()
			if err := c.binlogReader.ConnectBinlogStreamer(coords); err != nil {
				return err
			}
			retries += 1
		}
	}
}

func (c *Coordinator) ProcessEventsUntilNextChangelogEvent() (*binlog.BinlogDMLEvent, error) {
	databaseName := c.migrationContext.DatabaseName
	changelogTableName := c.migrationContext.GetChangelogTableName()

	for ev := range c.events {
		switch binlogEvent := ev.Event.(type) {
		case *replication.RowsEvent:
			dml := binlog.ToEventDML(ev.Header.EventType.String())
			if dml == binlog.NotDML {
				return nil, fmt.Errorf("unknown DML type: %s", ev.Header.EventType.String())
			}

			if !strings.EqualFold(databaseName, string(binlogEvent.Table.Schema)) {
				continue
			}

			if !strings.EqualFold(changelogTableName, string(binlogEvent.Table.Table)) {
				continue
			}

			for i, row := range binlogEvent.Rows {
				if dml == binlog.UpdateDML && i%2 == 1 {
					// An update has two rows (WHERE+SET)
					// We do both at the same time
					continue
				}
				dmlEvent := binlog.NewBinlogDMLEvent(
					string(binlogEvent.Table.Schema),
					string(binlogEvent.Table.Table),
					dml,
				)
				switch dml {
				case binlog.InsertDML:
					{
						dmlEvent.NewColumnValues = sql.ToColumnValues(row)
					}
				case binlog.UpdateDML:
					{
						dmlEvent.WhereColumnValues = sql.ToColumnValues(row)
						dmlEvent.NewColumnValues = sql.ToColumnValues(binlogEvent.Rows[i+1])
					}
				case binlog.DeleteDML:
					{
						dmlEvent.WhereColumnValues = sql.ToColumnValues(row)
					}
				}

				return dmlEvent, nil
			}
		}
	}

	//nolint:nilnil
	return nil, nil
}

// ProcessEventsUntilDrained reads binlog events and sends them to the workers to process.
// It exits when the event queue is empty and all the workers are returned to the workerQueue.
func (c *Coordinator) ProcessEventsUntilDrained() error {
	return c.processEvents(0)
}

// ProcessEventsWithBudget processes events for at most the given duration.
// After the budget expires, it waits for in-flight workers to complete and returns.
// A zero or negative budget behaves like ProcessEventsUntilDrained (unbounded).
func (c *Coordinator) ProcessEventsWithBudget(budget time.Duration) error {
	return c.processEvents(budget)
}

// EventQueueDepth returns the current and maximum size of the event queue.
func (c *Coordinator) EventQueueDepth() (current, capacity int) {
	return len(c.events), cap(c.events)
}

// processEvents is the shared implementation for bounded and unbounded event processing.
func (c *Coordinator) processEvents(budget time.Duration) error {
	startTime := time.Now()
	budgetExceeded := func() bool {
		return budget > 0 && time.Since(startTime) >= budget
	}

	for {
		// Check for fatal worker error first
		select {
		case <-c.failedCh:
			return fmt.Errorf("worker error: %w", c.getFatalError())
		default:
		}

		// Budget exceeded: stop reading new events, wait for in-flight workers
		if budgetExceeded() {
			for c.busyWorkers.Load() > 0 {
				select {
				case <-c.failedCh:
					return fmt.Errorf("worker error: %w", c.getFatalError())
				default:
				}
				time.Sleep(time.Millisecond)
			}
			return nil
		}

		select {
		// Read events from the binlog and submit them to the next worker
		case ev := <-c.events:
			{
				if c.finishedMigrating.Load() {
					return nil
				}

				switch binlogEvent := ev.Event.(type) {
				case *replication.GTIDEvent:
					c.mu.Lock()
					if c.lowWaterMark < 0 && binlogEvent.SequenceNumber > 0 {
						c.lowWaterMark = binlogEvent.SequenceNumber - 1
					}
					c.mu.Unlock()

					// Coordinator-side dependency wait: don't schedule this
					// transaction until all its dependencies are complete.
					// This matches MySQL's replication applier coordinator
					// semantics (schedule iff lwm >= lastCommitted).
					waitChannel := c.WaitForTransaction(binlogEvent.LastCommitted)
					if waitChannel != nil {
						select {
						case <-waitChannel:
						case <-c.failedCh:
							return fmt.Errorf("worker error: %w", c.getFatalError())
						}
					}
				case *replication.RotateEvent:
					c.migrationContext.Log.Infof("rotate to next log in %s", binlogEvent.NextLogName)
					// Binlog rotation resets sequence numbers. We must
					// drain all workers (old file) and reset the lwm so
					// that dependency checking uses the new file's
					// sequence number space.
					c.mu.Lock()
					needsReset := c.lowWaterMark >= 0
					c.mu.Unlock()
					if needsReset {
						for c.busyWorkers.Load() > 0 {
							select {
							case <-c.failedCh:
								return fmt.Errorf("worker error: %w", c.getFatalError())
							default:
							}
							time.Sleep(time.Millisecond)
						}
						c.mu.Lock()
						c.lowWaterMark = -1
						c.completedJobs = make(map[int64]struct{})
						c.waitingJobs = make(map[int64][]chan struct{})
						c.mu.Unlock()
					}
					continue
				default: // ignore all other events
					continue
				}

				// Acquire a worker, interruptible by fatal error
				var worker *Worker
				select {
				case worker = <-c.workerQueue:
				case <-c.failedCh:
					return fmt.Errorf("worker error: %w", c.getFatalError())
				}
				c.busyWorkers.Add(1)

				// Send GTID to worker, interruptible
				select {
				case worker.eventQueue <- ev:
				case <-c.failedCh:
					c.busyWorkers.Add(-1)
					return fmt.Errorf("worker error: %w", c.getFatalError())
				}

				ev = <-c.events

				switch binlogEvent := ev.Event.(type) {
				case *replication.QueryEvent:
					if bytes.Equal([]byte("BEGIN"), binlogEvent.Query) {
					} else {
						worker.eventQueue <- nil
						continue
					}
				default:
					worker.eventQueue <- nil
					continue
				}

			events:
				for {
					ev = <-c.events
					switch ev.Event.(type) {
					case *replication.RowsEvent:
						select {
						case worker.eventQueue <- ev:
						case <-c.failedCh:
							return fmt.Errorf("worker error: %w", c.getFatalError())
						}
					case *replication.XIDEvent:
						select {
						case worker.eventQueue <- ev:
						case <-c.failedCh:
							return fmt.Errorf("worker error: %w", c.getFatalError())
						}

						// We're done with this transaction
						break events
					}
				}
			}

		// No events in the queue. Check if all workers are sleeping now
		default:
			{
				select {
				case <-c.failedCh:
					return fmt.Errorf("worker error: %w", c.getFatalError())
				default:
				}
				if c.busyWorkers.Load() == 0 {
					return nil
				}
				// Budget exceeded: wait for in-flight workers to finish, then return
				if budgetExceeded() {
					for c.busyWorkers.Load() > 0 {
						select {
						case <-c.failedCh:
							return fmt.Errorf("worker error: %w", c.getFatalError())
						default:
						}
						time.Sleep(time.Millisecond)
					}
					return nil
				}
			}
		}
	}
}

func (c *Coordinator) InitializeWorkers(count int) {
	c.workerQueue = make(chan *Worker, count)
	for i := 0; i < count; i++ {
		w := &Worker{id: i, coordinator: c, eventQueue: make(chan *replication.BinlogEvent, 1000)}

		c.mu.Lock()
		c.workers = append(c.workers, w)
		c.mu.Unlock()

		c.workerQueue <- w
		go func() {
			if err := w.ProcessEvents(); err != nil {
				c.migrationContext.Log.Errorf("Worker %d fatal error: %v", w.id, err)
				c.setFatalError(err)
			}
		}()
	}
}

// GetWorkerStats collects profiling stats for ProcessEvents from each worker.
func (c *Coordinator) GetWorkerStats() []stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	statSlice := make([]stats, 0, len(c.workers))
	for _, w := range c.workers {
		stat := stats{}
		stat.dmlEventsApplied = w.dmlEventsApplied.Load()
		stat.executedJobs = w.executedJobs.Load()
		stat.busyTime = time.Duration(w.busyTimeNs.Load())
		stat.waitTime = time.Duration(w.waitTimeNs.Load())
		if stat.busyTime.Milliseconds() > 0 {
			stat.dmlRate = 1000.0 * float64(stat.dmlEventsApplied) / float64(stat.busyTime.Milliseconds())
			stat.trxRate = 1000.0 * float64(stat.executedJobs) / float64(stat.busyTime.Milliseconds())
		}
		statSlice = append(statSlice, stat)
	}
	return statSlice
}

func (c *Coordinator) WaitForTransaction(lastCommitted int64) chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()

	if lastCommitted <= c.lowWaterMark {
		return nil
	}

	// Buffered so MarkTransactionCompleted never blocks if the waiter
	// already exited (e.g. via failedCh).
	waitChannel := make(chan struct{}, 1)
	c.waitingJobs[lastCommitted] = append(c.waitingJobs[lastCommitted], waitChannel)

	return waitChannel
}

func (c *Coordinator) HandleChangeLogEvent(event *binlog.BinlogDMLEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onChangelogEvent(event)
}

func (c *Coordinator) MarkTransactionCompleted(sequenceNumber, logPos, eventSize int64) {
	var channelsToNotify []chan struct{}

	func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		// Mark the job as completed
		c.completedJobs[sequenceNumber] = struct{}{}

		// Then, update the low water mark if possible
		for {
			if _, ok := c.completedJobs[c.lowWaterMark+1]; ok {
				c.lowWaterMark++
				delete(c.completedJobs, c.lowWaterMark)
			} else {
				break
			}
		}
		channelsToNotify = make([]chan struct{}, 0)

		// Schedule any jobs that were waiting for this job to complete or for the low watermark
		for waitingForSequenceNumber, channels := range c.waitingJobs {
			if waitingForSequenceNumber <= c.lowWaterMark {
				channelsToNotify = append(channelsToNotify, channels...)
				delete(c.waitingJobs, waitingForSequenceNumber)
			}
		}
	}()

	for _, waitChannel := range channelsToNotify {
		waitChannel <- struct{}{}
	}
}

func (c *Coordinator) Teardown() {
	c.finishedMigrating.Store(true)
}
