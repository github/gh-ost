package logic

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/binlog"
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"
	"github.com/go-mysql-org/go-mysql/replication"
)

type Coordinator struct {
	migrationContext *base.MigrationContext

	binlogSyncer            *replication.BinlogSyncer
	currentCoordinates      mysql.BinlogCoordinates
	currentCoordinatesMutex *sync.Mutex

	onChangelogEvent func(dmlEvent *binlog.BinlogDMLEvent) error

	applier *Applier

	// Atomic counter for number of active workers
	busyWorkers atomic.Int64

	// Mutex to protect the fields below
	mu sync.Mutex

	// The low water mark. This is the sequence number of the last job that has been committed.
	lowWaterMark int64

	// This is a map of completed jobs by their sequence numbers.
	// This is used when updating the low water mark.
	completedJobs map[int64]bool

	// These are the jobs that are waiting for a previous job to complete.
	// They are indexed by the sequence number of the job they are waiting for.
	waitingJobs map[int64][]chan struct{}

	events chan *replication.BinlogEvent

	workerQueue chan *Worker
}

type Worker struct {
	id          int
	coordinator *Coordinator

	executedJobs int

	eventQueue chan *replication.BinlogEvent
}

func (w *Worker) ProcessEvents() error {
	databaseName := w.coordinator.migrationContext.DatabaseName
	originalTableName := w.coordinator.migrationContext.OriginalTableName
	changelogTableName := w.coordinator.migrationContext.GetChangelogTableName()

	for {
		ev := <-w.eventQueue
		// fmt.Printf("Worker %d processing event: %T\n", w.id, ev.Event)

		// Verify this is a GTID Event
		gtidEvent, ok := ev.Event.(*replication.GTIDEvent)
		if !ok {
			fmt.Printf("Received unexpected event: %v\n", ev)
		}

		// Wait for conditions to be met
		waitChannel := w.coordinator.WaitForTransaction(gtidEvent.LastCommitted)
		if waitChannel != nil {
			//fmt.Printf("Worker %d - transaction %d waiting for transaction: %d\n", w.id, gtidEvent.SequenceNumber, gtidEvent.LastCommitted)
			t := time.Now()
			<-waitChannel
			timeWaited := time.Since(t)
			fmt.Printf("Worker %d waited for transaction %d for: %d\n", w.id, gtidEvent.LastCommitted, timeWaited)
		}

		// Process the transaction

		var changelogEvent *binlog.BinlogDMLEvent

		dmlEvents := make([]*binlog.BinlogDMLEvent, 0, int(atomic.LoadInt64(&w.coordinator.migrationContext.DMLBatchSize)))

	events:
		for {
			ev := <-w.eventQueue
			if ev == nil {
				fmt.Printf("Worker %d ending transaction early\n", w.id)
				break events
			}

			// fmt.Printf("Worker %d processing event: %T\n", w.id, ev.Event)

			switch binlogEvent := ev.Event.(type) {
			case *replication.RowsEvent:
				// Is this an event that we're interested in?
				// We're only interested in events that:
				// * affect the table we're migrating
				// * affect the changelog table

				dml := binlog.ToEventDML(ev.Header.EventType.String())
				if dml == binlog.NotDML {
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
						// If this is a change on the changelog table, queue it up to be processed after
						// the end of the transaction.
						changelogEvent = dmlEvent
					} else {
						dmlEvents = append(dmlEvents, dmlEvent)

						if len(dmlEvents) == cap(dmlEvents) {
							w.coordinator.applier.ApplyDMLEventQueries(dmlEvents)
							dmlEvents = dmlEvents[:0]
						}
					}
				}
			case *replication.XIDEvent:
				if len(dmlEvents) > 0 {
					w.coordinator.applier.ApplyDMLEventQueries(dmlEvents)
				}

				break events
			}
		}

		w.coordinator.MarkTransactionCompleted(gtidEvent.SequenceNumber)

		// Did we see a changelog event?
		// Handle it now
		if changelogEvent != nil {
			waitChannel = w.coordinator.WaitForTransaction(gtidEvent.SequenceNumber - 1)
			if waitChannel != nil {
				<-waitChannel
			}
			w.coordinator.HandleChangeLogEvent(changelogEvent)
		}

		w.coordinator.workerQueue <- w
		w.coordinator.busyWorkers.Add(-1)
	}
}

func NewCoordinator(migrationContext *base.MigrationContext, applier *Applier, onChangelogEvent func(dmlEvent *binlog.BinlogDMLEvent) error) *Coordinator {
	connectionConfig := migrationContext.InspectorConnectionConfig

	return &Coordinator{
		migrationContext: migrationContext,

		onChangelogEvent: onChangelogEvent,

		currentCoordinates:      mysql.BinlogCoordinates{},
		currentCoordinatesMutex: &sync.Mutex{},

		binlogSyncer: replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
			ServerID:                uint32(migrationContext.ReplicaServerId),
			Flavor:                  gomysql.MySQLFlavor,
			Host:                    connectionConfig.Key.Hostname,
			Port:                    uint16(connectionConfig.Key.Port),
			User:                    connectionConfig.User,
			Password:                connectionConfig.Password,
			TLSConfig:               connectionConfig.TLSConfig(),
			UseDecimal:              true,
			MaxReconnectAttempts:    migrationContext.BinlogSyncerMaxReconnectAttempts,
			TimestampStringLocation: time.UTC,
		}),

		lowWaterMark:  0,
		completedJobs: make(map[int64]bool),
		waitingJobs:   make(map[int64][]chan struct{}),

		events: make(chan *replication.BinlogEvent, 1000),

		workerQueue: make(chan *Worker, 16),
	}
}

func (c *Coordinator) StartStreaming() error {
	ctx := context.TODO()

	streamer, err := c.binlogSyncer.StartSync(gomysql.Position{
		Name: c.currentCoordinates.LogFile,
		Pos:  uint32(c.currentCoordinates.LogPos),
	})

	if err != nil {
		return err
	}

	for {
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			return err
		}

		c.events <- ev
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

	return nil, nil
}

func (c *Coordinator) ProcessEventsUntilDrained() error {
	for {
		select {
		// Read events from the binlog and submit them to the next worker
		case ev := <-c.events:
			{
				//				c.migrationContext.Log.Infof("Received event: %T - %+v", ev.Event, ev.Event)

				switch binlogEvent := ev.Event.(type) {
				case *replication.GTIDEvent:
					if c.lowWaterMark == 0 && binlogEvent.SequenceNumber > 0 {
						c.lowWaterMark = binlogEvent.SequenceNumber - 1
					}
				default: // ignore all other events
					continue
				}

				worker := <-c.workerQueue
				c.busyWorkers.Add(1)

				// c.migrationContext.Log.Infof("Submitting job %d to worker", ev.Event.(*replication.GTIDEvent).SequenceNumber)
				worker.eventQueue <- ev

				ev = <-c.events

				// c.migrationContext.Log.Infof("Received event: %T - %+v", ev.Event, ev.Event)

				switch binlogEvent := ev.Event.(type) {
				case *replication.QueryEvent:
					if bytes.Equal([]byte("BEGIN"), binlogEvent.Query) {
						// c.migrationContext.Log.Infof("BEGIN for transaction in schema %s", binlogEvent.Schema)
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

					// c.migrationContext.Log.Infof("Received event: %T - %+v", ev.Event, ev.Event)

					switch ev.Event.(type) {
					case *replication.RowsEvent:
						worker.eventQueue <- ev
					case *replication.XIDEvent:
						worker.eventQueue <- ev

						// We're done with this transaction
						break events
					}
				}
			}

		// No events in the queue. Check if all workers are sleeping now
		default:
			c.migrationContext.Log.Info("No events in the queue")
			{
				busyWorkers := c.busyWorkers.Load()
				if busyWorkers == 0 {
					//c.migrationContext.Log.Info("All workers are sleeping")
					// All workers are sleeping. We're done.
					return nil
				} else {
					//c.migrationContext.Log.Infof("%d/%d workers are busy\n", busyWorkers, cap(c.workerQueue))
				}
			}
		}
	}
}

func (c *Coordinator) InitializeWorkers(count int) {
	c.workerQueue = make(chan *Worker, count)
	for i := 0; i < count; i++ {
		w := &Worker{id: i, coordinator: c, eventQueue: make(chan *replication.BinlogEvent, 1000)}
		c.workerQueue <- w
		go w.ProcessEvents()
	}
}

func (c *Coordinator) WaitForTransaction(lastCommitted int64) chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()

	if lastCommitted <= c.lowWaterMark {
		return nil
	}

	if c.completedJobs[lastCommitted] {
		return nil
	}

	waitChannel := make(chan struct{})
	c.waitingJobs[lastCommitted] = append(c.waitingJobs[lastCommitted], waitChannel)

	return waitChannel
}

func (c *Coordinator) HandleChangeLogEvent(event *binlog.BinlogDMLEvent) {
	fmt.Printf("Coordinator: Handling changelog event: %+v\n", event)

	c.mu.Lock()
	defer c.mu.Unlock()

	c.onChangelogEvent(event)
}

func (c *Coordinator) MarkTransactionCompleted(sequenceNumber int64) {
	var channelsToNotify []chan struct{}

	func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		c.migrationContext.Log.Infof("Coordinator: Marking job as completed: %d\n", sequenceNumber)

		// Mark the job as completed
		c.completedJobs[sequenceNumber] = true

		// Then, update the low water mark if possible
		for {
			if c.completedJobs[c.lowWaterMark+1] {
				c.lowWaterMark++

				// TODO: Remember the applied binlog coordinates
				delete(c.completedJobs, c.lowWaterMark)
			} else {
				break
			}
		}

		channelsToNotify = make([]chan struct{}, 0)

		// Schedule any jobs that were waiting for this job to complete
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
