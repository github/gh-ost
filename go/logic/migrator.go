/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-osc/blob/master/LICENSE
*/

package logic

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/github/gh-osc/go/base"
	"github.com/github/gh-osc/go/binlog"

	"github.com/outbrain/golib/log"
)

type ChangelogState string

const (
	TablesInPlace              ChangelogState = "TablesInPlace"
	AllEventsUpToLockProcessed                = "AllEventsUpToLockProcessed"
)

type tableWriteFunc func() error

const (
	applyEventsQueueBuffer = 100
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

	// copyRowsQueue should not be buffered; if buffered some non-damaging but
	//  excessive work happens at the end of the iteration as new copy-jobs arrive befroe realizing the copy is complete
	copyRowsQueue    chan tableWriteFunc
	applyEventsQueue chan tableWriteFunc
}

func NewMigrator() *Migrator {
	migrator := &Migrator{
		migrationContext:           base.GetMigrationContext(),
		tablesInPlace:              make(chan bool),
		rowCopyComplete:            make(chan bool),
		allEventsUpToLockProcessed: make(chan bool),

		copyRowsQueue:    make(chan tableWriteFunc),
		applyEventsQueue: make(chan tableWriteFunc, applyEventsQueueBuffer),
	}
	migrator.migrationContext.IsThrottled = func() bool {
		return migrator.shouldThrottle()
	}
	return migrator
}

func (this *Migrator) shouldThrottle() bool {
	lag := atomic.LoadInt64(&this.migrationContext.CurrentLag)

	shouldThrottle := false
	if time.Duration(lag) > time.Duration(this.migrationContext.MaxLagMillisecondsThrottleThreshold)*time.Millisecond {
		shouldThrottle = true
	} else if this.migrationContext.ThrottleFlagFile != "" {
		if _, err := os.Stat(this.migrationContext.ThrottleFlagFile); err == nil {
			//Throttle file defined and exists!
			shouldThrottle = true
		}
	}
	return shouldThrottle
}

func (this *Migrator) canStopStreaming() bool {
	return false
}

func (this *Migrator) onChangelogStateEvent(dmlEvent *binlog.BinlogDMLEvent) (err error) {
	// Hey, I created the changlog table, I know the type of columns it has!
	if hint := dmlEvent.NewColumnValues.StringColumn(2); hint != "state" {
		return
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
	log.Debugf("---- - - - - - state %+v", changelogState)
	return nil
}

func (this *Migrator) onChangelogHeartbeatEvent(dmlEvent *binlog.BinlogDMLEvent) (err error) {
	if hint := dmlEvent.NewColumnValues.StringColumn(2); hint != "heartbeat" {
		return nil
	}
	value := dmlEvent.NewColumnValues.StringColumn(3)
	heartbeatTime, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return log.Errore(err)
	}
	lag := time.Now().Sub(heartbeatTime)

	atomic.StoreInt64(&this.migrationContext.CurrentLag, int64(lag))

	return nil
}

func (this *Migrator) Migrate() (err error) {
	this.migrationContext.StartTime = time.Now()

	this.inspector = NewInspector()
	if err := this.inspector.InitDBConnections(); err != nil {
		return err
	}
	if err := this.inspector.ValidateOriginalTable(); err != nil {
		return err
	}
	uniqueKeys, err := this.inspector.InspectOriginalTable()
	if err != nil {
		return err
	}
	// So far so good, table is accessible and valid.
	if this.migrationContext.MasterConnectionConfig, err = this.inspector.getMasterConnectionConfig(); err != nil {
		return err
	}
	if this.migrationContext.IsRunningOnMaster() && !this.migrationContext.AllowedRunningOnMaster {
		return fmt.Errorf("It seems like this migration attempt to run directly on master. Preferably it would be executed on a replica (and this reduces load from the master). To proceed please provide --allow-on-master")
	}
	log.Infof("Master found to be %+v", this.migrationContext.MasterConnectionConfig.Key)

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

	this.migrationContext.UniqueKey = uniqueKeys[0] // TODO. Need to wait on replica till the ghost table exists and get shared keys
	if err := this.applier.ReadMigrationRangeValues(); err != nil {
		return err
	}
	go this.initiateStatus()
	go this.executeWriteFuncs()
	go this.iterateChunks()

	log.Debugf("Operating until row copy is complete")
	<-this.rowCopyComplete
	log.Debugf("Row copy complete")
	this.printStatus()

	throttleMigration(
		this.migrationContext,
		func() {
			log.Debugf("throttling before LOCK TABLES")
		},
		nil,
		func() {
			log.Debugf("done throttling")
		},
	)
	// TODO retries!!
	this.applier.LockTables()
	this.applier.WriteChangelog("state", string(AllEventsUpToLockProcessed))
	log.Debugf("Waiting for events up to lock")
	<-this.allEventsUpToLockProcessed
	log.Debugf("Done waiting for events up to lock")
	// TODO retries!!
	this.applier.UnlockTables()

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
	rowsEstimate := this.migrationContext.RowsEstimate
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

	status := fmt.Sprintf("Copy: %d/%d %.1f%% Backlog: %d/%d Elapsed: %+v(copy), %+v(total) ETA: N/A",
		totalRowsCopied, rowsEstimate, progressPct,
		len(this.applyEventsQueue), cap(this.applyEventsQueue),
		this.migrationContext.ElapsedRowCopyTime(), elapsedTime)
	fmt.Println(status)
}

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
		false,
		this.migrationContext.DatabaseName,
		this.migrationContext.GetChangelogTableName(),
		func(dmlEvent *binlog.BinlogDMLEvent) error {
			return this.onChangelogHeartbeatEvent(dmlEvent)
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

	this.applier.WriteChangelog("state", string(TablesInPlace))
	this.applier.InitiateHeartbeat()
	return nil
}

func (this *Migrator) iterateChunks() error {
	this.migrationContext.RowCopyStartTime = time.Now()
	terminateRowIteration := func(err error) error {
		this.rowCopyComplete <- true
		return log.Errore(err)
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
			this.migrationContext.Iteration++
			return nil
		}
		this.copyRowsQueue <- copyRowsFunc
	}
	return nil
}

func (this *Migrator) executeWriteFuncs() error {
	for {
		throttleMigration(
			this.migrationContext,
			func() {
				log.Debugf("throttling writes")
			},
			nil,
			func() {
				log.Debugf("done throttling writes")
			},
		)
		// We give higher priority to event processing, then secondary priority to
		// rowcopy
		select {
		case applyEventFunc := <-this.applyEventsQueue:
			{
				retryOperation(applyEventFunc, this.migrationContext.MaxRetries())
			}
		default:
			{
				select {
				case copyRowsFunc := <-this.copyRowsQueue:
					{
						retryOperation(copyRowsFunc, this.migrationContext.MaxRetries())
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
