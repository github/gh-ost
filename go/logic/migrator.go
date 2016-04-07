/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-osc/blob/master/LICENSE
*/

package logic

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/github/gh-osc/go/base"
	"github.com/github/gh-osc/go/binlog"

	"github.com/outbrain/golib/log"
)

// Migrator is the main schema migration flow manager.
type Migrator struct {
	inspector        *Inspector
	applier          *Applier
	eventsStreamer   *EventsStreamer
	migrationContext *base.MigrationContext

	tablesInPlace chan bool
}

func NewMigrator() *Migrator {
	migrator := &Migrator{
		migrationContext: base.GetMigrationContext(),
		tablesInPlace:    make(chan bool),
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
	log.Debugf("---- - - - - - lag %+v", lag)

	return nil
}

func (this *Migrator) Migrate() (err error) {
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

	<-this.tablesInPlace
	// Yay! We now know the Ghost and Changelog tables are good to examine!
	// When running on replica, this means the replica has those tables. When running
	// on master this is always true, of course, and yet it also implies this knowledge
	// is in the binlogs.

	this.migrationContext.UniqueKey = uniqueKeys[0] // TODO. Need to wait on replica till the ghost table exists and get shared keys
	if err := this.applier.ReadMigrationRangeValues(); err != nil {
		return err
	}
	for {
		throttleMigration(
			this.migrationContext,
			func() {
				log.Debugf("throttling rowcopy")
			},
			nil,
			func() {
				log.Debugf("done throttling rowcopy")
			},
		)
		isComplete, err := this.applier.IterationIsComplete()
		if err != nil {
			return err
		}
		if isComplete {
			break
		}
		if err = this.applier.CalculateNextIterationRangeEndValues(); err != nil {
			return err
		}
		if err = this.applier.ApplyIterationInsertQuery(); err != nil {
			return err
		}
		this.migrationContext.Iteration++
	}
	// temporary wait:
	heartbeatTick := time.Tick(10 * time.Second)
	for range heartbeatTick {
		return nil
	}

	return nil
}
