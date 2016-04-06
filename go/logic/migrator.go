/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-osc/blob/master/LICENSE
*/

package logic

import (
	"fmt"

	"github.com/github/gh-osc/go/base"

	"github.com/outbrain/golib/log"
)

// Migrator is the main schema migration flow manager.
type Migrator struct {
	inspector        *Inspector
	applier          *Applier
	eventsStreamer   *EventsStreamer
	migrationContext *base.MigrationContext
}

func NewMigrator() *Migrator {
	return &Migrator{
		migrationContext: base.GetMigrationContext(),
	}
}

func (this *Migrator) Migrate() (err error) {
	this.inspector = NewInspector()
	if err := this.inspector.InitDBConnections(); err != nil {
		return err
	}
	if this.migrationContext.MasterConnectionConfig, err = this.inspector.getMasterConnectionConfig(); err != nil {
		return err
	}
	if this.migrationContext.IsRunningOnMaster() && !this.migrationContext.AllowedRunningOnMaster {
		return fmt.Errorf("It seems like this migration attempt to run directly on master. Preferably it would be executed on a replica (and this reduces load from the master). To proceed please provide --allow-on-master")
	}
	log.Infof("Master found to be %+v", this.migrationContext.MasterConnectionConfig.Key)
	uniqueKeys, err := this.inspector.InspectOriginalTable()
	if err != nil {
		return err
	}

	this.eventsStreamer = NewEventsStreamer()
	if err := this.eventsStreamer.InitDBConnections(); err != nil {
		return err
	}

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
	this.migrationContext.UniqueKey = uniqueKeys[0] // TODO. Need to wait on replica till the ghost table exists and get shared keys
	if err := this.applier.ReadMigrationRangeValues(); err != nil {
		return err
	}
	for {
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
	// if err := this.applier.IterateTable(uniqueKeys[0]); err != nil {
	// 	return err
	// }

	return nil
}
