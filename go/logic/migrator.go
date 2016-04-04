/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-osc/blob/master/LICENSE
*/

package logic

import (
	"github.com/github/gh-osc/go/mysql"
)

// Migrator is the main schema migration flow manager.
type Migrator struct {
	connectionConfig *mysql.ConnectionConfig
	inspector        *Inspector
}

func NewMigrator(connectionConfig *mysql.ConnectionConfig) *Migrator {
	return &Migrator{
		connectionConfig: connectionConfig,
		inspector:        NewInspector(connectionConfig),
	}
}

func (this *Migrator) Migrate() error {
	if err := this.inspector.InitDBConnections(); err != nil {
		return err
	}
	if err := this.inspector.InspectTables(); err != nil {
		return err
	}
	return nil
}
