/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-osc/blob/master/LICENSE
*/

package base

import (
	"fmt"

	"github.com/github/gh-osc/go/mysql"
	"github.com/github/gh-osc/go/sql"
)

// RowsEstimateMethod is the type of row number estimation
type RowsEstimateMethod string

const (
	TableStatusRowsEstimate RowsEstimateMethod = "TableStatusRowsEstimate"
	ExplainRowsEstimate                        = "ExplainRowsEstimate"
	CountRowsEstimate                          = "CountRowsEstimate"
)

// MigrationContext has the general, global state of migration. It is used by
// all components throughout the migration process.
type MigrationContext struct {
	DatabaseName                     string
	OriginalTableName                string
	AlterStatement                   string
	TableEngine                      string
	CountTableRows                   bool
	RowsEstimate                     int64
	UsedRowsEstimateMethod           RowsEstimateMethod
	ChunkSize                        int
	OriginalBinlogFormat             string
	OriginalBinlogRowImage           string
	AllowedRunningOnMaster           bool
	InspectorConnectionConfig        *mysql.ConnectionConfig
	MasterConnectionConfig           *mysql.ConnectionConfig
	MigrationRangeMinValues          *sql.ColumnValues
	MigrationRangeMaxValues          *sql.ColumnValues
	Iteration                        int64
	MigrationIterationRangeMinValues *sql.ColumnValues
	MigrationIterationRangeMaxValues *sql.ColumnValues
	UniqueKey                        *sql.UniqueKey
}

var context *MigrationContext

func init() {
	context = newMigrationContext()
}

func newMigrationContext() *MigrationContext {
	return &MigrationContext{
		ChunkSize:                 1000,
		InspectorConnectionConfig: mysql.NewConnectionConfig(),
		MasterConnectionConfig:    mysql.NewConnectionConfig(),
	}
}

// GetMigrationContext
func GetMigrationContext() *MigrationContext {
	return context
}

// GetGhostTableName generates the name of ghost table, based on original table name
func (this *MigrationContext) GetGhostTableName() string {
	return fmt.Sprintf("_%s_New", this.OriginalTableName)
}

// RequiresBinlogFormatChange is `true` when the original binlog format isn't `ROW`
func (this *MigrationContext) RequiresBinlogFormatChange() bool {
	return this.OriginalBinlogFormat != "ROW"
}

// IsRunningOnMaster is `true` when the app connects directly to the master (typically
// it should be executed on replica and infer the master)
func (this *MigrationContext) IsRunningOnMaster() bool {
	return this.InspectorConnectionConfig.Equals(this.MasterConnectionConfig)
}

// HasMigrationRange tells us whether there's a range to iterate for copying rows.
// It will be `false` if the table is initially empty
func (this *MigrationContext) HasMigrationRange() bool {
	return this.MigrationRangeMinValues != nil && this.MigrationRangeMaxValues != nil
}
