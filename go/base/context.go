/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-osc/blob/master/LICENSE
*/

package base

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

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

const (
	maxRetries = 10
)

// MigrationContext has the general, global state of migration. It is used by
// all components throughout the migration process.
type MigrationContext struct {
	DatabaseName                        string
	OriginalTableName                   string
	AlterStatement                      string
	TableEngine                         string
	CountTableRows                      bool
	RowsEstimate                        int64
	UsedRowsEstimateMethod              RowsEstimateMethod
	ChunkSize                           int64
	OriginalBinlogFormat                string
	OriginalBinlogRowImage              string
	AllowedRunningOnMaster              bool
	InspectorConnectionConfig           *mysql.ConnectionConfig
	MasterConnectionConfig              *mysql.ConnectionConfig
	MigrationRangeMinValues             *sql.ColumnValues
	MigrationRangeMaxValues             *sql.ColumnValues
	Iteration                           int64
	MigrationIterationRangeMinValues    *sql.ColumnValues
	MigrationIterationRangeMaxValues    *sql.ColumnValues
	UniqueKey                           *sql.UniqueKey
	StartTime                           time.Time
	RowCopyStartTime                    time.Time
	CurrentLag                          int64
	MaxLagMillisecondsThrottleThreshold int64
	ThrottleFlagFile                    string
	TotalRowsCopied                     int64

	IsThrottled      func() bool
	CanStopStreaming func() bool
}

var context *MigrationContext

func init() {
	context = newMigrationContext()
}

func newMigrationContext() *MigrationContext {
	return &MigrationContext{
		ChunkSize:                           1000,
		InspectorConnectionConfig:           mysql.NewConnectionConfig(),
		MasterConnectionConfig:              mysql.NewConnectionConfig(),
		MaxLagMillisecondsThrottleThreshold: 1000,
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

// GetChangelogTableName generates the name of changelog table, based on original table name
func (this *MigrationContext) GetChangelogTableName() string {
	return fmt.Sprintf("_%s_OSC", this.OriginalTableName)
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

func (this *MigrationContext) MaxRetries() int {
	return maxRetries
}

func (this *MigrationContext) IsTransactionalTable() bool {
	switch strings.ToLower(this.TableEngine) {
	case "innodb":
		{
			return true
		}
	case "tokudb":
		{
			return true
		}
	}
	return false
}

// ElapsedTime returns time since very beginning of the process
func (this *MigrationContext) ElapsedTime() time.Duration {
	return time.Now().Sub(this.StartTime)
}

// ElapsedRowCopyTime returns time since starting to copy chunks of rows
func (this *MigrationContext) ElapsedRowCopyTime() time.Duration {
	return time.Now().Sub(this.RowCopyStartTime)
}

// GetTotalRowsCopied returns the accurate number of rows being copied (affected)
// This is not exactly the same as the rows being iterated via chunks, but potentially close enough
func (this *MigrationContext) GetTotalRowsCopied() int64 {
	return atomic.LoadInt64(&this.TotalRowsCopied)
}
