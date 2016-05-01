/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-osc/blob/master/LICENSE
*/

package base

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
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
	DatabaseName      string
	OriginalTableName string
	AlterStatement    string

	CountTableRows          bool
	AllowedRunningOnMaster  bool
	SwitchToRowBinlogFormat bool

	ChunkSize                           int64
	MaxLagMillisecondsThrottleThreshold int64
	ThrottleFlagFile                    string
	ThrottleAdditionalFlagFile          string
	MaxLoad                             map[string]int64
	SwapTablesTimeoutSeconds            int64

	Noop                    bool
	TestOnReplica           bool
	OkToDropTable           bool
	QuickAndBumpySwapTables bool

	TableEngine               string
	RowsEstimate              int64
	UsedRowsEstimateMethod    RowsEstimateMethod
	OriginalBinlogFormat      string
	OriginalBinlogRowImage    string
	InspectorConnectionConfig *mysql.ConnectionConfig
	ApplierConnectionConfig   *mysql.ConnectionConfig
	StartTime                 time.Time
	RowCopyStartTime          time.Time
	LockTablesStartTime       time.Time
	RenameTablesStartTime     time.Time
	RenameTablesEndTime       time.Time
	CurrentLag                int64
	TotalRowsCopied           int64
	TotalDMLEventsApplied     int64
	isThrottled               bool
	throttleReason            string
	throttleMutex             *sync.Mutex

	OriginalTableColumns             *sql.ColumnList
	OriginalTableUniqueKeys          [](*sql.UniqueKey)
	GhostTableColumns                *sql.ColumnList
	GhostTableUniqueKeys             [](*sql.UniqueKey)
	UniqueKey                        *sql.UniqueKey
	SharedColumns                    *sql.ColumnList
	MigrationRangeMinValues          *sql.ColumnValues
	MigrationRangeMaxValues          *sql.ColumnValues
	Iteration                        int64
	MigrationIterationRangeMinValues *sql.ColumnValues
	MigrationIterationRangeMaxValues *sql.ColumnValues

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
		ApplierConnectionConfig:             mysql.NewConnectionConfig(),
		MaxLagMillisecondsThrottleThreshold: 1000,
		SwapTablesTimeoutSeconds:            3,
		MaxLoad:                             make(map[string]int64),
		throttleMutex:                       &sync.Mutex{},
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

// GetOldTableName generates the name of the "old" table, into which the original table is renamed.
func (this *MigrationContext) GetOldTableName() string {
	return fmt.Sprintf("_%s_Old", this.OriginalTableName)
}

// GetChangelogTableName generates the name of changelog table, based on original table name
func (this *MigrationContext) GetChangelogTableName() string {
	return fmt.Sprintf("_%s_OSC", this.OriginalTableName)
}

// GetVoluntaryLockName returns a name of a voluntary lock to be used throughout
// the swap-tables process.
func (this *MigrationContext) GetVoluntaryLockName() string {
	return fmt.Sprintf("%s.%s.lock", this.DatabaseName, this.OriginalTableName)
}

// RequiresBinlogFormatChange is `true` when the original binlog format isn't `ROW`
func (this *MigrationContext) RequiresBinlogFormatChange() bool {
	return this.OriginalBinlogFormat != "ROW"
}

// InspectorIsAlsoApplier is `true` when the both inspector and applier are the
// same database instance. This would be true when running directly on master or when
// testing on replica.
func (this *MigrationContext) InspectorIsAlsoApplier() bool {
	return this.InspectorConnectionConfig.Equals(this.ApplierConnectionConfig)
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

func (this *MigrationContext) GetIteration() int64 {
	return atomic.LoadInt64(&this.Iteration)
}

func (this *MigrationContext) SetThrottled(throttle bool, reason string) {
	this.throttleMutex.Lock()
	defer func() { this.throttleMutex.Unlock() }()
	this.isThrottled = throttle
	this.throttleReason = reason
}

func (this *MigrationContext) IsThrottled() (bool, string) {
	this.throttleMutex.Lock()
	defer func() { this.throttleMutex.Unlock() }()
	return this.isThrottled, this.throttleReason
}

func (this *MigrationContext) ReadMaxLoad(maxLoadList string) error {
	if maxLoadList == "" {
		return nil
	}
	maxLoadConditions := strings.Split(maxLoadList, ",")
	for _, maxLoadCondition := range maxLoadConditions {
		maxLoadTokens := strings.Split(maxLoadCondition, "=")
		if len(maxLoadTokens) != 2 {
			return fmt.Errorf("Error parsing max-load condition: %s", maxLoadCondition)
		}
		if maxLoadTokens[0] == "" {
			return fmt.Errorf("Error parsing status variable in max-load condition: %s", maxLoadCondition)
		}
		if n, err := strconv.ParseInt(maxLoadTokens[1], 10, 0); err != nil {
			return fmt.Errorf("Error parsing numeric value in max-load condition: %s", maxLoadCondition)
		} else {
			this.MaxLoad[maxLoadTokens[0]] = n
		}
	}
	return nil
}
