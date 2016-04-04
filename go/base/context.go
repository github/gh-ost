/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-osc/blob/master/LICENSE
*/

package base

import (
	"fmt"

	"github.com/github/gh-osc/go/mysql"
)

type RowsEstimateMethod string

const (
	TableStatusRowsEstimate RowsEstimateMethod = "TableStatusRowsEstimate"
	ExplainRowsEstimate                        = "ExplainRowsEstimate"
	CountRowsEstimate                          = "CountRowsEstimate"
)

type MigrationContext struct {
	DatabaseName              string
	OriginalTableName         string
	AlterStatement            string
	TableEngine               string
	CountTableRows            bool
	RowsEstimate              int64
	UsedRowsEstimateMethod    RowsEstimateMethod
	ChunkSize                 int
	OriginalBinlogFormat      string
	OriginalBinlogRowImage    string
	AllowedRunningOnMaster    bool
	InspectorConnectionConfig *mysql.ConnectionConfig
	MasterConnectionConfig    *mysql.ConnectionConfig
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

func GetMigrationContext() *MigrationContext {
	return context
}

func (this *MigrationContext) GetGhostTableName() string {
	return fmt.Sprintf("_%s_New", this.OriginalTableName)
}

// RequiresBinlogFormatChange
func (this *MigrationContext) RequiresBinlogFormatChange() bool {
	return this.OriginalBinlogFormat != "ROW"
}

// RequiresBinlogFormatChange
func (this *MigrationContext) IsRunningOnMaster() bool {
	return this.InspectorConnectionConfig.Equals(this.MasterConnectionConfig)
}
