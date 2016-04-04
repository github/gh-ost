/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-osc/blob/master/LICENSE
*/

package base

import ()

type RowsEstimateMethod string

const (
	TableStatusRowsEstimate RowsEstimateMethod = "TableStatusRowsEstimate"
	ExplainRowsEstimate                        = "ExplainRowsEstimate"
	CountRowsEstimate                          = "CountRowsEstimate"
)

type MigrationContext struct {
	DatabaseName           string
	OriginalTableName      string
	GhostTableName         string
	AlterStatement         string
	TableEngine            string
	CountTableRows         bool
	RowsEstimate           int64
	UsedRowsEstimateMethod RowsEstimateMethod
	ChunkSize              int
	OriginalBinlogFormat   string
	OriginalBinlogRowImage string
}

var context *MigrationContext

func init() {
	context = newMigrationContext()
}

func newMigrationContext() *MigrationContext {
	return &MigrationContext{
		ChunkSize: 1000,
	}
}

func GetMigrationContext() *MigrationContext {
	return context
}

// RequiresBinlogFormatChange
func (this *MigrationContext) RequiresBinlogFormatChange() bool {
	return this.OriginalBinlogFormat != "ROW"
}
