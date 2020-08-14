/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package base

import (
	"fmt"

	"github.com/github/gh-ost/go/sql"
)

type ChecksumFunc func() (checksum string, err error)

// BinlogCoordinates described binary log coordinates in the form of log file & log position.
type ChecksumComparison struct {
	Iteration                        int64
	OriginalTableChecksumFunc        ChecksumFunc
	GhostTableChecksumFunc           ChecksumFunc
	MigrationIterationRangeMinValues *sql.ColumnValues
	MigrationIterationRangeMaxValues *sql.ColumnValues
	Attempts                         int
}

func NewChecksumComparison(
	iteration int64,
	originalTableChecksumFunc, ghostTableChecksumFunc ChecksumFunc,
	rangeMinValues, rangeMaxValues *sql.ColumnValues,
) *ChecksumComparison {
	return &ChecksumComparison{
		Iteration:                        iteration,
		OriginalTableChecksumFunc:        originalTableChecksumFunc,
		GhostTableChecksumFunc:           ghostTableChecksumFunc,
		MigrationIterationRangeMinValues: rangeMinValues,
		MigrationIterationRangeMaxValues: rangeMaxValues,
		Attempts:                         0,
	}
}

func (this *ChecksumComparison) IncrementAttempts() {
	this.Attempts = this.Attempts + 1
}

func (this *ChecksumComparison) String() string {
	return fmt.Sprintf("iteration: %d, range: [%s]..[%s], attempts: %d",
		this.Iteration, this.MigrationIterationRangeMinValues, this.MigrationIterationRangeMaxValues, this.Attempts,
	)
}
