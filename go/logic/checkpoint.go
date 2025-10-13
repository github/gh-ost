/*
   Copyright 2025 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"time"

	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"
)

// Checkpoint holds state necessary to resume a migration.
type Checkpoint struct {
	Id        int64
	Timestamp time.Time
	// LastTrxCoords are coordinates of a transaction
	// that has been applied on ghost table.
	LastTrxCoords mysql.BinlogCoordinates
	// IterationRangeMin is the min shared key value
	// for the chunk copier range.
	IterationRangeMin *sql.ColumnValues
	// IterationRangeMax is the max shared key value
	// for the chunk copier range.
	IterationRangeMax *sql.ColumnValues
	Iteration         int64
}
