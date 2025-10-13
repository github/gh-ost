package logic

import (
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"
	"time"
)

// Checkpoint holds state necessary to resume a migration.
type Checkpoint struct {
	Id                int64
	Timestamp         time.Time
	LastTrxCoords     mysql.BinlogCoordinates
	IterationRangeMin *sql.ColumnValues
	IterationRangeMax *sql.ColumnValues
	Iteration         int64
}
