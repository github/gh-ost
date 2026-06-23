/*
   Copyright 2025 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"
)

// Checkpoint holds state necessary to resume a migration.
type Checkpoint struct {
	Id        int64
	Timestamp time.Time
	// TableName is the migrated table this checkpoint row belongs to. Empty in
	// standard (single-table) mode; set per table in move-tables mode, where the
	// checkpoint table holds one row per migrated table.
	TableName string
	// LastTrxCoords are coordinates of a transaction
	// that has been applied on ghost table.
	LastTrxCoords mysql.BinlogCoordinates
	// IterationRangeMin is the min shared key value
	// for the chunk copier range.
	IterationRangeMin *sql.ColumnValues
	// IterationRangeMax is the max shared key value
	// for the chunk copier range.
	IterationRangeMax          *sql.ColumnValues
	Iteration                  int64
	RowsCopied                 int64
	DMLApplied                 int64
	IsCutover                  bool
	MoveTablesCutOverStarted   bool
	MoveTablesCutOverDrainGTID mysql.BinlogCoordinates
}

// moveTableCheckpointNullToken marks a NULL value in a serialized range. Hex
// encoding never produces "~", so it is unambiguous.
const moveTableCheckpointNullToken = "~"

// serializeRangeValues encodes a unique-key range (one or more column values)
// into a portable, table-agnostic text form: each value hex-encoded, comma-
// joined. This lets the single move-tables checkpoint table store ranges for
// tables with heterogeneous unique keys without per-key typed columns.
func serializeRangeValues(cv *sql.ColumnValues) string {
	if cv == nil {
		return ""
	}
	vals := cv.AbstractValues()
	parts := make([]string, len(vals))
	for i, v := range vals {
		if v == nil {
			parts[i] = moveTableCheckpointNullToken
			continue
		}
		var b []byte
		switch t := v.(type) {
		case []byte:
			b = t
		case string:
			b = []byte(t)
		default:
			b = []byte(fmt.Sprintf("%v", t))
		}
		parts[i] = hex.EncodeToString(b)
	}
	return strings.Join(parts, ",")
}

// deserializeRangeValues reverses serializeRangeValues for a key of arity n. The
// values come back as []byte (or nil), which are accepted as prepared-statement
// args and coerced by MySQL to the target column type for comparison.
func deserializeRangeValues(s string, n int) *sql.ColumnValues {
	abstract := make([]interface{}, n)
	if s != "" {
		parts := strings.Split(s, ",")
		for i := 0; i < n && i < len(parts); i++ {
			p := parts[i]
			if p == "" || p == moveTableCheckpointNullToken {
				continue // leave nil
			}
			if b, err := hex.DecodeString(p); err == nil {
				abstract[i] = b
			}
		}
	}
	return sql.ToColumnValues(abstract)
}

// isEmptyRange reports whether a deserialized range carries no usable boundary
// (zero columns, or every column value nil). Such a range means the table had no
// completed chunk when the checkpoint was written, so on resume it must start
// from the table minimum rather than from this empty boundary.
func isEmptyRange(cv *sql.ColumnValues) bool {
	if cv == nil {
		return true
	}
	vals := cv.AbstractValues()
	if len(vals) == 0 {
		return true
	}
	for _, v := range vals {
		if v != nil {
			return false
		}
	}
	return true
}
