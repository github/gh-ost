package canal

import (
	"fmt"

	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/schema"
)

// The action name for sync.
const (
	UpdateAction = "update"
	InsertAction = "insert"
	DeleteAction = "delete"
)

// RowsEvent is the event for row replication.
type RowsEvent struct {
	Table  *schema.Table
	Action string
	// changed row list
	// binlog has three update event version, v0, v1 and v2.
	// for v1 and v2, the rows number must be even.
	// Two rows for one event, format is [before update row, after update row]
	// for update v0, only one row for a event, and we don't support this version.
	Rows [][]interface{}
	// Header can be used to inspect the event
	Header *replication.EventHeader
}

func newRowsEvent(table *schema.Table, action string, rows [][]interface{}, header *replication.EventHeader) *RowsEvent {
	e := new(RowsEvent)

	e.Table = table
	e.Action = action
	e.Rows = rows
	e.Header = header

	e.handleUnsigned()

	return e
}

func (r *RowsEvent) handleUnsigned() {
	// Handle Unsigned Columns here, for binlog replication, we can't know the integer is unsigned or not,
	// so we use int type but this may cause overflow outside sometimes, so we must convert to the really .
	// unsigned type
	if len(r.Table.UnsignedColumns) == 0 {
		return
	}

	for i := 0; i < len(r.Rows); i++ {
		for _, index := range r.Table.UnsignedColumns {
			switch t := r.Rows[i][index].(type) {
			case int8:
				r.Rows[i][index] = uint8(t)
			case int16:
				r.Rows[i][index] = uint16(t)
			case int32:
				r.Rows[i][index] = uint32(t)
			case int64:
				r.Rows[i][index] = uint64(t)
			case int:
				r.Rows[i][index] = uint(t)
			default:
				// nothing to do
			}
		}
	}
}

// String implements fmt.Stringer interface.
func (r *RowsEvent) String() string {
	return fmt.Sprintf("%s %s %v", r.Action, r.Table, r.Rows)
}
