package binlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRowsInRowsEvent(t *testing.T) {
	require.Equal(t, 2, RowsInRowsEvent(2, InsertDML))
	require.Equal(t, 1, RowsInRowsEvent(2, UpdateDML))
	require.Equal(t, 3, RowsInRowsEvent(3, DeleteDML))
}

func TestEventTypeTag(t *testing.T) {
	require.Equal(t, "insert", InsertDML.EventTypeTag())
	require.Equal(t, "update", UpdateDML.EventTypeTag())
	require.Equal(t, "delete", DeleteDML.EventTypeTag())
}
