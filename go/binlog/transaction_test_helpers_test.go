package binlog

import (
	"sync"
	"testing"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"
)

func transactionTestReader(useGTIDs bool) *GoMySQLReader {
	ctx := base.NewMigrationContext()
	ctx.UseGTIDs = useGTIDs
	ctx.DatabaseName = "testdb"
	ctx.OriginalTableName = "testing"
	var coords mysql.BinlogCoordinates = &mysql.FileBinlogCoordinates{LogFile: "mysql-bin.000001", LogPos: 4}
	if useGTIDs {
		coords = &mysql.GTIDBinlogCoordinates{}
	}
	return &GoMySQLReader{
		migrationContext:        ctx,
		binlogStreamer:          replication.NewBinlogStreamer(),
		currentCoordinates:      coords,
		currentCoordinatesMutex: &sync.Mutex{},
	}
}

func transactionTestStream(t *testing.T, reader *GoMySQLReader, events ...*replication.BinlogEvent) []*BinlogEntry {
	t.Helper()
	for _, event := range events {
		require.NoError(t, reader.binlogStreamer.AddEventToStreamer(event))
	}
	entries := make(chan *BinlogEntry, 32)
	iterations := 0
	require.NoError(t, reader.StreamEvents(func() bool {
		iterations++
		return iterations > len(events)
	}, entries))
	close(entries)
	var result []*BinlogEntry
	for entry := range entries {
		result = append(result, entry)
	}
	return result
}
