/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package binlog

import (
	"fmt"
	"sync"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"

	"time"

	"context"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	uuid "github.com/google/uuid"
)

type GoMySQLReader struct {
	migrationContext        *base.MigrationContext
	connectionConfig        *mysql.ConnectionConfig
	binlogSyncer            *replication.BinlogSyncer
	binlogStreamer          *replication.BinlogStreamer
	currentCoordinates      mysql.BinlogCoordinates
	currentCoordinatesMutex *sync.Mutex
	// LastTrxCoords are the coordinates of the last transaction completely read.
	// If using the file coordinates it is binlog position of the transaction's XID event.
	LastTrxCoords mysql.BinlogCoordinates
}

func NewGoMySQLReader(migrationContext *base.MigrationContext) *GoMySQLReader {
	connectionConfig := migrationContext.InspectorConnectionConfig
	return &GoMySQLReader{
		migrationContext:        migrationContext,
		connectionConfig:        connectionConfig,
		currentCoordinatesMutex: &sync.Mutex{},
		binlogSyncer: replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
			ServerID:                uint32(migrationContext.ReplicaServerId),
			Flavor:                  gomysql.MySQLFlavor,
			Host:                    connectionConfig.Key.Hostname,
			Port:                    uint16(connectionConfig.Key.Port),
			User:                    connectionConfig.User,
			Password:                connectionConfig.Password,
			TLSConfig:               connectionConfig.TLSConfig(),
			UseDecimal:              true,
			TimestampStringLocation: time.UTC,
			MaxReconnectAttempts:    migrationContext.BinlogSyncerMaxReconnectAttempts,
		}),
	}
}

// ConnectBinlogStreamer
func (gmr *GoMySQLReader) ConnectBinlogStreamer(coordinates mysql.BinlogCoordinates) (err error) {
	if coordinates.IsEmpty() {
		return gmr.migrationContext.Log.Errorf("empty coordinates at ConnectBinlogStreamer()")
	}

	gmr.currentCoordinatesMutex.Lock()
	defer gmr.currentCoordinatesMutex.Unlock()
	gmr.currentCoordinates = coordinates
	gmr.migrationContext.Log.Infof("Connecting binlog streamer at %+v", coordinates)

	// Start sync with specified GTID set or binlog file and position
	if gmr.migrationContext.UseGTIDs {
		coords := coordinates.(*mysql.GTIDBinlogCoordinates)
		gmr.binlogStreamer, err = gmr.binlogSyncer.StartSyncGTID(coords.GTIDSet)
	} else {
		coords := gmr.currentCoordinates.(*mysql.FileBinlogCoordinates)
		gmr.binlogStreamer, err = gmr.binlogSyncer.StartSync(gomysql.Position{
			Name: coords.LogFile,
			Pos:  uint32(coords.LogPos)},
		)
	}
	return err
}

func (gmr *GoMySQLReader) GetCurrentBinlogCoordinates() mysql.BinlogCoordinates {
	gmr.currentCoordinatesMutex.Lock()
	defer gmr.currentCoordinatesMutex.Unlock()
	return gmr.currentCoordinates.Clone()
}

func (gmr *GoMySQLReader) handleRowsEvent(ev *replication.BinlogEvent, rowsEvent *replication.RowsEvent, entriesChannel chan<- *BinlogEntry) error {
	currentCoords := gmr.GetCurrentBinlogCoordinates()
	dml := ToEventDML(ev.Header.EventType.String())
	if dml == NotDML {
		return fmt.Errorf("unknown DML type: %s", ev.Header.EventType.String())
	}
	for i, row := range rowsEvent.Rows {
		if dml == UpdateDML && i%2 == 1 {
			// An update has two rows (WHERE+SET)
			// We do both at the same time
			continue
		}
		binlogEntry := NewBinlogEntryAt(currentCoords)
		binlogEntry.DmlEvent = NewBinlogDMLEvent(
			string(rowsEvent.Table.Schema),
			string(rowsEvent.Table.Table),
			dml,
		)
		switch dml {
		case InsertDML:
			{
				binlogEntry.DmlEvent.NewColumnValues = sql.ToColumnValues(row)
			}
		case UpdateDML:
			{
				binlogEntry.DmlEvent.WhereColumnValues = sql.ToColumnValues(row)
				binlogEntry.DmlEvent.NewColumnValues = sql.ToColumnValues(rowsEvent.Rows[i+1])
			}
		case DeleteDML:
			{
				binlogEntry.DmlEvent.WhereColumnValues = sql.ToColumnValues(row)
			}
		}

		// The channel will do the throttling. Whoever is reading from the channel
		// decides whether action is taken synchronously (meaning we wait before
		// next iteration) or asynchronously (we keep pushing more events)
		// In reality, reads will be synchronous
		entriesChannel <- binlogEntry
	}
	return nil
}

// StreamEvents
func (gmr *GoMySQLReader) StreamEvents(canStopStreaming func() bool, entriesChannel chan<- *BinlogEntry) error {
	for !canStopStreaming() {
		ev, err := gmr.binlogStreamer.GetEvent(context.Background())
		if err != nil {
			return err
		}

		// Update binlog coords if using file-based coords.
		// GTID coordinates are updated on receiving GTID events.
		if !gmr.migrationContext.UseGTIDs {
			gmr.currentCoordinatesMutex.Lock()
			coords := gmr.currentCoordinates.(*mysql.FileBinlogCoordinates)
			prevCoords := coords.Clone().(*mysql.FileBinlogCoordinates)
			coords.LogPos = int64(ev.Header.LogPos)
			coords.EventSize = int64(ev.Header.EventSize)
			if coords.IsLogPosOverflowBeyond4Bytes(prevCoords) {
				gmr.currentCoordinatesMutex.Unlock()
				return fmt.Errorf("unexpected rows event at %+v, the binlog end_log_pos is overflow 4 bytes", coords)
			}
			gmr.currentCoordinatesMutex.Unlock()
		}

		switch event := ev.Event.(type) {
		case *replication.GTIDEvent:
			if !gmr.migrationContext.UseGTIDs {
				continue
			}
			sid, err := uuid.FromBytes(event.SID)
			if err != nil {
				return err
			}
			gmr.currentCoordinatesMutex.Lock()
			if gmr.LastTrxCoords != nil {
				gmr.currentCoordinates = gmr.LastTrxCoords.Clone()
			}
			coords := gmr.currentCoordinates.(*mysql.GTIDBinlogCoordinates)
			trxGset := gomysql.NewUUIDSet(sid, gomysql.Interval{Start: event.GNO, Stop: event.GNO + 1})
			coords.GTIDSet.AddSet(trxGset)
			gmr.currentCoordinatesMutex.Unlock()
		case *replication.RotateEvent:
			if gmr.migrationContext.UseGTIDs {
				continue
			}
			gmr.currentCoordinatesMutex.Lock()
			coords := gmr.currentCoordinates.(*mysql.FileBinlogCoordinates)
			coords.LogFile = string(event.NextLogName)
			gmr.migrationContext.Log.Infof("rotate to next log from %s:%d to %s", coords.LogFile, int64(ev.Header.LogPos), event.NextLogName)
			gmr.currentCoordinatesMutex.Unlock()
		case *replication.XIDEvent:
			if gmr.migrationContext.UseGTIDs {
				gmr.LastTrxCoords = &mysql.GTIDBinlogCoordinates{GTIDSet: event.GSet.(*gomysql.MysqlGTIDSet)}
			} else {
				gmr.LastTrxCoords = gmr.currentCoordinates.Clone()
			}
		case *replication.RowsEvent:
			if err := gmr.handleRowsEvent(ev, event, entriesChannel); err != nil {
				return err
			}
		}
	}
	gmr.migrationContext.Log.Debugf("done streaming events")

	return nil
}

func (gmr *GoMySQLReader) Close() error {
	gmr.binlogSyncer.Close()
	return nil
}
