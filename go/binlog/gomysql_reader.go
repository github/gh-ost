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

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	uuid "github.com/google/uuid"
	"golang.org/x/net/context"
)

type GoMySQLReader struct {
	migrationContext        *base.MigrationContext
	connectionConfig        *mysql.ConnectionConfig
	binlogSyncer            *replication.BinlogSyncer
	binlogStreamer          *replication.BinlogStreamer
	currentCoordinates      mysql.BinlogCoordinates
	currentCoordinatesMutex *sync.Mutex
	// LastTrxCoords is the coordinates of the last transaction read.
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
			DisableRetrySync:        true, // we implement our own reconnect.
		}),
	}
}

// ConnectBinlogStreamer
func (this *GoMySQLReader) ConnectBinlogStreamer(coordinates mysql.BinlogCoordinates) (err error) {
	if coordinates.IsEmpty() {
		return this.migrationContext.Log.Errorf("Empty coordinates at ConnectBinlogStreamer()")
	}

	this.currentCoordinates = coordinates
	this.migrationContext.Log.Infof("Connecting binlog streamer at %+v", this.currentCoordinates)

	// Start sync with specified GTID set or binlog file and position
	if this.migrationContext.UseGTIDs {
		coords := this.currentCoordinates.(*mysql.GTIDBinlogCoordinates)
		this.binlogStreamer, err = this.binlogSyncer.StartSyncGTID(coords.GTIDSet)
	} else {
		coords := this.currentCoordinates.(*mysql.FileBinlogCoordinates)
		this.binlogStreamer, err = this.binlogSyncer.StartSync(gomysql.Position{
			Name: coords.LogFile,
			Pos:  uint32(coords.LogPos)},
		)
	}

	return err
}

func (this *GoMySQLReader) GetCurrentBinlogCoordinates() mysql.BinlogCoordinates {
	this.currentCoordinatesMutex.Lock()
	defer this.currentCoordinatesMutex.Unlock()
	return this.currentCoordinates.Clone()
}

func (this *GoMySQLReader) handleRowsEvent(ev *replication.BinlogEvent, rowsEvent *replication.RowsEvent, entriesChannel chan<- *BinlogEntry) error {
	this.currentCoordinatesMutex.Lock()
	currentCoords := this.currentCoordinates
	this.currentCoordinatesMutex.Unlock()
	lastTrx := this.LastTrxCoords

	if currentCoords.SmallerThan(lastTrx) {
		this.migrationContext.Log.Debugf("Skipping handled transaction %+v (last trx is %+v)", currentCoords, lastTrx)
	}

	dml := ToEventDML(ev.Header.EventType.String())
	if dml == NotDML {
		return fmt.Errorf("Unknown DML type: %s", ev.Header.EventType.String())
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
func (this *GoMySQLReader) StreamEvents(canStopStreaming func() bool, entriesChannel chan<- *BinlogEntry) error {
	if canStopStreaming() {
		return nil
	}
	for {
		if canStopStreaming() {
			break
		}
		ev, err := this.binlogStreamer.GetEvent(context.Background())
		if err != nil {
			return err
		}

		// update binlog coords if using file-based coords
		if !this.migrationContext.UseGTIDs {
			this.currentCoordinatesMutex.Lock()
			coords := this.currentCoordinates.(*mysql.FileBinlogCoordinates)
			prevCoords := coords.Clone().(*mysql.FileBinlogCoordinates)
			coords.LogPos = int64(ev.Header.LogPos)
			coords.EventSize = int64(ev.Header.EventSize)
			if coords.IsLogPosOverflowBeyond4Bytes(prevCoords) {
				this.currentCoordinatesMutex.Unlock()
				return fmt.Errorf("Unexpected rows event at %+v, the binlog end_log_pos is overflow 4 bytes", coords)
			}
			this.currentCoordinatesMutex.Unlock()
		}

		switch event := ev.Event.(type) {
		case *replication.GTIDEvent:
			if !this.migrationContext.UseGTIDs {
				continue
			}
			sid, err := uuid.FromBytes(event.SID)
			if err != nil {
				return err
			}
			this.currentCoordinatesMutex.Lock()
			coords := this.currentCoordinates.(*mysql.GTIDBinlogCoordinates)
			coords.GTIDSet.AddGTID(sid, event.GNO)
			this.currentCoordinatesMutex.Unlock()
		case *replication.RotateEvent:
			if this.migrationContext.UseGTIDs {
				continue
			}
			this.currentCoordinatesMutex.Lock()
			coords := this.currentCoordinates.(*mysql.FileBinlogCoordinates)
			coords.LogFile = string(event.NextLogName)
			this.migrationContext.Log.Infof("rotate to next log from %s:%d to %s", coords.LogFile, int64(ev.Header.LogPos), event.NextLogName)
			this.currentCoordinatesMutex.Unlock()
		case *replication.XIDEvent:
			this.currentCoordinatesMutex.Lock()
			this.LastTrxCoords = this.currentCoordinates.Clone()
			this.currentCoordinatesMutex.Unlock()
		case *replication.RowsEvent:
			if err := this.handleRowsEvent(ev, event, entriesChannel); err != nil {
				return err
			}
		}
	}
	this.migrationContext.Log.Debugf("done streaming events")

	return nil
}

func (this *GoMySQLReader) Close() error {
	this.binlogSyncer.Close()
	return nil
}
