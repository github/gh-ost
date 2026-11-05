/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package binlog

import (
	"fmt"
	"strings"
	"sync"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/metrics"
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"

	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

type RowsEventFilterFunc func(databaseName, tableName string) bool

func newRowsEventDecodeFunc(rowsEventFilter RowsEventFilterFunc) func(*replication.RowsEvent, []byte) error {
	if rowsEventFilter == nil {
		return nil
	}
	return func(rowsEvent *replication.RowsEvent, data []byte) error {
		pos, err := rowsEvent.DecodeHeader(data)
		if err != nil {
			return err
		}
		if !rowsEventFilter(string(rowsEvent.Table.Schema), string(rowsEvent.Table.Table)) {
			return nil
		}
		return rowsEvent.DecodeData(pos, data)
	}
}

type GoMySQLReader struct {
	migrationContext        *base.MigrationContext
	connectionConfig        *mysql.ConnectionConfig
	binlogSyncer            *replication.BinlogSyncer
	binlogStreamer          *replication.BinlogStreamer
	currentCoordinates      mysql.BinlogCoordinates
	currentCoordinatesMutex *sync.Mutex
	// LastTrxCoords are the coordinates of the last transaction completely read.
	// They are for streamer reconnects, not durable applier checkpoints.
	// For file coordinates, this is the end position of the transaction's XID
	// event or its enclosing compressed payload.
	LastTrxCoords mysql.BinlogCoordinates

	// Per-transaction counters for relevant (consumed) row events, flushed at transaction boundaries.
	transactionRowEventTotal int64
	transactionRowTotal      int64
	previousLastCommitted    int64
	transactionHasRows       bool
}

func NewGoMySQLReader(migrationContext *base.MigrationContext, rowsEventFilters ...RowsEventFilterFunc) *GoMySQLReader {
	connectionConfig := migrationContext.InspectorConnectionConfig
	var rowsEventFilter RowsEventFilterFunc
	if len(rowsEventFilters) > 0 {
		rowsEventFilter = rowsEventFilters[0]
	}
	config := replication.BinlogSyncerConfig{
		ServerID:                uint32(migrationContext.ReplicaServerId),
		Flavor:                  mysql.FlavorFor(migrationContext.InspectorMySQLVersion),
		Host:                    connectionConfig.Key.Hostname,
		Port:                    uint16(connectionConfig.Key.Port),
		User:                    connectionConfig.User,
		Password:                connectionConfig.Password,
		TLSConfig:               connectionConfig.TLSConfig(),
		UseDecimal:              true,
		TimestampStringLocation: time.UTC,
		MaxReconnectAttempts:    migrationContext.BinlogSyncerMaxReconnectAttempts,
	}
	config.RowsEventDecodeFunc = newRowsEventDecodeFunc(rowsEventFilter)
	return &GoMySQLReader{
		migrationContext:        migrationContext,
		connectionConfig:        connectionConfig,
		currentCoordinatesMutex: &sync.Mutex{},
		binlogSyncer:            replication.NewBinlogSyncer(config),
	}
}

// ConnectBinlogStreamer
func (gmr *GoMySQLReader) ConnectBinlogStreamer(coordinates mysql.BinlogCoordinates) (err error) {
	if coordinates.IsEmpty() {
		return gmr.migrationContext.Log.Errorf("empty coordinates at ConnectBinlogStreamer()")
	}

	gmr.currentCoordinatesMutex.Lock()
	defer gmr.currentCoordinatesMutex.Unlock()
	gmr.currentCoordinates = coordinates.Clone()
	gmr.migrationContext.Log.Infof("Connecting binlog streamer at %+v", coordinates)

	// Start sync with specified GTID set or binlog file and position
	if gmr.migrationContext.UseGTIDs {
		coords := coordinates.(*mysql.GTIDBinlogCoordinates)
		gmr.binlogStreamer, err = gmr.binlogSyncer.StartSyncGTID(coords.GTIDSet.Clone())
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

func (gmr *GoMySQLReader) isRelevantTable(databaseName, tableName string) bool {
	if !strings.EqualFold(databaseName, gmr.migrationContext.DatabaseName) {
		return false
	}
	if strings.EqualFold(tableName, gmr.migrationContext.OriginalTableName) {
		return true
	}
	return strings.EqualFold(tableName, gmr.migrationContext.GetChangelogTableName())
}

func (gmr *GoMySQLReader) flushTransactionMetrics() {
	if gmr.transactionRowEventTotal == 0 {
		return
	}
	emit := gmr.migrationContext.Metrics
	metrics.RecordBinlogTransactionSize(emit, gmr.transactionRowEventTotal, gmr.transactionRowTotal)
	gmr.transactionRowEventTotal = 0
	gmr.transactionRowTotal = 0
}

func (gmr *GoMySQLReader) onGTIDEvent(gtidEvent gomysql.BinlogGTIDEvent) {
	gmr.flushTransactionMetrics()

	// Both flavors mark transaction boundaries, but only MySQL GTID events
	// provide transaction length and parallel replication sequence numbers.
	event, ok := gtidEvent.(*replication.GTIDEvent)
	if !ok {
		return
	}

	emit := gmr.migrationContext.Metrics
	if event.TransactionLength > 0 {
		metrics.RecordGTIDTransactionLengthBytes(emit, event.TransactionLength)
	}
	if event.LastCommitted != gmr.previousLastCommitted {
		metrics.RecordUnfilteredCommitGroupSize(emit, float64(event.SequenceNumber-event.LastCommitted))
	}
	gmr.previousLastCommitted = event.LastCommitted
}

func (gmr *GoMySQLReader) handleRowsEvent(ev *replication.BinlogEvent, rowsEvent *replication.RowsEvent, entriesChannel chan<- *BinlogEntry) error {
	dml := ToEventDML(ev.Header.EventType.String())
	if dml == NotDML {
		return fmt.Errorf("unknown DML type: %s", ev.Header.EventType.String())
	}

	databaseName := string(rowsEvent.Table.Schema)
	tableName := string(rowsEvent.Table.Table)
	rowCount := int64(RowsInRowsEvent(len(rowsEvent.Rows), dml))
	eventType := dml.EventTypeTag()
	emit := gmr.migrationContext.Metrics
	metrics.RecordBinlogRowsEventProcessed(emit, tableName, eventType, rowCount)
	relevant := gmr.isRelevantTable(databaseName, tableName)
	if !relevant {
		metrics.RecordBinlogRowsEventFiltered(emit, tableName, eventType, rowCount)
	} else {
		metrics.RecordBinlogRowsEventConsumed(emit, tableName, eventType, rowCount)
		metrics.RecordBinlogRowsInEvent(emit, tableName, rowCount)
		gmr.transactionRowEventTotal++
		gmr.transactionRowTotal += rowCount
	}

	beforeChannel := time.Now()
	for i, row := range rowsEvent.Rows {
		if dml == UpdateDML && i%2 == 1 {
			// An update has two rows (WHERE+SET)
			// We do both at the same time
			continue
		}
		binlogEntry := NewBinlogEntryAt(gmr.GetCurrentBinlogCoordinates())
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
		if err := base.SendWithContext(gmr.migrationContext.GetContext(), entriesChannel, binlogEntry); err != nil {
			return err
		}
		gmr.transactionHasRows = true
	}
	if relevant {
		metrics.RecordBinlogStreamerBlockedOnOutChannel(emit, time.Since(beforeChannel))
	}
	return nil
}

func (gmr *GoMySQLReader) completeTransaction(event *replication.XIDEvent, entriesChannel chan<- *BinlogEntry) error {
	if !gmr.migrationContext.UseGTIDs {
		gmr.flushTransactionMetrics()
	}
	var coords mysql.BinlogCoordinates
	if gmr.migrationContext.UseGTIDs && event.GSet != nil {
		coords = (&mysql.GTIDBinlogCoordinates{GTIDSet: event.GSet}).Clone()
	} else {
		// File positions use the current outer event's coordinates. For GTID
		// events parsed without the syncer, fall back to the coordinates
		// already updated by the preceding GTID event.
		coords = gmr.GetCurrentBinlogCoordinates()
	}
	if gmr.transactionHasRows {
		marker := &BinlogEntry{
			Coordinates:         coords.Clone(),
			TransactionComplete: true,
		}
		if err := base.SendWithContext(gmr.migrationContext.GetContext(), entriesChannel, marker); err != nil {
			return err
		}
		gmr.transactionHasRows = false
	}
	// Reconnect only after all rows and their completion marker were delivered.
	gmr.LastTrxCoords = coords
	return nil
}

// StreamEvents
func (gmr *GoMySQLReader) StreamEvents(canStopStreaming func() bool, entriesChannel chan<- *BinlogEntry) error {
	for !canStopStreaming() {
		ev, err := gmr.binlogStreamer.GetEvent(gmr.migrationContext.GetContext())
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
		case *replication.GTIDEvent, *replication.MariadbGTIDEvent:
			// MySQL emits *GTIDEvent, MariaDB emits *MariadbGTIDEvent; both
			// implement BinlogGTIDEvent.GTIDNext() returning the GTID about to
			// be applied. We advance currentCoordinates by merging it into the
			// running GTID set, regardless of flavor.
			if !gmr.migrationContext.UseGTIDs {
				continue
			}
			gtidEvent, ok := ev.Event.(gomysql.BinlogGTIDEvent)
			if !ok {
				return fmt.Errorf("unexpected GTID event type: %T", ev.Event)
			}
			nextGTID, err := gtidEvent.GTIDNext()
			if err != nil {
				return err
			}
			gmr.onGTIDEvent(gtidEvent)
			gmr.currentCoordinatesMutex.Lock()
			if gmr.LastTrxCoords != nil {
				gmr.currentCoordinates = gmr.LastTrxCoords.Clone()
			}
			coords := gmr.currentCoordinates.(*mysql.GTIDBinlogCoordinates)
			if coords.GTIDSet == nil {
				coords.GTIDSet = nextGTID
			} else if err := coords.GTIDSet.Update(nextGTID.String()); err != nil {
				gmr.currentCoordinatesMutex.Unlock()
				return err
			}
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
			if err := gmr.completeTransaction(event, entriesChannel); err != nil {
				return err
			}
		case *replication.RowsEvent:
			if err := gmr.handleRowsEvent(ev, event, entriesChannel); err != nil {
				return err
			}
		case *replication.TransactionPayloadEvent:
			// Keep the outer payload's coordinates for every row and the commit,
			// rather than the synthetic boundary positions on nested events.
			// Process the whole transaction before checking canStopStreaming.
			for _, nested := range event.Events {
				switch nestedEvent := nested.Event.(type) {
				case *replication.RowsEvent:
					if err := gmr.handleRowsEvent(nested, nestedEvent, entriesChannel); err != nil {
						return err
					}
				case *replication.XIDEvent:
					if err := gmr.completeTransaction(nestedEvent, entriesChannel); err != nil {
						return err
					}
				}
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
