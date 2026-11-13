/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package binlog

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"

	"time"

	"context"

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
	// If using the file coordinates it is binlog position of the transaction's XID event.
	LastTrxCoords mysql.BinlogCoordinates
	// ReadSourceCoordinates enables ordered catch-up probes in move-tables mode.
	ReadSourceCoordinates func() (mysql.BinlogCoordinates, error)
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
		binlogEntry.Timestamp = time.Unix(int64(ev.Header.Timestamp), 0)
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
	processedCoordinates := gmr.GetCurrentBinlogCoordinates()
	interval := time.Duration(gmr.migrationContext.HeartbeatIntervalMilliseconds) * time.Millisecond
	lastProbe := time.Now()
	inTransaction := false
	for !canStopStreaming() {
		ctx := context.Background()
		cancel := func() {}
		if gmr.ReadSourceCoordinates != nil {
			ctx, cancel = context.WithTimeout(gmr.migrationContext.GetContext(), interval)
		}
		ev, err := gmr.binlogStreamer.GetEvent(ctx)
		cancel()
		if err != nil && !(gmr.ReadSourceCoordinates != nil && errors.Is(err, context.DeadlineExceeded) && gmr.migrationContext.GetContext().Err() == nil) {
			return err
		}
		if gmr.ReadSourceCoordinates != nil && time.Since(lastProbe) >= interval {
			// processedCoordinates excludes the event just fetched. The marker must
			// follow every row it claims to cover, including a complete GTID transaction.
			if err := gmr.sendStreamProgress(processedCoordinates, entriesChannel); err != nil {
				return err
			}
			lastProbe = time.Now()
		}
		if err != nil {
			continue
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
			processedCoordinates = &mysql.FileBinlogCoordinates{
				LogFile: string(event.NextLogName),
				LogPos:  int64(event.Position),
			}
		case *replication.XIDEvent:
			inTransaction = false
			if gmr.migrationContext.UseGTIDs {
				// event.GSet is the full executed GTID set maintained by the
				// syncer (MysqlGTIDSet or MariadbGTIDSet depending on flavor).
				if event.GSet != nil {
					gmr.LastTrxCoords = &mysql.GTIDBinlogCoordinates{GTIDSet: event.GSet}
					if gmr.ReadSourceCoordinates != nil {
						processedCoordinates = gmr.LastTrxCoords.Clone()
					}
				}
			} else {
				gmr.LastTrxCoords = gmr.currentCoordinates.Clone()
			}
		case *replication.QueryEvent:
			if gmr.ReadSourceCoordinates == nil || !gmr.migrationContext.UseGTIDs {
				break
			}
			switch strings.ToUpper(strings.TrimSpace(string(event.Query))) {
			case "BEGIN":
				inTransaction = true
			case "COMMIT", "ROLLBACK":
				inTransaction = false
			}
			// Standalone DDL has no XID event. BEGIN's GSet, however, already
			// includes the transaction whose row events have not arrived yet.
			if !inTransaction && event.GSet != nil {
				processedCoordinates = (&mysql.GTIDBinlogCoordinates{GTIDSet: event.GSet}).Clone()
			}
		case *replication.RowsEvent:
			if err := gmr.handleRowsEvent(ev, event, entriesChannel); err != nil {
				return err
			}
		}
		if gmr.ReadSourceCoordinates == nil {
			continue
		}
		// Startup's synthetic rotate/format events can have a zero log position.
		if !gmr.migrationContext.UseGTIDs && ev.Header.LogPos > 0 && ev.Header.EventType != replication.ROTATE_EVENT {
			processedCoordinates = gmr.GetCurrentBinlogCoordinates()
		}
	}
	gmr.migrationContext.Log.Debugf("done streaming events")

	return nil
}

func (gmr *GoMySQLReader) sendStreamProgress(processed mysql.BinlogCoordinates, entriesChannel chan<- *BinlogEntry) error {
	probeTime := time.Now()
	source, err := gmr.ReadSourceCoordinates()
	if err != nil {
		return err
	}
	// Equality works for file positions and both GTID flavors, without assuming
	// that a partially received transaction or an empty apply queue means caught up.
	if !processed.Equals(source) {
		return nil
	}
	return base.SendWithContext(gmr.migrationContext.GetContext(), entriesChannel, &BinlogEntry{
		Coordinates: processed.Clone(),
		Timestamp:   probeTime,
	})
}

func (gmr *GoMySQLReader) Close() error {
	gmr.binlogSyncer.Close()
	return nil
}
