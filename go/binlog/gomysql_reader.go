/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package binlog

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"

	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"golang.org/x/net/context"
)

type GoMySQLReader struct {
	migrationContext         *base.MigrationContext
	connectionConfig         *mysql.ConnectionConfig
	binlogSyncer             *replication.BinlogSyncer
	binlogStreamer           *replication.BinlogStreamer
	currentCoordinates       mysql.BinlogCoordinates
	currentCoordinatesMutex  *sync.Mutex
	LastAppliedRowsEventHint mysql.BinlogCoordinates
}

func NewGoMySQLReader(migrationContext *base.MigrationContext) *GoMySQLReader {
	connectionConfig := migrationContext.InspectorConnectionConfig
	return &GoMySQLReader{
		migrationContext:        migrationContext,
		connectionConfig:        connectionConfig,
		currentCoordinates:      mysql.BinlogCoordinates{},
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
			MaxReconnectAttempts:    migrationContext.BinlogSyncerMaxReconnectAttempts,
			TimestampStringLocation: time.UTC,
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
	// Start sync with specified binlog file and position
	this.binlogStreamer, err = this.binlogSyncer.StartSync(gomysql.Position{
		Name: this.currentCoordinates.LogFile,
		Pos:  uint32(this.currentCoordinates.LogPos),
	})

	return err
}

func (this *GoMySQLReader) GetCurrentBinlogCoordinates() *mysql.BinlogCoordinates {
	this.currentCoordinatesMutex.Lock()
	defer this.currentCoordinatesMutex.Unlock()
	returnCoordinates := this.currentCoordinates
	return &returnCoordinates
}

// handleRowsEvents processes a RowEvent from the binlog and sends the DML event to the entriesChannel.
func (this *GoMySQLReader) handleRowsEvent(ev *replication.BinlogEvent, rowsEvent *replication.RowsEvent, entriesChannel chan<- *BinlogEntry) error {
	if this.currentCoordinates.IsLogPosOverflowBeyond4Bytes(&this.LastAppliedRowsEventHint) {
		return fmt.Errorf("Unexpected rows event at %+v, the binlog end_log_pos is overflow 4 bytes", this.currentCoordinates)
	}

	if this.currentCoordinates.SmallerThanOrEquals(&this.LastAppliedRowsEventHint) {
		this.migrationContext.Log.Debugf("Skipping handled query at %+v", this.currentCoordinates)
		return nil
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
		binlogEntry := NewBinlogEntryAt(this.currentCoordinates)
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
	this.LastAppliedRowsEventHint = this.currentCoordinates
	return nil
}


// rowsEventToBinlogEntry processes MySQL RowsEvent into our BinlogEntry for later application.
// copied from handleRowEvents
func rowsEventToBinlogEntry(eventType replication.EventType, rowsEvent *replication.RowsEvent, binlogCoords mysql.BinlogCoordinates) (*BinlogEntry, error){
	dml := ToEventDML(eventType.String())
	if dml == NotDML {
		return nil, fmt.Errorf("Unknown DML type: %s", eventType.String())
	}
	binlogEntry := NewBinlogEntryAt(binlogCoords)
	for i, row := range rowsEvent.Rows {
		if dml == UpdateDML && i%2 == 1 {
			// An update has two rows (WHERE+SET)
			// We do both at the same time
			continue
		}
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
	}
	return binlogEntry, nil
}

type Transaction struct {
	DatabaseName   string
	TableName      string
	SequenceNumber int64
	LastCommitted  int64
	Changes        chan *BinlogEntry
}

func (this *GoMySQLReader) StreamTransactions(ctx context.Context, transactionsChannel chan<- *Transaction) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	previousSequenceNumber := int64(0)

groups:
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		ev, err := this.binlogStreamer.GetEvent(ctx)
		if err != nil {
			return err
		}
		func() {
			this.currentCoordinatesMutex.Lock()
			defer this.currentCoordinatesMutex.Unlock()
			this.currentCoordinates.LogPos = int64(ev.Header.LogPos)
			this.currentCoordinates.EventSize = int64(ev.Header.EventSize)
		}()

		fmt.Printf("Event: %s\n", ev.Header.EventType)

		// Read each event and do something with it
		//
		// First, ignore all events until we find the next GTID event so that we can start
		// at a transaction boundary.
		//
		// Once we find a GTID event, we can start an event group,
		// and then process all events in that group.
		// An event group is defined as all events that are part of the same transaction,
		// which is defined as all events between the GTID event, a `QueryEvent` containing a `BEGIN` query and ends with
		// either a XIDEvent or a `QueryEvent` containing a `COMMIT` or `ROLLBACK` query.
		//
		// Each group is a struct containing the SequenceNumber, LastCommitted, and a channel of events.
		//
		// Once the group has ended, we can start looking for the next GTID event.

		var group *Transaction
		switch binlogEvent := ev.Event.(type) {
		case *replication.GTIDEvent:
			this.migrationContext.Log.Infof("GTIDEvent: %+v", binlogEvent)

			// Bail out if we find a gap in the sequence numbers
			if previousSequenceNumber != 0 && binlogEvent.SequenceNumber != previousSequenceNumber+1 {
				return fmt.Errorf("unexpected sequence number: %d, expected %d", binlogEvent.SequenceNumber, previousSequenceNumber+1)
			}

			group = &Transaction{
				SequenceNumber: binlogEvent.SequenceNumber,
				LastCommitted:  binlogEvent.LastCommitted,
				Changes:        make(chan *BinlogEntry, 1000),
				// Table and Schema aren't known until the following TableMapEvent
				DatabaseName: "",
				TableName:    "",
			}

			previousSequenceNumber = binlogEvent.SequenceNumber

		default:
			this.migrationContext.Log.Infof("Ignoring Event: %+v", ev.Event)
			continue
		}

		// Next event should be a query event

		ev, err = this.binlogStreamer.GetEvent(ctx)
		if err != nil {
			close(group.Changes)
			return err
		}
		this.migrationContext.Log.Infof("1 - Event: %s", ev.Header.EventType)

		switch binlogEvent := ev.Event.(type) {
		case *replication.QueryEvent:
			if bytes.Equal([]byte("BEGIN"), binlogEvent.Query) {
				this.migrationContext.Log.Infof("BEGIN for transaction in schema %s", binlogEvent.Schema)
			} else {
				this.migrationContext.Log.Infof("QueryEvent: %+v", binlogEvent)
				this.migrationContext.Log.Infof("Query: %s", binlogEvent.Query)

				// wait for the next event group
				continue groups
			}
		default:
			this.migrationContext.Log.Infof("unexpected Event: %+v", ev.Event)
			close(group.Changes)

			// TODO: handle the group - we want to make sure we process the group's LastCommitted and SequenceNumber

			// wait for the next event group
			continue groups
		}

		// Next event should be a table map event

		ev, err = this.binlogStreamer.GetEvent(ctx)
		if err != nil {
			close(group.Changes)
			return err
		}
		this.migrationContext.Log.Infof("2 - Event: %s", ev.Header.EventType)

		switch binlogEvent := ev.Event.(type) {
		case *replication.TableMapEvent:
			// TODO: Can we be smart here and short circuit processing groups for tables that don't match the table in the migration context?

			group.TableName = string(binlogEvent.Table)
			group.DatabaseName = string(binlogEvent.Schema)
			// we are good to send the transaction, the transaction events arrive async
			transactionsChannel <- group
		default:
			this.migrationContext.Log.Infof("unexpected Event: %+v", ev.Event)

			close(group.Changes)

			// TODO: handle the group - we want to make sure we process the group's LastCommitted and SequenceNumber

			continue groups
		}

	events:
		// Now we can start processing the group
		for {
			ev, err = this.binlogStreamer.GetEvent(ctx)
			if err != nil {
				close(group.Changes)
				return err
			}
			this.migrationContext.Log.Infof("3 - Event: %s", ev.Header.EventType)

			switch binlogEvent := ev.Event.(type) {
			case *replication.RowsEvent:
				binlogEntry, err := rowsEventToBinlogEntry(ev.Header.EventType, binlogEvent, this.currentCoordinates)
				if err != nil {
					close(group.Changes)
					return err
				}
				group.Changes <- binlogEntry
			case *replication.XIDEvent:
				this.migrationContext.Log.Infof("XIDEvent: %+v", binlogEvent)
				this.migrationContext.Log.Infof("COMMIT for transaction")
				break events
			default:
				close(group.Changes)
				this.migrationContext.Log.Infof("unexpected Event: %+v", ev.Event)
				return fmt.Errorf("unexpected Event: %+v", ev.Event)
			}
		}

		close(group.Changes)

		this.migrationContext.Log.Infof("done processing group - %d events", len(group.Changes))
	}
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
		func() {
			this.currentCoordinatesMutex.Lock()
			defer this.currentCoordinatesMutex.Unlock()
			this.currentCoordinates.LogPos = int64(ev.Header.LogPos)
			this.currentCoordinates.EventSize = int64(ev.Header.EventSize)
		}()

		switch binlogEvent := ev.Event.(type) {
		case *replication.RotateEvent:
			func() {
				this.currentCoordinatesMutex.Lock()
				defer this.currentCoordinatesMutex.Unlock()
				this.currentCoordinates.LogFile = string(binlogEvent.NextLogName)
			}()
			this.migrationContext.Log.Infof("rotate to next log from %s:%d to %s", this.currentCoordinates.LogFile, int64(ev.Header.LogPos), binlogEvent.NextLogName)
		case *replication.RowsEvent:
			if err := this.handleRowsEvent(ev, binlogEvent, entriesChannel); err != nil {
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
