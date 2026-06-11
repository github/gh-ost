/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	gosql "database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/binlog"
	"github.com/github/gh-ost/go/mysql"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/openark/golib/sqlutils"
)

type BinlogEventListener struct {
	async        bool
	databaseName string
	tableName    string
	onDmlEvent   func(event *binlog.BinlogEntry) error
}

const (
	EventsChannelBufferSize       = 1
	ReconnectStreamerSleepSeconds = 1
)

// EventsStreamer reads data from binary logs and streams it on. It acts as a publisher,
// and interested parties may subscribe for per-table events.
type EventsStreamer struct {
	connectionConfig         *mysql.ConnectionConfig
	db                       *gosql.DB
	dbVersion                string
	migrationContext         *base.MigrationContext
	initialBinlogCoordinates mysql.BinlogCoordinates
	listeners                [](*BinlogEventListener)
	listenersMutex           *sync.Mutex
	eventsChannel            chan *binlog.BinlogEntry
	binlogReader             *binlog.GoMySQLReader
	name                     string
}

func NewEventsStreamer(migrationContext *base.MigrationContext) *EventsStreamer {
	return &EventsStreamer{
		connectionConfig:         migrationContext.InspectorConnectionConfig,
		migrationContext:         migrationContext,
		listeners:                [](*BinlogEventListener){},
		listenersMutex:           &sync.Mutex{},
		eventsChannel:            make(chan *binlog.BinlogEntry, EventsChannelBufferSize),
		name:                     "streamer",
		initialBinlogCoordinates: migrationContext.InitialStreamerCoords,
	}
}

// AddListener registers a new listener for binlog events, on a per-table basis
func (es *EventsStreamer) AddListener(
	async bool, databaseName string, tableName string, onDmlEvent func(event *binlog.BinlogEntry) error) (err error) {
	es.listenersMutex.Lock()
	defer es.listenersMutex.Unlock()

	if databaseName == "" {
		return fmt.Errorf("empty database name in AddListener")
	}
	if tableName == "" {
		return fmt.Errorf("empty table name in AddListener")
	}
	listener := &BinlogEventListener{
		async:        async,
		databaseName: databaseName,
		tableName:    tableName,
		onDmlEvent:   onDmlEvent,
	}
	es.listeners = append(es.listeners, listener)
	return nil
}

// notifyListeners will notify relevant listeners with given DML event. Only
// listeners registered for changes on the table on which the DML operates are notified.
func (es *EventsStreamer) notifyListeners(binlogEntry *binlog.BinlogEntry) {
	es.listenersMutex.Lock()
	defer es.listenersMutex.Unlock()

	for _, listener := range es.listeners {
		listener := listener
		if !strings.EqualFold(listener.databaseName, binlogEntry.DmlEvent.DatabaseName) {
			continue
		}
		if !strings.EqualFold(listener.tableName, binlogEntry.DmlEvent.TableName) {
			continue
		}
		if listener.async {
			go func() {
				listener.onDmlEvent(binlogEntry)
			}()
		} else {
			listener.onDmlEvent(binlogEntry)
		}
	}
}

func (es *EventsStreamer) InitDBConnections() (err error) {
	EventsStreamerUri := es.connectionConfig.GetDBUri(es.migrationContext.DatabaseName)
	if es.db, _, err = mysql.GetDB(es.migrationContext.Uuid, EventsStreamerUri); err != nil {
		return err
	}
	version, err := base.ValidateConnection(es.db, es.connectionConfig, es.migrationContext, es.name)
	if err != nil {
		return err
	}
	es.dbVersion = version
	if es.initialBinlogCoordinates == nil || es.initialBinlogCoordinates.IsEmpty() {
		if err := es.readCurrentBinlogCoordinates(); err != nil {
			return err
		}
	}
	if err := es.initBinlogReader(es.initialBinlogCoordinates); err != nil {
		return err
	}

	return nil
}

// initBinlogReader creates and connects the reader: we hook up to a MySQL server as a replica
func (es *EventsStreamer) initBinlogReader(binlogCoordinates mysql.BinlogCoordinates) error {
	goMySQLReader := binlog.NewGoMySQLReader(es.migrationContext)
	if err := goMySQLReader.ConnectBinlogStreamer(binlogCoordinates); err != nil {
		return err
	}
	es.binlogReader = goMySQLReader
	return nil
}

func (es *EventsStreamer) GetCurrentBinlogCoordinates() mysql.BinlogCoordinates {
	return es.binlogReader.GetCurrentBinlogCoordinates()
}

// readCurrentBinlogCoordinates reads master status from hooked server
func (es *EventsStreamer) readCurrentBinlogCoordinates() error {
	binaryLogStatusTerm := mysql.ReplicaTermFor(es.dbVersion, "master status")
	query := fmt.Sprintf("show /* gh-ost readCurrentBinlogCoordinates */ %s", binaryLogStatusTerm)
	foundMasterStatus := false
	err := sqlutils.QueryRowsMap(es.db, query, func(m sqlutils.RowMap) error {
		if es.migrationContext.UseGTIDs {
			execGtidSet := m.GetString("Executed_Gtid_Set")
			gtidSet, err := gomysql.ParseMysqlGTIDSet(execGtidSet)
			if err != nil {
				return err
			}
			es.initialBinlogCoordinates = &mysql.GTIDBinlogCoordinates{GTIDSet: gtidSet.(*gomysql.MysqlGTIDSet)}
		} else {
			es.initialBinlogCoordinates = &mysql.FileBinlogCoordinates{
				LogFile: m.GetString("File"),
				LogPos:  m.GetInt64("Position"),
			}
		}
		foundMasterStatus = true
		return nil
	})
	if err != nil {
		return err
	}
	if !foundMasterStatus {
		return fmt.Errorf("got no results from SHOW %s. Bailing out", strings.ToUpper(binaryLogStatusTerm))
	}
	es.migrationContext.Log.Debugf("Streamer binlog coordinates: %+v", es.initialBinlogCoordinates)
	return nil
}

// StreamEvents will begin streaming events. It will be blocking, so should be
// executed by a goroutine
func (es *EventsStreamer) StreamEvents(canStopStreaming func() bool) error {
	go func() {
		for binlogEntry := range es.eventsChannel {
			if binlogEntry.DmlEvent != nil {
				es.notifyListeners(binlogEntry)
			}
		}
	}()
	// The next should block and execute forever, unless there's a serious error.
	var successiveFailures int
	var reconnectCoords mysql.BinlogCoordinates
	ctx := es.migrationContext.GetContext()
	for {
		// Check for context cancellation each iteration
		if err := ctx.Err(); err != nil {
			return err
		}
		if canStopStreaming() {
			return nil
		}
		// We will reconnect the binlog streamer at the coordinates
		// of the last trx that was read completely from the streamer.
		// Since row event application is idempotent, it's OK if we reapply some events.
		if err := es.binlogReader.StreamEvents(canStopStreaming, es.eventsChannel); err != nil {
			if canStopStreaming() {
				return nil
			}

			es.migrationContext.Log.Infof("StreamEvents encountered unexpected error: %+v", err)
			es.migrationContext.MarkPointOfInterest()
			time.Sleep(ReconnectStreamerSleepSeconds * time.Second)

			// See if there's retry overflow
			if es.migrationContext.BinlogSyncerMaxReconnectAttempts > 0 && successiveFailures >= es.migrationContext.BinlogSyncerMaxReconnectAttempts {
				return fmt.Errorf("%d successive failures in streamer reconnect at coordinates %+v", successiveFailures, reconnectCoords)
			}

			// Reposition at same coordinates
			if es.binlogReader.LastTrxCoords != nil {
				reconnectCoords = es.binlogReader.LastTrxCoords.Clone()
			} else {
				reconnectCoords = es.initialBinlogCoordinates.Clone()
			}
			if !reconnectCoords.SmallerThan(es.GetCurrentBinlogCoordinates()) {
				successiveFailures += 1
			} else {
				successiveFailures = 0
			}

			es.migrationContext.Log.Infof("Reconnecting EventsStreamer... Will resume at %+v", reconnectCoords)
			_ = es.binlogReader.Close()
			if err := es.initBinlogReader(reconnectCoords); err != nil {
				return err
			}
		}
	}
}

func (es *EventsStreamer) Close() (err error) {
	err = es.binlogReader.Close()
	es.migrationContext.Log.Infof("Closed streamer connection. err=%+v", err)
	return err
}

func (es *EventsStreamer) Teardown() {
	es.db.Close()
}
