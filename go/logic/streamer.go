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
func (this *EventsStreamer) AddListener(
	async bool, databaseName string, tableName string, onDmlEvent func(event *binlog.BinlogEntry) error) (err error) {
	this.listenersMutex.Lock()
	defer this.listenersMutex.Unlock()

	if databaseName == "" {
		return fmt.Errorf("Empty database name in AddListener")
	}
	if tableName == "" {
		return fmt.Errorf("Empty table name in AddListener")
	}
	listener := &BinlogEventListener{
		async:        async,
		databaseName: databaseName,
		tableName:    tableName,
		onDmlEvent:   onDmlEvent,
	}
	this.listeners = append(this.listeners, listener)
	return nil
}

// notifyListeners will notify relevant listeners with given DML event. Only
// listeners registered for changes on the table on which the DML operates are notified.
func (this *EventsStreamer) notifyListeners(binlogEntry *binlog.BinlogEntry) {
	this.listenersMutex.Lock()
	defer this.listenersMutex.Unlock()

	for _, listener := range this.listeners {
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

func (this *EventsStreamer) InitDBConnections() (err error) {
	EventsStreamerUri := this.connectionConfig.GetDBUri(this.migrationContext.DatabaseName)
	if this.db, _, err = mysql.GetDB(this.migrationContext.Uuid, EventsStreamerUri); err != nil {
		return err
	}
	version, err := base.ValidateConnection(this.db, this.connectionConfig, this.migrationContext, this.name)
	if err != nil {
		return err
	}
	this.dbVersion = version
	if this.initialBinlogCoordinates == nil || this.initialBinlogCoordinates.IsEmpty() {
		if err := this.readCurrentBinlogCoordinates(); err != nil {
			return err
		}
	}
	if err := this.initBinlogReader(this.initialBinlogCoordinates); err != nil {
		return err
	}

	return nil
}

// initBinlogReader creates and connects the reader: we hook up to a MySQL server as a replica
func (this *EventsStreamer) initBinlogReader(binlogCoordinates mysql.BinlogCoordinates) error {
	goMySQLReader := binlog.NewGoMySQLReader(this.migrationContext)
	if err := goMySQLReader.ConnectBinlogStreamer(binlogCoordinates); err != nil {
		return err
	}
	this.binlogReader = goMySQLReader
	return nil
}

func (this *EventsStreamer) GetCurrentBinlogCoordinates() mysql.BinlogCoordinates {
	return this.binlogReader.GetCurrentBinlogCoordinates()
}

// readCurrentBinlogCoordinates reads master status from hooked server
func (this *EventsStreamer) readCurrentBinlogCoordinates() error {
	binaryLogStatusTerm := mysql.ReplicaTermFor(this.dbVersion, "master status")
	query := fmt.Sprintf("show /* gh-ost readCurrentBinlogCoordinates */ %s", binaryLogStatusTerm)
	foundMasterStatus := false
	err := sqlutils.QueryRowsMap(this.db, query, func(m sqlutils.RowMap) error {
		if this.migrationContext.UseGTIDs {
			execGtidSet := m.GetString("Executed_Gtid_Set")
			gtidSet, err := gomysql.ParseMysqlGTIDSet(execGtidSet)
			if err != nil {
				return err
			}
			this.initialBinlogCoordinates = &mysql.GTIDBinlogCoordinates{GTIDSet: gtidSet.(*gomysql.MysqlGTIDSet)}
		} else {
			this.initialBinlogCoordinates = &mysql.FileBinlogCoordinates{
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
		return fmt.Errorf("Got no results from SHOW %s. Bailing out", strings.ToUpper(binaryLogStatusTerm))
	}
	this.migrationContext.Log.Debugf("Streamer binlog coordinates: %+v", this.initialBinlogCoordinates)
	return nil
}

// StreamEvents will begin streaming events. It will be blocking, so should be
// executed by a goroutine
func (this *EventsStreamer) StreamEvents(canStopStreaming func() bool) error {
	go func() {
		for binlogEntry := range this.eventsChannel {
			if binlogEntry.DmlEvent != nil {
				this.notifyListeners(binlogEntry)
			}
		}
	}()
	// The next should block and execute forever, unless there's a serious error.
	var successiveFailures int
	var reconnectCoords mysql.BinlogCoordinates
	for {
		if canStopStreaming() {
			return nil
		}
		// We will reconnect the binlog streamer at the coordinates
		// of the last trx that was read completely from the streamer.
		// Since row event application is idempotent, it's OK if we reapply some events.
		if err := this.binlogReader.StreamEvents(canStopStreaming, this.eventsChannel); err != nil {
			if canStopStreaming() {
				return nil
			}

			this.migrationContext.Log.Infof("StreamEvents encountered unexpected error: %+v", err)
			this.migrationContext.MarkPointOfInterest()
			time.Sleep(ReconnectStreamerSleepSeconds * time.Second)

			// See if there's retry overflow
			if this.migrationContext.BinlogSyncerMaxReconnectAttempts > 0 && successiveFailures >= this.migrationContext.BinlogSyncerMaxReconnectAttempts {
				return fmt.Errorf("%d successive failures in streamer reconnect at coordinates %+v", successiveFailures, reconnectCoords)
			}

			// Reposition at same coordinates
			if this.binlogReader.LastTrxCoords != nil {
				reconnectCoords = this.binlogReader.LastTrxCoords.Clone()
			} else {
				reconnectCoords = this.initialBinlogCoordinates.Clone()
			}
			if !reconnectCoords.SmallerThan(this.GetCurrentBinlogCoordinates()) {
				successiveFailures += 1
			} else {
				successiveFailures = 0
			}

			this.migrationContext.Log.Infof("Reconnecting EventsStreamer... Will resume at %+v", reconnectCoords)
			_ = this.binlogReader.Close()
			if err := this.initBinlogReader(reconnectCoords); err != nil {
				return err
			}
		}
	}
}

func (this *EventsStreamer) Close() (err error) {
	err = this.binlogReader.Close()
	this.migrationContext.Log.Infof("Closed streamer connection. err=%+v", err)
	return err
}

func (this *EventsStreamer) Teardown() {
	this.db.Close()
}
