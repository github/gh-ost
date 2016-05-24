/*
   Copyright 2016 GitHub Inc.
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

	"github.com/outbrain/golib/log"
	"github.com/outbrain/golib/sqlutils"
)

type BinlogEventListener struct {
	async        bool
	databaseName string
	tableName    string
	onDmlEvent   func(event *binlog.BinlogDMLEvent) error
}

const (
	EventsChannelBufferSize       = 1
	ReconnectStreamerSleepSeconds = 5
)

// EventsStreamer reads data from binary logs and streams it on. It acts as a publisher,
// and interested parties may subscribe for per-table events.
type EventsStreamer struct {
	connectionConfig         *mysql.ConnectionConfig
	db                       *gosql.DB
	migrationContext         *base.MigrationContext
	initialBinlogCoordinates *mysql.BinlogCoordinates
	listeners                [](*BinlogEventListener)
	listenersMutex           *sync.Mutex
	eventsChannel            chan *binlog.BinlogEntry
	binlogReader             *binlog.GoMySQLReader
}

func NewEventsStreamer() *EventsStreamer {
	return &EventsStreamer{
		connectionConfig: base.GetMigrationContext().InspectorConnectionConfig,
		migrationContext: base.GetMigrationContext(),
		listeners:        [](*BinlogEventListener){},
		listenersMutex:   &sync.Mutex{},
		eventsChannel:    make(chan *binlog.BinlogEntry, EventsChannelBufferSize),
	}
}

func (this *EventsStreamer) AddListener(
	async bool, databaseName string, tableName string, onDmlEvent func(event *binlog.BinlogDMLEvent) error) (err error) {

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

func (this *EventsStreamer) notifyListeners(binlogEvent *binlog.BinlogDMLEvent) {
	this.listenersMutex.Lock()
	defer this.listenersMutex.Unlock()

	for _, listener := range this.listeners {
		listener := listener
		if strings.ToLower(listener.databaseName) != strings.ToLower(binlogEvent.DatabaseName) {
			continue
		}
		if strings.ToLower(listener.tableName) != strings.ToLower(binlogEvent.TableName) {
			continue
		}
		if listener.async {
			go func() {
				listener.onDmlEvent(binlogEvent)
			}()
		} else {
			listener.onDmlEvent(binlogEvent)
		}
	}
}

func (this *EventsStreamer) InitDBConnections() (err error) {
	EventsStreamerUri := this.connectionConfig.GetDBUri(this.migrationContext.DatabaseName)
	if this.db, _, err = sqlutils.GetDB(EventsStreamerUri); err != nil {
		return err
	}
	if err := this.validateConnection(); err != nil {
		return err
	}
	if err := this.readCurrentBinlogCoordinates(); err != nil {
		return err
	}
	if err := this.initBinlogReader(this.initialBinlogCoordinates); err != nil {
		return err
	}

	return nil
}

func (this *EventsStreamer) initBinlogReader(binlogCoordinates *mysql.BinlogCoordinates) error {
	goMySQLReader, err := binlog.NewGoMySQLReader(this.migrationContext.InspectorConnectionConfig)
	if err != nil {
		return err
	}
	if err := goMySQLReader.ConnectBinlogStreamer(*binlogCoordinates); err != nil {
		return err
	}
	this.binlogReader = goMySQLReader
	return nil
}

// validateConnection issues a simple can-connect to MySQL
func (this *EventsStreamer) validateConnection() error {
	query := `select @@global.port`
	var port int
	if err := this.db.QueryRow(query).Scan(&port); err != nil {
		return err
	}
	if port != this.connectionConfig.Key.Port {
		return fmt.Errorf("Unexpected database port reported: %+v", port)
	}
	log.Infof("connection validated on %+v", this.connectionConfig.Key)
	return nil
}

func (this *EventsStreamer) GetCurrentBinlogCoordinates() *mysql.BinlogCoordinates {
	return this.binlogReader.GetCurrentBinlogCoordinates()
}

func (this *EventsStreamer) GetReconnectBinlogCoordinates() *mysql.BinlogCoordinates {
	return &mysql.BinlogCoordinates{LogFile: this.GetCurrentBinlogCoordinates().LogFile, LogPos: 4}
}

// validateGrants verifies the user by which we're executing has necessary grants
// to do its thang.
func (this *EventsStreamer) readCurrentBinlogCoordinates() error {
	query := `show /* gh-ost readCurrentBinlogCoordinates */ master status`
	foundMasterStatus := false
	err := sqlutils.QueryRowsMap(this.db, query, func(m sqlutils.RowMap) error {
		this.initialBinlogCoordinates = &mysql.BinlogCoordinates{
			LogFile: m.GetString("File"),
			LogPos:  m.GetInt64("Position"),
		}
		foundMasterStatus = true

		return nil
	})
	if err != nil {
		return err
	}
	if !foundMasterStatus {
		return fmt.Errorf("Got no results from SHOW MASTER STATUS. Bailing out")
	}
	log.Debugf("Streamer binlog coordinates: %+v", *this.initialBinlogCoordinates)
	return nil
}

// StreamEvents will begin streaming events. It will be blocking, so should be
// executed by a goroutine
func (this *EventsStreamer) StreamEvents(canStopStreaming func() bool) error {
	go func() {
		for binlogEntry := range this.eventsChannel {
			if binlogEntry.DmlEvent != nil {
				this.notifyListeners(binlogEntry.DmlEvent)
			}
		}
	}()
	// The next should block and execute forever, unless there's a serious error
	for {
		if err := this.binlogReader.StreamEvents(canStopStreaming, this.eventsChannel); err != nil {
			log.Infof("StreamEvents encountered unexpected error: %+v", err)
			time.Sleep(ReconnectStreamerSleepSeconds * time.Second)

			// Reposition at same binlog file. Single attempt (TODO: make multiple attempts?)
			lastAppliedRowsEventHint := this.binlogReader.LastAppliedRowsEventHint
			log.Infof("Reconnecting... Will resume at %+v", lastAppliedRowsEventHint)
			// if err := this.binlogReader.Reconnect(); err != nil {
			// 	return err
			// }
			if err := this.initBinlogReader(this.GetReconnectBinlogCoordinates()); err != nil {
				return err
			}
			this.binlogReader.LastAppliedRowsEventHint = lastAppliedRowsEventHint
		}
	}
}
