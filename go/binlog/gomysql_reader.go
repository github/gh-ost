/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package binlog

import (
	"fmt"
	"sync"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"

	"github.com/outbrain/golib/log"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"golang.org/x/net/context"
)

type GoMySQLReader struct {
	connectionConfig         *mysql.ConnectionConfig
	binlogSyncer             *replication.BinlogSyncer
	binlogStreamer           *replication.BinlogStreamer
	currentCoordinates       mysql.BinlogCoordinates
	currentCoordinatesMutex  *sync.Mutex
	LastAppliedRowsEventHint mysql.BinlogCoordinates
	MigrationContext         *base.MigrationContext
}

func NewGoMySQLReader(connectionConfig *mysql.ConnectionConfig) (binlogReader *GoMySQLReader, err error) {
	binlogReader = &GoMySQLReader{
		connectionConfig:        connectionConfig,
		currentCoordinates:      mysql.BinlogCoordinates{},
		currentCoordinatesMutex: &sync.Mutex{},
		binlogSyncer:            nil,
		binlogStreamer:          nil,
		MigrationContext:        base.GetMigrationContext(),
	}

	serverId := uint32(binlogReader.MigrationContext.ReplicaServerId)

	binlogSyncerConfig := &replication.BinlogSyncerConfig{
		ServerID: serverId,
		Flavor:   "mysql",
		Host:     connectionConfig.Key.Hostname,
		Port:     uint16(connectionConfig.Key.Port),
		User:     connectionConfig.User,
		Password: connectionConfig.Password,
	}
	binlogReader.binlogSyncer = replication.NewBinlogSyncer(binlogSyncerConfig)

	return binlogReader, err
}

// ConnectBinlogStreamer
func (this *GoMySQLReader) ConnectBinlogStreamer(coordinates mysql.BinlogCoordinates) (err error) {
	if coordinates.IsEmpty() {
		return log.Errorf("Empty coordinates at ConnectBinlogStreamer()")
	}

	this.currentCoordinates = coordinates
	log.Infof("Connecting binlog streamer at %+v", this.currentCoordinates)
	// Start sync with specified binlog file and position
	this.binlogStreamer, err = this.binlogSyncer.StartSync(gomysql.Position{this.currentCoordinates.LogFile, uint32(this.currentCoordinates.LogPos)})

	return err
}

func (this *GoMySQLReader) GetCurrentBinlogCoordinates() *mysql.BinlogCoordinates {
	this.currentCoordinatesMutex.Lock()
	defer this.currentCoordinatesMutex.Unlock()
	returnCoordinates := this.currentCoordinates
	return &returnCoordinates
}

// StreamEvents
func (this *GoMySQLReader) handleRowsEvent(ev *replication.BinlogEvent, rowsEvent *replication.RowsEvent, entriesChannel chan<- *BinlogEntry) error {
	if this.currentCoordinates.SmallerThanOrEquals(&this.LastAppliedRowsEventHint) {
		log.Debugf("Skipping handled query at %+v", this.currentCoordinates)
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
		// The channel will do the throttling. Whoever is reding from the channel
		// decides whether action is taken synchronously (meaning we wait before
		// next iteration) or asynchronously (we keep pushing more events)
		// In reality, reads will be synchronous
		entriesChannel <- binlogEntry
	}
	this.LastAppliedRowsEventHint = this.currentCoordinates
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
		func() {
			this.currentCoordinatesMutex.Lock()
			defer this.currentCoordinatesMutex.Unlock()
			this.currentCoordinates.LogPos = int64(ev.Header.LogPos)
		}()
		if rotateEvent, ok := ev.Event.(*replication.RotateEvent); ok {
			func() {
				this.currentCoordinatesMutex.Lock()
				defer this.currentCoordinatesMutex.Unlock()
				this.currentCoordinates.LogFile = string(rotateEvent.NextLogName)
			}()
			log.Infof("rotate to next log name: %s", rotateEvent.NextLogName)
		} else if rowsEvent, ok := ev.Event.(*replication.RowsEvent); ok {
			if err := this.handleRowsEvent(ev, rowsEvent, entriesChannel); err != nil {
				return err
			}
		}
	}
	log.Debugf("done streaming events")

	return nil
}

func (this *GoMySQLReader) Close() error {
	// Historically there was a:
	//   this.binlogSyncer.Close()
	// here. A new go-mysql version closes the binlog syncer connection independently.
	// I will go against the sacred rules of comments and just leave this here.
	// This is the year 2017. Let's see what year these comments get deleted.
	return nil
}
