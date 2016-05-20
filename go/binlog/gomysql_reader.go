/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package binlog

import (
	"fmt"

	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"

	"github.com/outbrain/golib/log"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

var ()

const (
	serverId = 99999
)

type GoMySQLReader struct {
	connectionConfig       *mysql.ConnectionConfig
	binlogSyncer           *replication.BinlogSyncer
	binlogStreamer         *replication.BinlogStreamer
	tableMap               map[uint64]string
	currentCoordinates     mysql.BinlogCoordinates
	lastHandledCoordinates mysql.BinlogCoordinates
}

func NewGoMySQLReader(connectionConfig *mysql.ConnectionConfig) (binlogReader *GoMySQLReader, err error) {
	binlogReader = &GoMySQLReader{
		connectionConfig:   connectionConfig,
		tableMap:           make(map[uint64]string),
		currentCoordinates: mysql.BinlogCoordinates{},
		binlogStreamer:     nil,
	}
	binlogReader.binlogSyncer = replication.NewBinlogSyncer(serverId, "mysql")

	return binlogReader, err
}

// ConnectBinlogStreamer
func (this *GoMySQLReader) ConnectBinlogStreamer(coordinates mysql.BinlogCoordinates) (err error) {
	if coordinates.IsEmpty() {
		return log.Errorf("Emptry coordinates at ConnectBinlogStreamer()")
	}
	log.Infof("Registering replica at %+v:%+v", this.connectionConfig.Key.Hostname, uint16(this.connectionConfig.Key.Port))
	if err := this.binlogSyncer.RegisterSlave(this.connectionConfig.Key.Hostname, uint16(this.connectionConfig.Key.Port), this.connectionConfig.User, this.connectionConfig.Password); err != nil {
		return err
	}

	this.currentCoordinates = coordinates
	log.Infof("Connecting binlog streamer at %+v", this.currentCoordinates)
	// Start sync with sepcified binlog file and position
	this.binlogStreamer, err = this.binlogSyncer.StartSync(gomysql.Position{this.currentCoordinates.LogFile, uint32(this.currentCoordinates.LogPos)})

	return err
}

func (this *GoMySQLReader) Reconnect() error {
	this.binlogSyncer.Close()

	connectCoordinates := &this.lastHandledCoordinates
	if connectCoordinates.IsEmpty() {
		connectCoordinates = &this.currentCoordinates
	}
	if err := this.ConnectBinlogStreamer(*connectCoordinates); err != nil {
		return err
	}
	return nil
}

func (this *GoMySQLReader) GetCurrentBinlogCoordinates() *mysql.BinlogCoordinates {
	return &this.currentCoordinates
}

// StreamEvents
func (this *GoMySQLReader) handleRowsEvent(ev *replication.BinlogEvent, rowsEvent *replication.RowsEvent, entriesChannel chan<- *BinlogEntry) error {
	if this.currentCoordinates.SmallerThanOrEquals(&this.lastHandledCoordinates) && !this.lastHandledCoordinates.IsEmpty() {
		log.Infof("Skipping handled query at %+v", this.currentCoordinates)
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
		// decides whether action is taken sycnhronously (meaning we wait before
		// next iteration) or asynchronously (we keep pushing more events)
		// In reality, reads will be synchronous
		entriesChannel <- binlogEntry
	}
	return nil
}

// StreamEvents
func (this *GoMySQLReader) StreamEvents(canStopStreaming func() bool, entriesChannel chan<- *BinlogEntry) error {
	for {
		if canStopStreaming() {
			break
		}
		ev, err := this.binlogStreamer.GetEvent()
		if err != nil {
			return err
		}
		this.currentCoordinates.LogPos = int64(ev.Header.LogPos)
		if rotateEvent, ok := ev.Event.(*replication.RotateEvent); ok {
			this.currentCoordinates.LogFile = string(rotateEvent.NextLogName)
			log.Infof("rotate to next log name: %s", rotateEvent.NextLogName)
		} else if tableMapEvent, ok := ev.Event.(*replication.TableMapEvent); ok {
			// Actually not being used, since Table is available in RowsEvent.
			// Keeping this here in case I'm wrong about this. Sometime in the near
			// future I should remove this.
			this.tableMap[tableMapEvent.TableID] = string(tableMapEvent.Table)
		} else if rowsEvent, ok := ev.Event.(*replication.RowsEvent); ok {
			if err := this.handleRowsEvent(ev, rowsEvent, entriesChannel); err != nil {
				return err
			}
		}
		this.lastHandledCoordinates = this.currentCoordinates
	}
	log.Debugf("done streaming events")

	return nil
}
