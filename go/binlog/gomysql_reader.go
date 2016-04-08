/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-osc/blob/master/LICENSE
*/

package binlog

import (
	"fmt"

	"github.com/github/gh-osc/go/mysql"
	"github.com/github/gh-osc/go/sql"

	"github.com/outbrain/golib/log"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

var ()

const (
	serverId = 99999
)

type GoMySQLReader struct {
	connectionConfig   *mysql.ConnectionConfig
	binlogSyncer       *replication.BinlogSyncer
	binlogStreamer     *replication.BinlogStreamer
	tableMap           map[uint64]string
	currentCoordinates mysql.BinlogCoordinates
}

func NewGoMySQLReader(connectionConfig *mysql.ConnectionConfig) (binlogReader *GoMySQLReader, err error) {
	binlogReader = &GoMySQLReader{
		connectionConfig:   connectionConfig,
		tableMap:           make(map[uint64]string),
		currentCoordinates: mysql.BinlogCoordinates{},
		binlogStreamer:     nil,
	}
	binlogReader.binlogSyncer = replication.NewBinlogSyncer(serverId, "mysql")

	// Register slave, the MySQL master is at 127.0.0.1:3306, with user root and an empty password
	err = binlogReader.binlogSyncer.RegisterSlave(connectionConfig.Key.Hostname, uint16(connectionConfig.Key.Port), connectionConfig.User, connectionConfig.Password)
	if err != nil {
		return binlogReader, err
	}

	return binlogReader, err
}

// ConnectBinlogStreamer
func (this *GoMySQLReader) ConnectBinlogStreamer(coordinates mysql.BinlogCoordinates) (err error) {
	this.currentCoordinates = coordinates
	// Start sync with sepcified binlog file and position
	this.binlogStreamer, err = this.binlogSyncer.StartSync(gomysql.Position{coordinates.LogFile, uint32(coordinates.LogPos)})

	return err
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
		}
	}
	log.Debugf("done streaming events")

	return nil
}
