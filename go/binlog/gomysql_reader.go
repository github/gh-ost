/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-osc/blob/master/LICENSE
*/

package binlog

import (
	"fmt"
	"strings"

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
	tableMap           map[uint64]string
	currentCoordinates mysql.BinlogCoordinates
}

func NewGoMySQLReader(connectionConfig *mysql.ConnectionConfig) (binlogReader *GoMySQLReader, err error) {
	binlogReader = &GoMySQLReader{
		connectionConfig:   connectionConfig,
		tableMap:           make(map[uint64]string),
		currentCoordinates: mysql.BinlogCoordinates{},
	}
	binlogReader.binlogSyncer = replication.NewBinlogSyncer(serverId, "mysql")

	// Register slave, the MySQL master is at 127.0.0.1:3306, with user root and an empty password
	err = binlogReader.binlogSyncer.RegisterSlave(connectionConfig.Key.Hostname, uint16(connectionConfig.Key.Port), connectionConfig.User, connectionConfig.Password)
	if err != nil {
		return binlogReader, err
	}

	return binlogReader, err
}

func (this *GoMySQLReader) isDMLEvent(event *replication.BinlogEvent) bool {
	eventType := event.Header.EventType.String()
	if strings.HasPrefix(eventType, "WriteRows") {
		return true
	}
	if strings.HasPrefix(eventType, "UpdateRows") {
		return true
	}
	if strings.HasPrefix(eventType, "DeleteRows") {
		return true
	}
	return false
}

// ReadEntries will read binlog entries from parsed text output of `mysqlbinlog` utility
func (this *GoMySQLReader) ReadEntries(logFile string, startPos uint64, stopPos uint64) (entries [](*BinlogEntry), err error) {
	this.currentCoordinates.LogFile = logFile
	// Start sync with sepcified binlog file and position
	streamer, err := this.binlogSyncer.StartSync(gomysql.Position{logFile, uint32(startPos)})
	if err != nil {
		return entries, err
	}

	for {
		ev, err := streamer.GetEvent()
		if err != nil {
			return entries, err
		}
		this.currentCoordinates.LogPos = int64(ev.Header.LogPos)
		log.Infof("at: %+v", this.currentCoordinates)
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
				return entries, fmt.Errorf("Unknown DML type: %s", ev.Header.EventType.String())
			}
			for i, row := range rowsEvent.Rows {
				if dml == UpdateDML && i%2 == 1 {
					// An update has two rows (WHERE+SET)
					// We do both at the same time
					continue
				}
				binlogEntry := NewBinlogEntryAt(this.currentCoordinates)
				binlogEntry.dmlEvent = NewBinlogDMLEvent(
					string(rowsEvent.Table.Schema),
					string(rowsEvent.Table.Table),
					dml,
				)
				switch dml {
				case InsertDML:
					{
						binlogEntry.dmlEvent.NewColumnValues = sql.ToColumnValues(row)
						log.Debugf("insert: %+v", binlogEntry.dmlEvent.NewColumnValues)
					}
				case UpdateDML:
					{
						binlogEntry.dmlEvent.WhereColumnValues = sql.ToColumnValues(row)
						binlogEntry.dmlEvent.NewColumnValues = sql.ToColumnValues(rowsEvent.Rows[i+1])
						log.Debugf("update: %+v where %+v", binlogEntry.dmlEvent.NewColumnValues, binlogEntry.dmlEvent.WhereColumnValues)
					}
				case DeleteDML:
					{
						binlogEntry.dmlEvent.WhereColumnValues = sql.ToColumnValues(row)
						log.Debugf("delete: %+v", binlogEntry.dmlEvent.WhereColumnValues)
					}
				}
			}
		}
	}
	log.Debugf("done")
	return entries, err
}
