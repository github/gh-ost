/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-osc/blob/master/LICENSE
*/

package binlog

import (
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/github/gh-osc/go/mysql"
	"github.com/outbrain/golib/log"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

var ()

const (
	serverId = 99999
)

type GoMySQLReader struct {
	connectionConfig *mysql.ConnectionConfig
	binlogSyncer     *replication.BinlogSyncer
}

func NewGoMySQLReader(connectionConfig *mysql.ConnectionConfig) (binlogReader *GoMySQLReader, err error) {
	binlogReader = &GoMySQLReader{
		connectionConfig: connectionConfig,
	}
	binlogReader.binlogSyncer = replication.NewBinlogSyncer(serverId, "mysql")

	// Register slave, the MySQL master is at 127.0.0.1:3306, with user root and an empty password
	err = binlogReader.binlogSyncer.RegisterSlave(connectionConfig.Hostname, uint16(connectionConfig.Port), connectionConfig.User, connectionConfig.Password)
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
		if rowsEvent, ok := ev.Event.(*replication.RowsEvent); ok {
			if true {
				fmt.Println(ev.Header.EventType)
				fmt.Println(len(rowsEvent.Rows))

				for _, rows := range rowsEvent.Rows {
					for j, d := range rows {
						if _, ok := d.([]byte); ok {
							fmt.Print(fmt.Sprintf("yesbin %d:%q, %+v\n", j, d, reflect.TypeOf(d)))
						} else {
							fmt.Print(fmt.Sprintf("notbin %d:%#v, %+v\n", j, d, reflect.TypeOf(d)))
						}
					}
					fmt.Println("---")
				}
			} else {
				ev.Dump(os.Stdout)
			}
			// TODO : convert to entries
			// need to parse multi-row entries
			// insert & delete are just one row per db orw
			// update: where-row_>values-row, repeating
		}
	}
	log.Debugf("done")
	return entries, err
}
