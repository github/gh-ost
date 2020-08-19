/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package binlog

import (
	"fmt"
	"strings"

	"github.com/github/gh-ost/go/sql"
)

type EventDML string

const (
	NotDML    EventDML = "NoDML"
	InsertDML EventDML = "Insert"
	UpdateDML EventDML = "Update"
	DeleteDML EventDML = "Delete"
)

func ToEventDML(description string) EventDML {
	// description can be a statement (`UPDATE my_table ...`) or a RBR event name (`UpdateRowsEventV2`)
	description = strings.TrimSpace(strings.Split(description, " ")[0])
	switch strings.ToLower(description) {
	case "insert":
		return InsertDML
	case "update":
		return UpdateDML
	case "delete":
		return DeleteDML
	}
	if strings.HasPrefix(description, "WriteRows") {
		return InsertDML
	}
	if strings.HasPrefix(description, "UpdateRows") {
		return UpdateDML
	}
	if strings.HasPrefix(description, "DeleteRows") {
		return DeleteDML
	}
	return NotDML
}

// BinlogDMLEvent is a binary log rows (DML) event entry, with data
type BinlogDMLEvent struct {
	DatabaseName      string
	TableName         string
	DML               EventDML
	WhereColumnValues *sql.ColumnValues
	NewColumnValues   *sql.ColumnValues
}

func NewBinlogDMLEvent(databaseName, tableName string, dml EventDML) *BinlogDMLEvent {
	event := &BinlogDMLEvent{
		DatabaseName: databaseName,
		TableName:    tableName,
		DML:          dml,
	}
	return event
}

func (this *BinlogDMLEvent) String() string {
	return fmt.Sprintf("[%+v on %s:%s]", this.DML, this.DatabaseName, this.TableName)
}
