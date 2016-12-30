/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package base

import (
	"io"
	"testing"

	"github.com/outbrain/golib/log"
	test "github.com/outbrain/golib/tests"

	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"
)

func init() {
	log.SetLevel(log.ERROR)
}

func TestContextToJSON(t *testing.T) {
	context := NewMigrationContext()
	jsonString, err := context.ToJSON()
	test.S(t).ExpectNil(err)
	test.S(t).ExpectNotEquals(jsonString, "")
}

func TestContextLoadJSON(t *testing.T) {
	var jsonString string
	var err error
	{
		context := NewMigrationContext()
		context.AppliedBinlogCoordinates = mysql.BinlogCoordinates{LogFile: "mysql-bin.012345", LogPos: 6789}

		abstractValues := []interface{}{31, "2016-12-24 17:04:32"}
		context.MigrationRangeMinValues = sql.ToColumnValues(abstractValues)

		jsonString, err = context.ToJSON()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectNotEquals(jsonString, "")
	}
	{
		context := NewMigrationContext()
		err = context.LoadJSON(jsonString)
		test.S(t).ExpectEqualsAny(err, nil, io.EOF)
		test.S(t).ExpectEquals(context.AppliedBinlogCoordinates, mysql.BinlogCoordinates{LogFile: "mysql-bin.012345", LogPos: 6789})

		abstractValues := context.MigrationRangeMinValues.AbstractValues()
		test.S(t).ExpectEquals(len(abstractValues), 2)
		test.S(t).ExpectEquals(abstractValues[0], 31)
		test.S(t).ExpectEquals(abstractValues[1], "2016-12-24 17:04:32")
	}
}
