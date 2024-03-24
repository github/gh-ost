/*
   Copyright 2023 GitHub Inc.
         See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"testing"

	test "github.com/openark/golib/tests"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"
)

func TestInspectGetSharedUniqueKeys(t *testing.T) {
	origUniqKeys := []*sql.UniqueKey{
		{Columns: *sql.NewColumnList([]string{"id", "item_id"})},
		{Columns: *sql.NewColumnList([]string{"id", "org_id"})},
	}
	ghostUniqKeys := []*sql.UniqueKey{
		{Columns: *sql.NewColumnList([]string{"id", "item_id"})},
		{Columns: *sql.NewColumnList([]string{"id", "org_id"})},
		{Columns: *sql.NewColumnList([]string{"item_id", "user_id"})},
	}
	inspector := &Inspector{}
	sharedUniqKeys := inspector.getSharedUniqueKeys(origUniqKeys, ghostUniqKeys)
	test.S(t).ExpectEquals(len(sharedUniqKeys), 2)
	test.S(t).ExpectEquals(sharedUniqKeys[0].Columns.String(), "id,item_id")
	test.S(t).ExpectEquals(sharedUniqKeys[1].Columns.String(), "id,org_id")
}

func TestRequiresBinlogFormatChange(t *testing.T) {
	migrationContext := &base.MigrationContext{
		InspectorServerInfo: &mysql.ServerInfo{},
	}
	inspector := &Inspector{migrationContext: migrationContext}
	{
		migrationContext.InspectorServerInfo.BinlogFormat = "ROW"
		test.S(t).ExpectFalse(inspector.RequiresBinlogFormatChange())
	}
	{
		migrationContext.InspectorServerInfo.BinlogFormat = ""
		test.S(t).ExpectTrue(inspector.RequiresBinlogFormatChange())
	}
	{
		migrationContext.InspectorServerInfo.BinlogFormat = "MINIMAL"
		test.S(t).ExpectTrue(inspector.RequiresBinlogFormatChange())
	}
	{
		migrationContext.InspectorServerInfo.BinlogFormat = "MIXED"
		test.S(t).ExpectTrue(inspector.RequiresBinlogFormatChange())
	}
	{
		migrationContext.InspectorServerInfo = nil
		test.S(t).ExpectTrue(inspector.RequiresBinlogFormatChange())
	}
}
