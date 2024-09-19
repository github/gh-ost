/*
   Copyright 2022 GitHub Inc.
         See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"testing"

	test "github.com/openark/golib/tests"

	"github.com/github/gh-ost/go/sql"
)

func TestInspectGetSharedUniqueKeys(t *testing.T) {
	origUniqKeys := []*sql.UniqueKey{
		{Columns: *sql.NewColumnList([]string{"id", "item_id"})},
		{Columns: *sql.NewColumnList([]string{"id", "org_id"})},
		{Columns: *sql.NewColumnList([]string{"id"})},
	}
	ghostUniqKeys := []*sql.UniqueKey{
		{Columns: *sql.NewColumnList([]string{"id", "item_id"})},
		{Columns: *sql.NewColumnList([]string{"id", "org_id"})},
		{Columns: *sql.NewColumnList([]string{"item_id", "user_id"})},
	}
	inspector := &Inspector{}
	sharedUniqKeys := inspector.getSharedUniqueKeys(origUniqKeys, ghostUniqKeys)
	test.S(t).ExpectEquals(len(sharedUniqKeys), 3)
	test.S(t).ExpectEquals(sharedUniqKeys[0].Columns.String(), "id,item_id")
	test.S(t).ExpectEquals(sharedUniqKeys[1].Columns.String(), "id,org_id")
	test.S(t).ExpectEquals(sharedUniqKeys[2].Columns.String(), "id")
}
