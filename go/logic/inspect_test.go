/*
   Copyright 2022 GitHub Inc.
         See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"testing"

	"github.com/github/gh-ost/go/sql"
	"github.com/stretchr/testify/require"
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
	require.Len(t, sharedUniqKeys, 3)
	require.Equal(t, "id,item_id", sharedUniqKeys[0].Columns.String())
	require.Equal(t, "id,org_id", sharedUniqKeys[1].Columns.String())
	require.Equal(t, "id", sharedUniqKeys[2].Columns.String())
}
