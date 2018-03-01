/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package base

import (
	"testing"
	"time"

	"github.com/outbrain/golib/log"
	test "github.com/outbrain/golib/tests"
)

func init() {
	log.SetLevel(log.ERROR)
}

func TestGetTableNames(t *testing.T) {
	{
		context := NewMigrationContext()
		context.SrcTableName = "some_table"
		context.DstTableName = "some_table"
		test.S(t).ExpectEquals(context.GetOldTableName(), "_some_table_del")
		test.S(t).ExpectEquals(context.GetGhostTableName(), "_some_table_gho")
		test.S(t).ExpectEquals(context.GetChangelogTableName(), "_some_table_ghc")
	}
	{
		context := NewMigrationContext()
		context.SrcTableName = "a123456789012345678901234567890123456789012345678901234567890"
		context.DstTableName = "a123456789012345678901234567890123456789012345678901234567890"
		test.S(t).ExpectEquals(context.GetOldTableName(), "_a1234567890123456789012345678901234567890123456789012345678_del")
		test.S(t).ExpectEquals(context.GetGhostTableName(), "_a1234567890123456789012345678901234567890123456789012345678_gho")
		test.S(t).ExpectEquals(context.GetChangelogTableName(), "_a1234567890123456789012345678901234567890123456789012345678_ghc")
	}
	{
		context := NewMigrationContext()
		context.SrcTableName = "a123456789012345678901234567890123456789012345678901234567890123"
		context.DstTableName = "a123456789012345678901234567890123456789012345678901234567890123"
		oldTableName := context.GetOldTableName()
		test.S(t).ExpectEquals(oldTableName, "_a1234567890123456789012345678901234567890123456789012345678_del")
	}
	{
		context := NewMigrationContext()
		context.SrcTableName = "a123456789012345678901234567890123456789012345678901234567890123"
		context.DstTableName = "a123456789012345678901234567890123456789012345678901234567890123"
		context.TimestampOldTable = true
		longForm := "Jan 2, 2006 at 3:04pm (MST)"
		context.StartTime, _ = time.Parse(longForm, "Feb 3, 2013 at 7:54pm (PST)")
		oldTableName := context.GetOldTableName()
		test.S(t).ExpectEquals(oldTableName, "_a1234567890123456789012345678901234567890123_20130203195400_del")
	}
	{
		context := NewMigrationContext()
		context.SrcTableName = "foo_bar_baz"
		context.DstTableName = "foo_bar_baz"
		context.ForceTmpTableName = "tmp"
		test.S(t).ExpectEquals(context.GetOldTableName(), "_tmp_del")
		test.S(t).ExpectEquals(context.GetGhostTableName(), "_tmp_gho")
		test.S(t).ExpectEquals(context.GetChangelogTableName(), "_tmp_ghc")
	}
	{
		context := NewMigrationContext()
		context.SrcTableName = "a1234567890"
		context.DstTableName = "a12345678901234567890"
		oldTableName := context.GetOldTableName()
		ghostTableName := context.GetGhostTableName()
		test.S(t).ExpectEquals(oldTableName, "_a1234567890_del")
		test.S(t).ExpectEquals(ghostTableName, "_a12345678901234567890_gho")
	}
}
