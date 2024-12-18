/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package base

import (
	"os"
	"testing"
	"time"

	"github.com/openark/golib/log"
	"github.com/stretchr/testify/require"
)

func init() {
	log.SetLevel(log.ERROR)
}

func TestGetTableNames(t *testing.T) {
	{
		context := NewMigrationContext()
		context.OriginalTableName = "some_table"
		require.Equal(t, "_some_table_del", context.GetOldTableName())
		require.Equal(t, "_some_table_gho", context.GetGhostTableName())
		require.Equal(t, "_some_table_ghc", context.GetChangelogTableName(), "_some_table_ghc")
	}
	{
		context := NewMigrationContext()
		context.OriginalTableName = "a123456789012345678901234567890123456789012345678901234567890"
		require.Equal(t, "_a1234567890123456789012345678901234567890123456789012345678_del", context.GetOldTableName())
		require.Equal(t, "_a1234567890123456789012345678901234567890123456789012345678_gho", context.GetGhostTableName())
		require.Equal(t, "_a1234567890123456789012345678901234567890123456789012345678_ghc", context.GetChangelogTableName())
	}
	{
		context := NewMigrationContext()
		context.OriginalTableName = "a123456789012345678901234567890123456789012345678901234567890123"
		oldTableName := context.GetOldTableName()
		require.Equal(t, "_a1234567890123456789012345678901234567890123456789012345678_del", oldTableName)
	}
	{
		context := NewMigrationContext()
		context.OriginalTableName = "a123456789012345678901234567890123456789012345678901234567890123"
		context.TimestampOldTable = true
		longForm := "Jan 2, 2006 at 3:04pm (MST)"
		context.StartTime, _ = time.Parse(longForm, "Feb 3, 2013 at 7:54pm (PST)")
		oldTableName := context.GetOldTableName()
		require.Equal(t, "_a1234567890123456789012345678901234567890123_20130203195400_del", oldTableName)
	}
	{
		context := NewMigrationContext()
		context.OriginalTableName = "foo_bar_baz"
		context.ForceTmpTableName = "tmp"
		require.Equal(t, "_tmp_del", context.GetOldTableName())
		require.Equal(t, "_tmp_gho", context.GetGhostTableName())
		require.Equal(t, "_tmp_ghc", context.GetChangelogTableName())
	}
}

func TestReadConfigFile(t *testing.T) {
	{
		context := NewMigrationContext()
		context.ConfigFile = "/does/not/exist"
		if err := context.ReadConfigFile(); err == nil {
			t.Fatal("Expected .ReadConfigFile() to return an error, got nil")
		}
	}
	{
		f, err := os.CreateTemp("", t.Name())
		if err != nil {
			t.Fatalf("Failed to create tmp file: %v", err)
		}
		defer os.Remove(f.Name())

		f.Write([]byte("[client]"))
		context := NewMigrationContext()
		context.ConfigFile = f.Name()
		if err := context.ReadConfigFile(); err != nil {
			t.Fatalf(".ReadConfigFile() failed: %v", err)
		}
	}
	{
		f, err := os.CreateTemp("", t.Name())
		if err != nil {
			t.Fatalf("Failed to create tmp file: %v", err)
		}
		defer os.Remove(f.Name())

		f.Write([]byte("[client]\nuser=test\npassword=123456"))
		context := NewMigrationContext()
		context.ConfigFile = f.Name()
		if err := context.ReadConfigFile(); err != nil {
			t.Fatalf(".ReadConfigFile() failed: %v", err)
		}

		if context.config.Client.User != "test" {
			t.Fatalf("Expected client user %q, got %q", "test", context.config.Client.User)
		} else if context.config.Client.Password != "123456" {
			t.Fatalf("Expected client password %q, got %q", "123456", context.config.Client.Password)
		}
	}
	{
		f, err := os.CreateTemp("", t.Name())
		if err != nil {
			t.Fatalf("Failed to create tmp file: %v", err)
		}
		defer os.Remove(f.Name())

		f.Write([]byte("[osc]\nmax_load=10"))
		context := NewMigrationContext()
		context.ConfigFile = f.Name()
		if err := context.ReadConfigFile(); err != nil {
			t.Fatalf(".ReadConfigFile() failed: %v", err)
		}

		if context.config.Osc.Max_Load != "10" {
			t.Fatalf("Expected osc 'max_load' %q, got %q", "10", context.config.Osc.Max_Load)
		}
	}
}
