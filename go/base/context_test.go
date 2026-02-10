/*
   Copyright 2021 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package base

import (
	"os"
	"strings"
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

func TestGetTriggerNames(t *testing.T) {
	{
		context := NewMigrationContext()
		context.TriggerSuffix = "_gho"
		require.Equal(t, "my_trigger"+context.TriggerSuffix, context.GetGhostTriggerName("my_trigger"))
	}
	{
		context := NewMigrationContext()
		context.TriggerSuffix = "_gho"
		context.RemoveTriggerSuffix = true
		require.Equal(t, "my_trigger"+context.TriggerSuffix, context.GetGhostTriggerName("my_trigger"))
	}
	{
		context := NewMigrationContext()
		context.TriggerSuffix = "_gho"
		context.RemoveTriggerSuffix = true
		require.Equal(t, "my_trigger", context.GetGhostTriggerName("my_trigger_gho"))
	}
	{
		context := NewMigrationContext()
		context.TriggerSuffix = "_gho"
		context.RemoveTriggerSuffix = false
		require.Equal(t, "my_trigger_gho_gho", context.GetGhostTriggerName("my_trigger_gho"))
	}
}

func TestValidateGhostTriggerLengthBelowMaxLength(t *testing.T) {
	// Tests simulate the real call pattern: GetGhostTriggerName first, then validate the result.
	{
		// Short trigger name with suffix appended: well under 64 chars
		context := NewMigrationContext()
		context.TriggerSuffix = "_gho"
		ghostName := context.GetGhostTriggerName("my_trigger") // "my_trigger_gho" = 14 chars
		require.True(t, context.ValidateGhostTriggerLengthBelowMaxLength(ghostName))
	}
	{
		// 64-char original + "_ghost" suffix = 70 chars → exceeds limit
		context := NewMigrationContext()
		context.TriggerSuffix = "_ghost"
		ghostName := context.GetGhostTriggerName(strings.Repeat("my_trigger_ghost", 4)) // 64 + 6 = 70
		require.False(t, context.ValidateGhostTriggerLengthBelowMaxLength(ghostName))
	}
	{
		// 48-char original + "_ghost" suffix = 54 chars → valid
		context := NewMigrationContext()
		context.TriggerSuffix = "_ghost"
		ghostName := context.GetGhostTriggerName(strings.Repeat("my_trigger_ghost", 3)) // 48 + 6 = 54
		require.True(t, context.ValidateGhostTriggerLengthBelowMaxLength(ghostName))
	}
	{
		// RemoveTriggerSuffix: 64-char name ending in "_ghost" → suffix removed → 58 chars → valid
		context := NewMigrationContext()
		context.TriggerSuffix = "_ghost"
		context.RemoveTriggerSuffix = true
		ghostName := context.GetGhostTriggerName(strings.Repeat("my_trigger_ghost", 4)) // suffix removed → 58
		require.True(t, context.ValidateGhostTriggerLengthBelowMaxLength(ghostName))
	}
	{
		// RemoveTriggerSuffix: name doesn't end in suffix → suffix appended → 65 + 6 = 71 chars → exceeds
		context := NewMigrationContext()
		context.TriggerSuffix = "_ghost"
		context.RemoveTriggerSuffix = true
		ghostName := context.GetGhostTriggerName(strings.Repeat("my_trigger_ghost", 4) + "X") // no match, appended → 71
		require.False(t, context.ValidateGhostTriggerLengthBelowMaxLength(ghostName))
	}
	{
		// RemoveTriggerSuffix: 70-char name ending in "_ghost" → suffix removed → 64 chars → exactly at limit → valid
		context := NewMigrationContext()
		context.TriggerSuffix = "_ghost"
		context.RemoveTriggerSuffix = true
		ghostName := context.GetGhostTriggerName(strings.Repeat("my_trigger_ghost", 4) + "_ghost") // suffix removed → 64
		require.True(t, context.ValidateGhostTriggerLengthBelowMaxLength(ghostName))
	}
	{
		// Edge case: exactly 64 chars after transformation → valid (boundary test)
		context := NewMigrationContext()
		context.TriggerSuffix = "_ght"
		originalName := strings.Repeat("x", 60)                // 60 chars
		ghostName := context.GetGhostTriggerName(originalName) // 60 + 4 = 64
		require.Equal(t, 64, len(ghostName))
		require.True(t, context.ValidateGhostTriggerLengthBelowMaxLength(ghostName))
	}
	{
		// Edge case: 65 chars after transformation → exceeds (boundary test)
		context := NewMigrationContext()
		context.TriggerSuffix = "_ght"
		originalName := strings.Repeat("x", 61)                // 61 chars
		ghostName := context.GetGhostTriggerName(originalName) // 61 + 4 = 65
		require.Equal(t, 65, len(ghostName))
		require.False(t, context.ValidateGhostTriggerLengthBelowMaxLength(ghostName))
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
