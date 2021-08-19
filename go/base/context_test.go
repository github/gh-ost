/*
   Copyright 2021 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package base

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/openark/golib/log"
	test "github.com/openark/golib/tests"
)

func init() {
	log.SetLevel(log.ERROR)
}

func TestGetTableNames(t *testing.T) {
	{
		context := NewMigrationContext()
		context.OriginalTableName = "some_table"
		test.S(t).ExpectEquals(context.GetOldTableName(), "_some_table_del")
		test.S(t).ExpectEquals(context.GetGhostTableName(), "_some_table_gho")
		test.S(t).ExpectEquals(context.GetChangelogTableName(), "_some_table_ghc")
	}
	{
		context := NewMigrationContext()
		context.OriginalTableName = "a123456789012345678901234567890123456789012345678901234567890"
		test.S(t).ExpectEquals(context.GetOldTableName(), "_a1234567890123456789012345678901234567890123456789012345678_del")
		test.S(t).ExpectEquals(context.GetGhostTableName(), "_a1234567890123456789012345678901234567890123456789012345678_gho")
		test.S(t).ExpectEquals(context.GetChangelogTableName(), "_a1234567890123456789012345678901234567890123456789012345678_ghc")
	}
	{
		context := NewMigrationContext()
		context.OriginalTableName = "a123456789012345678901234567890123456789012345678901234567890123"
		oldTableName := context.GetOldTableName()
		test.S(t).ExpectEquals(oldTableName, "_a1234567890123456789012345678901234567890123456789012345678_del")
	}
	{
		context := NewMigrationContext()
		context.OriginalTableName = "a123456789012345678901234567890123456789012345678901234567890123"
		context.TimestampOldTable = true
		longForm := "Jan 2, 2006 at 3:04pm (MST)"
		context.StartTime, _ = time.Parse(longForm, "Feb 3, 2013 at 7:54pm (PST)")
		oldTableName := context.GetOldTableName()
		test.S(t).ExpectEquals(oldTableName, "_a1234567890123456789012345678901234567890123_20130203195400_del")
	}
	{
		context := NewMigrationContext()
		context.OriginalTableName = "foo_bar_baz"
		context.ForceTmpTableName = "tmp"
		test.S(t).ExpectEquals(context.GetOldTableName(), "_tmp_del")
		test.S(t).ExpectEquals(context.GetGhostTableName(), "_tmp_gho")
		test.S(t).ExpectEquals(context.GetChangelogTableName(), "_tmp_ghc")
	}
}

func TestGetTriggerNames(t *testing.T) {
	{
		context := NewMigrationContext()
		context.TriggerSuffix = "_gho"
		test.S(t).ExpectEquals(context.GetGhostTriggerName("my_trigger"), "my_trigger"+context.TriggerSuffix)
	}
	{
		context := NewMigrationContext()
		context.TriggerSuffix = "_gho"
		context.RemoveTriggerSuffix = true
		test.S(t).ExpectEquals(context.GetGhostTriggerName("my_trigger"), "my_trigger"+context.TriggerSuffix)
	}
	{
		context := NewMigrationContext()
		context.TriggerSuffix = "_gho"
		context.RemoveTriggerSuffix = true
		test.S(t).ExpectEquals(context.GetGhostTriggerName("my_trigger_gho"), "my_trigger")
	}
	{
		context := NewMigrationContext()
		context.TriggerSuffix = "_gho"
		context.RemoveTriggerSuffix = false
		test.S(t).ExpectEquals(context.GetGhostTriggerName("my_trigger_gho"), "my_trigger_gho_gho")
	}

}
func TestValidateGhostTriggerLengthBelow65chars(t *testing.T) {
	{
		context := NewMigrationContext()
		context.TriggerSuffix = "_gho"
		test.S(t).ExpectEquals(context.ValidateGhostTriggerLengthBelow65chars("my_trigger"), true)
	}
	{
		context := NewMigrationContext()
		context.TriggerSuffix = "_ghost"
		test.S(t).ExpectEquals(context.ValidateGhostTriggerLengthBelow65chars(strings.Repeat("my_trigger_ghost", 4)), false) // 64 characters + "_ghost"
	}
	{
		context := NewMigrationContext()
		context.TriggerSuffix = "_ghost"
		test.S(t).ExpectEquals(context.ValidateGhostTriggerLengthBelow65chars(strings.Repeat("my_trigger_ghost", 3)), true) // 48 characters + "_ghost"
	}
	{
		context := NewMigrationContext()
		context.TriggerSuffix = "_ghost"
		context.RemoveTriggerSuffix = true
		test.S(t).ExpectEquals(context.ValidateGhostTriggerLengthBelow65chars(strings.Repeat("my_trigger_ghost", 4)), true) // 64 characters + "_ghost" removed
	}
	{
		context := NewMigrationContext()
		context.TriggerSuffix = "_ghost"
		context.RemoveTriggerSuffix = true
		test.S(t).ExpectEquals(context.ValidateGhostTriggerLengthBelow65chars(strings.Repeat("my_trigger_ghost", 4)+"X"), false) // 65 characters + "_ghost" not removed
	}
	{
		context := NewMigrationContext()
		context.TriggerSuffix = "_ghost"
		context.RemoveTriggerSuffix = true
		test.S(t).ExpectEquals(context.ValidateGhostTriggerLengthBelow65chars(strings.Repeat("my_trigger_ghost", 4)+"_ghost"), true) // 70 characters + last "_ghost"  removed
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
		f, err := ioutil.TempFile("", t.Name())
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
		f, err := ioutil.TempFile("", t.Name())
		if err != nil {
			t.Fatalf("Failed to create tmp file: %v", err)
		}
		defer os.Remove(f.Name())

		f.Write([]byte(fmt.Sprintf("[client]\nuser=test\npassword=123456")))
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
		f, err := ioutil.TempFile("", t.Name())
		if err != nil {
			t.Fatalf("Failed to create tmp file: %v", err)
		}
		defer os.Remove(f.Name())

		f.Write([]byte(fmt.Sprintf("[osc]\nmax_load=10")))
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
