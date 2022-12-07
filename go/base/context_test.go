/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package base

import (
	"io/ioutil"
	"os"
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
		f, err := ioutil.TempFile("", t.Name())
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

func TestDynamicChunker(t *testing.T) {
	context := NewMigrationContext()
	context.chunkSize = 1000
	context.DynamicChunking = true
	context.DynamicChunkSizeTargetMillis = 50

	// Before feedback it should match the static chunk size
	test.S(t).ExpectEquals(context.GetChunkSize(), int64(1000))

	// 1s is >5x the target, so it should immediately /10 the target
	context.ChunkDurationFeedback(1 * time.Second)
	test.S(t).ExpectEquals(context.GetChunkSize(), int64(100))

	// Let's provide 10 pieces of feedback, and see the chunk size
	// be adjusted based on the p90th value.
	context.ChunkDurationFeedback(time.Duration(33 * time.Millisecond)) // 1st
	context.ChunkDurationFeedback(time.Duration(33 * time.Millisecond)) // 2nd
	context.ChunkDurationFeedback(time.Duration(32 * time.Millisecond)) // 3rd
	context.ChunkDurationFeedback(time.Duration(40 * time.Millisecond))
	context.ChunkDurationFeedback(time.Duration(61 * time.Millisecond))
	context.ChunkDurationFeedback(time.Duration(37 * time.Millisecond))
	context.ChunkDurationFeedback(time.Duration(38 * time.Millisecond))
	context.ChunkDurationFeedback(time.Duration(35 * time.Millisecond))
	context.ChunkDurationFeedback(time.Duration(29 * time.Millisecond))
	test.S(t).ExpectEquals(context.GetChunkSize(), int64(100))          // 9th
	context.ChunkDurationFeedback(time.Duration(38 * time.Millisecond)) // 10th
	// Because 10 items of feedback have been received,
	// the chunk size is recalculated. The p90 is 40ms (below our target)
	// so the adjusted chunk size increases 25% to 125
	test.S(t).ExpectEquals(context.GetChunkSize(), int64(125))

	// Collect some new feedback where the p90 is 500us (much lower than our target)
	// We have boundary checking on the value which limits it to 50% greater
	// than the previous chunk size.

	context.ChunkDurationFeedback(time.Duration(400 * time.Microsecond))
	context.ChunkDurationFeedback(time.Duration(450 * time.Microsecond))
	context.ChunkDurationFeedback(time.Duration(470 * time.Microsecond))
	context.ChunkDurationFeedback(time.Duration(520 * time.Microsecond))
	context.ChunkDurationFeedback(time.Duration(500 * time.Microsecond))
	context.ChunkDurationFeedback(time.Duration(490 * time.Microsecond))
	context.ChunkDurationFeedback(time.Duration(300 * time.Microsecond))
	context.ChunkDurationFeedback(time.Duration(450 * time.Microsecond))
	context.ChunkDurationFeedback(time.Duration(460 * time.Microsecond))
	context.ChunkDurationFeedback(time.Duration(480 * time.Microsecond))
	test.S(t).ExpectEquals(context.GetChunkSize(), int64(187)) // very minor increase

	// Test that the chunk size is not allowed to grow larger than 50x
	// the original chunk size. Because of the gradual step up, we need to
	// provide a lot of feedback first.
	for i := 0; i < 1000; i++ {
		context.ChunkDurationFeedback(time.Duration(480 * time.Microsecond))
		context.GetChunkSize()
	}
	test.S(t).ExpectEquals(context.GetChunkSize(), int64(50000))

	// Similarly, the minimum chunksize is 1000/50=20 rows no matter what the feedback.
	// The downscaling rule is /10 for values that immediately exceed 5x the target,
	// so it usually scales down before the feedback re-evaluation kicks in.
	for i := 0; i < 100; i++ {
		context.ChunkDurationFeedback(time.Duration(10 * time.Second))
		context.GetChunkSize()
	}
	test.S(t).ExpectEquals(context.GetChunkSize(), int64(20))

	// If we set the chunkSize to 100, then 100/50=2 is the minimum.
	// But there is a hard coded minimum of 10 rows for safety.
	context.chunkSize = 100
	for i := 0; i < 100; i++ {
		context.ChunkDurationFeedback(time.Duration(10 * time.Second))
		context.GetChunkSize()
	}
	test.S(t).ExpectEquals(context.GetChunkSize(), int64(10))
}
