/*
   Copyright 2022 GitHub Inc.
         See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openark/golib/tests"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/binlog"
	"github.com/github/gh-ost/go/sql"
)

func TestMigratorOnChangelogEvent(t *testing.T) {
	migrationContext := base.NewMigrationContext()
	migrator := NewMigrator(migrationContext, "1.2.3")

	t.Run("heartbeat", func(t *testing.T) {
		columnValues := sql.ToColumnValues([]interface{}{
			123,
			time.Now().Unix(),
			"heartbeat",
			"2022-08-16T00:45:10.52Z",
		})
		tests.S(t).ExpectNil(migrator.onChangelogEvent(&binlog.BinlogDMLEvent{
			DatabaseName:    "test",
			DML:             binlog.InsertDML,
			NewColumnValues: columnValues,
		}))
	})

	t.Run("state-AllEventsUpToLockProcessed", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			es := <-migrator.applyEventsQueue
			tests.S(t).ExpectNotNil(es)
			tests.S(t).ExpectNotNil(es.writeFunc)
		}(&wg)

		columnValues := sql.ToColumnValues([]interface{}{
			123,
			time.Now().Unix(),
			"state",
			AllEventsUpToLockProcessed,
		})
		tests.S(t).ExpectNil(migrator.onChangelogEvent(&binlog.BinlogDMLEvent{
			DatabaseName:    "test",
			DML:             binlog.InsertDML,
			NewColumnValues: columnValues,
		}))
		wg.Wait()
	})

	t.Run("state-GhostTableMigrated", func(t *testing.T) {
		go func() {
			tests.S(t).ExpectTrue(<-migrator.ghostTableMigrated)
		}()

		columnValues := sql.ToColumnValues([]interface{}{
			123,
			time.Now().Unix(),
			"state",
			GhostTableMigrated,
		})
		tests.S(t).ExpectNil(migrator.onChangelogEvent(&binlog.BinlogDMLEvent{
			DatabaseName:    "test",
			DML:             binlog.InsertDML,
			NewColumnValues: columnValues,
		}))
	})

	t.Run("state-Migrated", func(t *testing.T) {
		columnValues := sql.ToColumnValues([]interface{}{
			123,
			time.Now().Unix(),
			"state",
			Migrated,
		})
		tests.S(t).ExpectNil(migrator.onChangelogEvent(&binlog.BinlogDMLEvent{
			DatabaseName:    "test",
			DML:             binlog.InsertDML,
			NewColumnValues: columnValues,
		}))
	})

	t.Run("state-ReadMigrationRangeValues", func(t *testing.T) {
		columnValues := sql.ToColumnValues([]interface{}{
			123,
			time.Now().Unix(),
			"state",
			ReadMigrationRangeValues,
		})
		tests.S(t).ExpectNil(migrator.onChangelogEvent(&binlog.BinlogDMLEvent{
			DatabaseName:    "test",
			DML:             binlog.InsertDML,
			NewColumnValues: columnValues,
		}))
	})
}

func TestMigratorValidateStatement(t *testing.T) {
	t.Run("add-column", func(t *testing.T) {
		migrationContext := base.NewMigrationContext()
		migrator := NewMigrator(migrationContext, "1.2.3")
		tests.S(t).ExpectNil(migrator.parser.ParseAlterStatement(`ALTER TABLE test ADD test_new VARCHAR(64) NOT NULL`))

		tests.S(t).ExpectNil(migrator.validateAlterStatement())
		tests.S(t).ExpectEquals(len(migrator.migrationContext.DroppedColumnsMap), 0)
	})

	t.Run("drop-column", func(t *testing.T) {
		migrationContext := base.NewMigrationContext()
		migrator := NewMigrator(migrationContext, "1.2.3")
		tests.S(t).ExpectNil(migrator.parser.ParseAlterStatement(`ALTER TABLE test DROP abc`))

		tests.S(t).ExpectNil(migrator.validateAlterStatement())
		tests.S(t).ExpectEquals(len(migrator.migrationContext.DroppedColumnsMap), 1)
		_, exists := migrator.migrationContext.DroppedColumnsMap["abc"]
		tests.S(t).ExpectTrue(exists)
	})

	t.Run("rename-column", func(t *testing.T) {
		migrationContext := base.NewMigrationContext()
		migrator := NewMigrator(migrationContext, "1.2.3")
		tests.S(t).ExpectNil(migrator.parser.ParseAlterStatement(`ALTER TABLE test CHANGE test123 test1234 bigint unsigned`))

		err := migrator.validateAlterStatement()
		tests.S(t).ExpectNotNil(err)
		tests.S(t).ExpectTrue(strings.HasPrefix(err.Error(), "gh-ost believes the ALTER statement renames columns"))
		tests.S(t).ExpectEquals(len(migrator.migrationContext.DroppedColumnsMap), 0)
	})

	t.Run("rename-column-approved", func(t *testing.T) {
		migrationContext := base.NewMigrationContext()
		migrator := NewMigrator(migrationContext, "1.2.3")
		migrator.migrationContext.ApproveRenamedColumns = true
		tests.S(t).ExpectNil(migrator.parser.ParseAlterStatement(`ALTER TABLE test CHANGE test123 test1234 bigint unsigned`))

		tests.S(t).ExpectNil(migrator.validateAlterStatement())
		tests.S(t).ExpectEquals(len(migrator.migrationContext.DroppedColumnsMap), 0)
	})

	t.Run("rename-table", func(t *testing.T) {
		migrationContext := base.NewMigrationContext()
		migrator := NewMigrator(migrationContext, "1.2.3")
		tests.S(t).ExpectNil(migrator.parser.ParseAlterStatement(`ALTER TABLE test RENAME TO test_new`))

		err := migrator.validateAlterStatement()
		tests.S(t).ExpectNotNil(err)
		tests.S(t).ExpectTrue(errors.Is(err, ErrMigratorUnsupportedRenameAlter))
		tests.S(t).ExpectEquals(len(migrator.migrationContext.DroppedColumnsMap), 0)
	})
}

func TestMigratorCreateFlagFiles(t *testing.T) {
	tmpdir, err := os.MkdirTemp("", t.Name())
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpdir)

	migrationContext := base.NewMigrationContext()
	migrationContext.PostponeCutOverFlagFile = filepath.Join(tmpdir, "cut-over.flag")
	migrator := NewMigrator(migrationContext, "1.2.3")
	tests.S(t).ExpectNil(migrator.createFlagFiles())
	tests.S(t).ExpectNil(migrator.createFlagFiles()) // twice to test already-exists

	_, err = os.Stat(migrationContext.PostponeCutOverFlagFile)
	tests.S(t).ExpectNil(err)
}

func TestMigratorGetProgressPercent(t *testing.T) {
	migrationContext := base.NewMigrationContext()
	migrator := NewMigrator(migrationContext, "1.2.3")

	{
		tests.S(t).ExpectEquals(migrator.getProgressPercent(0), float64(100.0))
	}
	{
		migrationContext.TotalRowsCopied = 250
		tests.S(t).ExpectEquals(migrator.getProgressPercent(1000), float64(25.0))
	}
}

func TestMigratorGetMigrationStateAndETA(t *testing.T) {
	migrationContext := base.NewMigrationContext()
	migrator := NewMigrator(migrationContext, "1.2.3")
	now := time.Now()
	migrationContext.RowCopyStartTime = now.Add(-time.Minute)
	migrationContext.RowCopyEndTime = now

	{
		migrationContext.TotalRowsCopied = 456
		state, eta, etaDuration := migrator.getMigrationStateAndETA(123456)
		tests.S(t).ExpectEquals(state, "migrating")
		tests.S(t).ExpectEquals(eta, "4h29m44s")
		tests.S(t).ExpectEquals(etaDuration.String(), "4h29m44s")
	}
	{
		migrationContext.TotalRowsCopied = 456
		state, eta, etaDuration := migrator.getMigrationStateAndETA(456)
		tests.S(t).ExpectEquals(state, "migrating")
		tests.S(t).ExpectEquals(eta, "due")
		tests.S(t).ExpectEquals(etaDuration.String(), "0s")
	}
	{
		migrationContext.TotalRowsCopied = 123456
		state, eta, etaDuration := migrator.getMigrationStateAndETA(456)
		tests.S(t).ExpectEquals(state, "migrating")
		tests.S(t).ExpectEquals(eta, "due")
		tests.S(t).ExpectEquals(etaDuration.String(), "0s")
	}
	{
		atomic.StoreInt64(&migrationContext.CountingRowsFlag, 1)
		state, eta, etaDuration := migrator.getMigrationStateAndETA(123456)
		tests.S(t).ExpectEquals(state, "counting rows")
		tests.S(t).ExpectEquals(eta, "due")
		tests.S(t).ExpectEquals(etaDuration.String(), "0s")
	}
	{
		atomic.StoreInt64(&migrationContext.CountingRowsFlag, 0)
		atomic.StoreInt64(&migrationContext.IsPostponingCutOver, 1)
		state, eta, etaDuration := migrator.getMigrationStateAndETA(123456)
		tests.S(t).ExpectEquals(state, "postponing cut-over")
		tests.S(t).ExpectEquals(eta, "due")
		tests.S(t).ExpectEquals(etaDuration.String(), "0s")
	}
}

func TestMigratorShouldPrintStatus(t *testing.T) {
	migrationContext := base.NewMigrationContext()
	migrator := NewMigrator(migrationContext, "1.2.3")

	tests.S(t).ExpectTrue(migrator.shouldPrintStatus(NoPrintStatusRule, 10, time.Second))                  // test 'rule != HeuristicPrintStatusRule' return
	tests.S(t).ExpectTrue(migrator.shouldPrintStatus(HeuristicPrintStatusRule, 10, time.Second))           // test 'etaDuration.Seconds() <= 60'
	tests.S(t).ExpectTrue(migrator.shouldPrintStatus(HeuristicPrintStatusRule, 90, time.Second))           // test 'etaDuration.Seconds() <= 60' again
	tests.S(t).ExpectTrue(migrator.shouldPrintStatus(HeuristicPrintStatusRule, 90, time.Minute))           // test 'etaDuration.Seconds() <= 180'
	tests.S(t).ExpectTrue(migrator.shouldPrintStatus(HeuristicPrintStatusRule, 60, 90*time.Second))        // test 'elapsedSeconds <= 180'
	tests.S(t).ExpectFalse(migrator.shouldPrintStatus(HeuristicPrintStatusRule, 61, 90*time.Second))       // test 'elapsedSeconds <= 180'
	tests.S(t).ExpectFalse(migrator.shouldPrintStatus(HeuristicPrintStatusRule, 99, 210*time.Second))      // test 'elapsedSeconds <= 180'
	tests.S(t).ExpectFalse(migrator.shouldPrintStatus(HeuristicPrintStatusRule, 12345, 86400*time.Second)) // test 'else'
	tests.S(t).ExpectTrue(migrator.shouldPrintStatus(HeuristicPrintStatusRule, 30030, 86400*time.Second))  // test 'else' again
}
