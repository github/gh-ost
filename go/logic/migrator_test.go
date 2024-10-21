/*
   Copyright 2022 GitHub Inc.
         See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gosql "database/sql"

	"github.com/openark/golib/tests"
	"github.com/stretchr/testify/require"

	"sync"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/binlog"
	"github.com/github/gh-ost/go/mysql"
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

	// t.Run("state-AllEventsUpToLockProcessed", func(t *testing.T) {
	// 	var wg sync.WaitGroup
	// 	wg.Add(1)
	// 	go func(wg *sync.WaitGroup) {
	// 		defer wg.Done()
	// 		es := <-migrator.applyEventsQueue
	// 		tests.S(t).ExpectNotNil(es)
	// 		tests.S(t).ExpectNotNil(es.writeFunc)
	// 	}(&wg)

	// 	columnValues := sql.ToColumnValues([]interface{}{
	// 		123,
	// 		time.Now().Unix(),
	// 		"state",
	// 		AllEventsUpToLockProcessed,
	// 	})
	// 	tests.S(t).ExpectNil(migrator.onChangelogEvent(&binlog.BinlogDMLEvent{
	// 		DatabaseName:    "test",
	// 		DML:             binlog.InsertDML,
	// 		NewColumnValues: columnValues,
	// 	}))
	// 	wg.Wait()
	// })

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

func prepareDatabase(t *testing.T, db *gosql.DB) {
	_, err := db.Exec("RESET MASTER")
	require.NoError(t, err)

	_, err = db.Exec("SET @@GLOBAL.	binlog_transaction_dependency_tracking = WRITESET")
	require.NoError(t, err)

	_, err = db.Exec("DROP DATABASE test")
	require.NoError(t, err)

	_, err = db.Exec("CREATE DATABASE test")
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE test.gh_ost_test (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255)) ENGINE=InnoDB")
	require.NoError(t, err)
}

func TestMigrate(t *testing.T) {
	db, err := gosql.Open("mysql", "root:root@/")
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	_ = os.Remove("/tmp/gh-ost.sock")

	prepareDatabase(t, db)

	migrationContext := base.NewMigrationContext()
	migrationContext.Hostname = "localhost"
	migrationContext.DatabaseName = "test"
	migrationContext.OriginalTableName = "gh_ost_test"
	migrationContext.AlterStatement = "ALTER TABLE gh_ost_test ENGINE=InnoDB"
	migrationContext.AllowedRunningOnMaster = true
	migrationContext.ReplicaServerId = 99999
	migrationContext.HeartbeatIntervalMilliseconds = 100
	migrationContext.ServeSocketFile = "/tmp/gh-ost.sock"
	migrationContext.ThrottleHTTPIntervalMillis = 100

	migrationContext.InspectorConnectionConfig = &mysql.ConnectionConfig{
		Key: mysql.InstanceKey{
			Hostname: "localhost",
			Port:     3306,
		},
		User:     "root",
		Password: "root",
	}

	migrationContext.SetConnectionConfig("innodb")

	migrator := NewMigrator(migrationContext, "1.2.3")

	ctx, cancel := context.WithCancel(context.Background())

	rowsWritten := atomic.Int32{}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				_, err := db.ExecContext(ctx, "INSERT INTO test.gh_ost_test (name) VALUES ('test')")
				if errors.Is(err, context.Canceled) {
					return
				}
				require.NoError(t, err)
				rowsWritten.Add(1)

				time.Sleep(time.Millisecond)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				tx, err := db.BeginTx(ctx, &gosql.TxOptions{})
				if errors.Is(err, context.Canceled) {
					return
				} else if err != nil {
					fmt.Println(err.Error())
				}
				require.NoError(t, err)

				for i := 0; i < 10; i++ {
					_, err = tx.ExecContext(ctx, "INSERT INTO test.gh_ost_test (name) VALUES ('test')")
					if errors.Is(err, context.Canceled) {
						tx.Rollback()
						return
					}
					require.NoError(t, err)
					rowsWritten.Add(1)
				}
				tx.Commit()

				time.Sleep(time.Millisecond)
			}
		}
	}()

	go func() {
		time.Sleep(10 * time.Second)
		cancel()
	}()

	err = migrator.Migrate()
	require.NoError(t, err)
	wg.Wait()

	fmt.Printf("Rows written: %d\n", rowsWritten.Load())
}
