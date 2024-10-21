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

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

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
		require.Nil(t, migrator.onChangelogEvent(&binlog.BinlogDMLEvent{
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
	// 		require.NotNil(t, es)
	// 		require.NotNil(t, es.writeFunc)
	// 	}(&wg)

	// 	columnValues := sql.ToColumnValues([]interface{}{
	// 		123,
	// 		time.Now().Unix(),
	// 		"state",
	// 		AllEventsUpToLockProcessed,
	// 	})
	// 	require.Nil(t, migrator.onChangelogEvent(&binlog.BinlogDMLEvent{
	// 		DatabaseName:    "test",
	// 		DML:             binlog.InsertDML,
	// 		NewColumnValues: columnValues,
	// 	}))
	// 	wg.Wait()
	// })

	t.Run("state-GhostTableMigrated", func(t *testing.T) {
		go func() {
			require.True(t, <-migrator.ghostTableMigrated)
		}()

		columnValues := sql.ToColumnValues([]interface{}{
			123,
			time.Now().Unix(),
			"state",
			GhostTableMigrated,
		})
		require.Nil(t, migrator.onChangelogEvent(&binlog.BinlogDMLEvent{
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
		require.Nil(t, migrator.onChangelogEvent(&binlog.BinlogDMLEvent{
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
		require.Nil(t, migrator.onChangelogEvent(&binlog.BinlogDMLEvent{
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
		require.Nil(t, migrator.parser.ParseAlterStatement(`ALTER TABLE test ADD test_new VARCHAR(64) NOT NULL`))

		require.Nil(t, migrator.validateAlterStatement())
		require.Len(t, migrator.migrationContext.DroppedColumnsMap, 0)
	})

	t.Run("drop-column", func(t *testing.T) {
		migrationContext := base.NewMigrationContext()
		migrator := NewMigrator(migrationContext, "1.2.3")
		require.Nil(t, migrator.parser.ParseAlterStatement(`ALTER TABLE test DROP abc`))

		require.Nil(t, migrator.validateAlterStatement())
		require.Len(t, migrator.migrationContext.DroppedColumnsMap, 1)
		_, exists := migrator.migrationContext.DroppedColumnsMap["abc"]
		require.True(t, exists)
	})

	t.Run("rename-column", func(t *testing.T) {
		migrationContext := base.NewMigrationContext()
		migrator := NewMigrator(migrationContext, "1.2.3")
		require.Nil(t, migrator.parser.ParseAlterStatement(`ALTER TABLE test CHANGE test123 test1234 bigint unsigned`))

		err := migrator.validateAlterStatement()
		require.Error(t, err)
		require.True(t, strings.HasPrefix(err.Error(), "gh-ost believes the ALTER statement renames columns"))
		require.Len(t, migrator.migrationContext.DroppedColumnsMap, 0)
	})

	t.Run("rename-column-approved", func(t *testing.T) {
		migrationContext := base.NewMigrationContext()
		migrator := NewMigrator(migrationContext, "1.2.3")
		migrator.migrationContext.ApproveRenamedColumns = true
		require.Nil(t, migrator.parser.ParseAlterStatement(`ALTER TABLE test CHANGE test123 test1234 bigint unsigned`))

		require.Nil(t, migrator.validateAlterStatement())
		require.Len(t, migrator.migrationContext.DroppedColumnsMap, 0)
	})

	t.Run("rename-table", func(t *testing.T) {
		migrationContext := base.NewMigrationContext()
		migrator := NewMigrator(migrationContext, "1.2.3")
		require.Nil(t, migrator.parser.ParseAlterStatement(`ALTER TABLE test RENAME TO test_new`))

		err := migrator.validateAlterStatement()
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrMigratorUnsupportedRenameAlter))
		require.Len(t, migrator.migrationContext.DroppedColumnsMap, 0)
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
	require.Nil(t, migrator.createFlagFiles())
	require.Nil(t, migrator.createFlagFiles()) // twice to test already-exists

	_, err = os.Stat(migrationContext.PostponeCutOverFlagFile)
	require.NoError(t, err)
}

func TestMigratorGetProgressPercent(t *testing.T) {
	migrationContext := base.NewMigrationContext()
	migrator := NewMigrator(migrationContext, "1.2.3")

	{
		require.Equal(t, float64(100.0), migrator.getProgressPercent(0))
	}
	{
		migrationContext.TotalRowsCopied = 250
		require.Equal(t, float64(25.0), migrator.getProgressPercent(1000))
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
		require.Equal(t, "migrating", state)
		require.Equal(t, "4h29m44s", eta)
		require.Equal(t, "4h29m44s", etaDuration.String())
	}
	{
		migrationContext.TotalRowsCopied = 456
		state, eta, etaDuration := migrator.getMigrationStateAndETA(456)
		require.Equal(t, "migrating", state)
		require.Equal(t, "due", eta)
		require.Equal(t, "0s", etaDuration.String())
	}
	{
		migrationContext.TotalRowsCopied = 123456
		state, eta, etaDuration := migrator.getMigrationStateAndETA(456)
		require.Equal(t, "migrating", state)
		require.Equal(t, "due", eta)
		require.Equal(t, "0s", etaDuration.String())
	}
	{
		atomic.StoreInt64(&migrationContext.CountingRowsFlag, 1)
		state, eta, etaDuration := migrator.getMigrationStateAndETA(123456)
		require.Equal(t, "counting rows", state)
		require.Equal(t, "due", eta)
		require.Equal(t, "0s", etaDuration.String())
	}
	{
		atomic.StoreInt64(&migrationContext.CountingRowsFlag, 0)
		atomic.StoreInt64(&migrationContext.IsPostponingCutOver, 1)
		state, eta, etaDuration := migrator.getMigrationStateAndETA(123456)
		require.Equal(t, "postponing cut-over", state)
		require.Equal(t, "due", eta)
		require.Equal(t, "0s", etaDuration.String())
	}
}

func TestMigratorShouldPrintStatus(t *testing.T) {
	migrationContext := base.NewMigrationContext()
	migrator := NewMigrator(migrationContext, "1.2.3")

	require.True(t, migrator.shouldPrintStatus(NoPrintStatusRule, 10, time.Second))                  // test 'rule != HeuristicPrintStatusRule' return
	require.True(t, migrator.shouldPrintStatus(HeuristicPrintStatusRule, 10, time.Second))           // test 'etaDuration.Seconds() <= 60'
	require.True(t, migrator.shouldPrintStatus(HeuristicPrintStatusRule, 90, time.Second))           // test 'etaDuration.Seconds() <= 60' again
	require.True(t, migrator.shouldPrintStatus(HeuristicPrintStatusRule, 90, time.Minute))           // test 'etaDuration.Seconds() <= 180'
	require.True(t, migrator.shouldPrintStatus(HeuristicPrintStatusRule, 60, 90*time.Second))        // test 'elapsedSeconds <= 180'
	require.False(t, migrator.shouldPrintStatus(HeuristicPrintStatusRule, 61, 90*time.Second))       // test 'elapsedSeconds <= 180'
	require.False(t, migrator.shouldPrintStatus(HeuristicPrintStatusRule, 99, 210*time.Second))      // test 'elapsedSeconds <= 180'
	require.False(t, migrator.shouldPrintStatus(HeuristicPrintStatusRule, 12345, 86400*time.Second)) // test 'else'
	require.True(t, migrator.shouldPrintStatus(HeuristicPrintStatusRule, 30030, 86400*time.Second))  // test 'else' again
}

func prepareDatabase(t *testing.T, db *gosql.DB) {
	_, err := db.Exec("RESET MASTER")
	require.NoError(t, err)

	_, err = db.Exec("SET @@GLOBAL.	binlog_transaction_dependency_tracking = WRITESET")
	require.NoError(t, err)

	_, err = db.Exec("CREATE DATABASE test")
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE test.gh_ost_test (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255)) ENGINE=InnoDB")
	require.NoError(t, err)
}

func TestMigrate(t *testing.T) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:      "mysql:8.0",
		Env:        map[string]string{"MYSQL_ROOT_PASSWORD": "root"},
		WaitingFor: wait.ForLog("port: 3306  MySQL Community Server - GPL"),
	}

	mysqlContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx := context.Background()
		require.NoError(t, mysqlContainer.Terminate(ctx))
	})

	host, err := mysqlContainer.Host(ctx)
	mappedPort, err := mysqlContainer.MappedPort(ctx, "3306")
	db, err := gosql.Open("mysql", "root:root@tcp("+host+":"+mappedPort.Port()+")/")
	require.NoError(t, err)

	defer func() {
		require.NoError(t, db.Close())
	}()

	_ = os.Remove("/tmp/gh-ost.sock")

	prepareDatabase(t, db)

	migrationContext := base.NewMigrationContext()
	// Hack:
	migrationContext.AzureMySQL = true
	migrationContext.AssumeMasterHostname = host + ":" + mappedPort.Port()
	migrationContext.DatabaseName = "test"
	migrationContext.OriginalTableName = "gh_ost_test"
	migrationContext.AlterStatement = "ALTER TABLE gh_ost_test ENGINE=InnoDB"
	migrationContext.AllowedRunningOnMaster = true
	migrationContext.ReplicaServerId = 99999
	migrationContext.HeartbeatIntervalMilliseconds = 100
	migrationContext.ServeSocketFile = "/tmp/gh-ost.sock"
	migrationContext.ThrottleHTTPIntervalMillis = 100

	migrationContext.InspectorConnectionConfig = &mysql.ConnectionConfig{
		ImpliedKey: &mysql.InstanceKey{
			Hostname: host,
			Port:     mappedPort.Int(),
		},
		Key: mysql.InstanceKey{
			Hostname: host,
			Port:     mappedPort.Int(),
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
			if ctx.Err() != nil {
				return
			}
			_, err := db.ExecContext(ctx, "INSERT INTO test.gh_ost_test (name) VALUES ('test')")
			if errors.Is(err, context.Canceled) {
				return
			}
			require.NoError(t, err)
			rowsWritten.Add(1)

			time.Sleep(time.Millisecond)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			tx, err := db.BeginTx(ctx, &gosql.TxOptions{})
			if errors.Is(err, context.Canceled) {
				return
			}
			// if err != nil {
			// 	fmt.Println(err.Error())
			// }
			require.NoError(t, err)

			for i := 0; i < 10; i++ {
				_, err = tx.ExecContext(ctx, "INSERT INTO test.gh_ost_test (name) VALUES ('test')")
				if errors.Is(err, context.Canceled) {
					return
				}
				require.NoError(t, err)
				rowsWritten.Add(1)
			}
			err = tx.Commit()
			if errors.Is(err, context.Canceled) {
				return
			}
			require.NoError(t, err)

			time.Sleep(time.Millisecond)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Second)
		cancel()
	}()

	err = migrator.Migrate()
	wg.Wait()
	require.NoError(t, err)

	fmt.Printf("Rows written: %d\n", rowsWritten.Load())
}
