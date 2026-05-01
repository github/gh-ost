/*
   Copyright 2022 GitHub Inc.
         See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"bytes"
	"context"
	gosql "database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	testmysql "github.com/testcontainers/testcontainers-go/modules/mysql"

	"runtime"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/binlog"
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"
	"github.com/testcontainers/testcontainers-go"
)

func TestMigratorOnChangelogEvent(t *testing.T) {
	migrationContext := base.NewMigrationContext()
	migrator := NewMigrator(migrationContext, "1.2.3")
	migrator.applier = NewApplier(migrationContext)

	t.Run("heartbeat", func(t *testing.T) {
		columnValues := sql.ToColumnValues([]interface{}{
			123,
			time.Now().Unix(),
			"heartbeat",
			"2022-08-16T00:45:10.52Z",
		})
		require.Nil(t, migrator.onChangelogEvent(&binlog.BinlogEntry{
			DmlEvent: &binlog.BinlogDMLEvent{
				DatabaseName:    "test",
				DML:             binlog.InsertDML,
				NewColumnValues: columnValues},
			Coordinates: mysql.NewFileBinlogCoordinates("mysql-bin.000004", int64(4)),
		}))
	})

	t.Run("state-AllEventsUpToLockProcessed", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			es := <-migrator.applyEventsQueue
			require.NotNil(t, es)
			require.NotNil(t, es.writeFunc)
		}(&wg)

		columnValues := sql.ToColumnValues([]interface{}{
			123,
			time.Now().Unix(),
			"state",
			AllEventsUpToLockProcessed,
		})
		require.Nil(t, migrator.onChangelogEvent(&binlog.BinlogEntry{
			DmlEvent: &binlog.BinlogDMLEvent{
				DatabaseName:    "test",
				DML:             binlog.InsertDML,
				NewColumnValues: columnValues},
			Coordinates: mysql.NewFileBinlogCoordinates("mysql-bin.000004", int64(4)),
		}))
		wg.Wait()
	})

	t.Run("state-AllEventsUpToLockProcessed-overwrite-oldest", func(t *testing.T) {
		// Simulate the scenario where the receiver (waitForEventsUpToLock) timed out
		// and a stale message sits in the channel buffer. The next sentinel must
		// overwrite the stale one so the current attempt's message is delivered.
		m := NewMigrator(base.NewMigrationContext(), "test")
		m.applier = NewApplier(m.migrationContext)

		sendChangelogEvent := func(challenge string) {
			columnValues := sql.ToColumnValues([]interface{}{
				123,
				time.Now().Unix(),
				"state",
				challenge,
			})
			require.NoError(t, m.onChangelogEvent(&binlog.BinlogEntry{
				DmlEvent: &binlog.BinlogDMLEvent{
					DatabaseName:    "test",
					DML:             binlog.InsertDML,
					NewColumnValues: columnValues},
				Coordinates: mysql.NewFileBinlogCoordinates("mysql-bin.000004", int64(4)),
			}))
		}

		executeWriteFunc := func() {
			es := <-m.applyEventsQueue
			require.NotNil(t, es.writeFunc)
			require.NoError(t, (*es.writeFunc)())
		}

		// Attempt 1: send sentinel and execute the writeFunc to deliver it
		sendChangelogEvent("AllEventsUpToLockProcessed:attempt1")
		executeWriteFunc()

		// The message sits unconsumed in allEventsUpToLockProcessed (simulating a timeout)
		require.Len(t, m.allEventsUpToLockProcessed, 1)

		// Attempt 2: send a new sentinel — must overwrite the stale one
		sendChangelogEvent("AllEventsUpToLockProcessed:attempt2")
		executeWriteFunc()

		// The channel should contain exactly the latest message
		require.Len(t, m.allEventsUpToLockProcessed, 1)
		msg := <-m.allEventsUpToLockProcessed
		require.Equal(t, "AllEventsUpToLockProcessed:attempt2", msg.state)
	})

	t.Run("NewMigrator-with-extreme-MaxRetries", func(t *testing.T) {
		// Regression test: an extremely large --default-retries value must not
		// cause an OOM when creating the migrator. Before the fix,
		// allEventsUpToLockProcessed was buffered to MaxRetries(), which tried
		// to allocate a ~10 trillion element channel.
		ctx := base.NewMigrationContext()
		ctx.SetDefaultNumRetries(9999999999999)
		require.Equal(t, int64(9999999999999), ctx.MaxRetries())

		m := NewMigrator(ctx, "test")
		require.NotNil(t, m)
		require.Equal(t, 1, cap(m.allEventsUpToLockProcessed))
	})

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
		require.Nil(t, migrator.onChangelogEvent(&binlog.BinlogEntry{
			DmlEvent: &binlog.BinlogDMLEvent{
				DatabaseName:    "test",
				DML:             binlog.InsertDML,
				NewColumnValues: columnValues},
			Coordinates: mysql.NewFileBinlogCoordinates("mysql-bin.000004", int64(4)),
		}))
	})

	t.Run("state-Migrated", func(t *testing.T) {
		columnValues := sql.ToColumnValues([]interface{}{
			123,
			time.Now().Unix(),
			"state",
			Migrated,
		})
		require.Nil(t, migrator.onChangelogEvent(&binlog.BinlogEntry{
			DmlEvent: &binlog.BinlogDMLEvent{
				DatabaseName:    "test",
				DML:             binlog.InsertDML,
				NewColumnValues: columnValues},
			Coordinates: mysql.NewFileBinlogCoordinates("mysql-bin.000004", int64(4)),
		}))
	})

	t.Run("state-ReadMigrationRangeValues", func(t *testing.T) {
		columnValues := sql.ToColumnValues([]interface{}{
			123,
			time.Now().Unix(),
			"state",
			ReadMigrationRangeValues,
		})
		require.Nil(t, migrator.onChangelogEvent(&binlog.BinlogEntry{
			DmlEvent: &binlog.BinlogDMLEvent{
				DatabaseName:    "test",
				DML:             binlog.InsertDML,
				NewColumnValues: columnValues},
			Coordinates: mysql.NewFileBinlogCoordinates("mysql-bin.000004", int64(4)),
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
		// Test using rows-per-second added data.
		migrationContext.TotalRowsCopied = 456
		migrationContext.EtaRowsPerSecond = 100
		state, eta, etaDuration := migrator.getMigrationStateAndETA(123456)
		require.Equal(t, "migrating", state)
		require.Equal(t, "20m30s", eta)
		require.Equal(t, "20m30s", etaDuration.String())
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

type MigratorTestSuite struct {
	suite.Suite

	mysqlContainer testcontainers.Container
	db             *gosql.DB
}

func (suite *MigratorTestSuite) SetupSuite() {
	ctx := context.Background()
	mysqlContainer, err := testmysql.Run(ctx,
		testMysqlContainerImage,
		testmysql.WithDatabase(testMysqlDatabase),
		testmysql.WithUsername(testMysqlUser),
		testmysql.WithPassword(testMysqlPass),
		testmysql.WithConfigFile("my.cnf.test"),
	)
	suite.Require().NoError(err)

	suite.mysqlContainer = mysqlContainer
	dsn, err := mysqlContainer.ConnectionString(ctx)
	suite.Require().NoError(err)

	db, err := gosql.Open("mysql", dsn)
	suite.Require().NoError(err)

	suite.db = db
}

func (suite *MigratorTestSuite) TeardownSuite() {
	suite.Assert().NoError(suite.db.Close())
	suite.Assert().NoError(testcontainers.TerminateContainer(suite.mysqlContainer))
}

func (suite *MigratorTestSuite) SetupTest() {
	ctx := context.Background()

	_, err := suite.db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+testMysqlDatabase)
	suite.Require().NoError(err)

	os.Remove("/tmp/gh-ost.sock")
}

func (suite *MigratorTestSuite) TearDownTest() {
	ctx := context.Background()

	_, err := suite.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+getTestTableName())
	suite.Require().NoError(err)
	_, err = suite.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+getTestGhostTableName())
	suite.Require().NoError(err)
	_, err = suite.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+getTestRevertedTableName())
	suite.Require().NoError(err)
	_, err = suite.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+getTestOldTableName())
	suite.Require().NoError(err)
}

func (suite *MigratorTestSuite) TestMigrateEmpty() {
	ctx := context.Background()

	_, err := suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, name VARCHAR(64))", getTestTableName()))
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := newTestMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.InspectorConnectionConfig = connectionConfig
	migrationContext.SetConnectionConfig("innodb")
	migrationContext.InitiallyDropOldTable = true

	migrationContext.AlterStatementOptions = "ADD COLUMN foobar varchar(255), ENGINE=InnoDB"

	migrator := NewMigrator(migrationContext, "0.0.0")

	err = migrator.Migrate()
	suite.Require().NoError(err)

	// Verify the new column was added
	var tableName, createTableSQL string
	err = suite.db.QueryRow("SHOW CREATE TABLE "+getTestTableName()).Scan(&tableName, &createTableSQL)
	suite.Require().NoError(err)

	suite.Require().Equal("testing", tableName)
	suite.Require().Equal("CREATE TABLE `testing` (\n  `id` int NOT NULL,\n  `name` varchar(64) DEFAULT NULL,\n  `foobar` varchar(255) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", createTableSQL)

	// Verify the changelog table was claned up
	err = suite.db.QueryRow("SHOW TABLES IN test LIKE '_testing_ghc'").Scan(&tableName)
	suite.Require().Error(err)
	suite.Require().Equal(gosql.ErrNoRows, err)

	// Verify the old table was renamed
	err = suite.db.QueryRow("SHOW TABLES IN test LIKE '_testing_del'").Scan(&tableName)
	suite.Require().NoError(err)
	suite.Require().Equal("_testing_del", tableName)
}

func (suite *MigratorTestSuite) TestRetryBatchCopyWithHooks() {
	ctx := context.Background()

	_, err := suite.db.ExecContext(ctx, "CREATE TABLE test.test_retry_batch (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT)")
	suite.Require().NoError(err)

	const initStride = 1000
	const totalBatches = 3
	for i := 0; i < totalBatches; i++ {
		dataSize := 50 * i
		for j := 0; j < initStride; j++ {
			_, err = suite.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO test.test_retry_batch (name) VALUES ('%s')", strings.Repeat("a", dataSize)))
			suite.Require().NoError(err)
		}
	}

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("SET GLOBAL max_binlog_cache_size = %d", 1024*8))
	suite.Require().NoError(err)
	defer func() {
		_, err = suite.db.ExecContext(ctx, fmt.Sprintf("SET GLOBAL max_binlog_cache_size = %d", 1024*1024*1024))
		suite.Require().NoError(err)
	}()

	tmpDir, err := os.MkdirTemp("", "gh-ost-hooks")
	suite.Require().NoError(err)
	defer os.RemoveAll(tmpDir)

	hookScript := filepath.Join(tmpDir, "gh-ost-on-batch-copy-retry")
	hookContent := `#!/bin/bash
# Mock hook that reduces chunk size on binlog cache error
ERROR_MSG="$GH_OST_LAST_BATCH_COPY_ERROR"
SOCKET_PATH="/tmp/gh-ost.sock"

if ! [[ "$ERROR_MSG" =~ "max_binlog_cache_size" ]]; then
    echo "Nothing to do for error: $ERROR_MSG"
    exit 0
fi

CHUNK_SIZE=$(echo "chunk-size=?" | nc -U $SOCKET_PATH | tr -d '\n')

MIN_CHUNK_SIZE=10
NEW_CHUNK_SIZE=$(( CHUNK_SIZE * 8 / 10 ))
if [ $NEW_CHUNK_SIZE -lt $MIN_CHUNK_SIZE ]; then
    NEW_CHUNK_SIZE=$MIN_CHUNK_SIZE
fi

if [ $CHUNK_SIZE -eq $NEW_CHUNK_SIZE ]; then
    echo "Chunk size unchanged: $CHUNK_SIZE"
    exit 0
fi

echo "[gh-ost-on-batch-copy-retry]: Changing chunk size from $CHUNK_SIZE to $NEW_CHUNK_SIZE"
echo "chunk-size=$NEW_CHUNK_SIZE" | nc -U $SOCKET_PATH
echo "[gh-ost-on-batch-copy-retry]: Done, exiting..."
`
	err = os.WriteFile(hookScript, []byte(hookContent), 0755)
	suite.Require().NoError(err)

	origStdout := os.Stdout
	origStderr := os.Stderr

	rOut, wOut, _ := os.Pipe()
	rErr, wErr, _ := os.Pipe()
	os.Stdout = wOut
	os.Stderr = wErr

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := base.NewMigrationContext()
	migrationContext.AllowedRunningOnMaster = true
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.InspectorConnectionConfig = connectionConfig
	migrationContext.DatabaseName = "test"
	migrationContext.SkipPortValidation = true
	migrationContext.OriginalTableName = "test_retry_batch"
	migrationContext.SetConnectionConfig("innodb")
	migrationContext.AlterStatementOptions = "MODIFY name LONGTEXT, ENGINE=InnoDB"
	migrationContext.ReplicaServerId = 99999
	migrationContext.HeartbeatIntervalMilliseconds = 100
	migrationContext.ThrottleHTTPIntervalMillis = 100
	migrationContext.ThrottleHTTPTimeoutMillis = 1000
	migrationContext.HooksPath = tmpDir
	migrationContext.ChunkSize = 1000
	migrationContext.SetDefaultNumRetries(10)
	migrationContext.ServeSocketFile = "/tmp/gh-ost.sock"

	migrator := NewMigrator(migrationContext, "0.0.0")

	err = migrator.Migrate()
	suite.Require().NoError(err)

	wOut.Close()
	wErr.Close()
	os.Stdout = origStdout
	os.Stderr = origStderr

	var bufOut, bufErr bytes.Buffer
	io.Copy(&bufOut, rOut)
	io.Copy(&bufErr, rErr)

	outStr := bufOut.String()
	errStr := bufErr.String()

	suite.Assert().Contains(outStr, "chunk-size: 1000")
	suite.Assert().Contains(errStr, "[gh-ost-on-batch-copy-retry]: Changing chunk size from 1000 to 800")
	suite.Assert().Contains(outStr, "chunk-size: 800")

	suite.Assert().Contains(errStr, "[gh-ost-on-batch-copy-retry]: Changing chunk size from 800 to 640")
	suite.Assert().Contains(outStr, "chunk-size: 640")

	suite.Assert().Contains(errStr, "[gh-ost-on-batch-copy-retry]: Changing chunk size from 640 to 512")
	suite.Assert().Contains(outStr, "chunk-size: 512")

	var count int
	err = suite.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM test.test_retry_batch").Scan(&count)
	suite.Require().NoError(err)
	suite.Assert().Equal(3000, count)
}

func (suite *MigratorTestSuite) TestCopierIntPK() {
	ctx := context.Background()

	_, err := suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, name VARCHAR(64), age INT);", getTestTableName()))
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := newTestMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.InspectorConnectionConfig = connectionConfig
	migrationContext.SetConnectionConfig("innodb")

	migrationContext.AlterStatementOptions = "ENGINE=InnoDB"
	migrationContext.OriginalTableColumns = sql.NewColumnList([]string{"id", "name", "age"})
	migrationContext.SharedColumns = sql.NewColumnList([]string{"id", "name", "age"})
	migrationContext.MappedSharedColumns = sql.NewColumnList([]string{"id", "name", "age"})
	migrationContext.UniqueKey = &sql.UniqueKey{
		Name:             "PRIMARY",
		NameInGhostTable: "PRIMARY",
		Columns:          *sql.NewColumnList([]string{"id"}),
	}

	chunkSize := int64(73)
	migrationContext.ChunkSize = chunkSize

	// fill with some rows
	numRows := int64(3421)
	for i := range numRows {
		_, err = suite.db.ExecContext(ctx,
			fmt.Sprintf("INSERT INTO %s (id, name, age) VALUES (%d, 'user-%d', %d)", getTestTableName(), i, i, i%99))
		suite.Require().NoError(err)
	}

	migrator := NewMigrator(migrationContext, "0.0.0")
	suite.Require().NoError(migrator.initiateApplier())
	suite.Require().NoError(migrator.applier.prepareQueries())
	suite.Require().NoError(migrator.applier.ReadMigrationRangeValues())

	go migrator.iterateChunks()
	go func() {
		if err := <-migrator.rowCopyComplete; err != nil {
			migrator.migrationContext.PanicAbort <- err
		}
		atomic.StoreInt64(&migrator.rowCopyCompleteFlag, 1)
	}()

	for {
		if atomic.LoadInt64(&migrator.rowCopyCompleteFlag) == 1 {
			suite.Assert().Equal((numRows/chunkSize)+1, migrator.migrationContext.GetIteration())
			return
		}
		select {
		case copyRowsFunc := <-migrator.copyRowsQueue:
			{
				suite.Require().NoError(copyRowsFunc())

				// check ghost table has expected number of rows
				var ghostRows int64
				suite.db.QueryRowContext(ctx,
					fmt.Sprintf(`SELECT COUNT(*) FROM %s`, getTestGhostTableName()),
				).Scan(&ghostRows)
				suite.Assert().Equal(migrator.migrationContext.TotalRowsCopied, ghostRows)
			}
		default:
			time.Sleep(time.Second)
		}
	}
}

func (suite *MigratorTestSuite) TestCopierCompositePK() {
	ctx := context.Background()

	_, err := suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT UNSIGNED, t CHAR(32), PRIMARY KEY (t, id));", getTestTableName()))
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := newTestMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.InspectorConnectionConfig = connectionConfig
	migrationContext.SetConnectionConfig("innodb")

	migrationContext.AlterStatementOptions = "ENGINE=InnoDB"
	migrationContext.OriginalTableColumns = sql.NewColumnList([]string{"id", "t"})
	migrationContext.SharedColumns = sql.NewColumnList([]string{"id", "t"})
	migrationContext.MappedSharedColumns = sql.NewColumnList([]string{"id", "t"})
	migrationContext.UniqueKey = &sql.UniqueKey{
		Name:             "PRIMARY",
		NameInGhostTable: "PRIMARY",
		Columns:          *sql.NewColumnList([]string{"t", "id"}),
	}

	chunkSize := int64(100)
	migrationContext.ChunkSize = chunkSize

	// fill with some rows
	numRows := int64(2049)
	for i := range numRows {
		query := fmt.Sprintf(`INSERT INTO %s (id, t) VALUES (FLOOR(100000000 * RAND(%d)), MD5(RAND(%d)))`, getTestTableName(), i, i)
		_, err = suite.db.ExecContext(ctx, query)
		suite.Require().NoError(err)
	}

	migrator := NewMigrator(migrationContext, "0.0.0")
	suite.Require().NoError(migrator.initiateApplier())
	suite.Require().NoError(migrator.applier.prepareQueries())
	suite.Require().NoError(migrator.applier.ReadMigrationRangeValues())

	go migrator.iterateChunks()
	go func() {
		if err := <-migrator.rowCopyComplete; err != nil {
			migrator.migrationContext.PanicAbort <- err
		}
		atomic.StoreInt64(&migrator.rowCopyCompleteFlag, 1)
	}()

	for {
		if atomic.LoadInt64(&migrator.rowCopyCompleteFlag) == 1 {
			suite.Assert().Equal((numRows/chunkSize)+1, migrator.migrationContext.GetIteration())
			return
		}
		select {
		case copyRowsFunc := <-migrator.copyRowsQueue:
			{
				suite.Require().NoError(copyRowsFunc())

				// check ghost table has expected number of rows
				var ghostRows int64
				suite.db.QueryRowContext(ctx,
					fmt.Sprintf(`SELECT COUNT(*) FROM %s`, getTestGhostTableName()),
				).Scan(&ghostRows)
				suite.Assert().Equal(migrator.migrationContext.TotalRowsCopied, ghostRows)
			}
		default:
			time.Sleep(time.Second)
		}
	}
}

func TestMigratorRetry(t *testing.T) {
	oldRetrySleepFn := RetrySleepFn
	defer func() { RetrySleepFn = oldRetrySleepFn }()

	migrationContext := base.NewMigrationContext()
	migrationContext.SetDefaultNumRetries(100)
	migrator := NewMigrator(migrationContext, "1.2.3")

	var sleeps = 0
	RetrySleepFn = func(duration time.Duration) {
		assert.Equal(t, 1*time.Second, duration)
		sleeps++
	}

	var tries = 0
	retryable := func() error {
		tries++
		if tries < int(migrationContext.MaxRetries()) {
			return errors.New("Backoff")
		}
		return nil
	}

	result := migrator.retryOperation(retryable, false)
	assert.NoError(t, result)
	assert.Equal(t, sleeps, 99)
	assert.Equal(t, tries, 100)
}

func TestMigratorRetryWithExponentialBackoff(t *testing.T) {
	oldRetrySleepFn := RetrySleepFn
	defer func() { RetrySleepFn = oldRetrySleepFn }()

	migrationContext := base.NewMigrationContext()
	migrationContext.SetDefaultNumRetries(100)
	migrationContext.SetExponentialBackoffMaxInterval(42)
	migrator := NewMigrator(migrationContext, "1.2.3")

	var sleeps = 0
	expected := []int{
		1, 2, 4, 8, 16, 32, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42,
		42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42,
		42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42,
		42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42,
		42, 42, 42, 42, 42, 42,
	}
	RetrySleepFn = func(duration time.Duration) {
		assert.Equal(t, time.Duration(expected[sleeps])*time.Second, duration)
		sleeps++
	}

	var tries = 0
	retryable := func() error {
		tries++
		if tries < int(migrationContext.MaxRetries()) {
			return errors.New("Backoff")
		}
		return nil
	}

	result := migrator.retryOperationWithExponentialBackoff(retryable, false)
	assert.NoError(t, result)
	assert.Equal(t, sleeps, 99)
	assert.Equal(t, tries, 100)
}

func TestMigratorRetryAbortsOnContextCancellation(t *testing.T) {
	oldRetrySleepFn := RetrySleepFn
	defer func() { RetrySleepFn = oldRetrySleepFn }()

	migrationContext := base.NewMigrationContext()
	migrationContext.SetDefaultNumRetries(100)
	migrator := NewMigrator(migrationContext, "1.2.3")

	RetrySleepFn = func(duration time.Duration) {
		// No sleep needed for this test
	}

	var tries = 0
	retryable := func() error {
		tries++
		if tries == 5 {
			// Cancel context on 5th try
			migrationContext.CancelContext()
		}
		return errors.New("Simulated error")
	}

	result := migrator.retryOperation(retryable, false)
	assert.Error(t, result)
	// Should abort after 6 tries: 5 failures + 1 checkAbort detection
	assert.True(t, tries <= 6, "Expected tries <= 6, got %d", tries)
	// Verify we got context cancellation error
	assert.Contains(t, result.Error(), "context canceled")
}

func TestMigratorRetryWithExponentialBackoffAbortsOnContextCancellation(t *testing.T) {
	oldRetrySleepFn := RetrySleepFn
	defer func() { RetrySleepFn = oldRetrySleepFn }()

	migrationContext := base.NewMigrationContext()
	migrationContext.SetDefaultNumRetries(100)
	migrationContext.SetExponentialBackoffMaxInterval(42)
	migrator := NewMigrator(migrationContext, "1.2.3")

	RetrySleepFn = func(duration time.Duration) {
		// No sleep needed for this test
	}

	var tries = 0
	retryable := func() error {
		tries++
		if tries == 5 {
			// Cancel context on 5th try
			migrationContext.CancelContext()
		}
		return errors.New("Simulated error")
	}

	result := migrator.retryOperationWithExponentialBackoff(retryable, false)
	assert.Error(t, result)
	// Should abort after 6 tries: 5 failures + 1 checkAbort detection
	assert.True(t, tries <= 6, "Expected tries <= 6, got %d", tries)
	// Verify we got context cancellation error
	assert.Contains(t, result.Error(), "context canceled")
}

func TestMigratorRetrySkipsRetriesForWarnings(t *testing.T) {
	oldRetrySleepFn := RetrySleepFn
	defer func() { RetrySleepFn = oldRetrySleepFn }()

	migrationContext := base.NewMigrationContext()
	migrationContext.SetDefaultNumRetries(100)
	migrator := NewMigrator(migrationContext, "1.2.3")

	RetrySleepFn = func(duration time.Duration) {
		t.Fatal("Should not sleep/retry for warning errors")
	}

	var tries = 0
	retryable := func() error {
		tries++
		return errors.New("warnings detected in statement 1 of 1: [Warning: Duplicate entry 'test' for key 'idx' (1062)]")
	}

	result := migrator.retryOperation(retryable, false)
	assert.Error(t, result)
	// Should only try once - no retries for warnings
	assert.Equal(t, 1, tries, "Expected exactly 1 try (no retries) for warning error")
	assert.Contains(t, result.Error(), "warnings detected")
}

func TestMigratorRetryWithExponentialBackoffSkipsRetriesForWarnings(t *testing.T) {
	oldRetrySleepFn := RetrySleepFn
	defer func() { RetrySleepFn = oldRetrySleepFn }()

	migrationContext := base.NewMigrationContext()
	migrationContext.SetDefaultNumRetries(100)
	migrationContext.SetExponentialBackoffMaxInterval(42)
	migrator := NewMigrator(migrationContext, "1.2.3")

	RetrySleepFn = func(duration time.Duration) {
		t.Fatal("Should not sleep/retry for warning errors")
	}

	var tries = 0
	retryable := func() error {
		tries++
		return errors.New("warnings detected in statement 1 of 1: [Warning: Duplicate entry 'test' for key 'idx' (1062)]")
	}

	result := migrator.retryOperationWithExponentialBackoff(retryable, false)
	assert.Error(t, result)
	// Should only try once - no retries for warnings
	assert.Equal(t, 1, tries, "Expected exactly 1 try (no retries) for warning error")
	assert.Contains(t, result.Error(), "warnings detected")
}

func (suite *MigratorTestSuite) TestCutOverLossDataCaseLockGhostBeforeRename() {
	ctx := context.Background()

	_, err := suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, name VARCHAR(64))", getTestTableName()))
	suite.Require().NoError(err)

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("insert into %s values(1,'a')", getTestTableName()))
	suite.Require().NoError(err)

	done := make(chan error, 1)
	go func() {
		connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
		if err != nil {
			done <- err
			return
		}
		migrationContext := newTestMigrationContext()
		migrationContext.ApplierConnectionConfig = connectionConfig
		migrationContext.InspectorConnectionConfig = connectionConfig
		migrationContext.SetConnectionConfig("innodb")
		migrationContext.AllowSetupMetadataLockInstruments = true
		migrationContext.AlterStatementOptions = "ADD COLUMN foobar varchar(255)"
		migrationContext.HeartbeatIntervalMilliseconds = 100
		migrationContext.CutOverLockTimeoutSeconds = 4

		_, filename, _, _ := runtime.Caller(0)
		migrationContext.PostponeCutOverFlagFile = filepath.Join(filepath.Dir(filename), "../../tmp/ghost.postpone.flag")

		migrator := NewMigrator(migrationContext, "0.0.0")

		//nolint:contextcheck
		done <- migrator.Migrate()
	}()

	time.Sleep(2 * time.Second)
	//nolint:dogsled
	_, filename, _, _ := runtime.Caller(0)
	err = os.Remove(filepath.Join(filepath.Dir(filename), "../../tmp/ghost.postpone.flag"))
	if err != nil {
		suite.Require().NoError(err)
	}
	time.Sleep(1 * time.Second)
	go func() {
		holdConn, err := suite.db.Conn(ctx)
		suite.Require().NoError(err)
		_, err = holdConn.ExecContext(ctx, "SELECT *, sleep(2) FROM test._testing_gho WHERE id = 1")
		suite.Require().NoError(err)
	}()

	dmlConn, err := suite.db.Conn(ctx)
	suite.Require().NoError(err)

	_, err = dmlConn.ExecContext(ctx, fmt.Sprintf("insert into %s (id, name) values(2,'b')", getTestTableName()))
	fmt.Println("insert into table original table")
	suite.Require().NoError(err)

	migrateErr := <-done
	suite.Require().NoError(migrateErr)

	// Verify the new column was added
	var delValue, OriginalValue int64
	err = suite.db.QueryRow(
		fmt.Sprintf("select count(*) from %s._%s_del", testMysqlDatabase, testMysqlTableName),
	).Scan(&delValue)
	suite.Require().NoError(err)

	err = suite.db.QueryRow("select count(*) from " + getTestTableName()).Scan(&OriginalValue)
	suite.Require().NoError(err)

	suite.Require().LessOrEqual(delValue, OriginalValue)

	var tableName, createTableSQL string
	err = suite.db.QueryRow("SHOW CREATE TABLE "+getTestTableName()).Scan(&tableName, &createTableSQL)
	suite.Require().NoError(err)

	suite.Require().Equal(testMysqlTableName, tableName)
	suite.Require().Equal("CREATE TABLE `testing` (\n  `id` int NOT NULL,\n  `name` varchar(64) DEFAULT NULL,\n  `foobar` varchar(255) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", createTableSQL)
}

func (suite *MigratorTestSuite) TestRevertEmpty() {
	ctx := context.Background()

	_, err := suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, s CHAR(32))", getTestTableName()))
	suite.Require().NoError(err)

	var oldTableName string

	// perform original migration
	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)
	{
		migrationContext := newTestMigrationContext()
		migrationContext.ApplierConnectionConfig = connectionConfig
		migrationContext.InspectorConnectionConfig = connectionConfig
		migrationContext.SetConnectionConfig("innodb")
		migrationContext.AlterStatement = "ADD COLUMN newcol CHAR(32)"
		migrationContext.Checkpoint = true
		migrationContext.CheckpointIntervalSeconds = 10
		migrationContext.DropServeSocket = true
		migrationContext.InitiallyDropOldTable = true
		migrationContext.UseGTIDs = true

		migrator := NewMigrator(migrationContext, "0.0.0")

		err = migrator.Migrate()
		oldTableName = migrationContext.GetOldTableName()
		suite.Require().NoError(err)
	}

	// revert the original migration
	{
		migrationContext := newTestMigrationContext()
		migrationContext.ApplierConnectionConfig = connectionConfig
		migrationContext.InspectorConnectionConfig = connectionConfig
		migrationContext.SetConnectionConfig("innodb")
		migrationContext.DropServeSocket = true
		migrationContext.UseGTIDs = true
		migrationContext.Revert = true
		migrationContext.OkToDropTable = true
		migrationContext.OldTableName = oldTableName

		migrator := NewMigrator(migrationContext, "0.0.0")

		err = migrator.Revert()
		suite.Require().NoError(err)
	}
}

func (suite *MigratorTestSuite) TestRevert() {
	ctx := context.Background()

	_, err := suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, s CHAR(32))", getTestTableName()))
	suite.Require().NoError(err)

	numRows := 0
	for range 100 {
		_, err = suite.db.ExecContext(ctx,
			fmt.Sprintf("INSERT INTO %s (id, s) VALUES (%d, MD5('%d'))", getTestTableName(), numRows, numRows))
		suite.Require().NoError(err)
		numRows += 1
	}

	var oldTableName string

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)
	// perform original migration
	{
		migrationContext := newTestMigrationContext()
		migrationContext.ApplierConnectionConfig = connectionConfig
		migrationContext.InspectorConnectionConfig = connectionConfig
		migrationContext.SetConnectionConfig("innodb")
		migrationContext.AlterStatement = "ADD INDEX idx1 (s)"
		migrationContext.Checkpoint = true
		migrationContext.CheckpointIntervalSeconds = 10
		migrationContext.DropServeSocket = true
		migrationContext.InitiallyDropOldTable = true
		migrationContext.UseGTIDs = true

		migrator := NewMigrator(migrationContext, "0.0.0")

		err = migrator.Migrate()
		oldTableName = migrationContext.GetOldTableName()
		suite.Require().NoError(err)
	}

	// do some writes
	for range 100 {
		_, err = suite.db.ExecContext(ctx,
			fmt.Sprintf("INSERT INTO %s (id, s) VALUES (%d, MD5('%d'))", getTestTableName(), numRows, numRows))
		suite.Require().NoError(err)
		numRows += 1
	}
	for i := 0; i < numRows; i += 7 {
		_, err = suite.db.ExecContext(ctx,
			fmt.Sprintf("UPDATE %s SET s=MD5('%d') where id=%d", getTestTableName(), 2*i, i))
		suite.Require().NoError(err)
	}

	// revert the original migration
	{
		migrationContext := newTestMigrationContext()
		migrationContext.ApplierConnectionConfig = connectionConfig
		migrationContext.InspectorConnectionConfig = connectionConfig
		migrationContext.SetConnectionConfig("innodb")
		migrationContext.DropServeSocket = true
		migrationContext.UseGTIDs = true
		migrationContext.Revert = true
		migrationContext.OldTableName = oldTableName

		migrator := NewMigrator(migrationContext, "0.0.0")

		err = migrator.Revert()
		oldTableName = migrationContext.GetOldTableName()
		suite.Require().NoError(err)
	}

	// checksum original and reverted table
	var _tableName, checksum1, checksum2 string
	rows, err := suite.db.Query(fmt.Sprintf("CHECKSUM TABLE %s, %s", testMysqlTableName, oldTableName))
	suite.Require().NoError(err)
	defer rows.Close()
	suite.Require().True(rows.Next())
	suite.Require().NoError(rows.Scan(&_tableName, &checksum1))
	suite.Require().True(rows.Next())
	suite.Require().NoError(rows.Scan(&_tableName, &checksum2))
	suite.Require().NoError(rows.Err())

	suite.Require().Equal(checksum1, checksum2)
}

func TestMigrator(t *testing.T) {
	suite.Run(t, new(MigratorTestSuite))
}

func TestPanicAbort_PropagatesError(t *testing.T) {
	migrationContext := base.NewMigrationContext()
	migrator := NewMigrator(migrationContext, "1.0.0")

	// Start listenOnPanicAbort
	go migrator.listenOnPanicAbort()

	// Send an error to PanicAbort
	testErr := errors.New("test abort error")
	go func() {
		migrationContext.PanicAbort <- testErr
	}()

	// Wait a bit for error to be processed
	time.Sleep(100 * time.Millisecond)

	// Verify error was stored
	got := migrationContext.GetAbortError()
	if got != testErr { //nolint:errorlint // Testing pointer equality for sentinel error
		t.Errorf("Expected error %v, got %v", testErr, got)
	}

	// Verify context was cancelled
	ctx := migrationContext.GetContext()
	select {
	case <-ctx.Done():
		// Success - context was cancelled
	default:
		t.Error("Expected context to be cancelled")
	}
}

func TestPanicAbort_FirstErrorWins(t *testing.T) {
	migrationContext := base.NewMigrationContext()
	migrator := NewMigrator(migrationContext, "1.0.0")

	// Start listenOnPanicAbort
	go migrator.listenOnPanicAbort()

	// Send first error
	err1 := errors.New("first error")
	go func() {
		migrationContext.PanicAbort <- err1
	}()

	// Wait for first error to be processed
	time.Sleep(50 * time.Millisecond)

	// Try to send second error (should be ignored)
	err2 := errors.New("second error")
	migrationContext.SetAbortError(err2)

	// Verify only first error is stored
	got := migrationContext.GetAbortError()
	if got != err1 { //nolint:errorlint // Testing pointer equality for sentinel error
		t.Errorf("Expected first error %v, got %v", err1, got)
	}
}

func TestAbort_AfterRowCopy(t *testing.T) {
	migrationContext := base.NewMigrationContext()
	migrator := NewMigrator(migrationContext, "1.0.0")

	// Start listenOnPanicAbort
	go migrator.listenOnPanicAbort()

	// Give listenOnPanicAbort time to start
	time.Sleep(20 * time.Millisecond)

	// Simulate row copy error by sending to rowCopyComplete in a goroutine
	// (unbuffered channel, so send must be async)
	testErr := errors.New("row copy failed")
	go func() {
		migrator.rowCopyComplete <- testErr
	}()

	// Consume the error (simulating what Migrate() does)
	// This is a blocking call that waits for the error
	migrator.consumeRowCopyComplete()

	// Wait for the error to be processed by listenOnPanicAbort
	time.Sleep(50 * time.Millisecond)

	// Check that error was stored
	if got := migrationContext.GetAbortError(); got == nil {
		t.Fatal("Expected abort error to be stored after row copy error")
	} else if got.Error() != "row copy failed" {
		t.Errorf("Expected 'row copy failed', got %v", got)
	}

	// Verify context was cancelled
	ctx := migrationContext.GetContext()
	select {
	case <-ctx.Done():
		// Success
	case <-time.After(1 * time.Second):
		t.Error("Expected context to be cancelled after row copy error")
	}
}

func TestAbort_DuringInspection(t *testing.T) {
	migrationContext := base.NewMigrationContext()
	migrator := NewMigrator(migrationContext, "1.0.0")

	// Start listenOnPanicAbort
	go migrator.listenOnPanicAbort()

	// Simulate error during inspection phase
	testErr := errors.New("inspection failed")
	go func() {
		time.Sleep(10 * time.Millisecond)
		select {
		case migrationContext.PanicAbort <- testErr:
		case <-migrationContext.GetContext().Done():
		}
	}()

	// Wait for abort to be processed
	time.Sleep(50 * time.Millisecond)

	// Call checkAbort (simulating what Migrate() does after initiateInspector)
	err := migrator.checkAbort()
	if err == nil {
		t.Fatal("Expected checkAbort to return error after abort during inspection")
	}

	if err.Error() != "inspection failed" {
		t.Errorf("Expected 'inspection failed', got %v", err)
	}
}

func TestAbort_DuringStreaming(t *testing.T) {
	migrationContext := base.NewMigrationContext()
	migrator := NewMigrator(migrationContext, "1.0.0")

	// Start listenOnPanicAbort
	go migrator.listenOnPanicAbort()

	// Simulate error from streaming goroutine
	testErr := errors.New("streaming error")
	go func() {
		time.Sleep(10 * time.Millisecond)
		// Use select pattern like actual code does
		select {
		case migrationContext.PanicAbort <- testErr:
		case <-migrationContext.GetContext().Done():
		}
	}()

	// Wait for abort to be processed
	time.Sleep(50 * time.Millisecond)

	// Verify error stored and context cancelled
	if got := migrationContext.GetAbortError(); got == nil {
		t.Fatal("Expected abort error to be stored")
	} else if got.Error() != "streaming error" {
		t.Errorf("Expected 'streaming error', got %v", got)
	}

	// Verify checkAbort catches it
	err := migrator.checkAbort()
	if err == nil {
		t.Fatal("Expected checkAbort to return error after streaming abort")
	}
}

func TestRetryExhaustion_TriggersAbort(t *testing.T) {
	migrationContext := base.NewMigrationContext()
	migrationContext.SetDefaultNumRetries(2) // Only 2 retries
	migrator := NewMigrator(migrationContext, "1.0.0")

	// Start listenOnPanicAbort
	go migrator.listenOnPanicAbort()

	// Operation that always fails
	callCount := 0
	operation := func() error {
		callCount++
		return errors.New("persistent failure")
	}

	// Call retryOperation (with notFatalHint=false so it sends to PanicAbort)
	err := migrator.retryOperation(operation)

	// Should have called operation MaxRetries times
	if callCount != 2 {
		t.Errorf("Expected 2 retry attempts, got %d", callCount)
	}

	// Should return the error
	if err == nil {
		t.Fatal("Expected retryOperation to return error")
	}

	// Wait for abort to be processed
	time.Sleep(100 * time.Millisecond)

	// Verify error was sent to PanicAbort and stored
	if got := migrationContext.GetAbortError(); got == nil {
		t.Error("Expected abort error to be stored after retry exhaustion")
	}

	// Verify context was cancelled
	ctx := migrationContext.GetContext()
	select {
	case <-ctx.Done():
		// Success
	default:
		t.Error("Expected context to be cancelled after retry exhaustion")
	}
}

func TestRevert_AbortsOnError(t *testing.T) {
	migrationContext := base.NewMigrationContext()
	migrationContext.Revert = true
	migrationContext.OldTableName = "_test_del"
	migrationContext.OriginalTableName = "test"
	migrationContext.DatabaseName = "testdb"
	migrator := NewMigrator(migrationContext, "1.0.0")

	// Start listenOnPanicAbort
	go migrator.listenOnPanicAbort()

	// Simulate error during revert
	testErr := errors.New("revert failed")
	go func() {
		time.Sleep(10 * time.Millisecond)
		select {
		case migrationContext.PanicAbort <- testErr:
		case <-migrationContext.GetContext().Done():
		}
	}()

	// Wait for abort to be processed
	time.Sleep(50 * time.Millisecond)

	// Verify checkAbort catches it
	err := migrator.checkAbort()
	if err == nil {
		t.Fatal("Expected checkAbort to return error during revert")
	}

	if err.Error() != "revert failed" {
		t.Errorf("Expected 'revert failed', got %v", err)
	}

	// Verify context was cancelled
	ctx := migrationContext.GetContext()
	select {
	case <-ctx.Done():
		// Success
	default:
		t.Error("Expected context to be cancelled during revert abort")
	}
}

func TestCheckAbort_ReturnsNilWhenNoError(t *testing.T) {
	migrationContext := base.NewMigrationContext()
	migrator := NewMigrator(migrationContext, "1.0.0")

	// No error has occurred
	err := migrator.checkAbort()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestCheckAbort_DetectsContextCancellation(t *testing.T) {
	migrationContext := base.NewMigrationContext()
	migrator := NewMigrator(migrationContext, "1.0.0")

	// Cancel context directly (without going through PanicAbort)
	migrationContext.CancelContext()

	// checkAbort should detect the cancellation
	err := migrator.checkAbort()
	if err == nil {
		t.Fatal("Expected checkAbort to return error when context is cancelled")
	}
}

func (suite *MigratorTestSuite) TestPanicOnWarningsDuplicateDuringCutoverWithHighRetries() {
	ctx := context.Background()

	// Create table with email column (no unique constraint initially)
	_, err := suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY AUTO_INCREMENT, email VARCHAR(100))", getTestTableName()))
	suite.Require().NoError(err)

	// Insert initial rows with unique email values - passes pre-flight validation
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (email) VALUES ('user1@example.com')", getTestTableName()))
	suite.Require().NoError(err)
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (email) VALUES ('user2@example.com')", getTestTableName()))
	suite.Require().NoError(err)
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (email) VALUES ('user3@example.com')", getTestTableName()))
	suite.Require().NoError(err)

	// Verify we have 3 rows
	var count int
	err = suite.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", getTestTableName())).Scan(&count)
	suite.Require().NoError(err)
	suite.Require().Equal(3, count)

	// Create postpone flag file
	tmpDir, err := os.MkdirTemp("", "gh-ost-postpone-test")
	suite.Require().NoError(err)
	defer os.RemoveAll(tmpDir)
	postponeFlagFile := filepath.Join(tmpDir, "postpone.flag")
	err = os.WriteFile(postponeFlagFile, []byte{}, 0644)
	suite.Require().NoError(err)

	// Start migration in goroutine
	done := make(chan error, 1)
	go func() {
		connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
		if err != nil {
			done <- err
			return
		}

		migrationContext := newTestMigrationContext()
		migrationContext.ApplierConnectionConfig = connectionConfig
		migrationContext.InspectorConnectionConfig = connectionConfig
		migrationContext.SetConnectionConfig("innodb")
		migrationContext.AlterStatementOptions = "ADD UNIQUE KEY unique_email_idx (email)"
		migrationContext.HeartbeatIntervalMilliseconds = 100
		migrationContext.PostponeCutOverFlagFile = postponeFlagFile
		migrationContext.PanicOnWarnings = true

		// High retry count + exponential backoff means retries will take a long time and fail the test if not properly aborted
		migrationContext.SetDefaultNumRetries(30)
		migrationContext.CutOverExponentialBackoff = true
		migrationContext.SetExponentialBackoffMaxInterval(128)

		migrator := NewMigrator(migrationContext, "0.0.0")

		//nolint:contextcheck
		done <- migrator.Migrate()
	}()

	// Wait for migration to reach postponed state
	// TODO replace this with an actual check for postponed state
	time.Sleep(3 * time.Second)

	// Now insert a duplicate email value while migration is postponed
	// This simulates data arriving during migration that would violate the unique constraint
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (email) VALUES ('user1@example.com')", getTestTableName()))
	suite.Require().NoError(err)

	// Verify we now have 4 rows (including the duplicate)
	err = suite.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", getTestTableName())).Scan(&count)
	suite.Require().NoError(err)
	suite.Require().Equal(4, count)

	// Unpostpone the migration - gh-ost will now try to apply binlog events with the duplicate
	err = os.Remove(postponeFlagFile)
	suite.Require().NoError(err)

	// Wait for Migrate() to return - with timeout to detect if it hangs
	select {
	case migrateErr := <-done:
		// Success - Migrate() returned
		// It should return an error due to the duplicate
		suite.Require().Error(migrateErr, "Expected migration to fail due to duplicate key violation")
		suite.Require().Contains(migrateErr.Error(), "Duplicate entry", "Error should mention duplicate entry")
	case <-time.After(5 * time.Minute):
		suite.FailNow("Migrate() hung and did not return within 5 minutes - failure to abort on warnings in retry loop")
	}

	// Verify all 4 rows are still in the original table (no silent data loss)
	err = suite.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", getTestTableName())).Scan(&count)
	suite.Require().NoError(err)
	suite.Require().Equal(4, count, "Original table should still have all 4 rows")

	// Verify both user1@example.com entries still exist
	var duplicateCount int
	err = suite.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE email = 'user1@example.com'", getTestTableName())).Scan(&duplicateCount)
	suite.Require().NoError(err)
	suite.Require().Equal(2, duplicateCount, "Should have 2 duplicate email entries")
}
