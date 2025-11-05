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
	"github.com/testcontainers/testcontainers-go/wait"
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
		testcontainers.WithWaitStrategy(wait.ForExposedPort()),
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
	//nolint:execinquery
	err = suite.db.QueryRow("SHOW CREATE TABLE "+getTestTableName()).Scan(&tableName, &createTableSQL)
	suite.Require().NoError(err)

	suite.Require().Equal("testing", tableName)
	suite.Require().Equal("CREATE TABLE `testing` (\n  `id` int NOT NULL,\n  `name` varchar(64) DEFAULT NULL,\n  `foobar` varchar(255) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", createTableSQL)

	// Verify the changelog table was claned up
	//nolint:execinquery
	err = suite.db.QueryRow("SHOW TABLES IN test LIKE '_testing_ghc'").Scan(&tableName)
	suite.Require().Error(err)
	suite.Require().Equal(gosql.ErrNoRows, err)

	// Verify the old table was renamed
	//nolint:execinquery
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
	//nolint:execinquery
	err = suite.db.QueryRow("SHOW CREATE TABLE "+getTestTableName()).Scan(&tableName, &createTableSQL)
	suite.Require().NoError(err)

	suite.Require().Equal(testMysqlTableName, tableName)
	suite.Require().Equal("CREATE TABLE `testing` (\n  `id` int NOT NULL,\n  `name` varchar(64) DEFAULT NULL,\n  `foobar` varchar(255) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", createTableSQL)
}

func TestMigrator(t *testing.T) {
	suite.Run(t, new(MigratorTestSuite))
}
