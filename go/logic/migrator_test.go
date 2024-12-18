/*
   Copyright 2022 GitHub Inc.
         See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"context"
	gosql "database/sql"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

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
		require.Nil(t, migrator.onChangelogEvent(&binlog.BinlogDMLEvent{
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
			require.NotNil(t, es)
			require.NotNil(t, es.writeFunc)
		}(&wg)

		columnValues := sql.ToColumnValues([]interface{}{
			123,
			time.Now().Unix(),
			"state",
			AllEventsUpToLockProcessed,
		})
		require.Nil(t, migrator.onChangelogEvent(&binlog.BinlogDMLEvent{
			DatabaseName:    "test",
			DML:             binlog.InsertDML,
			NewColumnValues: columnValues,
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
	req := testcontainers.ContainerRequest{
		Image:        "mysql:8.0.40",
		Env:          map[string]string{"MYSQL_ROOT_PASSWORD": "root-password"},
		ExposedPorts: []string{"3306/tcp"},
		WaitingFor:   wait.ForListeningPort("3306/tcp"),
	}

	mysqlContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	suite.Require().NoError(err)

	suite.mysqlContainer = mysqlContainer

	dsn, err := GetDSN(ctx, mysqlContainer)
	suite.Require().NoError(err)

	db, err := gosql.Open("mysql", dsn)
	suite.Require().NoError(err)

	suite.db = db
}

func (suite *MigratorTestSuite) TeardownSuite() {
	ctx := context.Background()

	suite.Assert().NoError(suite.db.Close())
	suite.Assert().NoError(suite.mysqlContainer.Terminate(ctx))
}

func (suite *MigratorTestSuite) SetupTest() {
	ctx := context.Background()

	_, err := suite.db.ExecContext(ctx, "CREATE DATABASE test")
	suite.Require().NoError(err)
}

func (suite *MigratorTestSuite) TearDownTest() {
	ctx := context.Background()

	_, err := suite.db.ExecContext(ctx, "DROP DATABASE test")
	suite.Require().NoError(err)
}

func (suite *MigratorTestSuite) TestFoo() {
	ctx := context.Background()

	_, err := suite.db.ExecContext(ctx, "CREATE TABLE test.testing (id INT PRIMARY KEY, name VARCHAR(64))")
	suite.Require().NoError(err)

	connectionConfig, err := GetConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := base.NewMigrationContext()
	migrationContext.AllowedRunningOnMaster = true
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.InspectorConnectionConfig = connectionConfig
	migrationContext.DatabaseName = "test"
	migrationContext.SkipPortValidation = true
	migrationContext.OriginalTableName = "testing"
	migrationContext.SetConnectionConfig("innodb")
	migrationContext.AlterStatementOptions = "ADD COLUMN foobar varchar(255), ENGINE=InnoDB"
	migrationContext.ReplicaServerId = 99999
	migrationContext.HeartbeatIntervalMilliseconds = 100
	migrationContext.ThrottleHTTPIntervalMillis = 100
	migrationContext.ThrottleHTTPTimeoutMillis = 1000

	//nolint:dogsled
	_, filename, _, _ := runtime.Caller(0)
	migrationContext.ServeSocketFile = filepath.Join(filepath.Dir(filename), "../../tmp/gh-ost.sock")

	migrator := NewMigrator(migrationContext, "0.0.0")

	err = migrator.Migrate()
	suite.Require().NoError(err)

	// Verify the new column was added
	var tableName, createTableSQL string
	//nolint:execinquery
	err = suite.db.QueryRow("SHOW CREATE TABLE test.testing").Scan(&tableName, &createTableSQL)
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

func TestMigrator(t *testing.T) {
	suite.Run(t, new(MigratorTestSuite))
}
