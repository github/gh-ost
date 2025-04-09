package logic

import (
	"context"
	gosql "database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"path/filepath"
	"runtime"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/binlog"
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type CoordinatorTestSuite struct {
	suite.Suite

	mysqlContainer testcontainers.Container
	db             *gosql.DB
}

func (suite *CoordinatorTestSuite) SetupSuite() {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "mysql:8.0.40",
		Env:          map[string]string{"MYSQL_ROOT_PASSWORD": "root-password"},
		WaitingFor:   wait.ForListeningPort("3306/tcp"),
		ExposedPorts: []string{"3306/tcp"},
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

func (suite *CoordinatorTestSuite) SetupTest() {
	ctx := context.Background()
	_, err := suite.db.ExecContext(ctx, "RESET MASTER")
	suite.Require().NoError(err)

	_, err = suite.db.ExecContext(ctx, "SET @@GLOBAL.binlog_transaction_dependency_tracking = WRITESET")
	suite.Require().NoError(err)

	_, err = suite.db.ExecContext(ctx, "CREATE DATABASE test")
	suite.Require().NoError(err)
}

func (suite *CoordinatorTestSuite) TearDownTest() {
	ctx := context.Background()
	_, err := suite.db.ExecContext(ctx, "DROP DATABASE test")
	suite.Require().NoError(err)
}

func (suite *CoordinatorTestSuite) TeardownSuite() {
	ctx := context.Background()

	suite.Assert().NoError(suite.db.Close())
	suite.Assert().NoError(suite.mysqlContainer.Terminate(ctx))
}

func (suite *CoordinatorTestSuite) TestApplyDML() {
	ctx := context.Background()

	connectionConfig, err := GetConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	_ = os.Remove("/tmp/gh-ost.sock")

	_, err = suite.db.Exec("CREATE TABLE test.gh_ost_test (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255)) ENGINE=InnoDB")
	suite.Require().NoError(err)

	_, err = suite.db.Exec("CREATE TABLE test._gh_ost_test_gho (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255))")
	suite.Require().NoError(err)

	migrationContext := base.NewMigrationContext()
	migrationContext.DatabaseName = "test"
	migrationContext.OriginalTableName = "gh_ost_test"
	migrationContext.AlterStatement = "ALTER TABLE gh_ost_test ENGINE=InnoDB"
	migrationContext.AllowedRunningOnMaster = true
	migrationContext.ReplicaServerId = 99999
	migrationContext.HeartbeatIntervalMilliseconds = 100
	migrationContext.ThrottleHTTPIntervalMillis = 100
	migrationContext.DMLBatchSize = 10

	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.InspectorConnectionConfig = connectionConfig

	migrationContext.OriginalTableColumns = sql.NewColumnList([]string{"id", "name"})
	migrationContext.GhostTableColumns = sql.NewColumnList([]string{"id", "name"})
	migrationContext.SharedColumns = sql.NewColumnList([]string{"id", "name"})
	migrationContext.MappedSharedColumns = sql.NewColumnList([]string{"id", "name"})
	migrationContext.UniqueKey = &sql.UniqueKey{
		Name:            "PRIMARY",
		Columns:         *sql.NewColumnList([]string{"id"}),
		IsAutoIncrement: true,
	}

	migrationContext.SetConnectionConfig("innodb")
	migrationContext.SkipPortValidation = true
	migrationContext.NumWorkers = 4

	//nolint:dogsled
	_, filename, _, _ := runtime.Caller(0)
	migrationContext.ServeSocketFile = filepath.Join(filepath.Dir(filename), "../../tmp/gh-ost.sock")

	applier := NewApplier(migrationContext)
	err = applier.InitDBConnections(migrationContext.NumWorkers)
	suite.Require().NoError(err)

	err = applier.prepareQueries()
	suite.Require().NoError(err)

	err = applier.CreateChangelogTable()
	suite.Require().NoError(err)

	//  TODO: use errgroup
	for i := 0; i < 100; i++ {
		tx, err := suite.db.Begin()
		suite.Require().NoError(err)

		for j := 0; j < 100; j++ {
			_, err = tx.Exec("INSERT INTO test.gh_ost_test (name) VALUES ('test')")
			suite.Require().NoError(err)
		}

		err = tx.Commit()
		suite.Require().NoError(err)
	}

	_, err = suite.db.Exec("UPDATE test.gh_ost_test SET name = 'foobar' WHERE id = 1")
	suite.Require().NoError(err)

	_, err = suite.db.Exec("INSERT INTO test.gh_ost_test (name) VALUES ('test')")
	suite.Require().NoError(err)

	_, err = applier.WriteChangelogState("completed")
	suite.Require().NoError(err)

	ctx, cancel := context.WithCancel(context.Background())

	coord := NewCoordinator(migrationContext, applier, nil,
		func(dmlEvent *binlog.BinlogDMLEvent) error {
			fmt.Printf("Received Changelog DML event: %+v\n", dmlEvent)
			fmt.Printf("Rowdata: %v - %v\n", dmlEvent.NewColumnValues, dmlEvent.WhereColumnValues)

			cancel()

			return nil
		})
	coord.applier = applier
	coord.currentCoordinates = mysql.BinlogCoordinates{
		LogFile: "binlog.000001",
		LogPos:  int64(4),
	}
	coord.InitializeWorkers(4)

	streamCtx, cancelStreaming := context.WithCancel(context.Background())
	canStopStreaming := func() bool {
		return streamCtx.Err() != nil
	}
	go func() {
		err = coord.StartStreaming(streamCtx, canStopStreaming)
		suite.Require().Equal(context.Canceled, err)
	}()

	// Give streamer some time to start
	time.Sleep(1 * time.Second)

	startAt := time.Now()

	for {
		if ctx.Err() != nil {
			cancelStreaming()
			break
		}

		coord.ProcessEventsUntilDrained()
		suite.Require().NoError(err)
	}

	fmt.Printf("Time taken: %s\n", time.Since(startAt))
}

func TestCoordinator(t *testing.T) {
	suite.Run(t, new(CoordinatorTestSuite))
}
