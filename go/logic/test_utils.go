package logic

import (
	"context"

	"fmt"
	"path/filepath"
	"runtime"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/mysql"
	"github.com/testcontainers/testcontainers-go"
)

var (
	testMysqlContainerImage = "mysql:8.0.42"
	testMysqlUser           = "root"
	testMysqlPass           = "root-password"
	testMysqlDatabase       = "test"
	testMysqlTableName      = "testing"
)

func getTestTableName() string {
	return fmt.Sprintf("`%s`.`%s`", testMysqlDatabase, testMysqlTableName)
}

func getTestGhostTableName() string {
	return fmt.Sprintf("`%s`.`_%s_gho`", testMysqlDatabase, testMysqlTableName)
}

func getTestConnectionConfig(ctx context.Context, container testcontainers.Container) (*mysql.ConnectionConfig, error) {
	host, err := container.Host(ctx)
	if err != nil {
		return nil, err
	}

	port, err := container.MappedPort(ctx, "3306")
	if err != nil {
		return nil, err
	}

	connectionConfig := mysql.NewConnectionConfig()
	connectionConfig.Key.Hostname = host
	connectionConfig.Key.Port = port.Int()
	connectionConfig.User = testMysqlUser
	connectionConfig.Password = testMysqlPass

	return connectionConfig, nil
}

func newTestMigrationContext() *base.MigrationContext {
	migrationContext := base.NewMigrationContext()
	migrationContext.ReplicaServerId = 99999
	migrationContext.HeartbeatIntervalMilliseconds = 100
	migrationContext.ThrottleHTTPIntervalMillis = 100
	migrationContext.ThrottleHTTPTimeoutMillis = 1000
	migrationContext.DatabaseName = testMysqlDatabase
	migrationContext.OriginalTableName = testMysqlTableName
	migrationContext.SkipPortValidation = true
	migrationContext.PanicOnWarnings = true
	migrationContext.AllowedRunningOnMaster = true

	//nolint:dogsled
	_, filename, _, _ := runtime.Caller(0)
	migrationContext.ServeSocketFile = filepath.Join(filepath.Dir(filename), "../../tmp/gh-ost.sock")

	return migrationContext
}
