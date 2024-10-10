package logic

import (
	"context"
	gosql "database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/binlog"
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"
	"github.com/stretchr/testify/require"
)

func TestCoordinator(t *testing.T) {
	db, err := gosql.Open("mysql", "root:@/")
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	_ = os.Remove("/tmp/gh-ost.sock")

	prepareDatabase(t, db)

	_, err = db.Exec("CREATE TABLE testing._gh_ost_test_gho (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255))")
	require.NoError(t, err)

	migrationContext := base.NewMigrationContext()
	migrationContext.Hostname = "localhost"
	migrationContext.DatabaseName = "testing"
	migrationContext.OriginalTableName = "gh_ost_test"
	migrationContext.AlterStatement = "ALTER TABLE gh_ost_test ENGINE=InnoDB"
	migrationContext.AllowedRunningOnMaster = true
	migrationContext.ReplicaServerId = 99999
	migrationContext.HeartbeatIntervalMilliseconds = 100
	migrationContext.ServeSocketFile = "/tmp/gh-ost.sock"
	migrationContext.ThrottleHTTPIntervalMillis = 100
	migrationContext.DMLBatchSize = 10

	migrationContext.ApplierConnectionConfig = &mysql.ConnectionConfig{
		Key: mysql.InstanceKey{
			Hostname: "localhost",
			Port:     3306,
		},
		User:     "root",
		Password: "",
	}

	migrationContext.InspectorConnectionConfig = &mysql.ConnectionConfig{
		Key: mysql.InstanceKey{
			Hostname: "localhost",
			Port:     3306,
		},
		User:     "root",
		Password: "",
	}

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
	migrationContext.NumWorkers = 4

	applier := NewApplier(migrationContext)
	err = applier.InitDBConnections(migrationContext.NumWorkers)
	require.NoError(t, err)

	err = applier.CreateChangelogTable()
	require.NoError(t, err)

	// 	ctx, cancel := context.WithCancel(context.Background())

	for i := 0; i < 100; i++ {
		tx, err := db.Begin()
		require.NoError(t, err)

		for j := 0; j < 100; j++ {
			_, err = tx.Exec("INSERT INTO testing.gh_ost_test (name) VALUES ('test')")
			require.NoError(t, err)
		}

		err = tx.Commit()
		require.NoError(t, err)
	}

	_, err = db.Exec("UPDATE testing.gh_ost_test SET name = 'foobar' WHERE id = 1")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO testing.gh_ost_test (name) VALUES ('test')")
	require.NoError(t, err)

	_, err = applier.WriteChangelogState("completed")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	coord := NewCoordinator(migrationContext, applier, func(dmlEvent *binlog.BinlogDMLEvent) error {
		fmt.Printf("Received Changelog DML event: %+v\n", dmlEvent)
		fmt.Printf("Rowdata: %v - %v\n", dmlEvent.NewColumnValues, dmlEvent.WhereColumnValues)

		cancel()

		return nil
	})
	coord.applier = applier
	coord.InitializeWorkers(8)

	canStopStreaming := func() bool {
		return false
	}
	go func() {
		err = coord.StartStreaming(canStopStreaming)
		require.NoError(t, err)
	}()

	// Give streamer some time to start
	time.Sleep(1 * time.Second)

	startAt := time.Now()

	for {
		if ctx.Err() != nil {
			break
		}

		err = coord.ProcessEventsUntilDrained()
		require.NoError(t, err)
	}

	fmt.Printf("Time taken: %s\n", time.Since(startAt))
}
