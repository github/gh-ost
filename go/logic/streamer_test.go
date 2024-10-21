package logic

import (
	"fmt"
	"testing"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/binlog"
	"github.com/github/gh-ost/go/mysql"

	"github.com/stretchr/testify/require"
)

func TestStreamEvents(t *testing.T) {
	migrationContext := base.NewMigrationContext()
	migrationContext.InspectorConnectionConfig = &mysql.ConnectionConfig{
		Key: mysql.InstanceKey{
			Hostname: "localhost",
			Port:     3306,
		},
		User:     "root",
		Password: "root",
	}
	migrationContext.SetConnectionConfig("innodb")
	migrationContext.ReplicaServerId = 99999
	migrationContext.DatabaseName = "test"
	migrationContext.OriginalTableName = "gh_ost_test"
	migrationContext.AlterStatement = "ALTER TABLE gh_ost_test ENGINE=InnoDB"

	streamer := NewEventsStreamer(migrationContext)

	require.NoError(t, streamer.InitDBConnections())

	streamer.AddListener(
		false,
		migrationContext.DatabaseName,
		migrationContext.OriginalTableName,
		func(dmlEvent *binlog.BinlogDMLEvent) error {
			fmt.Printf("DML event: %+v\n", dmlEvent)

			return nil
		},
	)

	streamer.StreamEvents(func() bool {
		return false
	})
}
