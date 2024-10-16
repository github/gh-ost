package binlog

import (
	"database/sql"
	"testing"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/mysql"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

func getCurrentBinlogCoordinates(t *testing.T, db *sql.DB) mysql.BinlogCoordinates {
	var file string
	var position int64
	var binlogDoDb string
	var binlogIgnoreDb string
	var executedGtidSet string

	err := db.QueryRow("SHOW MASTER STATUS").Scan(&file, &position, &binlogDoDb, &binlogIgnoreDb, &executedGtidSet)
	require.NoError(t, err)

	return mysql.BinlogCoordinates{LogFile: file, LogPos: position}
}

func getMigrationContext() *base.MigrationContext {
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
	return migrationContext
}

func prepareDatabase(t *testing.T, db *sql.DB) {
	_, err := db.Exec("DROP DATABASE test")
	require.NoError(t, err)

	_, err = db.Exec("CREATE DATABASE test")
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE test.gh_ost_test (id int NOT NULL AUTO_INCREMENT, name varchar(255), PRIMARY KEY (id)) ENGINE=InnoDB")
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE test.gh_ost_test2 (id int NOT NULL AUTO_INCREMENT, name varchar(255), PRIMARY KEY (id)) ENGINE=InnoDB")
	require.NoError(t, err)
}

func TestStreamTransactionSingleAutoCommitChange(t *testing.T) {
	db, err := sql.Open("mysql", "root:root@/")
	require.NoError(t, err)
	defer db.Close()

	prepareDatabase(t, db)

	binlogCoordinates := getCurrentBinlogCoordinates(t, db)

	migrationContext := getMigrationContext()
	migrationContext.DatabaseName = "test"
	migrationContext.OriginalTableName = "gh_ost_test"
	migrationContext.AlterStatement = "ALTER TABLE gh_ost_test ENGINE=InnoDB"

	transactionsChan := make(chan *Transaction)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		reader := NewGoMySQLReader(migrationContext)
		reader.ConnectBinlogStreamer(binlogCoordinates)

		err := reader.StreamTransactions(ctx, transactionsChan)
		require.Equal(t, err, context.Canceled)
	}()

	_, err = db.Exec("INSERT INTO test.gh_ost_test (name) VALUES ('test')")
	require.NoError(t, err)

	tx := <-transactionsChan

	changes := make([]*BinlogEntry, 0)
	for change := range tx.Changes {
		changes = append(changes, change)
	}
	require.Len(t, changes, 1)

	cancel()
	close(transactionsChan)
	require.Len(t, transactionsChan, 0)
}

func TestStreamTransactionSingleChangeInTransaction(t *testing.T) {
	db, err := sql.Open("mysql", "root:root@/")
	require.NoError(t, err)
	defer db.Close()

	prepareDatabase(t, db)

	binlogCoordinates := getCurrentBinlogCoordinates(t, db)

	migrationContext := getMigrationContext()
	migrationContext.DatabaseName = "test"
	migrationContext.OriginalTableName = "gh_ost_test"
	migrationContext.AlterStatement = "ALTER TABLE gh_ost_test ENGINE=InnoDB"

	transactionsChan := make(chan *Transaction)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		reader := NewGoMySQLReader(migrationContext)
		reader.ConnectBinlogStreamer(binlogCoordinates)

		err := reader.StreamTransactions(ctx, transactionsChan)
		require.Equal(t, err, context.Canceled)
	}()

	sqlTx, err := db.Begin()
	require.NoError(t, err)

	_, err = sqlTx.Exec("INSERT INTO test.gh_ost_test (name) VALUES ('test')")
	require.NoError(t, err)

	err = sqlTx.Commit()
	require.NoError(t, err)

	tx := <-transactionsChan

	changes := make([]*BinlogEntry, 0)
	for change := range tx.Changes {
		changes = append(changes, change)
	}
	require.Len(t, changes, 1)

	cancel()
	close(transactionsChan)
	require.Len(t, transactionsChan, 0)
}

func TestStreamTransactionMultipleChangesInTransaction(t *testing.T) {
	db, err := sql.Open("mysql", "root:root@/")
	require.NoError(t, err)
	defer db.Close()

	prepareDatabase(t, db)

	binlogCoordinates := getCurrentBinlogCoordinates(t, db)

	migrationContext := getMigrationContext()
	migrationContext.DatabaseName = "test"
	migrationContext.OriginalTableName = "gh_ost_test"
	migrationContext.AlterStatement = "ALTER TABLE gh_ost_test ENGINE=InnoDB"

	transactionsChan := make(chan *Transaction)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		reader := NewGoMySQLReader(migrationContext)
		reader.ConnectBinlogStreamer(binlogCoordinates)

		err := reader.StreamTransactions(ctx, transactionsChan)
		require.Equal(t, err, context.Canceled)
	}()

	sqlTx, err := db.Begin()
	require.NoError(t, err)

	_, err = sqlTx.Exec("INSERT INTO test.gh_ost_test (name) VALUES ('test1')")
	require.NoError(t, err)

	_, err = sqlTx.Exec("INSERT INTO test.gh_ost_test (name) VALUES ('test2')")
	require.NoError(t, err)

	_, err = sqlTx.Exec("INSERT INTO test.gh_ost_test (name) VALUES ('test3')")
	require.NoError(t, err)

	err = sqlTx.Commit()
	require.NoError(t, err)

	tx := <-transactionsChan
	require.NotNil(t, tx)

	changes := make([]*BinlogEntry, 0)
	for change := range tx.Changes {
		changes = append(changes, change)
	}
	require.Len(t, changes, 3)

	cancel()
	close(transactionsChan)
	require.Len(t, transactionsChan, 0)
}

func TestStreamTransactionWithDDL(t *testing.T) {
	db, err := sql.Open("mysql", "root:root@/")
	require.NoError(t, err)
	defer db.Close()

	prepareDatabase(t, db)

	binlogCoordinates := getCurrentBinlogCoordinates(t, db)

	migrationContext := getMigrationContext()
	migrationContext.DatabaseName = "test"
	migrationContext.OriginalTableName = "gh_ost_test"
	migrationContext.AlterStatement = "ALTER TABLE gh_ost_test ENGINE=InnoDB"

	transactionsChan := make(chan *Transaction)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		reader := NewGoMySQLReader(migrationContext)
		reader.ConnectBinlogStreamer(binlogCoordinates)

		err := reader.StreamTransactions(ctx, transactionsChan)
		require.Equal(t, err, context.Canceled)
	}()

	_, err = db.Exec("ALTER TABLE test.gh_ost_test ADD COLUMN age INT")
	require.NoError(t, err)

	tx := <-transactionsChan

	changes := make([]*BinlogEntry, 0)
	for change := range tx.Changes {
		changes = append(changes, change)
	}
	require.Len(t, changes, 0)

	cancel()
	close(transactionsChan)
	require.Len(t, transactionsChan, 0)
}
