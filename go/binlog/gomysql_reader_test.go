package binlog

import (
	"context"
	"database/sql"
	"testing"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/mysql"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func getCurrentBinlogCoordinates(t *testing.T, db *sql.DB) mysql.BinlogCoordinates {
	var file string
	var position int64
	var binlogDoDb string
	var binlogIgnoreDb string
	var executedGtidSet string

	//nolint:execinquery // SHOW MASTER STATUS returns a result set
	err := db.QueryRow("SHOW MASTER STATUS").Scan(&file, &position, &binlogDoDb, &binlogIgnoreDb, &executedGtidSet)
	require.NoError(t, err)

	return mysql.BinlogCoordinates{LogFile: file, LogPos: position}
}

func getMigrationContext(host string) *base.MigrationContext {
	migrationContext := base.NewMigrationContext()
	migrationContext.InspectorConnectionConfig = &mysql.ConnectionConfig{
		Key: mysql.InstanceKey{
			Hostname: host,
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
	_, err := db.Exec("CREATE TABLE test.gh_ost_test (id int NOT NULL AUTO_INCREMENT, name varchar(255), PRIMARY KEY (id)) ENGINE=InnoDB")
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE test.gh_ost_test2 (id int NOT NULL AUTO_INCREMENT, name varchar(255), PRIMARY KEY (id)) ENGINE=InnoDB")
	require.NoError(t, err)
}

type GoMySQLReaderTestSuite struct {
	suite.Suite

	mysqlContainer testcontainers.Container
}

func (suite *GoMySQLReaderTestSuite) SetupSuite() {
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
	suite.Require().NoError(err)
	suite.mysqlContainer = mysqlContainer
}

func (suite *GoMySQLReaderTestSuite) TearDownSuite() {
	ctx := context.Background()
	suite.Require().NoError(suite.mysqlContainer.Terminate(ctx))
}

func (suite *GoMySQLReaderTestSuite) SetupTest() {
	ctx := context.Background()

	rc, _, err := suite.mysqlContainer.Exec(ctx, []string{"mysql", "-proot", "-e", "CREATE DATABASE test"})
	suite.Require().NoError(err)
	suite.Require().Equal(0, rc, "expected exit code 0")
}

func (suite *GoMySQLReaderTestSuite) TearDownTest() {
	ctx := context.Background()

	rc, _, err := suite.mysqlContainer.Exec(ctx, []string{"mysql", "-proot", "-e", "DROP DATABASE test"})
	suite.Require().NoError(err)
	suite.Require().Equal(0, rc, "expected exit code 0")
}

func (suite *GoMySQLReaderTestSuite) TestStreamTransactionSingleAutoCommitChange() {
	t := suite.T()

	ctx := context.Background()
	host, err := suite.mysqlContainer.ContainerIP(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", "root:root@tcp("+host+")/")
	require.NoError(t, err)
	defer db.Close()

	prepareDatabase(t, db)

	binlogCoordinates := getCurrentBinlogCoordinates(t, db)

	migrationContext := getMigrationContext(host)
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

func (suite *GoMySQLReaderTestSuite) TestStreamTransactionSingleChangeInTransaction() {
	t := suite.T()

	ctx := context.Background()
	host, err := suite.mysqlContainer.ContainerIP(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", "root:root@tcp("+host+")/")
	require.NoError(t, err)
	defer db.Close()

	prepareDatabase(t, db)

	binlogCoordinates := getCurrentBinlogCoordinates(t, db)

	migrationContext := getMigrationContext(host)
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

func (suite *GoMySQLReaderTestSuite) TestStreamTransactionMultipleChangesInTransaction() {
	t := suite.T()

	ctx := context.Background()
	host, err := suite.mysqlContainer.ContainerIP(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", "root:root@tcp("+host+")/")
	require.NoError(t, err)
	defer db.Close()

	prepareDatabase(t, db)

	binlogCoordinates := getCurrentBinlogCoordinates(t, db)

	migrationContext := getMigrationContext(host)
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

func (suite *GoMySQLReaderTestSuite) TestStreamTransactionWithDDL() {
	t := suite.T()

	ctx := context.Background()
	host, err := suite.mysqlContainer.ContainerIP(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", "root:root@tcp("+host+")/")
	require.NoError(t, err)
	defer db.Close()

	prepareDatabase(t, db)

	binlogCoordinates := getCurrentBinlogCoordinates(t, db)

	migrationContext := getMigrationContext(host)
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

func TestGoMySQLReader(t *testing.T) {
	suite.Run(t, new(GoMySQLReaderTestSuite))
}
