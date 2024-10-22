/*
   Copyright 2022 GitHub Inc.
         See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"context"
	gosql "database/sql"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/binlog"
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"
)

func TestApplierGenerateSqlModeQuery(t *testing.T) {
	migrationContext := base.NewMigrationContext()
	applier := NewApplier(migrationContext)

	{
		require.Equal(t,
			`sql_mode = CONCAT(@@session.sql_mode, ',NO_AUTO_VALUE_ON_ZERO,STRICT_ALL_TABLES')`,
			applier.generateSqlModeQuery(),
		)
	}
	{
		migrationContext.SkipStrictMode = true
		migrationContext.AllowZeroInDate = false
		require.Equal(t,
			`sql_mode = CONCAT(@@session.sql_mode, ',NO_AUTO_VALUE_ON_ZERO')`,
			applier.generateSqlModeQuery(),
		)
	}
	{
		migrationContext.SkipStrictMode = false
		migrationContext.AllowZeroInDate = true
		require.Equal(t,
			`sql_mode = REPLACE(REPLACE(CONCAT(@@session.sql_mode, ',NO_AUTO_VALUE_ON_ZERO,STRICT_ALL_TABLES'), 'NO_ZERO_IN_DATE', ''), 'NO_ZERO_DATE', '')`,
			applier.generateSqlModeQuery(),
		)
	}
	{
		migrationContext.SkipStrictMode = true
		migrationContext.AllowZeroInDate = true
		require.Equal(t,
			`sql_mode = REPLACE(REPLACE(CONCAT(@@session.sql_mode, ',NO_AUTO_VALUE_ON_ZERO'), 'NO_ZERO_IN_DATE', ''), 'NO_ZERO_DATE', '')`,
			applier.generateSqlModeQuery(),
		)
	}
}

func TestApplierUpdateModifiesUniqueKeyColumns(t *testing.T) {
	columns := sql.NewColumnList([]string{"id", "item_id"})
	columnValues := sql.ToColumnValues([]interface{}{123456, 42})

	migrationContext := base.NewMigrationContext()
	migrationContext.OriginalTableColumns = columns
	migrationContext.UniqueKey = &sql.UniqueKey{
		Name:    t.Name(),
		Columns: *columns,
	}

	applier := NewApplier(migrationContext)

	t.Run("unmodified", func(t *testing.T) {
		modifiedColumn, isModified := applier.updateModifiesUniqueKeyColumns(&binlog.BinlogDMLEvent{
			DatabaseName:      "test",
			DML:               binlog.UpdateDML,
			NewColumnValues:   columnValues,
			WhereColumnValues: columnValues,
		})
		require.Equal(t, "", modifiedColumn)
		require.False(t, isModified)
	})

	t.Run("modified", func(t *testing.T) {
		modifiedColumn, isModified := applier.updateModifiesUniqueKeyColumns(&binlog.BinlogDMLEvent{
			DatabaseName:      "test",
			DML:               binlog.UpdateDML,
			NewColumnValues:   sql.ToColumnValues([]interface{}{123456, 24}),
			WhereColumnValues: columnValues,
		})
		require.Equal(t, "item_id", modifiedColumn)
		require.True(t, isModified)
	})
}

func TestApplierBuildDMLEventQuery(t *testing.T) {
	columns := sql.NewColumnList([]string{"id", "item_id"})
	columnValues := sql.ToColumnValues([]interface{}{123456, 42})

	migrationContext := base.NewMigrationContext()
	migrationContext.OriginalTableName = "test"
	migrationContext.OriginalTableColumns = columns
	migrationContext.SharedColumns = columns
	migrationContext.MappedSharedColumns = columns
	migrationContext.UniqueKey = &sql.UniqueKey{
		Name:    t.Name(),
		Columns: *columns,
	}

	applier := NewApplier(migrationContext)

	t.Run("delete", func(t *testing.T) {
		binlogEvent := &binlog.BinlogDMLEvent{
			DatabaseName:      "test",
			DML:               binlog.DeleteDML,
			WhereColumnValues: columnValues,
		}

		res := applier.buildDMLEventQuery(binlogEvent)
		require.Len(t, res, 1)
		require.NoError(t, res[0].err)
		require.Equal(t, `delete /* gh-ost `+"`test`.`_test_gho`"+` */
		from
			`+"`test`.`_test_gho`"+`
		where
			((`+"`id`"+` = ?) and (`+"`item_id`"+` = ?))`,
			strings.TrimSpace(res[0].query))
		require.Len(t, res[0].args, 2)
		require.Equal(t, 123456, res[0].args[0])
		require.Equal(t, 42, res[0].args[1])
	})

	t.Run("insert", func(t *testing.T) {
		binlogEvent := &binlog.BinlogDMLEvent{
			DatabaseName:    "test",
			DML:             binlog.InsertDML,
			NewColumnValues: columnValues,
		}
		res := applier.buildDMLEventQuery(binlogEvent)
		require.Len(t, res, 1)
		require.NoError(t, res[0].err)
		require.Equal(t,
			`replace /* gh-ost `+"`test`.`_test_gho`"+` */
		into
			`+"`test`.`_test_gho`"+`
			`+"(`id`, `item_id`)"+`
		values
			(?, ?)`,
			strings.TrimSpace(res[0].query))
		require.Len(t, res[0].args, 2)
		require.Equal(t, 123456, res[0].args[0])
		require.Equal(t, 42, res[0].args[1])
	})

	t.Run("update", func(t *testing.T) {
		binlogEvent := &binlog.BinlogDMLEvent{
			DatabaseName:      "test",
			DML:               binlog.UpdateDML,
			NewColumnValues:   columnValues,
			WhereColumnValues: columnValues,
		}
		res := applier.buildDMLEventQuery(binlogEvent)
		require.Len(t, res, 1)
		require.NoError(t, res[0].err)
		require.Equal(t,
			`update /* gh-ost `+"`test`.`_test_gho`"+` */
			`+"`test`.`_test_gho`"+`
		set
			`+"`id`"+`=?, `+"`item_id`"+`=?
		where
			((`+"`id`"+` = ?) and (`+"`item_id`"+` = ?))`,
			strings.TrimSpace(res[0].query))
		require.Len(t, res[0].args, 4)
		require.Equal(t, 123456, res[0].args[0])
		require.Equal(t, 42, res[0].args[1])
		require.Equal(t, 123456, res[0].args[2])
		require.Equal(t, 42, res[0].args[3])
	})
}

func TestApplierInstantDDL(t *testing.T) {
	migrationContext := base.NewMigrationContext()
	migrationContext.DatabaseName = "test"
	migrationContext.OriginalTableName = "mytable"
	migrationContext.AlterStatementOptions = "ADD INDEX (foo)"
	applier := NewApplier(migrationContext)

	t.Run("instantDDLstmt", func(t *testing.T) {
		stmt := applier.generateInstantDDLQuery()
		require.Equal(t, "ALTER /* gh-ost */ TABLE `test`.`mytable` ADD INDEX (foo), ALGORITHM=INSTANT", stmt)
	})
}

type ApplierTestSuite struct {
	suite.Suite

	mysqlContainer testcontainers.Container
}

func (suite *ApplierTestSuite) getConnectionConfig(ctx context.Context) (*mysql.ConnectionConfig, error) {
	host, err := suite.mysqlContainer.ContainerIP(ctx)
	if err != nil {
		return nil, err
	}

	config := mysql.NewConnectionConfig()
	config.Key.Hostname = host
	config.Key.Port = 3306
	config.User = "root"
	config.Password = "root-password"

	return config, nil
}

func (suite *ApplierTestSuite) getDb(ctx context.Context) (*gosql.DB, error) {
	host, err := suite.mysqlContainer.ContainerIP(ctx)
	if err != nil {
		return nil, err
	}

	return gosql.Open("mysql", "root:root-password@tcp("+host+":3306)/test")
}

func (suite *ApplierTestSuite) SetupSuite() {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:      "mysql:8.0",
		Env:        map[string]string{"MYSQL_ROOT_PASSWORD": "root-password"},
		WaitingFor: wait.ForLog("port: 3306  MySQL Community Server - GPL"),
	}

	mysqlContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	suite.Require().NoError(err)

	suite.mysqlContainer = mysqlContainer
}

func (suite *ApplierTestSuite) TeardownSuite() {
	ctx := context.Background()

	suite.Require().NoError(suite.mysqlContainer.Terminate(ctx))
}

func (suite *ApplierTestSuite) SetupTest() {
	ctx := context.Background()

	rc, _, err := suite.mysqlContainer.Exec(ctx, []string{"mysql", "-uroot", "-proot-password", "-e", "CREATE DATABASE test;"})
	suite.Require().NoError(err)
	suite.Require().Equalf(0, rc, "failed to created database: expected exit code 0, got %d", rc)

	rc, _, err = suite.mysqlContainer.Exec(ctx, []string{"mysql", "-uroot", "-proot-password", "-e", "CREATE TABLE test.testing (id INT, item_id INT, PRIMARY KEY (id));"})
	suite.Require().NoError(err)
	suite.Require().Equalf(0, rc, "failed to created table: expected exit code 0, got %d", rc)
}

func (suite *ApplierTestSuite) TearDownTest() {
	ctx := context.Background()

	rc, _, err := suite.mysqlContainer.Exec(ctx, []string{"mysql", "-uroot", "-proot-password", "-e", "DROP DATABASE test;"})
	suite.Require().NoError(err)
	suite.Require().Equalf(0, rc, "failed to created database: expected exit code 0, got %d", rc)
}

func (suite *ApplierTestSuite) TestInitDBConnections() {
	ctx := context.Background()

	connectionConfig, err := suite.getConnectionConfig(ctx)
	suite.Require().NoError(err)

	migrationContext := base.NewMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.DatabaseName = "test"
	migrationContext.OriginalTableName = "testing"
	migrationContext.SetConnectionConfig("innodb")

	applier := NewApplier(migrationContext)
	defer applier.Teardown()

	err = applier.InitDBConnections()
	suite.Require().NoError(err)

	suite.Require().Equal("8.0.40", migrationContext.ApplierMySQLVersion)
	suite.Require().Equal(int64(28800), migrationContext.ApplierWaitTimeout)
	suite.Require().Equal("SYSTEM", migrationContext.ApplierTimeZone)

	suite.Require().Equal(sql.NewColumnList([]string{"id", "item_id"}), migrationContext.OriginalTableColumnsOnApplier)
}

func (suite *ApplierTestSuite) TestApplyDMLEventQueries() {
	ctx := context.Background()

	connectionConfig, err := suite.getConnectionConfig(ctx)
	suite.Require().NoError(err)

	migrationContext := base.NewMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.DatabaseName = "test"
	migrationContext.OriginalTableName = "testing"
	migrationContext.SetConnectionConfig("innodb")

	migrationContext.OriginalTableColumns = sql.NewColumnList([]string{"id", "item_id"})
	migrationContext.SharedColumns = sql.NewColumnList([]string{"id", "item_id"})
	migrationContext.MappedSharedColumns = sql.NewColumnList([]string{"id", "item_id"})

	applier := NewApplier(migrationContext)
	defer applier.Teardown()

	err = applier.InitDBConnections()
	suite.Require().NoError(err)

	rc, _, err := suite.mysqlContainer.Exec(ctx, []string{"mysql", "-uroot", "-proot-password", "-e", "CREATE TABLE test._testing_gho (id INT, item_id INT);"})
	suite.Require().NoError(err)
	suite.Require().Equalf(0, rc, "failed to created table: expected exit code 0, got %d", rc)

	dmlEvents := []*binlog.BinlogDMLEvent{
		{
			DatabaseName:    "test",
			TableName:       "testing",
			DML:             binlog.InsertDML,
			NewColumnValues: sql.ToColumnValues([]interface{}{123456, 42}),
		},
	}
	err = applier.ApplyDMLEventQueries(dmlEvents)
	suite.Require().NoError(err)

	// Check that the row was inserted
	db, err := suite.getDb(ctx)
	suite.Require().NoError(err)
	defer db.Close()

	rows, err := db.Query("SELECT * FROM test._testing_gho")
	suite.Require().NoError(err)
	defer rows.Close()

	var count, id, item_id int
	for rows.Next() {
		err = rows.Scan(&id, &item_id)
		suite.Require().NoError(err)
		count += 1
	}
	suite.Require().NoError(rows.Err())

	suite.Require().Equal(1, count)
	suite.Require().Equal(123456, id)
	suite.Require().Equal(42, item_id)

	suite.Require().Equal(int64(1), migrationContext.TotalDMLEventsApplied)
	suite.Require().Equal(int64(0), migrationContext.RowsDeltaEstimate)
}

func (suite *ApplierTestSuite) TestValidateOrDropExistingTables() {
	ctx := context.Background()

	connectionConfig, err := suite.getConnectionConfig(ctx)
	suite.Require().NoError(err)

	migrationContext := base.NewMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.DatabaseName = "test"
	migrationContext.OriginalTableName = "testing"
	migrationContext.SetConnectionConfig("innodb")

	migrationContext.OriginalTableColumns = sql.NewColumnList([]string{"id", "item_id"})
	migrationContext.SharedColumns = sql.NewColumnList([]string{"id", "item_id"})
	migrationContext.MappedSharedColumns = sql.NewColumnList([]string{"id", "item_id"})

	applier := NewApplier(migrationContext)
	defer applier.Teardown()

	err = applier.InitDBConnections()
	suite.Require().NoError(err)

	err = applier.ValidateOrDropExistingTables()
	suite.Require().NoError(err)
}

func (suite *ApplierTestSuite) TestApplyIterationInsertQuery() {
	ctx := context.Background()

	connectionConfig, err := suite.getConnectionConfig(ctx)
	suite.Require().NoError(err)

	migrationContext := base.NewMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.DatabaseName = "test"
	migrationContext.OriginalTableName = "testing"
	migrationContext.ChunkSize = 10
	migrationContext.SetConnectionConfig("innodb")

	db, err := suite.getDb(ctx)
	suite.Require().NoError(err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test._testing_gho (id INT, item_id INT, PRIMARY KEY (id))")
	suite.Require().NoError(err)

	// Insert some test values
	for i := 1; i <= 10; i++ {
		_, err = db.Exec("INSERT INTO test.testing (id, item_id) VALUES (?, ?)", i, i)
		suite.Require().NoError(err)
	}

	migrationContext.SharedColumns = sql.NewColumnList([]string{"id", "item_id"})
	migrationContext.MappedSharedColumns = sql.NewColumnList([]string{"id", "item_id"})
	migrationContext.UniqueKey = &sql.UniqueKey{
		Name:    "PRIMARY",
		Columns: *sql.NewColumnList([]string{"id"}),
	}

	migrationContext.MigrationIterationRangeMinValues = sql.ToColumnValues([]interface{}{1})
	migrationContext.MigrationIterationRangeMaxValues = sql.ToColumnValues([]interface{}{10})

	applier := NewApplier(migrationContext)
	defer applier.Teardown()

	err = applier.InitDBConnections()
	suite.Require().NoError(err)

	chunkSize, rowsAffected, duration, err := applier.ApplyIterationInsertQuery()
	suite.Require().NoError(err)

	suite.Require().Equal(migrationContext.ChunkSize, chunkSize)
	suite.Require().Equal(int64(10), rowsAffected)
	suite.Require().Greater(duration, time.Duration(0))

	// Check that the rows were inserted
	rows, err := db.Query("SELECT * FROM test._testing_gho")
	suite.Require().NoError(err)
	defer rows.Close()

	var count, id, item_id int
	for rows.Next() {
		err = rows.Scan(&id, &item_id)
		suite.Require().NoError(err)
		count += 1
	}
	suite.Require().NoError(rows.Err())

	suite.Require().Equal(10, count)
}

func (suite *ApplierTestSuite) TestApplyIterationInsertQueryFailsFastWhenSelectingLockedRows() {
	ctx := context.Background()

	connectionConfig, err := suite.getConnectionConfig(ctx)
	suite.Require().NoError(err)

	migrationContext := base.NewMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.DatabaseName = "test"
	migrationContext.OriginalTableName = "testing"
	migrationContext.ChunkSize = 10
	migrationContext.SetConnectionConfig("innodb")

	db, err := suite.getDb(ctx)
	suite.Require().NoError(err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test._testing_gho (id INT, item_id INT, PRIMARY KEY (id))")
	suite.Require().NoError(err)

	// Insert some test values
	for i := 1; i <= 10; i++ {
		_, err = db.Exec("INSERT INTO test.testing (id, item_id) VALUES (?, ?)", i, i)
		suite.Require().NoError(err)
	}

	migrationContext.SharedColumns = sql.NewColumnList([]string{"id", "item_id"})
	migrationContext.MappedSharedColumns = sql.NewColumnList([]string{"id", "item_id"})
	migrationContext.UniqueKey = &sql.UniqueKey{
		Name:    "PRIMARY",
		Columns: *sql.NewColumnList([]string{"id"}),
	}

	migrationContext.MigrationIterationRangeMinValues = sql.ToColumnValues([]interface{}{1})
	migrationContext.MigrationIterationRangeMaxValues = sql.ToColumnValues([]interface{}{10})

	applier := NewApplier(migrationContext)
	defer applier.Teardown()

	err = applier.InitDBConnections()
	suite.Require().NoError(err)

	// Lock one of the rows
	tx, err := db.Begin()
	suite.Require().NoError(err)
	defer func() {
		suite.Require().NoError(tx.Rollback())
	}()

	_, err = tx.Exec("SELECT * FROM test.testing WHERE id = 5 FOR UPDATE")
	suite.Require().NoError(err)

	chunkSize, rowsAffected, duration, err := applier.ApplyIterationInsertQuery()
	suite.Require().Error(err)
	suite.Require().EqualError(err, "Error 3572 (HY000): Statement aborted because lock(s) could not be acquired immediately and NOWAIT is set.")

	suite.Require().Equal(migrationContext.ChunkSize, chunkSize)
	suite.Require().Equal(int64(0), rowsAffected)
	suite.Require().Equal(time.Duration(0), duration)

	// Check that the no rows were inserted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test._testing_gho").Scan(&count)
	suite.Require().NoError(err)

	suite.Require().Equal(0, count)
}

func TestApplier(t *testing.T) {
	suite.Run(t, new(ApplierTestSuite))
}
