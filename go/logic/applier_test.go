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

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/binlog"
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
	migrationContext.DatabaseName = "test"
	migrationContext.OriginalTableName = "test"
	migrationContext.OriginalTableColumns = columns
	migrationContext.SharedColumns = columns
	migrationContext.MappedSharedColumns = columns
	migrationContext.UniqueKey = &sql.UniqueKey{
		Name:    t.Name(),
		Columns: *columns,
	}

	applier := NewApplier(migrationContext)
	applier.prepareQueries()

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
	migrationContext.SkipPortValidation = true
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
	db             *gosql.DB
}

func (suite *ApplierTestSuite) SetupSuite() {
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

func (suite *ApplierTestSuite) TeardownSuite() {
	ctx := context.Background()

	suite.Assert().NoError(suite.db.Close())
	suite.Assert().NoError(suite.mysqlContainer.Terminate(ctx))
}

func (suite *ApplierTestSuite) SetupTest() {
	ctx := context.Background()

	_, err := suite.db.ExecContext(ctx, "CREATE DATABASE test")
	suite.Require().NoError(err)
}

func (suite *ApplierTestSuite) TearDownTest() {
	ctx := context.Background()

	_, err := suite.db.ExecContext(ctx, "DROP DATABASE test")
	suite.Require().NoError(err)
}

func (suite *ApplierTestSuite) TestInitDBConnections() {
	ctx := context.Background()

	var err error

	_, err = suite.db.ExecContext(ctx, "CREATE TABLE test.testing (id INT, item_id INT);")
	suite.Require().NoError(err)

	connectionConfig, err := GetConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := base.NewMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.DatabaseName = "test"
	migrationContext.SkipPortValidation = true
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

	var err error

	_, err = suite.db.ExecContext(ctx, "CREATE TABLE test.testing (id INT, item_id INT);")
	suite.Require().NoError(err)

	_, err = suite.db.ExecContext(ctx, "CREATE TABLE test._testing_gho (id INT, item_id INT);")
	suite.Require().NoError(err)

	connectionConfig, err := GetConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := base.NewMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.DatabaseName = "test"
	migrationContext.SkipPortValidation = true
	migrationContext.OriginalTableName = "testing"
	migrationContext.SetConnectionConfig("innodb")

	migrationContext.OriginalTableColumns = sql.NewColumnList([]string{"id", "item_id"})
	migrationContext.SharedColumns = sql.NewColumnList([]string{"id", "item_id"})
	migrationContext.MappedSharedColumns = sql.NewColumnList([]string{"id", "item_id"})
	migrationContext.UniqueKey = &sql.UniqueKey{
		Name:    "primary_key",
		Columns: *sql.NewColumnList([]string{"id"}),
	}

	applier := NewApplier(migrationContext)
	suite.Require().NoError(applier.prepareQueries())
	defer applier.Teardown()

	err = applier.InitDBConnections()
	suite.Require().NoError(err)

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
	rows, err := suite.db.Query("SELECT * FROM test._testing_gho")
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

	var err error

	_, err = suite.db.ExecContext(ctx, "CREATE TABLE test.testing (id INT, item_id INT);")
	suite.Require().NoError(err)

	connectionConfig, err := GetConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := base.NewMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.DatabaseName = "test"
	migrationContext.SkipPortValidation = true
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

func (suite *ApplierTestSuite) TestValidateOrDropExistingTablesWithGhostTableExisting() {
	ctx := context.Background()

	var err error

	_, err = suite.db.ExecContext(ctx, "CREATE TABLE test.testing (id INT, item_id INT);")
	suite.Require().NoError(err)

	_, err = suite.db.ExecContext(ctx, "CREATE TABLE test._testing_gho (id INT, item_id INT);")
	suite.Require().NoError(err)

	connectionConfig, err := GetConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := base.NewMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.DatabaseName = "test"
	migrationContext.SkipPortValidation = true
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
	suite.Require().Error(err)
	suite.Require().EqualError(err, "Table `_testing_gho` already exists. Panicking. Use --initially-drop-ghost-table to force dropping it, though I really prefer that you drop it or rename it away")
}

func (suite *ApplierTestSuite) TestValidateOrDropExistingTablesWithGhostTableExistingAndInitiallyDropGhostTableSet() {
	ctx := context.Background()

	var err error

	_, err = suite.db.ExecContext(ctx, "CREATE TABLE test.testing (id INT, item_id INT);")
	suite.Require().NoError(err)

	_, err = suite.db.ExecContext(ctx, "CREATE TABLE test._testing_gho (id INT, item_id INT);")
	suite.Require().NoError(err)

	connectionConfig, err := GetConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := base.NewMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.DatabaseName = "test"
	migrationContext.SkipPortValidation = true
	migrationContext.OriginalTableName = "testing"
	migrationContext.SetConnectionConfig("innodb")

	migrationContext.InitiallyDropGhostTable = true

	applier := NewApplier(migrationContext)
	defer applier.Teardown()

	err = applier.InitDBConnections()
	suite.Require().NoError(err)

	err = applier.ValidateOrDropExistingTables()
	suite.Require().NoError(err)

	// Check that the ghost table was dropped
	var tableName string
	//nolint:execinquery
	err = suite.db.QueryRow("SHOW TABLES IN test LIKE '_testing_gho'").Scan(&tableName)
	suite.Require().Error(err)
	suite.Require().Equal(gosql.ErrNoRows, err)
}

func (suite *ApplierTestSuite) TestCreateGhostTable() {
	ctx := context.Background()

	var err error

	_, err = suite.db.ExecContext(ctx, "CREATE TABLE test.testing (id INT, item_id INT);")
	suite.Require().NoError(err)

	connectionConfig, err := GetConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := base.NewMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.DatabaseName = "test"
	migrationContext.SkipPortValidation = true
	migrationContext.OriginalTableName = "testing"
	migrationContext.SetConnectionConfig("innodb")

	migrationContext.OriginalTableColumns = sql.NewColumnList([]string{"id", "item_id"})
	migrationContext.SharedColumns = sql.NewColumnList([]string{"id", "item_id"})
	migrationContext.MappedSharedColumns = sql.NewColumnList([]string{"id", "item_id"})

	migrationContext.InitiallyDropGhostTable = true

	applier := NewApplier(migrationContext)
	defer applier.Teardown()

	err = applier.InitDBConnections()
	suite.Require().NoError(err)

	err = applier.CreateGhostTable()
	suite.Require().NoError(err)

	// Check that the ghost table was created
	var tableName string
	//nolint:execinquery
	err = suite.db.QueryRow("SHOW TABLES IN test LIKE '_testing_gho'").Scan(&tableName)
	suite.Require().NoError(err)
	suite.Require().Equal("_testing_gho", tableName)

	// Check that the ghost table has the same columns as the original table
	var createDDL string
	//nolint:execinquery
	err = suite.db.QueryRow("SHOW CREATE TABLE test._testing_gho").Scan(&tableName, &createDDL)
	suite.Require().NoError(err)
	suite.Require().Equal("CREATE TABLE `_testing_gho` (\n  `id` int DEFAULT NULL,\n  `item_id` int DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", createDDL)
}

func TestApplier(t *testing.T) {
	suite.Run(t, new(ApplierTestSuite))
}
