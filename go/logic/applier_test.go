/*
   Copyright 2025 GitHub Inc.
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
	testmysql "github.com/testcontainers/testcontainers-go/modules/mysql"

	"fmt"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/binlog"
	"github.com/github/gh-ost/go/sql"
	"github.com/testcontainers/testcontainers-go/wait"
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
	columns := sql.NewColumnList([]string{"id", "item_id", "item_text"})
	columnValues := sql.ToColumnValues([]interface{}{123456, 42, []uint8{116, 101, 115, 116}})

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

// func TestApplierInstantDDL(t *testing.T) {
// 	migrationContext := base.NewMigrationContext()
// 	migrationContext.DatabaseName = "test"
// 	migrationContext.SkipPortValidation = true
// 	migrationContext.OriginalTableName = "mytable"
// 	migrationContext.AlterStatementOptions = "ADD INDEX (foo)"
// 	applier := NewApplier(migrationContext)

// 	t.Run("instantDDLstmt", func(t *testing.T) {
// 		stmt := applier.generateInstantDDLQuery()
// 		require.Equal(t, "ALTER /* gh-ost */ TABLE `test`.`mytable` ADD INDEX (foo), ALGORITHM=INSTANT", stmt)
// 	})
// }

type ApplierTestSuite struct {
	suite.Suite

	mysqlContainer testcontainers.Container
	db             *gosql.DB
}

func (suite *ApplierTestSuite) SetupSuite() {
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

func (suite *ApplierTestSuite) TeardownSuite() {
	suite.Assert().NoError(suite.db.Close())
	suite.Assert().NoError(testcontainers.TerminateContainer(suite.mysqlContainer))
}

func (suite *ApplierTestSuite) SetupTest() {
	ctx := context.Background()
	_, err := suite.db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", testMysqlDatabase))
	suite.Require().NoError(err)
}

func (suite *ApplierTestSuite) TearDownTest() {
	ctx := context.Background()

	_, err := suite.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+getTestTableName())
	suite.Require().NoError(err)
	_, err = suite.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+getTestGhostTableName())
	suite.Require().NoError(err)
}

func (suite *ApplierTestSuite) TestInitDBConnections() {
	ctx := context.Background()

	var err error

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, item_id INT);", getTestTableName()))
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := newTestMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.SetConnectionConfig("innodb")

	applier := NewApplier(migrationContext)
	defer applier.Teardown()

	err = applier.InitDBConnections()
	suite.Require().NoError(err)

	mysqlVersion, _ := strings.CutPrefix(testMysqlContainerImage, "mysql:")
	suite.Require().Equal(mysqlVersion, migrationContext.ApplierMySQLVersion)
	suite.Require().Equal(int64(28800), migrationContext.ApplierWaitTimeout)
	suite.Require().Equal("SYSTEM", migrationContext.ApplierTimeZone)

	suite.Require().Equal(sql.NewColumnList([]string{"id", "item_id"}), migrationContext.OriginalTableColumnsOnApplier)
}

func (suite *ApplierTestSuite) TestApplyDMLEventQueries() {
	ctx := context.Background()

	var err error

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, item_id INT);", getTestTableName()))
	suite.Require().NoError(err)

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, item_id INT);", getTestGhostTableName()))
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := newTestMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
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
			DatabaseName:    testMysqlDatabase,
			TableName:       testMysqlTableName,
			DML:             binlog.InsertDML,
			NewColumnValues: sql.ToColumnValues([]interface{}{123456, 42}),
		},
	}
	err = applier.ApplyDMLEventQueries(dmlEvents)
	suite.Require().NoError(err)

	// Check that the row was inserted
	rows, err := suite.db.Query("SELECT * FROM " + getTestGhostTableName())
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

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, item_id INT);", getTestTableName()))
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := newTestMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
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

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, item_id INT);", getTestTableName()))
	suite.Require().NoError(err)

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, item_id INT);", getTestGhostTableName()))
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := newTestMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
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

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, item_id INT);", getTestTableName()))
	suite.Require().NoError(err)

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, item_id INT);", getTestGhostTableName()))
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := newTestMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
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
	err = suite.db.QueryRow(fmt.Sprintf("SHOW TABLES IN test LIKE '_%s_gho'", testMysqlTableName)).Scan(&tableName)
	suite.Require().Error(err)
	suite.Require().Equal(gosql.ErrNoRows, err)
}

func (suite *ApplierTestSuite) TestCreateGhostTable() {
	ctx := context.Background()

	var err error

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, item_id INT);", getTestTableName()))
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := newTestMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
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
	err = suite.db.QueryRow(fmt.Sprintf("SHOW CREATE TABLE %s", getTestGhostTableName())).Scan(&tableName, &createDDL)
	suite.Require().NoError(err)
	suite.Require().Equal("CREATE TABLE `_testing_gho` (\n  `id` int DEFAULT NULL,\n  `item_id` int DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", createDDL)
}

func (suite *ApplierTestSuite) TestPanicOnWarningsInApplyIterationInsertQuerySucceedsWithUniqueKeyWarningInsertedByDMLEvent() {
	ctx := context.Background()

	var err error

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, item_id INT, UNIQUE KEY (item_id));", getTestTableName()))
	suite.Require().NoError(err)

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, item_id INT, UNIQUE KEY (item_id));", getTestGhostTableName()))
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := newTestMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.SetConnectionConfig("innodb")

	migrationContext.PanicOnWarnings = true

	migrationContext.OriginalTableColumns = sql.NewColumnList([]string{"id", "item_id"})
	migrationContext.SharedColumns = sql.NewColumnList([]string{"id", "item_id"})
	migrationContext.MappedSharedColumns = sql.NewColumnList([]string{"id", "item_id"})
	migrationContext.UniqueKey = &sql.UniqueKey{
		Name:             "item_id",
		NameInGhostTable: "item_id",
		Columns:          *sql.NewColumnList([]string{"item_id"}),
	}

	applier := NewApplier(migrationContext)
	suite.Require().NoError(applier.prepareQueries())
	defer applier.Teardown()

	err = applier.InitDBConnections()
	suite.Require().NoError(err)

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, item_id) VALUES (123456, 42);", getTestTableName()))
	suite.Require().NoError(err)

	dmlEvents := []*binlog.BinlogDMLEvent{
		{
			DatabaseName:    testMysqlDatabase,
			TableName:       testMysqlTableName,
			DML:             binlog.InsertDML,
			NewColumnValues: sql.ToColumnValues([]interface{}{123456, 42}),
		},
	}
	err = applier.ApplyDMLEventQueries(dmlEvents)
	suite.Require().NoError(err)

	err = applier.CreateChangelogTable()
	suite.Require().NoError(err)
	err = applier.ReadMigrationRangeValues()
	suite.Require().NoError(err)

	hasFurtherRange, err := applier.CalculateNextIterationRangeEndValues()
	suite.Require().NoError(err)
	suite.Require().True(hasFurtherRange)

	_, rowsAffected, _, err := applier.ApplyIterationInsertQuery()
	suite.Require().NoError(err)
	suite.Require().Equal(int64(0), rowsAffected)

	// Ensure Duplicate entry '42' for key '_testing_gho.item_id' is ignored correctly
	suite.Require().Empty(applier.migrationContext.MigrationLastInsertSQLWarnings)

	// Check that the row was inserted
	rows, err := suite.db.Query("SELECT * FROM " + getTestGhostTableName())
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

	suite.Require().
		Equal(int64(1), migrationContext.TotalDMLEventsApplied)
	suite.Require().
		Equal(int64(0), migrationContext.RowsDeltaEstimate)
}

// func (suite *ApplierTestSuite) TestPanicOnWarningsInApplyIterationInsertQueryFailsWithTruncationWarning() {
// 	ctx := context.Background()

// 	var err error

// 	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id int not null, name varchar(20), primary key(id))", getTestTableName()))
// 	suite.Require().NoError(err)

// 	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, name varchar(20), primary key(id));", getTestGhostTableName()))
// 	suite.Require().NoError(err)

// 	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'this string is long')", getTestTableName()))
// 	suite.Require().NoError(err)

// 	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
// 	suite.Require().NoError(err)

// 	migrationContext := newTestMigrationContext()
// 	migrationContext.ApplierConnectionConfig = connectionConfig
// 	migrationContext.SetConnectionConfig("innodb")

// 	migrationContext.AlterStatementOptions = "modify column name varchar(10)"
// 	migrationContext.OriginalTableColumns = sql.NewColumnList([]string{"id", "name"})
// 	migrationContext.SharedColumns = sql.NewColumnList([]string{"id", "name"})
// 	migrationContext.MappedSharedColumns = sql.NewColumnList([]string{"id", "name"})
// 	migrationContext.UniqueKey = &sql.UniqueKey{
// 		Name:             "PRIMARY",
// 		NameInGhostTable: "PRIMARY",
// 		Columns:          *sql.NewColumnList([]string{"id"}),
// 	}
// 	applier := NewApplier(migrationContext)

// 	err = applier.InitDBConnections()
// 	suite.Require().NoError(err)

// 	err = applier.CreateChangelogTable()
// 	suite.Require().NoError(err)

// 	err = applier.ReadMigrationRangeValues()
// 	suite.Require().NoError(err)

// 	err = applier.AlterGhost()
// 	suite.Require().NoError(err)

// 	hasFurtherRange, err := applier.CalculateNextIterationRangeEndValues()
// 	suite.Require().NoError(err)
// 	suite.Require().True(hasFurtherRange)

// 	_, rowsAffected, _, err := applier.ApplyIterationInsertQuery()
// 	suite.Equal(int64(1), rowsAffected)
// 	suite.Require().NoError(err)

// 	// Verify the warning was recorded and will cause the migrator to panic
// 	suite.Require().NotEmpty(applier.migrationContext.MigrationLastInsertSQLWarnings)
// 	suite.Require().Contains(applier.migrationContext.MigrationLastInsertSQLWarnings[0], "Warning: Data truncated for column 'name' at row 1")
// }

// func (suite *ApplierTestSuite) TestWriteCheckpoint() {
// 	ctx := context.Background()

// 	var err error

// 	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id int not null, id2 char(4) CHARACTER SET utf8mb4, primary key(id, id2))", getTestTableName()))
// 	suite.Require().NoError(err)

// 	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, id2 char(4) CHARACTER SET utf8mb4, name varchar(20), primary key(id, id2));", getTestGhostTableName()))
// 	suite.Require().NoError(err)

// 	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, id2) VALUES (?,?), (?,?), (?,?)", getTestTableName()), 411, "君子懷德", 411, "小人懷土", 212, "君子不器")
// 	suite.Require().NoError(err)

// 	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
// 	suite.Require().NoError(err)

// 	migrationContext := newTestMigrationContext()
// 	migrationContext.ApplierConnectionConfig = connectionConfig
// 	migrationContext.InspectorConnectionConfig = connectionConfig
// 	migrationContext.SetConnectionConfig("innodb")

// 	migrationContext.AlterStatementOptions = "add column name varchar(20)"
// 	migrationContext.OriginalTableColumns = sql.NewColumnList([]string{"id", "id2"})
// 	migrationContext.SharedColumns = sql.NewColumnList([]string{"id", "id2"})
// 	migrationContext.MappedSharedColumns = sql.NewColumnList([]string{"id", "id2"})
// 	migrationContext.Checkpoint = true
// 	migrationContext.UniqueKey = &sql.UniqueKey{
// 		Name:             "PRIMARY",
// 		NameInGhostTable: "PRIMARY",
// 		Columns:          *sql.NewColumnList([]string{"id", "id2"}),
// 	}

// 	inspector := NewInspector(migrationContext)
// 	suite.Require().NoError(inspector.InitDBConnections())

// 	err = inspector.applyColumnTypes(testMysqlDatabase, testMysqlTableName, &migrationContext.UniqueKey.Columns)
// 	suite.Require().NoError(err)

// 	applier := NewApplier(migrationContext)

// 	err = applier.InitDBConnections()
// 	suite.Require().NoError(err)

// 	err = applier.CreateChangelogTable()
// 	suite.Require().NoError(err)

// 	err = applier.CreateCheckpointTable()
// 	suite.Require().NoError(err)

// 	err = applier.prepareQueries()
// 	suite.Require().NoError(err)

// 	err = applier.ReadMigrationRangeValues()
// 	suite.Require().NoError(err)

// 	// checkpoint table is empty
// 	_, err = applier.ReadLastCheckpoint()
// 	suite.Require().ErrorIs(err, ErrNoCheckpointFound)

// 	// write a checkpoint and read it back
// 	coords := mysql.NewFileBinlogCoordinates("mysql-bin.000003", int64(219202907))

// 	chk := &Checkpoint{
// 		LastTrxCoords:     coords,
// 		IterationRangeMin: applier.migrationContext.MigrationRangeMinValues,
// 		IterationRangeMax: applier.migrationContext.MigrationRangeMaxValues,
// 		Iteration:         2,
// 		RowsCopied:        100000,
// 		DMLApplied:        200000,
// 	}
// 	id, err := applier.WriteCheckpoint(chk)
// 	suite.Require().NoError(err)
// 	suite.Require().Equal(int64(1), id)

// 	gotChk, err := applier.ReadLastCheckpoint()
// 	suite.Require().NoError(err)

// 	suite.Require().Equal(chk.Iteration, gotChk.Iteration)
// 	suite.Require().Equal(chk.LastTrxCoords.String(), gotChk.LastTrxCoords.String())
// 	suite.Require().Equal(chk.IterationRangeMin.String(), gotChk.IterationRangeMin.String())
// 	suite.Require().Equal(chk.IterationRangeMax.String(), gotChk.IterationRangeMax.String())
// 	suite.Require().Equal(chk.RowsCopied, gotChk.RowsCopied)
// 	suite.Require().Equal(chk.DMLApplied, gotChk.DMLApplied)
// }

func TestApplier(t *testing.T) {
	suite.Run(t, new(ApplierTestSuite))
}
