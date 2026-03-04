/*
   Copyright 2025 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"context"
	"fmt"
	"testing"

	"github.com/github/gh-ost/go/binlog"
	"github.com/github/gh-ost/go/sql"
	"github.com/stretchr/testify/suite"
)

type DeleteInsertTestSuite struct {
	ApplierTestSuite
}

// TestUpdateModifyingUniqueKeyWithDuplicateOnOtherIndex tests the scenario where:
// 1. An UPDATE modifies the unique key (converted to DELETE+INSERT)
// 2. The INSERT would create a duplicate on a NON-migration unique index
// 3. Without warning detection: DELETE succeeds, INSERT IGNORE skips = DATA LOSS
// 4. With PanicOnWarnings: Warning detected, transaction rolled back, no data loss
// This test verifies that PanicOnWarnings correctly prevents the data loss scenario.
func (suite *DeleteInsertTestSuite) TestUpdateModifyingUniqueKeyWithDuplicateOnOtherIndex() {
	ctx := context.Background()

	var err error

	// Create table with id (PRIMARY) and email (NO unique constraint yet)
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, email VARCHAR(100));", getTestTableName()))
	suite.Require().NoError(err)

	// Create ghost table with id (PRIMARY) AND email unique index (being added)
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, email VARCHAR(100), UNIQUE KEY email_unique (email));", getTestGhostTableName()))
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := newTestMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.SetConnectionConfig("innodb")

	migrationContext.PanicOnWarnings = true

	migrationContext.OriginalTableColumns = sql.NewColumnList([]string{"id", "email"})
	migrationContext.SharedColumns = sql.NewColumnList([]string{"id", "email"})
	migrationContext.MappedSharedColumns = sql.NewColumnList([]string{"id", "email"})
	migrationContext.UniqueKey = &sql.UniqueKey{
		Name:             "PRIMARY",
		NameInGhostTable: "PRIMARY",
		Columns:          *sql.NewColumnList([]string{"id"}),
	}

	applier := NewApplier(migrationContext)
	suite.Require().NoError(applier.prepareQueries())
	defer applier.Teardown()

	err = applier.InitDBConnections()
	suite.Require().NoError(err)

	// Setup: Insert initial rows into ghost table
	// Row 1: id=1, email='bob@example.com'
	// Row 2: id=2, email='charlie@example.com'
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, email) VALUES (1, 'bob@example.com'), (2, 'charlie@example.com');", getTestGhostTableName()))
	suite.Require().NoError(err)

	// Simulate binlog event: UPDATE that changes BOTH PRIMARY KEY and email
	// From: id=2, email='charlie@example.com'
	// To:   id=3, email='bob@example.com'  (duplicate email with id=1)
	// This will be converted to DELETE (id=2) + INSERT (id=3, 'bob@example.com')
	// With INSERT IGNORE, the INSERT will skip because email='bob@example.com' already exists in id=1
	// Result: id=2 deleted, id=3 never inserted = DATA LOSS
	dmlEvents := []*binlog.BinlogDMLEvent{
		{
			DatabaseName:      testMysqlDatabase,
			TableName:         testMysqlTableName,
			DML:               binlog.UpdateDML,
			NewColumnValues:   sql.ToColumnValues([]interface{}{3, "bob@example.com"}),     // new: id=3, email='bob@example.com'
			WhereColumnValues: sql.ToColumnValues([]interface{}{2, "charlie@example.com"}), // old: id=2, email='charlie@example.com'
		},
	}

	// First verify this would be converted to DELETE+INSERT
	buildResults := applier.buildDMLEventQuery(dmlEvents[0])
	suite.Require().Len(buildResults, 2, "UPDATE modifying unique key should be converted to DELETE+INSERT")

	// Apply the event - this should FAIL because INSERT will have duplicate email warning
	err = applier.ApplyDMLEventQueries(dmlEvents)
	suite.Require().Error(err, "Should fail when DELETE+INSERT causes duplicate on non-migration unique key")
	suite.Require().Contains(err.Error(), "Duplicate entry", "Error should mention duplicate entry")

	// Verify that BOTH rows still exist (transaction rolled back)
	rows, err := suite.db.Query("SELECT id, email FROM " + getTestGhostTableName() + " ORDER BY id")
	suite.Require().NoError(err)
	defer rows.Close()

	var count int
	var ids []int
	var emails []string
	for rows.Next() {
		var id int
		var email string
		err = rows.Scan(&id, &email)
		suite.Require().NoError(err)
		ids = append(ids, id)
		emails = append(emails, email)
		count++
	}
	suite.Require().NoError(rows.Err())

	// Transaction should have rolled back, so original 2 rows should still be there
	suite.Require().Equal(2, count, "Should still have 2 rows after failed transaction")
	suite.Require().Equal([]int{1, 2}, ids, "Should have original ids")
	suite.Require().Equal([]string{"bob@example.com", "charlie@example.com"}, emails)
}

// TestNormalUpdateWithPanicOnWarnings tests that normal UPDATEs (not modifying unique key) work correctly
func (suite *DeleteInsertTestSuite) TestNormalUpdateWithPanicOnWarnings() {
	ctx := context.Background()

	var err error

	// Create table with id (PRIMARY) and email
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, email VARCHAR(100));", getTestTableName()))
	suite.Require().NoError(err)

	// Create ghost table with same schema plus unique index on email
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, email VARCHAR(100), UNIQUE KEY email_unique (email));", getTestGhostTableName()))
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := newTestMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.SetConnectionConfig("innodb")

	migrationContext.PanicOnWarnings = true

	migrationContext.OriginalTableColumns = sql.NewColumnList([]string{"id", "email"})
	migrationContext.SharedColumns = sql.NewColumnList([]string{"id", "email"})
	migrationContext.MappedSharedColumns = sql.NewColumnList([]string{"id", "email"})
	migrationContext.UniqueKey = &sql.UniqueKey{
		Name:             "PRIMARY",
		NameInGhostTable: "PRIMARY",
		Columns:          *sql.NewColumnList([]string{"id"}),
	}

	applier := NewApplier(migrationContext)
	suite.Require().NoError(applier.prepareQueries())
	defer applier.Teardown()

	err = applier.InitDBConnections()
	suite.Require().NoError(err)

	// Setup: Insert initial rows into ghost table
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, email) VALUES (1, 'alice@example.com'), (2, 'bob@example.com');", getTestGhostTableName()))
	suite.Require().NoError(err)

	// Simulate binlog event: Normal UPDATE that only changes email (not PRIMARY KEY)
	// This should use UPDATE query, not DELETE+INSERT
	dmlEvents := []*binlog.BinlogDMLEvent{
		{
			DatabaseName:      testMysqlDatabase,
			TableName:         testMysqlTableName,
			DML:               binlog.UpdateDML,
			NewColumnValues:   sql.ToColumnValues([]interface{}{2, "robert@example.com"}), // update email only
			WhereColumnValues: sql.ToColumnValues([]interface{}{2, "bob@example.com"}),
		},
	}

	// Verify this generates a single UPDATE query (not DELETE+INSERT)
	buildResults := applier.buildDMLEventQuery(dmlEvents[0])
	suite.Require().Len(buildResults, 1, "Normal UPDATE should generate single UPDATE query")

	// Apply the event - should succeed
	err = applier.ApplyDMLEventQueries(dmlEvents)
	suite.Require().NoError(err)

	// Verify the update was applied correctly
	rows, err := suite.db.Query("SELECT id, email FROM " + getTestGhostTableName() + " WHERE id = 2")
	suite.Require().NoError(err)
	defer rows.Close()

	var id int
	var email string
	suite.Require().True(rows.Next(), "Should find updated row")
	err = rows.Scan(&id, &email)
	suite.Require().NoError(err)
	suite.Require().Equal(2, id)
	suite.Require().Equal("robert@example.com", email)
	suite.Require().False(rows.Next(), "Should only have one row")
	suite.Require().NoError(rows.Err())
}

func TestDeleteInsert(t *testing.T) {
	suite.Run(t, new(DeleteInsertTestSuite))
}
