/*
   Copyright 2022 GitHub Inc.
         See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"strings"
	"testing"

	test "github.com/openark/golib/tests"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/binlog"
	"github.com/github/gh-ost/go/sql"
)

func TestApplierGenerateSqlModeQuery(t *testing.T) {
	migrationContext := base.NewMigrationContext()
	applier := NewApplier(migrationContext)

	{
		test.S(t).ExpectEquals(
			applier.generateSqlModeQuery(),
			`sql_mode = CONCAT(@@session.sql_mode, ',NO_AUTO_VALUE_ON_ZERO,STRICT_ALL_TABLES')`,
		)
	}
	{
		migrationContext.SkipStrictMode = true
		migrationContext.AllowZeroInDate = false
		test.S(t).ExpectEquals(
			applier.generateSqlModeQuery(),
			`sql_mode = CONCAT(@@session.sql_mode, ',NO_AUTO_VALUE_ON_ZERO')`,
		)
	}
	{
		migrationContext.SkipStrictMode = false
		migrationContext.AllowZeroInDate = true
		test.S(t).ExpectEquals(
			applier.generateSqlModeQuery(),
			`sql_mode = REPLACE(REPLACE(CONCAT(@@session.sql_mode, ',NO_AUTO_VALUE_ON_ZERO,STRICT_ALL_TABLES'), 'NO_ZERO_IN_DATE', ''), 'NO_ZERO_DATE', '')`,
		)
	}
	{
		migrationContext.SkipStrictMode = true
		migrationContext.AllowZeroInDate = true
		test.S(t).ExpectEquals(
			applier.generateSqlModeQuery(),
			`sql_mode = REPLACE(REPLACE(CONCAT(@@session.sql_mode, ',NO_AUTO_VALUE_ON_ZERO'), 'NO_ZERO_IN_DATE', ''), 'NO_ZERO_DATE', '')`,
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
		test.S(t).ExpectEquals(modifiedColumn, "")
		test.S(t).ExpectFalse(isModified)
	})

	t.Run("modified", func(t *testing.T) {
		modifiedColumn, isModified := applier.updateModifiesUniqueKeyColumns(&binlog.BinlogDMLEvent{
			DatabaseName:      "test",
			DML:               binlog.UpdateDML,
			NewColumnValues:   sql.ToColumnValues([]interface{}{123456, 24}),
			WhereColumnValues: columnValues,
		})
		test.S(t).ExpectEquals(modifiedColumn, "item_id")
		test.S(t).ExpectTrue(isModified)
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
		test.S(t).ExpectEquals(len(res), 1)
		test.S(t).ExpectNil(res[0].err)
		test.S(t).ExpectEquals(strings.TrimSpace(res[0].query),
			`delete /* gh-ost `+"`test`.`_test_gho`"+` */
				from
					`+"`test`.`_test_gho`"+`
				where
					((`+"`id`"+` = ?) and (`+"`item_id`"+` = ?))`)

		test.S(t).ExpectEquals(len(res[0].args), 2)
		test.S(t).ExpectEquals(res[0].args[0], 123456)
		test.S(t).ExpectEquals(res[0].args[1], 42)
	})

	t.Run("insert", func(t *testing.T) {
		binlogEvent := &binlog.BinlogDMLEvent{
			DatabaseName:    "test",
			DML:             binlog.InsertDML,
			NewColumnValues: columnValues,
		}
		res := applier.buildDMLEventQuery(binlogEvent)
		test.S(t).ExpectEquals(len(res), 1)
		test.S(t).ExpectNil(res[0].err)
		test.S(t).ExpectEquals(strings.TrimSpace(res[0].query),
			`replace /* gh-ost `+"`test`.`_test_gho`"+` */ into
				`+"`test`.`_test_gho`"+`
					`+"(`id`, `item_id`)"+`
				values
					(?, ?)`)
		test.S(t).ExpectEquals(len(res[0].args), 2)
		test.S(t).ExpectEquals(res[0].args[0], 123456)
		test.S(t).ExpectEquals(res[0].args[1], 42)
	})

	t.Run("update", func(t *testing.T) {
		binlogEvent := &binlog.BinlogDMLEvent{
			DatabaseName:      "test",
			DML:               binlog.UpdateDML,
			NewColumnValues:   columnValues,
			WhereColumnValues: columnValues,
		}
		res := applier.buildDMLEventQuery(binlogEvent)
		test.S(t).ExpectEquals(len(res), 1)
		test.S(t).ExpectNil(res[0].err)
		test.S(t).ExpectEquals(strings.TrimSpace(res[0].query),
			`update /* gh-ost `+"`test`.`_test_gho`"+` */
					`+"`test`.`_test_gho`"+`
				set
					`+"`id`"+`=?, `+"`item_id`"+`=?
				where
					((`+"`id`"+` = ?) and (`+"`item_id`"+` = ?))`)
		test.S(t).ExpectEquals(len(res[0].args), 4)
		test.S(t).ExpectEquals(res[0].args[0], 123456)
		test.S(t).ExpectEquals(res[0].args[1], 42)
		test.S(t).ExpectEquals(res[0].args[2], 123456)
		test.S(t).ExpectEquals(res[0].args[3], 42)
	})
}
