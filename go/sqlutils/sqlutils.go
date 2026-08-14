package sqlutils

import (
	"database/sql"
	"fmt"
	"strconv"

	"github.com/openark/golib/log"
)

// CellData stores a nullable database string.
type CellData sql.NullString

// RowMap represents one result row keyed by column name.
type RowMap map[string]CellData

func (rowMap RowMap) GetString(key string) string {
	return rowMap[key].String
}

func (rowMap RowMap) GetInt(key string) int {
	value, _ := strconv.Atoi(rowMap.GetString(key))
	return value
}

func (rowMap RowMap) GetInt64(key string) int64 {
	value, _ := strconv.ParseInt(rowMap.GetString(key), 10, 0)
	return value
}

func (rowMap RowMap) GetNullInt64(key string) sql.NullInt64 {
	value, err := strconv.ParseInt(rowMap.GetString(key), 10, 0)
	if err != nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: value, Valid: true}
}

func (rowMap RowMap) GetUint(key string) uint {
	value, _ := strconv.ParseUint(rowMap.GetString(key), 10, 0)
	return uint(value)
}

func (rowMap RowMap) GetUint64(key string) uint64 {
	value, _ := strconv.ParseUint(rowMap.GetString(key), 10, 0)
	return value
}

func (rowMap RowMap) GetBool(key string) bool {
	return rowMap.GetInt(key) != 0
}

// Args returns its arguments without changing their order.
func Args(args ...interface{}) []interface{} {
	return args
}

// ExecNoPrepare executes a query directly without explicitly preparing a statement.
func ExecNoPrepare(db *sql.DB, query string, args ...interface{}) (result sql.Result, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = nil
			err = fmt.Errorf("ExecNoPrepare unexpected error: %+v", recovered)
		}
	}()

	result, err = db.Exec(query, args...)
	if err != nil {
		log.Errore(err)
	}
	return result, err
}

// QueryRowsMap executes a query and invokes onRow once for each result row.
func QueryRowsMap(db *sql.DB, query string, onRow func(RowMap) error, args ...interface{}) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("QueryRowsMap unexpected error: %+v", recovered)
		}
	}()

	rows, err := db.Query(query, args...)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return log.Errore(err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	for rows.Next() {
		row := make([]CellData, len(columns))
		destinations := make([]interface{}, len(columns))
		for index := range row {
			destinations[index] = (*sql.NullString)(&row[index])
		}
		if err := rows.Scan(destinations...); err != nil {
			return err
		}

		rowMap := make(RowMap, len(columns))
		for index, column := range columns {
			rowMap[column] = row[index]
		}
		if err := onRow(rowMap); err != nil {
			return err
		}
	}
	return rows.Err()
}
