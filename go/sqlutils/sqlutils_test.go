package sqlutils

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"
)

const stubDriverName = "gh-ost-sqlutils-test"

var (
	stubStateMu sync.Mutex
	stubState   *databaseStub
)

type databaseStub struct {
	query func(string, []driver.NamedValue) (driver.Rows, error)
	exec  func(string, []driver.NamedValue) (driver.Result, error)
}

type stubDriver struct{}

func (stubDriver) Open(string) (driver.Conn, error) {
	stubStateMu.Lock()
	state := stubState
	stubStateMu.Unlock()
	return stubConnection{state: state}, nil
}

type stubConnection struct {
	state *databaseStub
}

func (stubConnection) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("Prepare should not be called")
}

func (stubConnection) Close() error {
	return nil
}

func (stubConnection) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are unsupported")
}

func (connection stubConnection) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return connection.state.query(query, args)
}

func (connection stubConnection) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return connection.state.exec(query, args)
}

type stubRows struct {
	columns []string
	values  [][]driver.Value
	nextErr error
	index   int
}

func (rows *stubRows) Columns() []string {
	return rows.columns
}

func (rows *stubRows) Close() error {
	return nil
}

func (rows *stubRows) Next(dest []driver.Value) error {
	if rows.index >= len(rows.values) {
		if rows.nextErr != nil {
			return rows.nextErr
		}
		return io.EOF
	}
	copy(dest, rows.values[rows.index])
	rows.index++
	return nil
}

type stubResult struct {
	lastInsertID int64
	rowsAffected int64
}

func (result stubResult) LastInsertId() (int64, error) {
	return result.lastInsertID, nil
}

func (result stubResult) RowsAffected() (int64, error) {
	return result.rowsAffected, nil
}

func init() {
	sql.Register(stubDriverName, stubDriver{})
}

func openStubDB(t *testing.T, state *databaseStub) *sql.DB {
	t.Helper()
	stubStateMu.Lock()
	stubState = state
	stubStateMu.Unlock()
	database, err := sql.Open(stubDriverName, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		database.Close()
	})
	return database
}

func TestRowMapGettersAndArgs(t *testing.T) {
	row := RowMap{
		"string": CellData{String: "text", Valid: true},
		"int":    CellData{String: "-12", Valid: true},
		"uint":   CellData{String: "42", Valid: true},
		"true":   CellData{String: "1", Valid: true},
		"false":  CellData{String: "0", Valid: true},
		"bad":    CellData{String: "not-a-number", Valid: true},
		"null":   CellData{},
	}

	if row.GetString("string") != "text" || row.GetString("missing") != "" {
		t.Fatalf("unexpected strings: %#v", row)
	}
	if row.GetInt("int") != -12 || row.GetInt64("int") != -12 {
		t.Fatalf("unexpected signed values: %#v", row)
	}
	if row.GetUint("uint") != 42 || row.GetUint64("uint") != 42 {
		t.Fatalf("unexpected unsigned values: %#v", row)
	}
	if !row.GetBool("true") || row.GetBool("false") || row.GetBool("bad") {
		t.Fatalf("unexpected boolean values: %#v", row)
	}
	if row.GetInt("bad") != 0 || row.GetInt64("missing") != 0 || row.GetUint("bad") != 0 || row.GetUint64("missing") != 0 {
		t.Fatalf("unexpected invalid numeric value: %#v", row)
	}
	if value := row.GetNullInt64("int"); !value.Valid || value.Int64 != -12 {
		t.Fatalf("GetNullInt64(int) = %#v", value)
	}
	if value := row.GetNullInt64("null"); value.Valid {
		t.Fatalf("GetNullInt64(null) = %#v", value)
	}
	arguments := Args("first", 2, true)
	if !reflect.DeepEqual(arguments, []interface{}{"first", 2, true}) {
		t.Fatalf("Args() = %#v", arguments)
	}
}

func TestQueryRowsMapMapsRowsAndForwardsArguments(t *testing.T) {
	var gotQuery string
	var gotArgs []driver.NamedValue
	database := openStubDB(t, &databaseStub{
		query: func(query string, args []driver.NamedValue) (driver.Rows, error) {
			gotQuery, gotArgs = query, args
			return &stubRows{
				columns: []string{"id", "name", "nullable"},
				values:  [][]driver.Value{{int64(1), "one", nil}, {int64(2), "two", "present"}},
			}, nil
		},
	})

	var rows []RowMap
	err := QueryRowsMap(database, "select * from items where id > ?", func(row RowMap) error {
		rows = append(rows, row)
		return nil
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if gotQuery != "select * from items where id > ?" || len(gotArgs) != 1 || gotArgs[0].Ordinal != 1 || gotArgs[0].Value != int64(0) {
		t.Fatalf("query arguments = %q %#v", gotQuery, gotArgs)
	}
	if len(rows) != 2 || rows[0].GetString("id") != "1" || rows[1].GetString("name") != "two" {
		t.Fatalf("mapped rows = %#v", rows)
	}
	if rows[0]["nullable"].Valid || !rows[1]["nullable"].Valid {
		t.Fatalf("NULL mapping = %#v", rows)
	}
}

func TestQueryRowsMapErrors(t *testing.T) {
	callbackErr := errors.New("stop")
	database := openStubDB(t, &databaseStub{
		query: func(_ string, _ []driver.NamedValue) (driver.Rows, error) {
			return &stubRows{columns: []string{"id"}, values: [][]driver.Value{{int64(1)}, {int64(2)}}}, nil
		},
	})
	callbackCalls := 0
	err := QueryRowsMap(database, "rows", func(RowMap) error {
		callbackCalls++
		return callbackErr
	})
	if !errors.Is(err, callbackErr) || callbackCalls != 1 {
		t.Fatalf("callback result = %v after %d calls", err, callbackCalls)
	}

	queryErr := errors.New("query failed")
	database = openStubDB(t, &databaseStub{
		query: func(_ string, _ []driver.NamedValue) (driver.Rows, error) { return nil, queryErr },
	})
	if err := QueryRowsMap(database, "broken", func(RowMap) error { return nil }); !errors.Is(err, queryErr) {
		t.Fatalf("query error = %v", err)
	}

	database = openStubDB(t, &databaseStub{
		query: func(_ string, _ []driver.NamedValue) (driver.Rows, error) { return nil, sql.ErrNoRows },
	})
	if err := QueryRowsMap(database, "empty", func(RowMap) error { return nil }); err != nil {
		t.Fatalf("sql.ErrNoRows handling = %v", err)
	}
}

func TestQueryRowsMapScanAndIterationErrors(t *testing.T) {
	scanDB := openStubDB(t, &databaseStub{
		query: func(_ string, _ []driver.NamedValue) (driver.Rows, error) {
			return &stubRows{columns: []string{"value"}, values: [][]driver.Value{{struct{}{}}}}, nil
		},
	})
	if err := QueryRowsMap(scanDB, "scan", func(RowMap) error { return nil }); err == nil {
		t.Fatal("expected scan error")
	}

	iterationErr := errors.New("iteration failed")
	iterationDB := openStubDB(t, &databaseStub{
		query: func(_ string, _ []driver.NamedValue) (driver.Rows, error) {
			return &stubRows{columns: []string{"id"}, nextErr: iterationErr}, nil
		},
	})
	if err := QueryRowsMap(iterationDB, "iteration", func(RowMap) error { return nil }); !errors.Is(err, iterationErr) {
		t.Fatalf("iteration error = %v", err)
	}
}

func TestQueryRowsMapRecoversPanics(t *testing.T) {
	err := QueryRowsMap(nil, "panic", func(RowMap) error { return nil })
	if err == nil || err.Error() != "QueryRowsMap unexpected error: runtime error: invalid memory address or nil pointer dereference" {
		t.Fatalf("panic conversion = %v", err)
	}
}

func TestExecNoPrepare(t *testing.T) {
	var gotQuery string
	var gotArgs []driver.NamedValue
	database := openStubDB(t, &databaseStub{
		exec: func(query string, args []driver.NamedValue) (driver.Result, error) {
			gotQuery, gotArgs = query, args
			return stubResult{lastInsertID: 8, rowsAffected: 3}, nil
		},
	})
	result, err := ExecNoPrepare(database, "update items set value = ?", "new")
	if err != nil {
		t.Fatal(err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil || rowsAffected != 3 || gotQuery != "update items set value = ?" || len(gotArgs) != 1 || gotArgs[0].Value != "new" {
		t.Fatalf("exec result = %v, %v; query = %q %#v", rowsAffected, err, gotQuery, gotArgs)
	}

	execErr := errors.New("exec failed")
	database = openStubDB(t, &databaseStub{
		exec: func(_ string, _ []driver.NamedValue) (driver.Result, error) { return nil, execErr },
	})
	if result, err := ExecNoPrepare(database, "broken"); result != nil || !errors.Is(err, execErr) {
		t.Fatalf("exec error result = %#v, %v", result, err)
	}

	if result, err := ExecNoPrepare(nil, "panic"); result != nil || err == nil {
		t.Fatalf("panic conversion = %#v, %v", result, err)
	}
}
