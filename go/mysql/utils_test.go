/*
   Copyright 2026 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import (
	"context"
	gosql "database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type topologyTestNode struct {
	version    string
	masterKey  *InstanceKey
	versionErr error
	statusErr  error
	queries    []string
}

type topologyTestConnector struct {
	node *topologyTestNode
}

func (connector *topologyTestConnector) Connect(context.Context) (driver.Conn, error) {
	return &topologyTestConn{node: connector.node}, nil
}

func (connector *topologyTestConnector) Driver() driver.Driver {
	return topologyTestDriver{}
}

type topologyTestDriver struct{}

func (topologyTestDriver) Open(string) (driver.Conn, error) {
	return nil, driver.ErrSkip
}

type topologyTestConn struct {
	node *topologyTestNode
}

func (conn *topologyTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, driver.ErrSkip
}

func (conn *topologyTestConn) Close() error {
	return nil
}

func (conn *topologyTestConn) Begin() (driver.Tx, error) {
	return nil, driver.ErrSkip
}

func (conn *topologyTestConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	query = strings.ToLower(strings.TrimSpace(query))
	conn.node.queries = append(conn.node.queries, query)

	if query == "select @@global.version" {
		if conn.node.versionErr != nil {
			return nil, conn.node.versionErr
		}
		return &topologyTestRows{
			columns: []string{"@@global.version"},
			values:  [][]driver.Value{{conn.node.version}},
		}, nil
	}

	expectedQuery := "show " + ReplicaTermFor(conn.node.version, "slave status")
	if query != expectedQuery {
		return nil, fmt.Errorf("unexpected query %q, expected %q", query, expectedQuery)
	}
	if conn.node.statusErr != nil {
		return nil, conn.node.statusErr
	}

	rows := &topologyTestRows{columns: []string{
		ReplicaTermFor(conn.node.version, "Master_Log_File"),
		ReplicaTermFor(conn.node.version, "Slave_IO_Running"),
		ReplicaTermFor(conn.node.version, "Slave_SQL_Running"),
		ReplicaTermFor(conn.node.version, "Master_Host"),
		ReplicaTermFor(conn.node.version, "Master_Port"),
	}}
	if conn.node.masterKey != nil {
		rows.values = [][]driver.Value{{
			"mysql-bin.000001",
			"Yes",
			"Yes",
			conn.node.masterKey.Hostname,
			int64(conn.node.masterKey.Port),
		}}
	}
	return rows, nil
}

type topologyTestRows struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func (rows *topologyTestRows) Columns() []string {
	return rows.columns
}

func (rows *topologyTestRows) Close() error {
	return nil
}

func (rows *topologyTestRows) Next(dest []driver.Value) error {
	if rows.index >= len(rows.values) {
		return io.EOF
	}
	copy(dest, rows.values[rows.index])
	rows.index++
	return nil
}

func TestGetMasterConnectionConfigSafeUsesEachNodeVersion(t *testing.T) {
	versionErr := errors.New("version query failed")
	statusErr := errors.New("replication status query failed")
	tests := []struct {
		name               string
		inspectorVersion   string
		masterVersion      string
		wantInspectorQuery string
		wantMasterQueries  []string
		masterVersionErr   error
		masterStatusErr    error
		wantErr            error
	}{
		{
			name:               "MySQL 8.0 inspector to MySQL 8.4 primary",
			inspectorVersion:   "8.0.40",
			masterVersion:      "8.4.6",
			wantInspectorQuery: "show slave status",
			wantMasterQueries:  []string{"select @@global.version", "show replica status"},
		},
		{
			name:               "MySQL 8.4 inspector to MySQL 8.0 primary",
			inspectorVersion:   "8.4.6",
			masterVersion:      "8.0.21",
			wantInspectorQuery: "show replica status",
			wantMasterQueries:  []string{"select @@global.version", "show slave status"},
		},
		{
			name:               "same-version topology",
			inspectorVersion:   "8.4.6",
			masterVersion:      "8.4.6",
			wantInspectorQuery: "show replica status",
			wantMasterQueries:  []string{"select @@global.version", "show replica status"},
		},
		{
			name:               "MariaDB topology",
			inspectorVersion:   "11.4.8-MariaDB-ubu2404-log",
			masterVersion:      "11.4.8-MariaDB-ubu2404-log",
			wantInspectorQuery: "show slave status",
			wantMasterQueries:  []string{"select @@global.version", "show slave status"},
		},
		{
			name:               "upstream version query error",
			inspectorVersion:   "8.0.40",
			masterVersionErr:   versionErr,
			wantInspectorQuery: "show slave status",
			wantMasterQueries:  []string{"select @@global.version"},
			wantErr:            versionErr,
		},
		{
			name:               "upstream replication status query error",
			inspectorVersion:   "8.0.40",
			masterVersion:      "8.4.6",
			masterStatusErr:    statusErr,
			wantInspectorQuery: "show slave status",
			wantMasterQueries:  []string{"select @@global.version", "show replica status"},
			wantErr:            statusErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			inspectorConfig := NewConnectionConfig()
			inspectorConfig.Key = InstanceKey{Hostname: "inspector", Port: 3306}
			inspectorConfig.User = "gh-ost"
			masterKey := InstanceKey{Hostname: "primary", Port: 3306}
			masterConfig := inspectorConfig.DuplicateCredentials(masterKey)

			inspectorNode := &topologyTestNode{version: tc.inspectorVersion, masterKey: &masterKey}
			masterNode := &topologyTestNode{
				version:    tc.masterVersion,
				versionErr: tc.masterVersionErr,
				statusErr:  tc.masterStatusErr,
			}
			nodes := map[string]*topologyTestNode{
				inspectorConfig.GetDBUri("information_schema"): inspectorNode,
				masterConfig.GetDBUri("information_schema"):    masterNode,
			}
			openDB := func(uri string) (*gosql.DB, error) {
				node, ok := nodes[uri]
				if !ok {
					return nil, fmt.Errorf("unexpected database URI %q", uri)
				}
				return gosql.OpenDB(&topologyTestConnector{node: node}), nil
			}

			actual, err := getMasterConnectionConfigSafe(tc.inspectorVersion, inspectorConfig, NewInstanceKeyMap(), false, openDB)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				require.Nil(t, actual)
			} else {
				require.NoError(t, err)
				require.Equal(t, masterKey, actual.Key)
			}
			require.Equal(t, []string{"select @@global.version", tc.wantInspectorQuery}, inspectorNode.queries)
			require.Equal(t, tc.wantMasterQueries, masterNode.queries)
		})
	}
}
