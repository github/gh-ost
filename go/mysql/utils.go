/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import (
	"context"
	gosql "database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/github/gh-ost/go/sql"

	gomysql "github.com/go-sql-driver/mysql"
	"github.com/openark/golib/log"
	"github.com/openark/golib/sqlutils"
)

const (
	MaxTableNameLength   = 64
	MaxDBPoolConnections = 3
)

type ReplicationLagResult struct {
	Key InstanceKey
	Lag time.Duration
	Err error
}

type Trigger struct {
	Name      string
	Event     string
	Statement string
	Timing    string
}

func NewNoReplicationLagResult() *ReplicationLagResult {
	return &ReplicationLagResult{Lag: 0, Err: nil}
}

func (rlg *ReplicationLagResult) HasLag() bool {
	return rlg.Lag > 0
}

// knownDBs is a DB cache by uri
var knownDBs map[string]*gosql.DB = make(map[string]*gosql.DB)
var knownDBsMutex = &sync.Mutex{}

// initConnector wraps a driver.Connector to run a fixed set of statements on
// every newly established connection (e.g. setting the transaction isolation
// level), which the DSN-param mechanism can't express portably.
type initConnector struct {
	driver.Connector
	statements []string
}

func (c *initConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	execer, ok := conn.(driver.ExecerContext)
	if !ok {
		conn.Close()
		return nil, fmt.Errorf("mysql: driver connection does not implement driver.ExecerContext")
	}
	for _, stmt := range c.statements {
		if _, err := execer.ExecContext(ctx, stmt, nil); err != nil {
			conn.Close()
			return nil, err
		}
	}
	return conn, nil
}

// OpenDB opens a MySQL connection pool for the given DSN. A transaction_isolation
// param is applied via the SQL-standard "SET SESSION TRANSACTION ISOLATION LEVEL"
// statement on each new connection rather than being passed to the driver as a
// system variable:
// - "transaction_isolation" doesn't exist on MariaDB < 11.1, while
// - "tx_isolation" doesn't exist on MySQL 8.0+ anymore, so no single variable name is portable.
// The standard statement is accepted by every supported MySQL and MariaDB version.
func OpenDB(mysql_uri string) (*gosql.DB, error) {
	cfg, err := gomysql.ParseDSN(mysql_uri)
	if err != nil {
		return nil, err
	}
	var statements []string
	if iso := strings.Trim(cfg.Params["transaction_isolation"], `"`); iso != "" {
		delete(cfg.Params, "transaction_isolation")
		statements = append(statements, "SET SESSION TRANSACTION ISOLATION LEVEL "+strings.ReplaceAll(iso, "-", " "))
	}
	connector, err := gomysql.NewConnector(cfg)
	if err != nil {
		return nil, err
	}
	if len(statements) > 0 {
		connector = &initConnector{Connector: connector, statements: statements}
	}
	return gosql.OpenDB(connector), nil
}

func GetDB(migrationUuid string, mysql_uri string) (db *gosql.DB, exists bool, err error) {
	cacheKey := migrationUuid + ":" + mysql_uri

	knownDBsMutex.Lock()
	defer knownDBsMutex.Unlock()

	if db, exists = knownDBs[cacheKey]; !exists {
		db, err = OpenDB(mysql_uri)
		if err != nil {
			return nil, false, err
		}
		db.SetMaxOpenConns(MaxDBPoolConnections)
		db.SetMaxIdleConns(MaxDBPoolConnections)
		knownDBs[cacheKey] = db
	}
	return db, exists, nil
}

// GetReplicationLagFromSlaveStatus returns replication lag for a given db; via SHOW SLAVE STATUS
func GetReplicationLagFromSlaveStatus(dbVersion string, informationSchemaDb *gosql.DB) (replicationLag time.Duration, err error) {
	showReplicaStatusQuery := fmt.Sprintf("show %s", ReplicaTermFor(dbVersion, `slave status`))
	err = sqlutils.QueryRowsMap(informationSchemaDb, showReplicaStatusQuery, func(m sqlutils.RowMap) error {
		ioRunningTerm := ReplicaTermFor(dbVersion, "Slave_IO_Running")
		sqlRunningTerm := ReplicaTermFor(dbVersion, "Slave_SQL_Running")
		slaveIORunning := m.GetString(ioRunningTerm)
		slaveSQLRunning := m.GetString(sqlRunningTerm)
		secondsBehindMaster := m.GetNullInt64(ReplicaTermFor(dbVersion, "Seconds_Behind_Master"))
		if !secondsBehindMaster.Valid {
			return fmt.Errorf("replication not running; %s=%+v, %s=%+v", ioRunningTerm, slaveIORunning, sqlRunningTerm, slaveSQLRunning)
		}
		replicationLag = time.Duration(secondsBehindMaster.Int64) * time.Second
		return nil
	})

	return replicationLag, err
}

func GetMasterKeyFromSlaveStatus(dbVersion string, connectionConfig *ConnectionConfig) (masterKey *InstanceKey, err error) {
	currentUri := connectionConfig.GetDBUri("information_schema")
	// This function is only called once, okay to not have a cached connection pool
	db, err := OpenDB(currentUri)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	showReplicaStatusQuery := fmt.Sprintf("show %s", ReplicaTermFor(dbVersion, `slave status`))
	err = sqlutils.QueryRowsMap(db, showReplicaStatusQuery, func(rowMap sqlutils.RowMap) error {
		// We wish to recognize the case where the topology's master actually has replication configuration.
		// This can happen when a DBA issues a `RESET SLAVE` instead of `RESET SLAVE ALL`.

		// An empty log file indicates this is a master:
		if rowMap.GetString(ReplicaTermFor(dbVersion, "Master_Log_File")) == "" {
			return nil
		}

		ioRunningTerm := ReplicaTermFor(dbVersion, "Slave_IO_Running")
		sqlRunningTerm := ReplicaTermFor(dbVersion, "Slave_SQL_Running")
		slaveIORunning := rowMap.GetString(ioRunningTerm)
		slaveSQLRunning := rowMap.GetString(sqlRunningTerm)

		if slaveIORunning != "Yes" || slaveSQLRunning != "Yes" {
			return fmt.Errorf("replication on %+v is broken: %s: %s, %s: %s. Please make sure replication runs before using gh-ost",
				connectionConfig.Key,
				ioRunningTerm,
				slaveIORunning,
				sqlRunningTerm,
				slaveSQLRunning,
			)
		}

		masterKey = &InstanceKey{
			Hostname: rowMap.GetString(ReplicaTermFor(dbVersion, "Master_Host")),
			Port:     rowMap.GetInt(ReplicaTermFor(dbVersion, "Master_Port")),
		}
		return nil
	})

	return masterKey, err
}

func GetMasterConnectionConfigSafe(dbVersion string, connectionConfig *ConnectionConfig, visitedKeys *InstanceKeyMap, allowMasterMaster bool) (masterConfig *ConnectionConfig, err error) {
	log.Debugf("Looking for %s on %+v", ReplicaTermFor(dbVersion, "master"), connectionConfig.Key)

	masterKey, err := GetMasterKeyFromSlaveStatus(dbVersion, connectionConfig)
	if err != nil {
		return nil, err
	}
	if masterKey == nil {
		return connectionConfig, nil
	}
	if !masterKey.IsValid() {
		return connectionConfig, nil
	}

	masterConfig = connectionConfig.DuplicateCredentials(*masterKey)
	if err := masterConfig.RegisterTLSConfig(); err != nil {
		return nil, err
	}

	log.Debugf("%s of %+v is %+v", ReplicaTermFor(dbVersion, "master"), connectionConfig.Key, masterConfig.Key)
	if visitedKeys.HasKey(masterConfig.Key) {
		if allowMasterMaster {
			return connectionConfig, nil
		}
		return nil, fmt.Errorf("there seems to be a master-master setup at %+v. This is unsupported. Bailing out", masterConfig.Key)
	}
	visitedKeys.AddKey(masterConfig.Key)
	return GetMasterConnectionConfigSafe(dbVersion, masterConfig, visitedKeys, allowMasterMaster)
}

func GetReplicationBinlogCoordinates(dbVersion string, db *gosql.DB, gtid bool) (readBinlogCoordinates, executeBinlogCoordinates BinlogCoordinates, err error) {
	if gtid && IsMariaDB(dbVersion) {
		return getMariaDBReplicationGTIDCoordinates(db)
	}
	showReplicaStatusQuery := fmt.Sprintf("show %s", ReplicaTermFor(dbVersion, `slave status`))
	err = sqlutils.QueryRowsMap(db, showReplicaStatusQuery, func(m sqlutils.RowMap) error {
		if gtid {
			executeBinlogCoordinates, err = NewGTIDBinlogCoordinates(MySQLFlavor, m.GetString("Executed_Gtid_Set"))
			if err != nil {
				return err
			}
			readBinlogCoordinates, err = NewGTIDBinlogCoordinates(MySQLFlavor, m.GetString("Retrieved_Gtid_Set"))
			if err != nil {
				return err
			}
		} else {
			readBinlogCoordinates = NewFileBinlogCoordinates(
				m.GetString(ReplicaTermFor(dbVersion, "Master_Log_File")),
				m.GetInt64(ReplicaTermFor(dbVersion, "Read_Master_Log_Pos")),
			)
			executeBinlogCoordinates = NewFileBinlogCoordinates(
				m.GetString(ReplicaTermFor(dbVersion, "Relay_Master_Log_File")),
				m.GetInt64(ReplicaTermFor(dbVersion, "Exec_Master_Log_Pos")),
			)
		}
		return nil
	})
	return readBinlogCoordinates, executeBinlogCoordinates, err
}

func GetSelfBinlogCoordinates(dbVersion string, db *gosql.DB, gtid bool) (selfBinlogCoordinates BinlogCoordinates, err error) {
	if gtid && IsMariaDB(dbVersion) {
		// MariaDB does not expose a GTID column in SHOW MASTER STATUS; the
		// executed GTID position of this server's own binary log is in
		// @@global.gtid_binlog_pos.
		var gtidBinlogPos string
		if err = db.QueryRow(`select @@global.gtid_binlog_pos`).Scan(&gtidBinlogPos); err != nil {
			return nil, err
		}
		return NewGTIDBinlogCoordinates(MariaDBFlavor, gtidBinlogPos)
	}
	binaryLogStatusTerm := ReplicaTermFor(dbVersion, "master status")
	err = sqlutils.QueryRowsMap(db, fmt.Sprintf("show %s", binaryLogStatusTerm), func(m sqlutils.RowMap) error {
		if gtid {
			selfBinlogCoordinates, err = NewGTIDBinlogCoordinates(MySQLFlavor, m.GetString("Executed_Gtid_Set"))
		} else {
			selfBinlogCoordinates = NewFileBinlogCoordinates(
				m.GetString("File"),
				m.GetInt64("Position"),
			)
		}
		return nil
	})
	return selfBinlogCoordinates, err
}

// getMariaDBReplicationGTIDCoordinates reports the IO/SQL thread GTID positions
// of a MariaDB replica. MariaDB has no Executed_Gtid_Set/Retrieved_Gtid_Set
// columns: the IO thread position is in SHOW SLAVE STATUS's Gtid_IO_Pos, and the
// applied position is in @@global.gtid_slave_pos.
func getMariaDBReplicationGTIDCoordinates(db *gosql.DB) (readBinlogCoordinates, executeBinlogCoordinates BinlogCoordinates, err error) {
	err = sqlutils.QueryRowsMap(db, "show slave status", func(m sqlutils.RowMap) error {
		readBinlogCoordinates, err = NewGTIDBinlogCoordinates(MariaDBFlavor, m.GetString("Gtid_IO_Pos"))
		return err
	})
	if err != nil {
		return readBinlogCoordinates, executeBinlogCoordinates, err
	}
	var gtidSlavePos string
	if err = db.QueryRow(`select @@global.gtid_slave_pos`).Scan(&gtidSlavePos); err != nil {
		return readBinlogCoordinates, executeBinlogCoordinates, err
	}
	executeBinlogCoordinates, err = NewGTIDBinlogCoordinates(MariaDBFlavor, gtidSlavePos)
	return readBinlogCoordinates, executeBinlogCoordinates, err
}

// GetInstanceKey reads hostname and port on given DB
func GetInstanceKey(db *gosql.DB) (instanceKey *InstanceKey, err error) {
	instanceKey = &InstanceKey{}
	err = db.QueryRow(`select @@global.hostname, @@global.port`).Scan(&instanceKey.Hostname, &instanceKey.Port)
	return instanceKey, err
}

// GetTableColumns reads column list from given table
func GetTableColumns(db *gosql.DB, databaseName, tableName string) (*sql.ColumnList, *sql.ColumnList, error) {
	query := fmt.Sprintf(`
		show columns from %s.%s
		`,
		sql.EscapeName(databaseName),
		sql.EscapeName(tableName),
	)
	columnNames := []string{}
	virtualColumnNames := []string{}
	err := sqlutils.QueryRowsMap(db, query, func(rowMap sqlutils.RowMap) error {
		columnName := rowMap.GetString("Field")
		columnNames = append(columnNames, columnName)
		if strings.Contains(rowMap.GetString("Extra"), " GENERATED") {
			log.Debugf("%s is a generated column", columnName)
			virtualColumnNames = append(virtualColumnNames, columnName)
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	if len(columnNames) == 0 {
		return nil, nil, log.Errorf("found 0 columns on %s.%s. Bailing out",
			sql.EscapeName(databaseName),
			sql.EscapeName(tableName),
		)
	}
	return sql.NewColumnList(columnNames), sql.NewColumnList(virtualColumnNames), nil
}

// Kill executes a KILL QUERY by connection id
func Kill(db *gosql.DB, connectionID string) error {
	_, err := db.Exec(`KILL QUERY %s`, connectionID)
	return err
}

// GetTriggers reads trigger list from given table
func GetTriggers(db *gosql.DB, databaseName, tableName string) (triggers []Trigger, err error) {
	query := `select trigger_name as name, event_manipulation as event, action_statement as statement, action_timing as timing
	from information_schema.triggers
	where trigger_schema = ? and event_object_table = ?`

	err = sqlutils.QueryRowsMap(db, query, func(rowMap sqlutils.RowMap) error {
		triggers = append(triggers, Trigger{
			Name:      rowMap.GetString("name"),
			Event:     rowMap.GetString("event"),
			Statement: rowMap.GetString("statement"),
			Timing:    rowMap.GetString("timing"),
		})
		return nil
	}, databaseName, tableName)
	if err != nil {
		return nil, err
	}
	return triggers, nil
}
