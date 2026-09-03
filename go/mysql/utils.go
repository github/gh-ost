/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/hanchuanchuan/gh-ost/blob/master/LICENSE
*/

package mysql

import (
	gosql "database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/hanchuanchuan/gh-ost/go/sql"

	"github.com/hanchuanchuan/golib/sqlutils"
	log "github.com/sirupsen/logrus"
)

const MaxTableNameLength = 64
const MaxReplicationPasswordLength = 32

type ReplicationLagResult struct {
	Key InstanceKey
	Lag time.Duration
	Err error
}

func NewNoReplicationLagResult() *ReplicationLagResult {
	return &ReplicationLagResult{Lag: 0, Err: nil}
}

func (this *ReplicationLagResult) HasLag() bool {
	return this.Lag > 0
}

// knownDBs is a DB cache by uri
var knownDBs map[string]*gosql.DB = make(map[string]*gosql.DB)
var knownDBsMutex = &sync.Mutex{}

func GetDB(migrationUuid string, mysql_uri string) (*gosql.DB, bool, error) {
	cacheKey := migrationUuid + ":" + mysql_uri

	knownDBsMutex.Lock()
	defer func() {
		knownDBsMutex.Unlock()
	}()

	var exists bool
	if _, exists = knownDBs[cacheKey]; !exists {
		if db, err := gosql.Open("mysql", mysql_uri); err == nil {
			knownDBs[cacheKey] = db
		} else {
			return db, exists, err
		}
	}
	return knownDBs[cacheKey], exists, nil
}

// queryReplicaStatus runs SHOW REPLICA STATUS (MySQL 8.0.22+/MariaDB 10.5+), falling back to the
// legacy SHOW SLAVE STATUS on servers that don't support the new syntax (removed entirely in MySQL 8.4).
func queryReplicaStatus(db *gosql.DB, onRow func(sqlutils.RowMap) error) (err error) {
	if err = sqlutils.QueryRowsMap(db, `show replica status`, onRow); err != nil {
		err = sqlutils.QueryRowsMap(db, `show slave status`, onRow)
	}
	return err
}

// queryBinaryLogStatus runs SHOW BINARY LOG STATUS (MySQL 8.0.22+), falling back to the legacy
// SHOW MASTER STATUS on servers that don't support the new syntax (removed entirely in MySQL 8.4).
func queryBinaryLogStatus(db *gosql.DB, onRow func(sqlutils.RowMap) error) (err error) {
	if err = sqlutils.QueryRowsMap(db, `show binary log status`, onRow); err != nil {
		err = sqlutils.QueryRowsMap(db, `show master status`, onRow)
	}
	return err
}

// getRowMapString reads the first present/non-empty column among the given (old, new) name candidates.
func getRowMapString(m sqlutils.RowMap, keys ...string) string {
	for _, key := range keys {
		if v := m.GetString(key); v != "" {
			return v
		}
	}
	return ""
}

// getRowMapInt64 reads the first present/non-zero column among the given (old, new) name candidates.
func getRowMapInt64(m sqlutils.RowMap, keys ...string) int64 {
	for _, key := range keys {
		if v := m.GetInt64(key); v != 0 {
			return v
		}
	}
	return 0
}

// GetReplicationLagFromSlaveStatus returns replication lag for a given db; via SHOW REPLICA STATUS / SHOW SLAVE STATUS
func GetReplicationLagFromSlaveStatus(informationSchemaDb *gosql.DB) (replicationLag time.Duration, err error) {
	err = queryReplicaStatus(informationSchemaDb, func(m sqlutils.RowMap) error {
		replicaIORunning := getRowMapString(m, "Replica_IO_Running", "Slave_IO_Running")
		replicaSQLRunning := getRowMapString(m, "Replica_SQL_Running", "Slave_SQL_Running")
		secondsBehindSource := m.GetNullInt64("Seconds_Behind_Source")
		if !secondsBehindSource.Valid {
			secondsBehindSource = m.GetNullInt64("Seconds_Behind_Master")
		}
		if !secondsBehindSource.Valid {
			return fmt.Errorf("replication not running; Replica_IO_Running=%+v, Replica_SQL_Running=%+v", replicaIORunning, replicaSQLRunning)
		}
		replicationLag = time.Duration(secondsBehindSource.Int64) * time.Second
		return nil
	})

	return replicationLag, err
}

func GetMasterKeyFromSlaveStatus(connectionConfig *ConnectionConfig) (masterKey *InstanceKey, err error) {
	currentUri := connectionConfig.GetDBUri("information_schema")
	// This function is only called once, okay to not have a cached connection pool
	db, err := gosql.Open("mysql", currentUri)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	err = queryReplicaStatus(db, func(rowMap sqlutils.RowMap) error {
		// We wish to recognize the case where the topology's master actually has replication configuration.
		// This can happen when a DBA issues a `RESET SLAVE` instead of `RESET SLAVE ALL`.

		// An empty log file indicates this is a master:
		if getRowMapString(rowMap, "Source_Log_File", "Master_Log_File") == "" {
			return nil
		}

		replicaIORunning := getRowMapString(rowMap, "Replica_IO_Running", "Slave_IO_Running")
		replicaSQLRunning := getRowMapString(rowMap, "Replica_SQL_Running", "Slave_SQL_Running")

		if replicaIORunning != "Yes" || replicaSQLRunning != "Yes" {
			return fmt.Errorf("Replication on %+v is broken: Replica_IO_Running: %s, Replica_SQL_Running: %s. Please make sure replication runs before using gh-ost.",
				connectionConfig.Key,
				replicaIORunning,
				replicaSQLRunning,
			)
		}

		masterKey = &InstanceKey{
			Hostname: getRowMapString(rowMap, "Source_Host", "Master_Host"),
			Port:     int(getRowMapInt64(rowMap, "Source_Port", "Master_Port")),
		}
		return nil
	})

	return masterKey, err
}

func GetMasterConnectionConfigSafe(connectionConfig *ConnectionConfig, visitedKeys *InstanceKeyMap, allowMasterMaster bool) (masterConfig *ConnectionConfig, err error) {
	log.Debugf("Looking for master on %+v", connectionConfig.Key)

	masterKey, err := GetMasterKeyFromSlaveStatus(connectionConfig)
	if err != nil {
		return nil, err
	}
	if masterKey == nil {
		return connectionConfig, nil
	}
	if !masterKey.IsValid() {
		return connectionConfig, nil
	}
	masterConfig = connectionConfig.Duplicate()
	masterConfig.Key = *masterKey

	log.Debugf("Master of %+v is %+v", connectionConfig.Key, masterConfig.Key)
	if visitedKeys.HasKey(masterConfig.Key) {
		if allowMasterMaster {
			return connectionConfig, nil
		}
		return nil, fmt.Errorf("There seems to be a master-master setup at %+v. This is unsupported. Bailing out", masterConfig.Key)
	}
	visitedKeys.AddKey(masterConfig.Key)
	return GetMasterConnectionConfigSafe(masterConfig, visitedKeys, allowMasterMaster)
}

func GetReplicationBinlogCoordinates(db *gosql.DB) (readBinlogCoordinates *BinlogCoordinates, executeBinlogCoordinates *BinlogCoordinates, err error) {
	err = queryReplicaStatus(db, func(m sqlutils.RowMap) error {
		readBinlogCoordinates = &BinlogCoordinates{
			LogFile: getRowMapString(m, "Source_Log_File", "Master_Log_File"),
			LogPos:  getRowMapInt64(m, "Read_Source_Log_Pos", "Read_Master_Log_Pos"),
		}
		executeBinlogCoordinates = &BinlogCoordinates{
			LogFile: getRowMapString(m, "Relay_Source_Log_File", "Relay_Master_Log_File"),
			LogPos:  getRowMapInt64(m, "Exec_Source_Log_Pos", "Exec_Master_Log_Pos"),
		}
		return nil
	})
	return readBinlogCoordinates, executeBinlogCoordinates, err
}

func GetSelfBinlogCoordinates(db *gosql.DB) (selfBinlogCoordinates *BinlogCoordinates, err error) {
	err = queryBinaryLogStatus(db, func(m sqlutils.RowMap) error {
		selfBinlogCoordinates = &BinlogCoordinates{
			LogFile: m.GetString("File"),
			LogPos:  m.GetInt64("Position"),
		}
		return nil
	})
	return selfBinlogCoordinates, err
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
		err := fmt.Errorf("Found 0 columns on %s.%s. Bailing out",
			sql.EscapeName(databaseName),
			sql.EscapeName(tableName),
		)
		log.Error(err)
		return nil, nil, err
	}
	return sql.NewColumnList(columnNames), sql.NewColumnList(virtualColumnNames), nil
}
