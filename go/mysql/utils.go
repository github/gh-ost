/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import (
	gosql "database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/github/gh-ost/go/sql"

	"github.com/outbrain/golib/log"
	"github.com/outbrain/golib/sqlutils"
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

// GetReplicationLagFromSlaveStatus returns replication lag for a given db; via SHOW SLAVE STATUS
func GetReplicationLagFromSlaveStatus(informationSchemaDb *gosql.DB) (replicationLag time.Duration, err error) {
	err = sqlutils.QueryRowsMap(informationSchemaDb, `show slave status`, func(m sqlutils.RowMap) error {
		slaveIORunning := m.GetString("Slave_IO_Running")
		slaveSQLRunning := m.GetString("Slave_SQL_Running")
		secondsBehindMaster := m.GetNullInt64("Seconds_Behind_Master")
		if !secondsBehindMaster.Valid {
			return fmt.Errorf("replication not running; Slave_IO_Running=%+v, Slave_SQL_Running=%+v", slaveIORunning, slaveSQLRunning)
		}
		replicationLag = time.Duration(secondsBehindMaster.Int64) * time.Second
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

	err = sqlutils.QueryRowsMap(db, `show slave status`, func(rowMap sqlutils.RowMap) error {
		// We wish to recognize the case where the topology's master actually has replication configuration.
		// This can happen when a DBA issues a `RESET SLAVE` instead of `RESET SLAVE ALL`.

		// An empty log file indicates this is a master:
		if rowMap.GetString("Master_Log_File") == "" {
			return nil
		}

		slaveIORunning := rowMap.GetString("Slave_IO_Running")
		slaveSQLRunning := rowMap.GetString("Slave_SQL_Running")

		if slaveIORunning != "Yes" || slaveSQLRunning != "Yes" {
			return fmt.Errorf("Replication on %+v is broken: Slave_IO_Running: %s, Slave_SQL_Running: %s. Please make sure replication runs before using gh-ost.",
				connectionConfig.Key,
				slaveIORunning,
				slaveSQLRunning,
			)
		}

		masterKey = &InstanceKey{
			Hostname: rowMap.GetString("Master_Host"),
			Port:     rowMap.GetInt("Master_Port"),
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
	err = sqlutils.QueryRowsMap(db, `show slave status`, func(m sqlutils.RowMap) error {
		readBinlogCoordinates = &BinlogCoordinates{
			LogFile: m.GetString("Master_Log_File"),
			LogPos:  m.GetInt64("Read_Master_Log_Pos"),
		}
		executeBinlogCoordinates = &BinlogCoordinates{
			LogFile: m.GetString("Relay_Master_Log_File"),
			LogPos:  m.GetInt64("Exec_Master_Log_Pos"),
		}
		return nil
	})
	return readBinlogCoordinates, executeBinlogCoordinates, err
}

func GetSelfBinlogCoordinates(db *gosql.DB) (selfBinlogCoordinates *BinlogCoordinates, err error) {
	err = sqlutils.QueryRowsMap(db, `show master status`, func(m sqlutils.RowMap) error {
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
		return nil, nil, log.Errorf("Found 0 columns on %s.%s. Bailing out",
			sql.EscapeName(databaseName),
			sql.EscapeName(tableName),
		)
	}
	return sql.NewColumnList(columnNames), sql.NewColumnList(virtualColumnNames), nil
}
