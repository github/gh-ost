/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import (
	gosql "database/sql"
	"fmt"
	"time"

	"github.com/github/gh-ost/go/sql"

	"github.com/outbrain/golib/log"
	"github.com/outbrain/golib/sqlutils"
)

type ReplicationLagResult struct {
	Key InstanceKey
	Lag time.Duration
	Err error
}

// GetReplicationLag returns replication lag for a given connection config; either by explicit query
// or via SHOW SLAVE STATUS
func GetReplicationLag(connectionConfig *ConnectionConfig, replicationLagQuery string) (replicationLag time.Duration, err error) {
	dbUri := connectionConfig.GetDBUri("information_schema")
	var db *gosql.DB
	if db, _, err = sqlutils.GetDB(dbUri); err != nil {
		return replicationLag, err
	}

	if replicationLagQuery != "" {
		var floatLag float64
		err = db.QueryRow(replicationLagQuery).Scan(&floatLag)
		return time.Duration(int64(floatLag*1000)) * time.Millisecond, err
	}
	// No explicit replication lag query.
	err = sqlutils.QueryRowsMap(db, `show slave status`, func(m sqlutils.RowMap) error {
		secondsBehindMaster := m.GetNullInt64("Seconds_Behind_Master")
		if !secondsBehindMaster.Valid {
			return fmt.Errorf("replication not running")
		}
		replicationLag = time.Duration(secondsBehindMaster.Int64) * time.Second
		return nil
	})
	return replicationLag, err
}

// GetMaxReplicationLag concurrently checks for replication lag on given list of instance keys,
// each via GetReplicationLag
func GetMaxReplicationLag(baseConnectionConfig *ConnectionConfig, instanceKeyMap *InstanceKeyMap, replicationLagQuery string) (result *ReplicationLagResult) {
	result = &ReplicationLagResult{Lag: 0}
	if instanceKeyMap.Len() == 0 {
		return result
	}
	lagResults := make(chan *ReplicationLagResult, instanceKeyMap.Len())
	for key := range *instanceKeyMap {
		connectionConfig := baseConnectionConfig.Duplicate()
		connectionConfig.Key = key
		result := &ReplicationLagResult{Key: connectionConfig.Key}
		go func() {
			result.Lag, result.Err = GetReplicationLag(connectionConfig, replicationLagQuery)
			lagResults <- result
		}()
	}
	for range *instanceKeyMap {
		lagResult := <-lagResults
		if lagResult.Err != nil {
			result = lagResult
		} else if lagResult.Lag.Nanoseconds() > result.Lag.Nanoseconds() {
			result = lagResult
		}
	}
	return result
}

func GetMasterKeyFromSlaveStatus(connectionConfig *ConnectionConfig) (masterKey *InstanceKey, err error) {
	currentUri := connectionConfig.GetDBUri("information_schema")
	db, _, err := sqlutils.GetDB(currentUri)
	if err != nil {
		return nil, err
	}
	err = sqlutils.QueryRowsMap(db, `show slave status`, func(rowMap sqlutils.RowMap) error {
		// We wish to recognize the case where the topology's master actually has replication configuration.
		// This can happen when a DBA issues a `RESET SLAVE` instead of `RESET SLAVE ALL`.

		// An empty log file indicates this is a master:
		if rowMap.GetString("Master_Log_File") == "" {
			return nil
		}

		slaveIORunning := rowMap.GetString("Slave_IO_Running")
		slaveSQLRunning := rowMap.GetString("Slave_SQL_Running")

		//
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
func GetTableColumns(db *gosql.DB, databaseName, tableName string) (*sql.ColumnList, error) {
	query := fmt.Sprintf(`
		show columns from %s.%s
		`,
		sql.EscapeName(databaseName),
		sql.EscapeName(tableName),
	)
	columnNames := []string{}
	err := sqlutils.QueryRowsMap(db, query, func(rowMap sqlutils.RowMap) error {
		columnNames = append(columnNames, rowMap.GetString("Field"))
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(columnNames) == 0 {
		return nil, log.Errorf("Found 0 columns on %s.%s. Bailing out",
			sql.EscapeName(databaseName),
			sql.EscapeName(tableName),
		)
	}
	return sql.NewColumnList(columnNames), nil
}
