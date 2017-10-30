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

	"github.com/outbrain/golib/sqlutils"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
)

const MaxTableNameLength = 64

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

// GetReplicationLag returns replication lag for a given connection config; either by explicit query
// or via SHOW SLAVE STATUS
func GetReplicationLag(connectionConfig *ConnectionConfig) (replicationLag time.Duration, err error) {
	dbUri := connectionConfig.GetDBUri("information_schema")
	var db *gosql.DB
	if db, _, err = sqlutils.GetDB(dbUri); err != nil {
		return replicationLag, err
	}

	err = sqlutils.QueryRowsMap(db, `show slave status`, func(m sqlutils.RowMap) error {
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

// 得到master key
func GetMasterKeyFromSlaveStatus(connectionConfig *ConnectionConfig) (masterKey *InstanceKey, err error) {
	// 这个DB有什么特别的意义?
	currentUri := connectionConfig.GetDBUri("information_schema")
	db, _, err := sqlutils.GetDB(currentUri)
	if err != nil {
		log.ErrorErrorf(err, "Get db failed")
		return nil, err
	} else {
		log.Printf("current uri: %s", currentUri)
	}
	// 执行show slave status?
	err = sqlutils.QueryRowsMap(db, `show slave status`, func(rowMap sqlutils.RowMap) error {
		// We wish to recognize the case where the topology's master actually has replication configuration.
		// This can happen when a DBA issues a `RESET SLAVE` instead of `RESET SLAVE ALL`.

		// An empty log file indicates this is a master:
		if rowMap.GetString("Master_Log_File") == "" {
			return nil
		}

		// 如果存在Master信息， 且Slave正常工作
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

		// 得到master key
		masterKey = &InstanceKey{
			Hostname: rowMap.GetString("Master_Host"),
			Port:     rowMap.GetInt("Master_Port"),
		}
		return nil
	})

	return masterKey, err
}

//
//
//   Master
//    |    \
//   Slave Slave
//  可能是一个Tree, 直接从Slave反向到Master
//
func GetMasterConnectionConfigSafe(connectionConfig *ConnectionConfig, visitedKeys *InstanceKeyMap, allowMasterMaster bool) (masterConfig *ConnectionConfig, err error) {
	log.Debugf("Looking for master on %+v", connectionConfig.Key)

	masterKey, err := GetMasterKeyFromSlaveStatus(connectionConfig)
	if err != nil {
		return nil, err
	}

	// 如果找不到master, 则当前的host就是maser
	if masterKey == nil {
		return connectionConfig, nil
	}

	// 找到的master无效?
	if !masterKey.IsValid() {
		return connectionConfig, nil
	}
	masterConfig = connectionConfig.Duplicate()
	masterConfig.Key = *masterKey

	// 找到一个Master, 然后呢?
	log.Debugf("Master of %+v is %+v", connectionConfig.Key, masterConfig.Key)
	if visitedKeys.HasKey(masterConfig.Key) {
		if allowMasterMaster {
			// 如果碰到: master <--> master 就直接返回, 不处理 master <--> master整体作为一个slave的情况
			return connectionConfig, nil
		}
		return nil, fmt.Errorf("There seems to be a master-master setup at %+v. This is unsupported. Bailing out", masterConfig.Key)
	}
	visitedKeys.AddKey(masterConfig.Key)
	return GetMasterConnectionConfigSafe(masterConfig, visitedKeys, allowMasterMaster)
}

// 获取当前的slave的复制情况?
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
	// 获取全局的信息
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
		return nil, fmt.Errorf("Found 0 columns on %s.%s. Bailing out",
			sql.EscapeName(databaseName),
			sql.EscapeName(tableName),
		)
	}
	return sql.NewColumnList(columnNames), nil
}
