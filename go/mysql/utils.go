/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-osc/blob/master/LICENSE
*/

package mysql

import (
	gosql "database/sql"
	"fmt"
	"time"

	"github.com/outbrain/golib/log"
	"github.com/outbrain/golib/sqlutils"
)

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
			return fmt.Errorf("Replication not running on %+v", connectionConfig.Key)
		}
		replicationLag = time.Duration(secondsBehindMaster.Int64) * time.Second
		return nil
	})
	return replicationLag, err
}

// GetMaxReplicationLag concurrently checks for replication lag on given list of instance keys,
// each via GetReplicationLag
func GetMaxReplicationLag(baseConnectionConfig *ConnectionConfig, instanceKeyMap *InstanceKeyMap, replicationLagQuery string) (replicationLag time.Duration, err error) {
	if instanceKeyMap.Len() == 0 {
		return 0, nil
	}
	lagsChan := make(chan time.Duration, instanceKeyMap.Len())
	errorsChan := make(chan error, instanceKeyMap.Len())
	for key := range *instanceKeyMap {
		connectionConfig := baseConnectionConfig.Duplicate()
		connectionConfig.Key = key
		go func() {
			lag, err := GetReplicationLag(connectionConfig, replicationLagQuery)
			lagsChan <- lag
			errorsChan <- err
		}()
	}
	for range *instanceKeyMap {
		if lagError := <-errorsChan; lagError != nil {
			err = lagError
		}
		if lag := <-lagsChan; lag.Nanoseconds() > replicationLag.Nanoseconds() {
			replicationLag = lag
		}
	}
	return replicationLag, err
}

func GetMasterKeyFromSlaveStatus(connectionConfig *ConnectionConfig) (masterKey *InstanceKey, err error) {
	currentUri := connectionConfig.GetDBUri("information_schema")
	db, _, err := sqlutils.GetDB(currentUri)
	if err != nil {
		return nil, err
	}
	err = sqlutils.QueryRowsMap(db, `show slave status`, func(rowMap sqlutils.RowMap) error {
		masterKey = &InstanceKey{
			Hostname: rowMap.GetString("Master_Host"),
			Port:     rowMap.GetInt("Master_Port"),
		}
		return nil
	})
	return masterKey, err
}

func GetMasterConnectionConfigSafe(connectionConfig *ConnectionConfig, visitedKeys *InstanceKeyMap) (masterConfig *ConnectionConfig, err error) {
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
		return nil, fmt.Errorf("There seems to be a master-master setup at %+v. This is unsupported. Bailing out", masterConfig.Key)
	}
	visitedKeys.AddKey(masterConfig.Key)
	return GetMasterConnectionConfigSafe(masterConfig, visitedKeys)
}
