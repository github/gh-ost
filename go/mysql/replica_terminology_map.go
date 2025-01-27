package mysql

import (
	version "github.com/hashicorp/go-version"
)

const (
	MysqlVersionCutoff = "8.4"
)

var MysqlReplicaTermMap = map[string]string{
	"Seconds_Behind_Master": "Seconds_Behind_Source",
	"Master_Log_File":       "Source_Log_File",
	"Master_Host":           "Source_Host",
	"Master_Port":           "Source_Port",
	"Exec_Master_Log_Pos":   "Exec_Source_Log_Pos",
	"Read_Master_Log_Pos":   "Read_Source_Log_Pos",
	"Relay_Master_Log_File": "Relay_Source_Log_File",
	"Slave_IO_Running":      "Replica_IO_Running",
	"Slave_SQL_Running":     "Replica_SQL_Running",
	"master status":         "binary log status",
	"slave hosts":           "replicas",
	"slave status":          "replica status",
	"slave":                 "replica",
}

func ReplicaTermFor(mysqlVersion string, term string) string {
	vs, err := version.NewVersion(mysqlVersion)
	if err != nil {
		// default to returning the same term if we cannot determine the version
		return term
	}

	mysqlVersionCutoff, _ := version.NewVersion(MysqlVersionCutoff)
	if vs.GreaterThanOrEqual(mysqlVersionCutoff) {
		return MysqlReplicaTermMap[term]
	}
	return term
}
