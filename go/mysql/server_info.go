/*
   Copyright 2023 GitHub Inc.
         See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import gosql "database/sql"

// ServerInfo represents the online config of a MySQL server.
type ServerInfo struct {
	Version         string
	VersionComment  string
	Hostname        string
	Port            gosql.NullInt64
	BinlogFormat    string
	BinlogRowImage  string
	LogBin          bool
	LogSlaveUpdates bool
	SQLMode         string
	TimeZone        string

	// @@global.extra_port is Percona/MariaDB-only
	ExtraPort gosql.NullInt64
}

// GetServerInfo returns a ServerInfo struct representing
// the online config of a MySQL server.
func GetServerInfo(db *gosql.DB) (*ServerInfo, error) {
	var info ServerInfo
	query := `select /* gh-ost */ @@global.version, @@global.version_comment, @@global.hostname,
		@@global.port, @@global.binlog_format, @@global.binlog_row_image, @@global.log_bin,
		@@global.log_slave_updates, @@global.sql_mode, @@global.time_zone`
	if err := db.QueryRow(query).Scan(&info.Version, &info.VersionComment, &info.Hostname,
		&info.Port, &info.BinlogFormat, &info.BinlogRowImage, &info.LogBin,
		&info.LogSlaveUpdates, &info.SQLMode, &info.TimeZone,
	); err != nil {
		return nil, err
	}

	extraPortQuery := `select @@global.extra_port`
	// swallow possible error. not all servers support extra_port
	_ = db.QueryRow(extraPortQuery).Scan(&info.ExtraPort)

	return &info, nil
}
