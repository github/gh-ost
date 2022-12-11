/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package base

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	gosql "database/sql"

	"github.com/github/gh-ost/go/mysql"
)

var (
	prettifyDurationRegexp = regexp.MustCompile("([.][0-9]+)")
)

func PrettifyDurationOutput(d time.Duration) string {
	if d < time.Second {
		return "0s"
	}
	return prettifyDurationRegexp.ReplaceAllString(d.String(), "")
}

func FileExists(fileName string) bool {
	if _, err := os.Stat(fileName); err == nil {
		return true
	}
	return false
}

func TouchFile(fileName string) error {
	f, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	return f.Close()
}

// StringContainsAll returns true if `s` contains all non empty given `substrings`
// The function returns `false` if no non-empty arguments are given.
func StringContainsAll(s string, substrings ...string) bool {
	nonEmptyStringsFound := false
	for _, substring := range substrings {
		if substring == "" {
			continue
		}
		if strings.Contains(s, substring) {
			nonEmptyStringsFound = true
		} else {
			// Immediate failure
			return false
		}
	}
	return nonEmptyStringsFound
}

func ValidateConnection(db *gosql.DB, connectionConfig *mysql.ConnectionConfig, migrationContext *MigrationContext, name string) (version string, err error) {
	query := `select @@global.version, @@global.port`
	var port gosql.NullInt64
	if err = db.QueryRow(query).Scan(&version, &port); err != nil {
		return "", err
	}

	extraPortQuery := `select @@global.extra_port`
	var extraPort int64
	// swallow possible error. not all servers support extra_port
	_ = db.QueryRow(extraPortQuery).Scan(&extraPort)

	// AliyunRDS set users port to "NULL", replace it by gh-ost param
	// GCP set users port to "NULL", replace it by gh-ost param
	// Azure MySQL set users port to a different value by design, replace it by gh-ost param
	if migrationContext.AliyunRDS || migrationContext.GoogleCloudPlatform || migrationContext.AzureMySQL {
		port.Int64 = connectionConfig.Key.Port
		port.Valid = connectionConfig.Key.Port > 0
	}

	if !port.Valid || extraPort == 0 {
		return "", fmt.Errorf("Unexpected database port reported: %+v", port.Int64)
	} else if connectionConfig.Key.Port != port.Int64 && (extraPort == 0 || connectionConfig.Key.Port != extraPort) {
		return "", fmt.Errorf("Unexpected database port reported: %+v / extra_port: %+v", port.Int64, extraPort)
	}

	migrationContext.Log.Infof("%s connection validated on %+v", name, connectionConfig.Key)
	return version, err
}
