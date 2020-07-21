/*
   Copyright 2016 GitHub Inc.
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
	"github.com/outbrain/golib/log"
)

var (
	prettifyDurationRegexp = regexp.MustCompile("([.][0-9]+)")
)

func PrettifyDurationOutput(d time.Duration) string {
	if d < time.Second {
		return "0s"
	}
	result := fmt.Sprintf("%s", d)
	result = prettifyDurationRegexp.ReplaceAllString(result, "")
	return result
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

func ValidateConnection(db *gosql.DB, connectionConfig *mysql.ConnectionConfig, migrationContext *MigrationContext) (string, error) {
	versionQuery := `select @@global.version`
	var port, extraPort int
	var version string
	if err := db.QueryRow(versionQuery).Scan(&version); err != nil {
		return "", err
	}
	extraPortQuery := `select @@global.extra_port`
	if err := db.QueryRow(extraPortQuery).Scan(&extraPort); err != nil {
		// swallow this error. not all servers support extra_port
	}
	// AliyunRDS set users port to "NULL", replace it by gh-ost param
	// GCP set users port to "NULL", replace it by gh-ost param
	if migrationContext.AliyunRDS || migrationContext.GoogleCloudPlatformV1 {
		port = connectionConfig.Key.Port
	} else {
		portQuery := `select @@global.port`
		if err := db.QueryRow(portQuery).Scan(&port); err != nil {
			return "", err
		}
	}

	if connectionConfig.Key.Port == port || (extraPort > 0 && connectionConfig.Key.Port == extraPort) {
		log.Infof("connection validated on %+v", connectionConfig.Key)
		return version, nil
	} else if extraPort == 0 {
		return "", fmt.Errorf("Unexpected database port reported: %+v", port)
	} else {
		return "", fmt.Errorf("Unexpected database port reported: %+v / extra_port: %+v", port, extraPort)
	}
}
