/*
   Copyright 2023 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package base

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

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

// ValidateConnection confirms the database server info matches the provided connection config.
func ValidateConnection(serverInfo *mysql.ServerInfo, connectionConfig *mysql.ConnectionConfig, migrationContext *MigrationContext, name string) error {
	// AliyunRDS set users port to "NULL", replace it by gh-ost param
	// GCP set users port to "NULL", replace it by gh-ost param
	// Azure MySQL set users port to a different value by design, replace it by gh-ost param
	if migrationContext.AliyunRDS || migrationContext.GoogleCloudPlatform || migrationContext.AzureMySQL {
		serverInfo.Port.Int64 = connectionConfig.Key.Port
		serverInfo.Port.Valid = connectionConfig.Key.Port > 0
	}

	if !serverInfo.Port.Valid && !serverInfo.ExtraPort.Valid {
		return fmt.Errorf("Unexpected database port reported: %+v", serverInfo.Port.Int64)
	} else if connectionConfig.Key.Port != serverInfo.Port.Int64 && connectionConfig.Key.Port != serverInfo.ExtraPort.Int64 {
		return fmt.Errorf("Unexpected database port reported: %+v / extra_port: %+v", serverInfo.Port.Int64, serverInfo.ExtraPort.Int64)
	}

	migrationContext.Log.Infof("%s connection validated on %+v", name, connectionConfig.Key)
	return nil
}
