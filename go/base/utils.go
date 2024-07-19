/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package base

import (
	"errors"
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

func ValidateConnection(db *gosql.DB, connectionConfig *mysql.ConnectionConfig, migrationContext *MigrationContext, name string) (string, error) {
	if err := validateOceanBaseConnection(db, migrationContext); err != nil {
		return "", err
	}

	versionQuery := `select @@global.version`

	var version string
	if err := db.QueryRow(versionQuery).Scan(&version); err != nil {
		return "", err
	}

	if migrationContext.SkipPortValidation {
		return version, nil
	}

	var extraPort int

	extraPortQuery := `select @@global.extra_port`
	if err := db.QueryRow(extraPortQuery).Scan(&extraPort); err != nil { //nolint:staticcheck
		// swallow this error. not all servers support extra_port
	}

	// AliyunRDS set users port to "NULL", replace it by gh-ost param
	// GCP set users port to "NULL", replace it by gh-ost param
	// Azure MySQL set users port to a different value by design, replace it by gh-ost para
	var port int
	if migrationContext.AliyunRDS || migrationContext.GoogleCloudPlatform || migrationContext.AzureMySQL || migrationContext.OceanBase {
		port = connectionConfig.Key.Port
	} else {
		portQuery := `select @@global.port`
		if err := db.QueryRow(portQuery).Scan(&port); err != nil {
			return "", err
		}
	}

	if connectionConfig.Key.Port == port || (extraPort > 0 && connectionConfig.Key.Port == extraPort) {
		migrationContext.Log.Infof("%s connection validated on %+v", name, connectionConfig.Key)
		return version, nil
	} else if extraPort == 0 {
		return "", fmt.Errorf("Unexpected database port reported: %+v", port)
	} else {
		return "", fmt.Errorf("Unexpected database port reported: %+v / extra_port: %+v", port, extraPort)
	}
}

func validateOceanBaseConnection(db *gosql.DB, migrationContext *MigrationContext) error {
	versionCommentQuery := `select @@global.version_comment`
	var versionComment string
	if err := db.QueryRow(versionCommentQuery).Scan(&versionComment); err != nil {
		return nil
	}
	if !strings.Contains(versionComment, "OceanBase") {
		return nil
	}

	migrationContext.Log.Infof("OceanBase connection identified, version_comment: %v", versionComment)
	migrationContext.OceanBase = true

	enableLockPriorityQuery := `select value from oceanbase.GV$OB_PARAMETERS where name='enable_lock_priority'`
	var enableLockPriority bool
	if err := db.QueryRow(enableLockPriorityQuery).Scan(&enableLockPriority); err != nil {
		return err
	}
	if !enableLockPriority {
		return errors.New("system parameter 'enable_lock_priority' should be true to support cut-over")
	}
	return nil
}
