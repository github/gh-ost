package mysql

import (
	"fmt"
	"strings"
)

func ValidateFlavor(flavor string) error {
	switch strings.ToLower(flavor) {
	case MySQLFlavor:
		return nil
	case MariaDBFlavor:
		return nil
	default:
		return fmt.Errorf("%s is not a valid flavor", flavor)
	}
}
