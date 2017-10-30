package base

import (
	"testing"

	"fmt"
	test "github.com/outbrain/golib/tests"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"strings"
)

func init() {
	log.SetLevel(log.LEVEL_ERROR)
}

// go test github.com/github/gh-ost/go/base -v -run "TestServerListParse"
func TestServerListParse(t *testing.T) {

	config, err := NewConfigWithFile("dbs.toml")
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(len(config.Databases), 32)
	fmt.Printf("dbs: %s", strings.Join(config.Databases, ", "))

	db, host, port := config.GetDB("shard29")
	test.S(t).ExpectEquals(db, "shard_sm_29")
	test.S(t).ExpectEquals(host, "shard03-r1.db.test.com")
	test.S(t).ExpectEquals(port, 3306)

	for key, value := range config.SlaveMasterMap {
		fmt.Printf("%s --> %s\n", key, value)
	}
}
