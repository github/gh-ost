package base

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/fatih/color"
	"github.com/juju/errors"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
)

type DatabaseConfig struct {
	Databases []string `toml:"dbs"`
}

func NewConfigWithFile(name string) (*DatabaseConfig, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return NewConfig(string(data))
}

func NewConfig(data string) (*DatabaseConfig, error) {
	var c DatabaseConfig
	_, err := toml.Decode(data, &c)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &c, nil
}

func (c *DatabaseConfig) GetDB(alias string) (dbName string, hostname string, port int) {
	for _, db := range c.Databases {
		// db格式:
		// alias:db@host@port
		if strings.HasPrefix(db, "#") {
			continue
		}
		fields := strings.Split(db, ":")
		if len(fields) != 2 {
			fmt.Printf(color.RedString("Invalid db config found: %s\n"), db)
			continue
		} else if fields[0] == alias {
			items := strings.Split(fields[1], "@")
			if len(items) < 2 {
				log.Panic(color.RedString("Invalid db config found: %s\n"), db)
			} else {
				dbName = items[0]
				hostname = items[1]
				if len(items) > 2 {
					p, err := strconv.ParseInt(items[2], 10, 64)
					if err != nil {
						log.Panic(color.RedString("Invalid db config found: %s\n"), db)
					}
					port = int(p)
				} else {
					port = 3306
				}
				return dbName, hostname, port
			}
		} else {
			continue
		}
	}

	log.Panic(color.RedString("No db found for alias: %s\n"), alias)
	return
}
