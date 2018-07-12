package base

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/fatih/color"
	"github.com/juju/errors"
	"github.com/outbrain/golib/log"
	"io/ioutil"
	"strconv"
	"strings"
)

type DatabaseConfig struct {
	Databases    []string `toml:"dbs"`
	MaxLoad      string   `toml:"max_load"`
	CriticalLoad string   `toml:"critical_load"`
	ChunkSize    int64    `toml:"chunk_size"`

	InitiallyDropOldTable   bool `toml:"initially_drop_old_table"`
	InitiallyDropGhosTable  bool `toml:"initially_drop_ghost_table"`
	InitiallyDropSocketFile bool `toml:"initially_drop_socket_file"`

	User       string `toml:"user"`
	Password   string `toml:"password"`
	IsRdsMySQL bool   `toml:"is_rds_mysql"`
	// RDS中:
	// 1. 不能调用start slave/stop slave, 也不能通过 CALL mysql.rds_start_replication 之类函数
	AssumeRbr bool `toml:"assume_rbr"`

	// 在rds中通过 show slave status, 看到的是内网IP, 这个IP可能和我们的服务器不再同一个网段内，因此不能使用默认的方法
	// 而是直接指定slave对应的master
	//Master_Host: 10.3.0.49
	//Master_User: rdsrepladmin
	//Master_Port: 3306
	SlaveMasterMapping [][]string `toml:"slave_master_mapping"`
	Slave2Master       map[string]string

	// Throttle控制
	MaxLagMillis int64 `toml:"max_lag_millis"`
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

	c.Slave2Master = make(map[string]string)
	for _, mapping := range c.SlaveMasterMapping {
		c.Slave2Master[mapping[0]] = mapping[1]
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
				log.Fatalf(color.RedString("Invalid db config found: %s\n"), db)
			} else {
				dbName = items[0]
				hostname = items[1]
				if len(items) > 2 {
					p, err := strconv.ParseInt(items[2], 10, 64)
					if err != nil {
						log.Fatalf(color.RedString("Invalid db config found: %s\n"), db)
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

	log.Fatalf(color.RedString("No db found for alias: %s\n"), alias)
	return
}
