package canal

import (
	"strconv"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/siddontang/go-mysql/dump"
	"github.com/siddontang/go-mysql/schema"
)

type dumpParseHandler struct {
	c    *Canal
	name string
	pos  uint64
}

func (h *dumpParseHandler) BinLog(name string, pos uint64) error {
	h.name = name
	h.pos = pos
	return nil
}

func (h *dumpParseHandler) Data(db string, table string, values []string) error {
	if h.c.isClosed() {
		return errCanalClosed
	}

	tableInfo, err := h.c.GetTable(db, table)
	if err != nil {
		log.Errorf("get %s.%s information err: %v", db, table, err)
		return errors.Trace(err)
	}

	vs := make([]interface{}, len(values))

	for i, v := range values {
		if v == "NULL" {
			vs[i] = nil
		} else if v[0] != '\'' {
			if tableInfo.Columns[i].Type == schema.TYPE_NUMBER {
				n, err := strconv.ParseInt(v, 10, 64)
				if err != nil {
					log.Errorf("parse row %v at %d error %v, skip", values, i, err)
					return dump.ErrSkip
				}
				vs[i] = n
			} else if tableInfo.Columns[i].Type == schema.TYPE_FLOAT {
				f, err := strconv.ParseFloat(v, 64)
				if err != nil {
					log.Errorf("parse row %v at %d error %v, skip", values, i, err)
					return dump.ErrSkip
				}
				vs[i] = f
			} else {
				log.Errorf("parse row %v error, invalid type at %d, skip", values, i)
				return dump.ErrSkip
			}
		} else {
			vs[i] = v[1 : len(v)-1]
		}
	}

	events := newRowsEvent(tableInfo, InsertAction, [][]interface{}{vs})
	return h.c.travelRowsEventHandler(events)
}

func (c *Canal) AddDumpDatabases(dbs ...string) {
	if c.dumper == nil {
		return
	}

	c.dumper.AddDatabases(dbs...)
}

func (c *Canal) AddDumpTables(db string, tables ...string) {
	if c.dumper == nil {
		return
	}

	c.dumper.AddTables(db, tables...)
}

func (c *Canal) AddDumpIgnoreTables(db string, tables ...string) {
	if c.dumper == nil {
		return
	}

	c.dumper.AddIgnoreTables(db, tables...)
}

func (c *Canal) tryDump() error {
	if len(c.master.Name) > 0 && c.master.Position > 0 {
		// we will sync with binlog name and position
		log.Infof("skip dump, use last binlog replication pos (%s, %d)", c.master.Name, c.master.Position)
		return nil
	}

	if c.dumper == nil {
		log.Info("skip dump, no mysqldump")
		return nil
	}

	h := &dumpParseHandler{c: c}

	start := time.Now()
	log.Info("try dump MySQL and parse")
	if err := c.dumper.DumpAndParse(h); err != nil {
		return errors.Trace(err)
	}

	log.Infof("dump MySQL and parse OK, use %0.2f seconds, start binlog replication at (%s, %d)",
		time.Now().Sub(start).Seconds(), h.name, h.pos)

	c.master.Update(h.name, uint32(h.pos))
	c.master.Save(true)

	return nil
}
