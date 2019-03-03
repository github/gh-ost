package canal

import (
	"fmt"
	"regexp"
	"time"

	"github.com/juju/errors"
	"github.com/satori/go.uuid"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/schema"
)

var (
	expCreateTable   = regexp.MustCompile("(?i)^CREATE\\sTABLE(\\sIF\\sNOT\\sEXISTS)?\\s`{0,1}(.*?)`{0,1}\\.{0,1}`{0,1}([^`\\.]+?)`{0,1}\\s.*")
	expAlterTable    = regexp.MustCompile("(?i)^ALTER\\sTABLE\\s.*?`{0,1}(.*?)`{0,1}\\.{0,1}`{0,1}([^`\\.]+?)`{0,1}\\s.*")
	expRenameTable   = regexp.MustCompile("(?i)^RENAME\\sTABLE\\s.*?`{0,1}(.*?)`{0,1}\\.{0,1}`{0,1}([^`\\.]+?)`{0,1}\\s{1,}TO\\s.*?")
	expDropTable     = regexp.MustCompile("(?i)^DROP\\sTABLE(\\sIF\\sEXISTS){0,1}\\s`{0,1}(.*?)`{0,1}\\.{0,1}`{0,1}([^`\\.]+?)`{0,1}(?:$|\\s)")
	expTruncateTable = regexp.MustCompile("(?i)^TRUNCATE\\s+(?:TABLE\\s+)?(?:`?([^`\\s]+)`?\\.`?)?([^`\\s]+)`?")
)

func (c *Canal) startSyncer() (*replication.BinlogStreamer, error) {
	gset := c.master.GTIDSet()
	if gset == nil {
		pos := c.master.Position()
		s, err := c.syncer.StartSync(pos)
		if err != nil {
			return nil, errors.Errorf("start sync replication at binlog %v error %v", pos, err)
		}
		log.Infof("start sync binlog at binlog file %v", pos)
		return s, nil
	} else {
		s, err := c.syncer.StartSyncGTID(gset)
		if err != nil {
			return nil, errors.Errorf("start sync replication at GTID set %v error %v", gset, err)
		}
		log.Infof("start sync binlog at GTID set %v", gset)
		return s, nil
	}
}

func (c *Canal) runSyncBinlog() error {
	s, err := c.startSyncer()
	if err != nil {
		return err
	}

	savePos := false
	force := false
	for {
		ev, err := s.GetEvent(c.ctx)

		if err != nil {
			return errors.Trace(err)
		}
		savePos = false
		force = false
		pos := c.master.Position()

		curPos := pos.Pos
		//next binlog pos
		pos.Pos = ev.Header.LogPos

		// We only save position with RotateEvent and XIDEvent.
		// For RowsEvent, we can't save the position until meeting XIDEvent
		// which tells the whole transaction is over.
		// TODO: If we meet any DDL query, we must save too.
		switch e := ev.Event.(type) {
		case *replication.RotateEvent:
			pos.Name = string(e.NextLogName)
			pos.Pos = uint32(e.Position)
			log.Infof("rotate binlog to %s", pos)
			savePos = true
			force = true
			if err = c.eventHandler.OnRotate(e); err != nil {
				return errors.Trace(err)
			}
		case *replication.RowsEvent:
			// we only focus row based event
			err = c.handleRowsEvent(ev)
			if err != nil {
				e := errors.Cause(err)
				// if error is not ErrExcludedTable or ErrTableNotExist or ErrMissingTableMeta, stop canal
				if e != ErrExcludedTable &&
					e != schema.ErrTableNotExist &&
					e != schema.ErrMissingTableMeta {
					log.Errorf("handle rows event at (%s, %d) error %v", pos.Name, curPos, err)
					return errors.Trace(err)
				}
			}
			continue
		case *replication.XIDEvent:
			if e.GSet != nil {
				c.master.UpdateGTIDSet(e.GSet)
			}
			savePos = true
			// try to save the position later
			if err := c.eventHandler.OnXID(pos); err != nil {
				return errors.Trace(err)
			}
		case *replication.MariadbGTIDEvent:
			// try to save the GTID later
			gtid, err := mysql.ParseMariadbGTIDSet(e.GTID.String())
			if err != nil {
				return errors.Trace(err)
			}
			if err := c.eventHandler.OnGTID(gtid); err != nil {
				return errors.Trace(err)
			}
		case *replication.GTIDEvent:
			u, _ := uuid.FromBytes(e.SID)
			gtid, err := mysql.ParseMysqlGTIDSet(fmt.Sprintf("%s:%d", u.String(), e.GNO))
			if err != nil {
				return errors.Trace(err)
			}
			if err := c.eventHandler.OnGTID(gtid); err != nil {
				return errors.Trace(err)
			}
		case *replication.QueryEvent:
			if e.GSet != nil {
				c.master.UpdateGTIDSet(e.GSet)
			}
			var (
				mb    [][]byte
				db    []byte
				table []byte
			)
			regexps := []regexp.Regexp{*expCreateTable, *expAlterTable, *expRenameTable, *expDropTable, *expTruncateTable}
			for _, reg := range regexps {
				mb = reg.FindSubmatch(e.Query)
				if len(mb) != 0 {
					break
				}
			}
			mbLen := len(mb)
			if mbLen == 0 {
				continue
			}

			// the first last is table name, the second last is database name(if exists)
			if len(mb[mbLen-2]) == 0 {
				db = e.Schema
			} else {
				db = mb[mbLen-2]
			}
			table = mb[mbLen-1]

			savePos = true
			force = true
			c.ClearTableCache(db, table)
			log.Infof("table structure changed, clear table cache: %s.%s\n", db, table)
			if err = c.eventHandler.OnTableChanged(string(db), string(table)); err != nil && errors.Cause(err) != schema.ErrTableNotExist {
				return errors.Trace(err)
			}

			// Now we only handle Table Changed DDL, maybe we will support more later.
			if err = c.eventHandler.OnDDL(pos, e); err != nil {
				return errors.Trace(err)
			}
		default:
			continue
		}

		if savePos {
			c.master.Update(pos)
			c.master.UpdateTimestamp(ev.Header.Timestamp)
			if err := c.eventHandler.OnPosSynced(pos, force); err != nil {
				return errors.Trace(err)
			}
		}
	}

	return nil
}

func (c *Canal) handleRowsEvent(e *replication.BinlogEvent) error {
	ev := e.Event.(*replication.RowsEvent)

	// Caveat: table may be altered at runtime.
	schema := string(ev.Table.Schema)
	table := string(ev.Table.Table)

	t, err := c.GetTable(schema, table)
	if err != nil {
		return err
	}
	var action string
	switch e.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		action = InsertAction
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		action = DeleteAction
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		action = UpdateAction
	default:
		return errors.Errorf("%s not supported now", e.Header.EventType)
	}
	events := newRowsEvent(t, action, ev.Rows, e.Header)
	return c.eventHandler.OnRow(events)
}

func (c *Canal) FlushBinlog() error {
	_, err := c.Execute("FLUSH BINARY LOGS")
	return errors.Trace(err)
}

func (c *Canal) WaitUntilPos(pos mysql.Position, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	for {
		select {
		case <-timer.C:
			return errors.Errorf("wait position %v too long > %s", pos, timeout)
		default:
			err := c.FlushBinlog()
			if err != nil {
				return errors.Trace(err)
			}
			curPos := c.master.Position()
			if curPos.Compare(pos) >= 0 {
				return nil
			} else {
				log.Debugf("master pos is %v, wait catching %v", curPos, pos)
				time.Sleep(100 * time.Millisecond)
			}
		}
	}

	return nil
}

func (c *Canal) GetMasterPos() (mysql.Position, error) {
	rr, err := c.Execute("SHOW MASTER STATUS")
	if err != nil {
		return mysql.Position{}, errors.Trace(err)
	}

	name, _ := rr.GetString(0, 0)
	pos, _ := rr.GetInt(0, 1)

	return mysql.Position{Name: name, Pos: uint32(pos)}, nil
}

func (c *Canal) GetMasterGTIDSet() (mysql.GTIDSet, error) {
	query := ""
	switch c.cfg.Flavor {
	case mysql.MariaDBFlavor:
		query = "SELECT @@GLOBAL.gtid_current_pos"
	default:
		query = "SELECT @@GLOBAL.GTID_EXECUTED"
	}
	rr, err := c.Execute(query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	gx, err := rr.GetString(0, 0)
	if err != nil {
		return nil, errors.Trace(err)
	}
	gset, err := mysql.ParseGTIDSet(c.cfg.Flavor, gx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return gset, nil
}

func (c *Canal) CatchMasterPos(timeout time.Duration) error {
	pos, err := c.GetMasterPos()
	if err != nil {
		return errors.Trace(err)
	}

	return c.WaitUntilPos(pos, timeout)
}
