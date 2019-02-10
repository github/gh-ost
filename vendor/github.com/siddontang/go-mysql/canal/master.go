package canal

import (
	"sync"

	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/mysql"
)

type masterInfo struct {
	sync.RWMutex

	pos mysql.Position

	gset mysql.GTIDSet

	timestamp uint32
}

func (m *masterInfo) Update(pos mysql.Position) {
	log.Debugf("update master position %s", pos)

	m.Lock()
	m.pos = pos
	m.Unlock()
}

func (m *masterInfo) UpdateTimestamp(ts uint32) {
	log.Debugf("update master timestamp %s", ts)

	m.Lock()
	m.timestamp = ts
	m.Unlock()
}

func (m *masterInfo) UpdateGTIDSet(gset mysql.GTIDSet) {
	log.Debugf("update master gtid set %s", gset)

	m.Lock()
	m.gset = gset
	m.Unlock()
}

func (m *masterInfo) Position() mysql.Position {
	m.RLock()
	defer m.RUnlock()

	return m.pos
}

func (m *masterInfo) Timestamp() uint32 {
	m.RLock()
	defer m.RUnlock()

	return m.timestamp
}

func (m *masterInfo) GTIDSet() mysql.GTIDSet {
	m.RLock()
	defer m.RUnlock()

	if m.gset == nil {
		return nil
	}
	return m.gset.Clone()
}
