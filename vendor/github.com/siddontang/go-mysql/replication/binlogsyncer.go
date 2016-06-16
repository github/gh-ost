package replication

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/satori/go.uuid"
	"github.com/siddontang/go-mysql/client"
	. "github.com/siddontang/go-mysql/mysql"
)

var (
	errSyncRunning   = errors.New("Sync is running, must Close first")
	errNotRegistered = errors.New("Syncer is not registered as a slave")
)

type BinlogSyncer struct {
	m sync.Mutex

	flavor string

	c        *client.Conn
	serverID uint32

	localhost string
	host      string
	port      uint16
	user      string
	password  string

	masterID uint32

	wg sync.WaitGroup

	parser *BinlogParser

	nextPos Position

	running         bool
	semiSyncEnabled bool

	stopCh chan struct{}
}

func NewBinlogSyncer(serverID uint32, flavor string) *BinlogSyncer {
	b := new(BinlogSyncer)
	b.flavor = flavor

	b.serverID = serverID

	b.masterID = 0

	b.parser = NewBinlogParser()

	b.running = false
	b.semiSyncEnabled = false

	b.stopCh = make(chan struct{}, 1)

	return b
}

// LocalHostname returns the hostname that register slave would register as.
func (b *BinlogSyncer) LocalHostname() string {

	if b.localhost == "" {
		h, _ := os.Hostname()
		return h
	}
	return b.localhost
}

// SetLocalHostname set's the hostname that register salve would register as.
func (b *BinlogSyncer) SetLocalHostname(name string) {
	b.localhost = name
}

func (b *BinlogSyncer) Close() {
	b.m.Lock()
	defer b.m.Unlock()

	b.close()
}

func (b *BinlogSyncer) close() {
	if b.c != nil {
		b.c.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	}

	select {
	case b.stopCh <- struct{}{}:
	default:
	}

	b.wg.Wait()

	if b.c != nil {
		b.c.Close()
	}

	b.running = false
	b.c = nil
}

func (b *BinlogSyncer) checkExec() error {
	if b.running {
		return errSyncRunning
	} else if b.c == nil {
		return errNotRegistered
	}

	return nil
}

func (b *BinlogSyncer) GetMasterUUID() (uuid.UUID, error) {
	b.m.Lock()
	defer b.m.Unlock()

	if err := b.checkExec(); err != nil {
		return uuid.UUID{}, err
	}

	if r, err := b.c.Execute("SHOW GLOBAL VARIABLES LIKE 'SERVER_UUID'"); err != nil {
		return uuid.UUID{}, err
	} else {
		s, _ := r.GetString(0, 1)
		if s == "" || s == "NONE" {
			return uuid.UUID{}, nil
		} else {
			return uuid.FromString(s)
		}
	}
}

// You must register slave at first before you do other operations
// This function will close old replication sync if exists
func (b *BinlogSyncer) RegisterSlave(host string, port uint16, user string, password string) error {
	b.m.Lock()
	defer b.m.Unlock()

	// first, close old replication sync
	b.close()

	b.host = host
	b.port = port
	b.user = user
	b.password = password

	err := b.registerSlave()
	if err != nil {
		b.close()
	}
	return errors.Trace(err)
}

// If you close sync before and want to restart again, you can call this before other operations
// This function will close old replication sync if exists
func (b *BinlogSyncer) ReRegisterSlave() error {
	b.m.Lock()
	defer b.m.Unlock()

	if len(b.host) == 0 || len(b.user) == 0 {
		return errors.Errorf("empty host and user, you must register slave before")
	}

	b.close()

	err := b.registerSlave()
	if err != nil {
		b.close()
	}

	return errors.Trace(err)
}

func (b *BinlogSyncer) registerSlave() error {
	var err error
	b.c, err = client.Connect(fmt.Sprintf("%s:%d", b.host, b.port), b.user, b.password, "")
	if err != nil {
		return errors.Trace(err)
	}

	//for mysql 5.6+, binlog has a crc32 checksum
	//before mysql 5.6, this will not work, don't matter.:-)
	if r, err := b.c.Execute("SHOW GLOBAL VARIABLES LIKE 'BINLOG_CHECKSUM'"); err != nil {
		return errors.Trace(err)
	} else {
		s, _ := r.GetString(0, 1)
		if s != "" {
			// maybe CRC32 or NONE

			// mysqlbinlog.cc use NONE, see its below comments:
			// Make a notice to the server that this client
			// is checksum-aware. It does not need the first fake Rotate
			// necessary checksummed.
			// That preference is specified below.

			if _, err = b.c.Execute(`SET @master_binlog_checksum='NONE'`); err != nil {
				return errors.Trace(err)
			}

			// if _, err = b.c.Execute(`SET @master_binlog_checksum=@@global.binlog_checksum`); err != nil {
			// 	return errors.Trace(err)
			// }

		}
	}

	if err = b.writeRegisterSlaveCommand(); err != nil {
		return errors.Trace(err)
	}

	if _, err = b.c.ReadOKPacket(); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (b *BinlogSyncer) EnableSemiSync() error {
	b.m.Lock()
	defer b.m.Unlock()

	if err := b.checkExec(); err != nil {
		return errors.Trace(err)
	}

	if r, err := b.c.Execute("SHOW VARIABLES LIKE 'rpl_semi_sync_master_enabled';"); err != nil {
		return errors.Trace(err)
	} else {
		s, _ := r.GetString(0, 1)
		if s != "ON" {
			return errors.Errorf("master does not support semi synchronous replication")
		}
	}

	_, err := b.c.Execute(`SET @rpl_semi_sync_slave = 1;`)
	if err != nil {
		b.semiSyncEnabled = true
	}
	return errors.Trace(err)
}

func (b *BinlogSyncer) startDumpStream() *BinlogStreamer {
	b.running = true
	b.stopCh = make(chan struct{}, 1)

	s := newBinlogStreamer()

	b.wg.Add(1)
	go b.onStream(s)
	return s
}

func (b *BinlogSyncer) StartSync(pos Position) (*BinlogStreamer, error) {
	b.m.Lock()
	defer b.m.Unlock()

	if err := b.checkExec(); err != nil {
		return nil, err
	}

	//always start from position 4
	if pos.Pos < 4 {
		pos.Pos = 4
	}

	err := b.writeBinglogDumpCommand(pos)
	if err != nil {
		return nil, err
	}

	return b.startDumpStream(), nil
}

func (b *BinlogSyncer) SetRawMode(mode bool) error {
	b.m.Lock()
	defer b.m.Unlock()

	if err := b.checkExec(); err != nil {
		return errors.Trace(err)
	}

	b.parser.SetRawMode(mode)
	return nil
}

func (b *BinlogSyncer) ExecuteSql(query string, args ...interface{}) (*Result, error) {
	b.m.Lock()
	defer b.m.Unlock()

	if err := b.checkExec(); err != nil {
		return nil, err
	}

	return b.c.Execute(query, args...)
}

func (b *BinlogSyncer) StartSyncGTID(gset GTIDSet) (*BinlogStreamer, error) {
	b.m.Lock()
	defer b.m.Unlock()

	if err := b.checkExec(); err != nil {
		return nil, err
	}

	var err error
	switch b.flavor {
	case MySQLFlavor:
		err = b.writeBinlogDumpMysqlGTIDCommand(gset)
	case MariaDBFlavor:
		err = b.writeBinlogDumpMariadbGTIDCommand(gset)
	default:
		err = fmt.Errorf("invalid flavor %s", b.flavor)
	}

	if err != nil {
		return nil, err
	}

	return b.startDumpStream(), nil
}

func (b *BinlogSyncer) writeBinglogDumpCommand(p Position) error {
	b.c.ResetSequence()

	data := make([]byte, 4+1+4+2+4+len(p.Name))

	pos := 4
	data[pos] = COM_BINLOG_DUMP
	pos++

	binary.LittleEndian.PutUint32(data[pos:], p.Pos)
	pos += 4

	binary.LittleEndian.PutUint16(data[pos:], BINLOG_DUMP_NEVER_STOP)
	pos += 2

	binary.LittleEndian.PutUint32(data[pos:], b.serverID)
	pos += 4

	copy(data[pos:], p.Name)

	return b.c.WritePacket(data)
}

func (b *BinlogSyncer) writeBinlogDumpMysqlGTIDCommand(gset GTIDSet) error {
	p := Position{"", 4}
	gtidData := gset.Encode()

	b.c.ResetSequence()

	data := make([]byte, 4+1+2+4+4+len(p.Name)+8+4+len(gtidData))
	pos := 4
	data[pos] = COM_BINLOG_DUMP_GTID
	pos++

	binary.LittleEndian.PutUint16(data[pos:], 0)
	pos += 2

	binary.LittleEndian.PutUint32(data[pos:], b.serverID)
	pos += 4

	binary.LittleEndian.PutUint32(data[pos:], uint32(len(p.Name)))
	pos += 4

	n := copy(data[pos:], p.Name)
	pos += n

	binary.LittleEndian.PutUint64(data[pos:], uint64(p.Pos))
	pos += 8

	binary.LittleEndian.PutUint32(data[pos:], uint32(len(gtidData)))
	pos += 4
	n = copy(data[pos:], gtidData)
	pos += n

	data = data[0:pos]

	return b.c.WritePacket(data)
}

func (b *BinlogSyncer) writeBinlogDumpMariadbGTIDCommand(gset GTIDSet) error {
	// Copy from vitess

	startPos := gset.String()

	// Tell the server that we understand GTIDs by setting our slave capability
	// to MARIA_SLAVE_CAPABILITY_GTID = 4 (MariaDB >= 10.0.1).
	if _, err := b.c.Execute("SET @mariadb_slave_capability=4"); err != nil {
		return errors.Errorf("failed to set @mariadb_slave_capability=4: %v", err)
	}

	// Set the slave_connect_state variable before issuing COM_BINLOG_DUMP to
	// provide the start position in GTID form.
	query := fmt.Sprintf("SET @slave_connect_state='%s'", startPos)

	if _, err := b.c.Execute(query); err != nil {
		return errors.Errorf("failed to set @slave_connect_state='%s': %v", startPos, err)
	}

	// Real slaves set this upon connecting if their gtid_strict_mode option was
	// enabled. We always use gtid_strict_mode because we need it to make our
	// internal GTID comparisons safe.
	if _, err := b.c.Execute("SET @slave_gtid_strict_mode=1"); err != nil {
		return errors.Errorf("failed to set @slave_gtid_strict_mode=1: %v", err)
	}

	// Since we use @slave_connect_state, the file and position here are ignored.
	return b.writeBinglogDumpCommand(Position{"", 0})
}

func (b *BinlogSyncer) writeRegisterSlaveCommand() error {
	b.c.ResetSequence()

	hostname := b.LocalHostname()

	// This should be the name of slave host not the host we are connecting to.
	data := make([]byte, 4+1+4+1+len(hostname)+1+len(b.user)+1+len(b.password)+2+4+4)
	pos := 4

	data[pos] = COM_REGISTER_SLAVE
	pos++

	binary.LittleEndian.PutUint32(data[pos:], b.serverID)
	pos += 4

	// This should be the name of slave hostname not the host we are connecting to.
	data[pos] = uint8(len(hostname))
	pos++
	n := copy(data[pos:], hostname)
	pos += n

	data[pos] = uint8(len(b.user))
	pos++
	n = copy(data[pos:], b.user)
	pos += n

	data[pos] = uint8(len(b.password))
	pos++
	n = copy(data[pos:], b.password)
	pos += n

	binary.LittleEndian.PutUint16(data[pos:], b.port)
	pos += 2

	//replication rank, not used
	binary.LittleEndian.PutUint32(data[pos:], 0)
	pos += 4

	binary.LittleEndian.PutUint32(data[pos:], b.masterID)

	return b.c.WritePacket(data)
}

func (b *BinlogSyncer) replySemiSyncACK(p Position) error {
	b.c.ResetSequence()

	data := make([]byte, 4+1+8+len(p.Name))
	pos := 4
	// semi sync indicator
	data[pos] = SemiSyncIndicator
	pos++

	binary.LittleEndian.PutUint64(data[pos:], uint64(p.Pos))
	pos += 8

	copy(data[pos:], p.Name)

	err := b.c.WritePacket(data)
	if err != nil {
		return errors.Trace(err)
	}

	_, err = b.c.ReadOKPacket()
	if err != nil {
	}
	return errors.Trace(err)
}

func (b *BinlogSyncer) onStream(s *BinlogStreamer) {
	defer func() {
		if e := recover(); e != nil {
			s.closeWithError(fmt.Errorf("Err: %v\n Stack: %s", e, Pstack()))
		}
		b.wg.Done()
	}()

	for {
		data, err := b.c.ReadPacket()
		if err != nil {
			s.closeWithError(err)
			return
		}

		switch data[0] {
		case OK_HEADER:
			if err = b.parseEvent(s, data); err != nil {
				s.closeWithError(err)
				return
			}
		case ERR_HEADER:
			err = b.c.HandleErrorPacket(data)
			s.closeWithError(err)
			return
		default:
			s.closeWithError(fmt.Errorf("invalid stream header %c", data[0]))
			return
		}
	}
}

func (b *BinlogSyncer) parseEvent(s *BinlogStreamer, data []byte) error {
	//skip OK byte, 0x00
	data = data[1:]

	needACK := false
	if b.semiSyncEnabled && (data[0] == SemiSyncIndicator) {
		needACK = (data[1] == 0x01)
		//skip semi sync header
		data = data[2:]
	}

	e, err := b.parser.parse(data)
	if err != nil {
		return errors.Trace(err)
	}

	b.nextPos.Pos = e.Header.LogPos

	if re, ok := e.Event.(*RotateEvent); ok {
		b.nextPos.Name = string(re.NextLogName)
		b.nextPos.Pos = uint32(re.Position)
	}

	needStop := false
	select {
	case s.ch <- e:
	case <-b.stopCh:
		needStop = true
	}

	if needACK {
		err := b.replySemiSyncACK(b.nextPos)
		if err != nil {
			return errors.Trace(err)
		}
	}

	if needStop {
		return errors.New("sync is been closing...")
	}

	return nil
}
