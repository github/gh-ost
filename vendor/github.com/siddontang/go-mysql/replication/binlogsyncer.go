package replication

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/siddontang/go-mysql/client"
	. "github.com/siddontang/go-mysql/mysql"
)

var (
	errSyncRunning = errors.New("Sync is running, must Close first")
)

// BinlogSyncerConfig is the configuration for BinlogSyncer.
type BinlogSyncerConfig struct {
	// ServerID is the unique ID in cluster.
	ServerID uint32
	// Flavor is "mysql" or "mariadb", if not set, use "mysql" default.
	Flavor string

	// Host is for MySQL server host.
	Host string
	// Port is for MySQL server port.
	Port uint16
	// User is for MySQL user.
	User string
	// Password is for MySQL password.
	Password string

	// Localhost is local hostname if register salve.
	// If not set, use os.Hostname() instead.
	Localhost string

	// SemiSyncEnabled enables semi-sync or not.
	SemiSyncEnabled bool

	// RawModeEanbled is for not parsing binlog event.
	RawModeEanbled bool
}

// BinlogSyncer syncs binlog event from server.
type BinlogSyncer struct {
	m sync.RWMutex

	cfg *BinlogSyncerConfig

	c *client.Conn

	wg sync.WaitGroup

	parser *BinlogParser

	nextPos Position

	running bool

	ctx    context.Context
	cancel context.CancelFunc
}

// NewBinlogSyncer creates the BinlogSyncer with cfg.
func NewBinlogSyncer(cfg *BinlogSyncerConfig) *BinlogSyncer {
	log.Infof("create BinlogSyncer with config %v", cfg)

	b := new(BinlogSyncer)

	b.cfg = cfg
	b.parser = NewBinlogParser()
	b.parser.SetRawMode(b.cfg.RawModeEanbled)

	b.running = false
	b.ctx, b.cancel = context.WithCancel(context.Background())

	return b
}

// Close closes the BinlogSyncer.
func (b *BinlogSyncer) Close() {
	b.m.Lock()
	defer b.m.Unlock()

	b.close()
}

func (b *BinlogSyncer) close() {
	if b.isClosed() {
		return
	}

	log.Info("syncer is closing...")

	b.running = false
	b.cancel()

	if b.c != nil {
		b.c.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	}

	b.wg.Wait()

	if b.c != nil {
		b.c.Close()
	}

	log.Info("syncer is closed")
}

func (b *BinlogSyncer) isClosed() bool {
	select {
	case <-b.ctx.Done():
		return true
	default:
		return false
	}
}

func (b *BinlogSyncer) registerSlave() error {
	if b.c != nil {
		b.c.Close()
	}

	log.Infof("register slave for master server %s:%d", b.cfg.Host, b.cfg.Port)
	var err error
	b.c, err = client.Connect(fmt.Sprintf("%s:%d", b.cfg.Host, b.cfg.Port), b.cfg.User, b.cfg.Password, "")
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

func (b *BinlogSyncer) enalbeSemiSync() error {
	if !b.cfg.SemiSyncEnabled {
		return nil
	}

	if r, err := b.c.Execute("SHOW VARIABLES LIKE 'rpl_semi_sync_master_enabled';"); err != nil {
		return errors.Trace(err)
	} else {
		s, _ := r.GetString(0, 1)
		if s != "ON" {
			log.Errorf("master does not support semi synchronous replication, use no semi-sync")
			b.cfg.SemiSyncEnabled = false
			return nil
		}
	}

	_, err := b.c.Execute(`SET @rpl_semi_sync_slave = 1;`)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (b *BinlogSyncer) prepare() error {
	if b.isClosed() {
		return errors.Trace(ErrSyncClosed)
	}

	if err := b.registerSlave(); err != nil {
		return errors.Trace(err)
	}

	if err := b.enalbeSemiSync(); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (b *BinlogSyncer) startDumpStream() *BinlogStreamer {
	b.running = true

	s := newBinlogStreamer()

	b.wg.Add(1)
	go b.onStream(s)
	return s
}

// StartSync starts syncing from the `pos` position.
func (b *BinlogSyncer) StartSync(pos Position) (*BinlogStreamer, error) {
	log.Infof("begin to sync binlog from position %s", pos)

	b.m.Lock()
	defer b.m.Unlock()

	if b.running {
		return nil, errors.Trace(errSyncRunning)
	}

	if err := b.prepareSyncPos(pos); err != nil {
		return nil, errors.Trace(err)
	}

	return b.startDumpStream(), nil
}

// StartSyncGTID starts syncing from the `gset` GTIDSet.
func (b *BinlogSyncer) StartSyncGTID(gset GTIDSet) (*BinlogStreamer, error) {
	log.Infof("begin to sync binlog from GTID %s", gset)

	b.m.Lock()
	defer b.m.Unlock()

	if b.running {
		return nil, errors.Trace(errSyncRunning)
	}

	if err := b.prepare(); err != nil {
		return nil, errors.Trace(err)
	}

	var err error
	if b.cfg.Flavor != MariaDBFlavor {
		// default use MySQL
		err = b.writeBinlogDumpMysqlGTIDCommand(gset)
	} else {
		err = b.writeBinlogDumpMariadbGTIDCommand(gset)
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

	binary.LittleEndian.PutUint32(data[pos:], b.cfg.ServerID)
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

	binary.LittleEndian.PutUint32(data[pos:], b.cfg.ServerID)
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

// localHostname returns the hostname that register slave would register as.
func (b *BinlogSyncer) localHostname() string {
	if len(b.cfg.Localhost) == 0 {
		h, _ := os.Hostname()
		return h
	}
	return b.cfg.Localhost
}

func (b *BinlogSyncer) writeRegisterSlaveCommand() error {
	b.c.ResetSequence()

	hostname := b.localHostname()

	// This should be the name of slave host not the host we are connecting to.
	data := make([]byte, 4+1+4+1+len(hostname)+1+len(b.cfg.User)+1+len(b.cfg.Password)+2+4+4)
	pos := 4

	data[pos] = COM_REGISTER_SLAVE
	pos++

	binary.LittleEndian.PutUint32(data[pos:], b.cfg.ServerID)
	pos += 4

	// This should be the name of slave hostname not the host we are connecting to.
	data[pos] = uint8(len(hostname))
	pos++
	n := copy(data[pos:], hostname)
	pos += n

	data[pos] = uint8(len(b.cfg.User))
	pos++
	n = copy(data[pos:], b.cfg.User)
	pos += n

	data[pos] = uint8(len(b.cfg.Password))
	pos++
	n = copy(data[pos:], b.cfg.Password)
	pos += n

	binary.LittleEndian.PutUint16(data[pos:], b.cfg.Port)
	pos += 2

	//replication rank, not used
	binary.LittleEndian.PutUint32(data[pos:], 0)
	pos += 4

	// master ID, 0 is OK
	binary.LittleEndian.PutUint32(data[pos:], 0)

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

func (b *BinlogSyncer) retrySync() error {
	b.m.Lock()
	defer b.m.Unlock()

	log.Infof("begin to re-sync from %s", b.nextPos)

	b.parser.Reset()
	if err := b.prepareSyncPos(b.nextPos); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (b *BinlogSyncer) prepareSyncPos(pos Position) error {
	// always start from position 4
	if pos.Pos < 4 {
		pos.Pos = 4
	}

	if err := b.prepare(); err != nil {
		return errors.Trace(err)
	}

	if err := b.writeBinglogDumpCommand(pos); err != nil {
		return errors.Trace(err)
	}

	return nil
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
			log.Error(err)

			// we meet connection error, should re-connect again with
			// last nextPos we got.
			if len(b.nextPos.Name) == 0 {
				// we can't get the correct position, close.
				s.closeWithError(err)
				return
			}

			// TODO: add a max retry count.
			for {
				select {
				case <-b.ctx.Done():
					s.close()
					return
				case <-time.After(time.Second):
					if err = b.retrySync(); err != nil {
						log.Errorf("retry sync err: %v, wait 1s and retry again", err)
						continue
					}
				}

				break
			}

			// we connect the server and begin to re-sync again.
			continue
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
	if b.cfg.SemiSyncEnabled && (data[0] == SemiSyncIndicator) {
		needACK = (data[1] == 0x01)
		//skip semi sync header
		data = data[2:]
	}

	e, err := b.parser.parse(data)
	if err != nil {
		return errors.Trace(err)
	}

	if e.Header.LogPos > 0 {
		// Some events like FormatDescriptionEvent return 0, ignore.
		b.nextPos.Pos = e.Header.LogPos
	}

	if re, ok := e.Event.(*RotateEvent); ok {
		b.nextPos.Name = string(re.NextLogName)
		b.nextPos.Pos = uint32(re.Position)
		log.Infof("rotate to %s", b.nextPos)
	}

	needStop := false
	select {
	case s.ch <- e:
	case <-b.ctx.Done():
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
