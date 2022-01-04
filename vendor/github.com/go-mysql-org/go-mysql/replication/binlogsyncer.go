package replication

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	. "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/siddontang/go-log/log"
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

	// Charset is for MySQL client character set
	Charset string

	// SemiSyncEnabled enables semi-sync or not.
	SemiSyncEnabled bool

	// RawModeEnabled is for not parsing binlog event.
	RawModeEnabled bool

	// If not nil, use the provided tls.Config to connect to the database using TLS/SSL.
	TLSConfig *tls.Config

	// Use replication.Time structure for timestamp and datetime.
	// We will use Local location for timestamp and UTC location for datatime.
	ParseTime bool

	// If ParseTime is false, convert TIMESTAMP into this specified timezone. If
	// ParseTime is true, this option will have no effect and TIMESTAMP data will
	// be parsed into the local timezone and a full time.Time struct will be
	// returned.
	//
	// Note that MySQL TIMESTAMP columns are offset from the machine local
	// timezone while DATETIME columns are offset from UTC. This is consistent
	// with documented MySQL behaviour as it return TIMESTAMP in local timezone
	// and DATETIME in UTC.
	//
	// Setting this to UTC effectively equalizes the TIMESTAMP and DATETIME time
	// strings obtained from MySQL.
	TimestampStringLocation *time.Location

	// Use decimal.Decimal structure for decimals.
	UseDecimal bool

	// RecvBufferSize sets the size in bytes of the operating system's receive buffer associated with the connection.
	RecvBufferSize int

	// master heartbeat period
	HeartbeatPeriod time.Duration

	// read timeout
	ReadTimeout time.Duration

	// maximum number of attempts to re-establish a broken connection, zero or negative number means infinite retry.
	// this configuration will not work if DisableRetrySync is true
	MaxReconnectAttempts int

	// whether disable re-sync for broken connection
	DisableRetrySync bool

	// Only works when MySQL/MariaDB variable binlog_checksum=CRC32.
	// For MySQL, binlog_checksum was introduced since 5.6.2, but CRC32 was set as default value since 5.6.6 .
	// https://dev.mysql.com/doc/refman/5.6/en/replication-options-binary-log.html#option_mysqld_binlog-checksum
	// For MariaDB, binlog_checksum was introduced since MariaDB 5.3, but CRC32 was set as default value since MariaDB 10.2.1 .
	// https://mariadb.com/kb/en/library/replication-and-binary-log-server-system-variables/#binlog_checksum
	VerifyChecksum bool

	// DumpCommandFlag is used to send binglog dump command. Default 0, aka BINLOG_DUMP_NEVER_STOP.
	// For MySQL, BINLOG_DUMP_NEVER_STOP and BINLOG_DUMP_NON_BLOCK are available.
	// https://dev.mysql.com/doc/internals/en/com-binlog-dump.html#binlog-dump-non-block
	// For MariaDB, BINLOG_DUMP_NEVER_STOP, BINLOG_DUMP_NON_BLOCK and BINLOG_SEND_ANNOTATE_ROWS_EVENT are available.
	// https://mariadb.com/kb/en/library/com_binlog_dump/
	// https://mariadb.com/kb/en/library/annotate_rows_event/
	DumpCommandFlag uint16

	//Option function is used to set outside of BinlogSyncerConfig， between mysql connection and COM_REGISTER_SLAVE
	//For MariaDB: slave_gtid_ignore_duplicates、skip_replication、slave_until_gtid
	Option func(*client.Conn) error
}

// BinlogSyncer syncs binlog event from server.
type BinlogSyncer struct {
	m sync.RWMutex

	cfg BinlogSyncerConfig

	c *client.Conn

	wg sync.WaitGroup

	parser *BinlogParser

	nextPos Position

	prevGset, currGset GTIDSet

	running bool

	ctx    context.Context
	cancel context.CancelFunc

	lastConnectionID uint32

	retryCount int
}

// NewBinlogSyncer creates the BinlogSyncer with cfg.
func NewBinlogSyncer(cfg BinlogSyncerConfig) *BinlogSyncer {
	if cfg.ServerID == 0 {
		log.Fatal("can't use 0 as the server ID")
	}

	// Clear the Password to avoid outputing it in log.
	pass := cfg.Password
	cfg.Password = ""
	log.Infof("create BinlogSyncer with config %v", cfg)
	cfg.Password = pass

	b := new(BinlogSyncer)

	b.cfg = cfg
	b.parser = NewBinlogParser()
	b.parser.SetFlavor(cfg.Flavor)
	b.parser.SetRawMode(b.cfg.RawModeEnabled)
	b.parser.SetParseTime(b.cfg.ParseTime)
	b.parser.SetTimestampStringLocation(b.cfg.TimestampStringLocation)
	b.parser.SetUseDecimal(b.cfg.UseDecimal)
	b.parser.SetVerifyChecksum(b.cfg.VerifyChecksum)
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
		err := b.c.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		if err != nil {
			log.Warnf(`could not set read deadline: %s`, err)
		}
	}

	// kill last connection id
	if b.lastConnectionID > 0 {
		// Use a new connection to kill the binlog syncer
		// because calling KILL from the same connection
		// doesn't actually disconnect it.
		c, err := b.newConnection()
		if err == nil {
			b.killConnection(c, b.lastConnectionID)
			c.Close()
		}
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

	var err error
	b.c, err = b.newConnection()
	if err != nil {
		return errors.Trace(err)
	}

	if b.cfg.Option != nil {
		if err = b.cfg.Option(b.c); err != nil {
			return errors.Trace(err)
		}
	}

	if len(b.cfg.Charset) != 0 {
		if err = b.c.SetCharset(b.cfg.Charset); err != nil {
			return errors.Trace(err)
		}
	}

	//set read timeout
	if b.cfg.ReadTimeout > 0 {
		_ = b.c.SetReadDeadline(time.Now().Add(b.cfg.ReadTimeout))
	}

	if b.cfg.RecvBufferSize > 0 {
		if tcp, ok := b.c.Conn.Conn.(*net.TCPConn); ok {
			_ = tcp.SetReadBuffer(b.cfg.RecvBufferSize)
		}
	}

	// kill last connection id
	if b.lastConnectionID > 0 {
		b.killConnection(b.c, b.lastConnectionID)
	}

	// save last last connection id for kill
	b.lastConnectionID = b.c.GetConnectionID()

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

	if b.cfg.Flavor == MariaDBFlavor {
		// Refer https://github.com/alibaba/canal/wiki/BinlogChange(MariaDB5&10)
		// Tell the server that we understand GTIDs by setting our slave capability
		// to MARIA_SLAVE_CAPABILITY_GTID = 4 (MariaDB >= 10.0.1).
		if _, err := b.c.Execute("SET @mariadb_slave_capability=4"); err != nil {
			return errors.Errorf("failed to set @mariadb_slave_capability=4: %v", err)
		}
	}

	if b.cfg.HeartbeatPeriod > 0 {
		_, err = b.c.Execute(fmt.Sprintf("SET @master_heartbeat_period=%d;", b.cfg.HeartbeatPeriod))
		if err != nil {
			log.Errorf("failed to set @master_heartbeat_period=%d, err: %v", b.cfg.HeartbeatPeriod, err)
			return errors.Trace(err)
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

func (b *BinlogSyncer) enableSemiSync() error {
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

	if err := b.enableSemiSync(); err != nil {
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

// GetNextPosition returns the next position of the syncer
func (b *BinlogSyncer) GetNextPosition() Position {
	return b.nextPos
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
	log.Infof("begin to sync binlog from GTID set %s", gset)

	b.prevGset = gset

	b.m.Lock()
	defer b.m.Unlock()

	if b.running {
		return nil, errors.Trace(errSyncRunning)
	}

	// establishing network connection here and will start getting binlog events from "gset + 1", thus until first
	// MariadbGTIDEvent/GTIDEvent event is received - we effectively do not have a "current GTID"
	b.currGset = nil

	if err := b.prepare(); err != nil {
		return nil, errors.Trace(err)
	}

	var err error
	switch b.cfg.Flavor {
	case MariaDBFlavor:
		err = b.writeBinlogDumpMariadbGTIDCommand(gset)
	default:
		// default use MySQL
		err = b.writeBinlogDumpMysqlGTIDCommand(gset)
	}

	if err != nil {
		return nil, err
	}

	return b.startDumpStream(), nil
}

func (b *BinlogSyncer) writeBinlogDumpCommand(p Position) error {
	b.c.ResetSequence()

	data := make([]byte, 4+1+4+2+4+len(p.Name))

	pos := 4
	data[pos] = COM_BINLOG_DUMP
	pos++

	binary.LittleEndian.PutUint32(data[pos:], p.Pos)
	pos += 4

	binary.LittleEndian.PutUint16(data[pos:], b.cfg.DumpCommandFlag)
	pos += 2

	binary.LittleEndian.PutUint32(data[pos:], b.cfg.ServerID)
	pos += 4

	copy(data[pos:], p.Name)

	return b.c.WritePacket(data)
}

func (b *BinlogSyncer) writeBinlogDumpMysqlGTIDCommand(gset GTIDSet) error {
	p := Position{Name: "", Pos: 4}
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
	return b.writeBinlogDumpCommand(Position{Name: "", Pos: 0})
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

	return nil
}

func (b *BinlogSyncer) retrySync() error {
	b.m.Lock()
	defer b.m.Unlock()

	b.parser.Reset()

	if b.prevGset != nil {
		msg := fmt.Sprintf("begin to re-sync from %s", b.prevGset.String())
		if b.currGset != nil {
			msg = fmt.Sprintf("%v (last read GTID=%v)", msg, b.currGset)
		}
		log.Infof(msg)

		if err := b.prepareSyncGTID(b.prevGset); err != nil {
			return errors.Trace(err)
		}
	} else {
		log.Infof("begin to re-sync from %s", b.nextPos)
		if err := b.prepareSyncPos(b.nextPos); err != nil {
			return errors.Trace(err)
		}
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

	if err := b.writeBinlogDumpCommand(pos); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (b *BinlogSyncer) prepareSyncGTID(gset GTIDSet) error {
	var err error

	// re establishing network connection here and will start getting binlog events from "gset + 1", thus until first
	// MariadbGTIDEvent/GTIDEvent event is received - we effectively do not have a "current GTID"
	b.currGset = nil

	if err = b.prepare(); err != nil {
		return errors.Trace(err)
	}

	switch b.cfg.Flavor {
	case MariaDBFlavor:
		err = b.writeBinlogDumpMariadbGTIDCommand(gset)
	default:
		// default use MySQL
		err = b.writeBinlogDumpMysqlGTIDCommand(gset)
	}

	if err != nil {
		return err
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
		select {
		case <-b.ctx.Done():
			s.close()
			return
		default:
		}

		if err != nil {
			log.Error(err)
			// we meet connection error, should re-connect again with
			// last nextPos or nextGTID we got.
			if len(b.nextPos.Name) == 0 && b.prevGset == nil {
				// we can't get the correct position, close.
				s.closeWithError(err)
				return
			}

			if b.cfg.DisableRetrySync {
				log.Warn("retry sync is disabled")
				s.closeWithError(err)
				return
			}

			for {
				select {
				case <-b.ctx.Done():
					s.close()
					return
				case <-time.After(time.Second):
					b.retryCount++
					if err = b.retrySync(); err != nil {
						if b.cfg.MaxReconnectAttempts > 0 && b.retryCount >= b.cfg.MaxReconnectAttempts {
							log.Errorf("retry sync err: %v, exceeded max retries (%d)", err, b.cfg.MaxReconnectAttempts)
							s.closeWithError(err)
							return
						}

						log.Errorf("retry sync err: %v, wait 1s and retry again", err)
						continue
					}
				}

				break
			}

			// we connect the server and begin to re-sync again.
			continue
		}

		//set read timeout
		if b.cfg.ReadTimeout > 0 {
			_ = b.c.SetReadDeadline(time.Now().Add(b.cfg.ReadTimeout))
		}

		// Reset retry count on successful packet receieve
		b.retryCount = 0

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
		case EOF_HEADER:
			// Refer http://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html
			// In the MySQL client/server protocol, EOF and OK packets serve the same purpose.
			// Some users told me that they received EOF packet here, but I don't know why.
			// So we only log a message and retry ReadPacket.
			log.Info("receive EOF packet, retry ReadPacket")
			continue
		default:
			log.Errorf("invalid stream header %c", data[0])
			continue
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

	e, err := b.parser.Parse(data)
	if err != nil {
		return errors.Trace(err)
	}

	if e.Header.LogPos > 0 {
		// Some events like FormatDescriptionEvent return 0, ignore.
		b.nextPos.Pos = e.Header.LogPos
	}

	getCurrentGtidSet := func() GTIDSet {
		if b.currGset == nil {
			return nil
		}
		return b.currGset.Clone()
	}

	advanceCurrentGtidSet := func(gtid string) error {
		if b.currGset == nil {
			b.currGset = b.prevGset.Clone()
		}
		prev := b.currGset.Clone()
		err := b.currGset.Update(gtid)
		if err == nil {
			// right after reconnect we will see same gtid as we saw before, thus currGset will not get changed
			if !b.currGset.Equal(prev) {
				b.prevGset = prev
			}
		}
		return err
	}

	switch event := e.Event.(type) {
	case *RotateEvent:
		b.nextPos.Name = string(event.NextLogName)
		b.nextPos.Pos = uint32(event.Position)
		log.Infof("rotate to %s", b.nextPos)
	case *GTIDEvent:
		if b.prevGset == nil {
			break
		}
		u, _ := uuid.FromBytes(event.SID)
		err := advanceCurrentGtidSet(fmt.Sprintf("%s:%d", u.String(), event.GNO))
		if err != nil {
			return errors.Trace(err)
		}
	case *MariadbGTIDEvent:
		if b.prevGset == nil {
			break
		}
		GTID := event.GTID
		err := advanceCurrentGtidSet(fmt.Sprintf("%d-%d-%d", GTID.DomainID, GTID.ServerID, GTID.SequenceNumber))
		if err != nil {
			return errors.Trace(err)
		}
	case *XIDEvent:
		event.GSet = getCurrentGtidSet()
	case *QueryEvent:
		event.GSet = getCurrentGtidSet()
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

// LastConnectionID returns last connectionID.
func (b *BinlogSyncer) LastConnectionID() uint32 {
	return b.lastConnectionID
}

func (b *BinlogSyncer) newConnection() (*client.Conn, error) {
	var addr string
	if b.cfg.Port != 0 {
		addr = fmt.Sprintf("%s:%d", b.cfg.Host, b.cfg.Port)
	} else {
		addr = b.cfg.Host
	}

	return client.Connect(addr, b.cfg.User, b.cfg.Password, "", func(c *client.Conn) {
		c.SetTLSConfig(b.cfg.TLSConfig)
	})
}

func (b *BinlogSyncer) killConnection(conn *client.Conn, id uint32) {
	cmd := fmt.Sprintf("KILL %d", id)
	if _, err := conn.Execute(cmd); err != nil {
		log.Errorf("kill connection %d error %v", id, err)
		// Unknown thread id
		if code := ErrorCode(err.Error()); code != ER_NO_SUCH_THREAD {
			log.Error(errors.Trace(err))
		}
	}
	log.Infof("kill last connection id %d", id)
}
