package replication

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/utils"
)

var errSyncRunning = errors.New("Sync is running, must Close first")

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
	// We will use Local location for timestamp and UTC location for datetime.
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

	// FloatWithTrailingZero structure for floats.
	UseFloatWithTrailingZero bool

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

	// Option function is used to set outside of BinlogSyncerConfig， between mysql connection and COM_REGISTER_SLAVE
	// For MariaDB: slave_gtid_ignore_duplicates、skip_replication、slave_until_gtid
	Option func(*client.Conn) error

	// Set Logger
	Logger *slog.Logger

	// Set Dialer
	Dialer client.Dialer

	RowsEventDecodeFunc func(*RowsEvent, []byte) error

	TableMapOptionalMetaDecodeFunc func([]byte) error

	DiscardGTIDSet bool

	EventCacheCount int

	// FillZeroLogPos enables dynamic LogPos calculation for MariaDB.
	// When enabled, automatically adds BINLOG_SEND_ANNOTATE_ROWS_EVENT flag
	// to ensure correct position calculation in MariaDB 11.4+.
	// Only works with MariaDB flavor.
	FillZeroLogPos bool

	// PayloadDecoderConcurrency is used to control concurrency for decoding TransactionPayloadEvent.
	// Default 0,  this will be set to GOMAXPROCS.
	PayloadDecoderConcurrency int

	// SynchronousEventHandler is used for synchronous event handling.
	// This should not be used together with StartBackupWithHandler.
	// If this is not nil, GetEvent does not need to be called.
	SynchronousEventHandler EventHandler
}

// EventHandler defines the interface for processing binlog events.
type EventHandler interface {
	HandleEvent(e *BinlogEvent) error
}

// BinlogSyncer syncs binlog events from the server.
type BinlogSyncer struct {
	m sync.RWMutex

	cfg BinlogSyncerConfig

	c *client.Conn

	wg sync.WaitGroup

	parser *BinlogParser

	nextPos mysql.Position

	prevGset, currGset mysql.GTIDSet

	// instead of GTIDSet.Clone, use this to speed up calculate prevGset
	prevMySQLGTIDEvent *GTIDEvent

	running bool

	ctx    context.Context
	cancel context.CancelFunc

	lastConnectionID uint32

	retryCount int
}

// NewBinlogSyncer creates the BinlogSyncer with the given configuration.
func NewBinlogSyncer(cfg BinlogSyncerConfig) *BinlogSyncer {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.ServerID == 0 {
		cfg.Logger.Error("can't use 0 as the server ID, will panic")
		panic("can't use 0 as the server ID")
	}
	if cfg.Dialer == nil {
		dialer := &net.Dialer{}
		cfg.Dialer = dialer.DialContext
	}
	if cfg.EventCacheCount == 0 {
		cfg.EventCacheCount = 10240
	}

	// Clear the Password to avoid outputting it in logs.
	pass := cfg.Password
	cfg.Password = ""
	cfg.Logger.Info("create BinlogSyncer", slog.Any("config", cfg))
	cfg.Password = pass

	b := new(BinlogSyncer)

	b.cfg = cfg
	b.parser = NewBinlogParser()
	b.parser.SetFlavor(cfg.Flavor)
	b.parser.SetRawMode(b.cfg.RawModeEnabled)
	b.parser.SetParseTime(b.cfg.ParseTime)
	b.parser.SetTimestampStringLocation(b.cfg.TimestampStringLocation)
	b.parser.SetUseDecimal(b.cfg.UseDecimal)
	b.parser.SetUseFloatWithTrailingZero(b.cfg.UseFloatWithTrailingZero)
	b.parser.SetVerifyChecksum(b.cfg.VerifyChecksum)
	b.parser.SetPayloadDecoderConcurrency(cfg.PayloadDecoderConcurrency)
	b.parser.SetRowsEventDecodeFunc(b.cfg.RowsEventDecodeFunc)
	b.parser.SetTableMapOptionalMetaDecodeFunc(b.cfg.TableMapOptionalMetaDecodeFunc)
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

	b.cfg.Logger.Info("syncer is closing...")

	b.running = false
	b.cancel()

	if b.c != nil {
		// SetReadDeadline unblocks ReadPacket so onStream notices ctx cancellation.
		// Some transports (notably SSH tunnels) refuse deadlines with an error, in
		// which case close the underlying connection to force ReadPacket to return.
		// Otherwise wg.Wait below parks forever when KILL also fails to reach server
		// (e.g. thread already reaped, "ERROR 1094 unknown thread id")
		if err := b.c.SetReadDeadline(utils.Now().Add(100 * time.Millisecond)); err != nil {
			b.cfg.Logger.Warn("could not set read deadline, closing connection to unblock reader",
				slog.Any("error", err))
			if err := b.c.Close(); err != nil {
				b.cfg.Logger.Warn("could not close connection", slog.Any("error", err))
			}
		}
	}

	// kill last connection id
	if b.lastConnectionID > 0 {
		// Use a new connection to kill the binlog syncer
		// because calling KILL from the same connection
		// doesn't actually disconnect it.
		c, err := b.newConnection(context.Background())
		if err == nil {
			b.killConnection(c, b.lastConnectionID)
			c.Close()
		}
	}

	b.wg.Wait()

	if b.c != nil {
		b.c.Close()
	}

	b.cfg.Logger.Info("syncer is closed")
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
	b.c, err = b.newConnection(b.ctx)
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

	// set read timeout
	if b.cfg.ReadTimeout > 0 {
		_ = b.c.SetReadDeadline(utils.Now().Add(b.cfg.ReadTimeout))
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

	// for mysql 5.6+, binlog has a crc32 checksum
	// before mysql 5.6, this will not work, don't matter.:-)
	r, err := b.c.Execute("SHOW GLOBAL VARIABLES LIKE 'BINLOG_CHECKSUM'")
	if err != nil {
		return errors.Trace(err)
	}
	s, _ := r.GetString(0, 1)
	if s != "" {
		// maybe CRC32 or NONE

		// mysqlbinlog.cc use NONE, see its below comments:
		// Make a notice to the server that this client
		// is checksum-aware. It does not need the first fake Rotate
		// necessary checksummed.
		// That preference is specified below.

		if _, err = b.c.Execute(`SET @master_binlog_checksum='NONE', @source_binlog_checksum='NONE'`); err != nil {
			return errors.Trace(err)
		}
	}

	if b.cfg.Flavor == mysql.MariaDBFlavor {
		// Refer https://github.com/alibaba/canal/wiki/BinlogChange(MariaDB5&10)
		// Tell the server that we understand GTIDs by setting our slave capability
		// to MARIA_SLAVE_CAPABILITY_GTID = 4 (MariaDB >= 10.0.1).
		if _, err := b.c.Execute("SET @mariadb_slave_capability=4"); err != nil {
			return errors.Errorf("failed to set @mariadb_slave_capability=4: %v", err)
		}
	}

	if b.cfg.HeartbeatPeriod > 0 {
		_, err = b.c.Execute(fmt.Sprintf("SET @master_heartbeat_period = %d, @source_heartbeat_period = %d",
			b.cfg.HeartbeatPeriod, b.cfg.HeartbeatPeriod))
		if err != nil {
			b.cfg.Logger.Error(
				fmt.Sprintf("failed to set @master_heartbeat_period=%d, @source_heartbeat_period=%d",
					b.cfg.HeartbeatPeriod, b.cfg.HeartbeatPeriod), slog.Any("error", err))
			return errors.Trace(err)
		}
	}

	serverUUID, err := uuid.NewUUID()
	if err != nil {
		b.cfg.Logger.Error("failed to get new uuid", slog.Any("error", err))
		return errors.Trace(err)
	}
	if _, err = b.c.Execute(fmt.Sprintf("SET @slave_uuid = '%s', @replica_uuid = '%s'", serverUUID, serverUUID)); err != nil {
		b.cfg.Logger.Error(fmt.Sprintf("failed to set @slave_uuid = '%s', @replica_uuid = '%s'", serverUUID, serverUUID), slog.Any("error", err))
		return errors.Trace(err)
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

	r, err := b.c.Execute("SHOW VARIABLES LIKE 'rpl_semi_sync_master_enabled';")
	if err != nil {
		return errors.Trace(err)
	}
	s, _ := r.GetString(0, 1)
	if s != "ON" {
		b.cfg.Logger.Error("master does not support semi synchronous replication, use no semi-sync")
		b.cfg.SemiSyncEnabled = false
		return nil
	}

	_, err = b.c.Execute(`SET @rpl_semi_sync_slave = 1;`)
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

	b.cfg.Logger.Info("Connected to server", slog.String("flavor", b.cfg.Flavor), slog.String("version", b.c.GetServerVersion()))

	return nil
}

func (b *BinlogSyncer) startDumpStream() *BinlogStreamer {
	b.running = true

	s := NewBinlogStreamerWithChanSize(b.cfg.EventCacheCount)

	b.wg.Add(1)
	go b.onStream(s)
	return s
}

// GetNextPosition returns the next position of the syncer
func (b *BinlogSyncer) GetNextPosition() mysql.Position {
	return b.nextPos
}

func (b *BinlogSyncer) checkFlavor() {
	serverVersion := b.c.GetServerVersion()
	if b.cfg.Flavor != mysql.MariaDBFlavor &&
		strings.Contains(serverVersion, "MariaDB") {
		// Setting the flavor to `mysql` causes MariaDB to try and behave
		// in a MySQL compatible way. In this mode MariaDB won't use
		// MariaDB specific binlog event types, but may used dummy events instead.
		b.cfg.Logger.Error("misconfigured flavor for server", slog.String("flavor", b.cfg.Flavor), slog.String("version", serverVersion))
	}
}

// StartSync starts syncing from the `pos` position.
func (b *BinlogSyncer) StartSync(pos mysql.Position) (*BinlogStreamer, error) {
	b.cfg.Logger.Info("begin to sync binlog from position", slog.Any("position", pos))

	b.m.Lock()
	defer b.m.Unlock()

	if b.running {
		return nil, errors.Trace(errSyncRunning)
	}

	if err := b.prepareSyncPos(pos); err != nil {
		return nil, errors.Trace(err)
	}

	b.checkFlavor()

	return b.startDumpStream(), nil
}

// StartSyncGTID starts syncing from the `gset` GTIDSet.
func (b *BinlogSyncer) StartSyncGTID(gset mysql.GTIDSet) (*BinlogStreamer, error) {
	b.cfg.Logger.Info("begin to sync binlog from GTID set", slog.Any("GTID set", gset))

	b.prevMySQLGTIDEvent = nil
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
	case mysql.MariaDBFlavor:
		err = b.writeBinlogDumpMariadbGTIDCommand(gset)
	default:
		// default use MySQL
		err = b.writeBinlogDumpMysqlGTIDCommand(gset)
	}

	if err != nil {
		return nil, err
	}

	b.checkFlavor()

	return b.startDumpStream(), nil
}

func (b *BinlogSyncer) writeBinlogDumpCommand(p mysql.Position) error {
	b.c.ResetSequence()

	data := make([]byte, 4+1+4+2+4+len(p.Name))

	pos := 4
	data[pos] = mysql.COM_BINLOG_DUMP
	pos++

	binary.LittleEndian.PutUint32(data[pos:], p.Pos)
	pos += 4

	dumpCommandFlag := b.cfg.DumpCommandFlag
	if b.cfg.FillZeroLogPos && b.cfg.Flavor == mysql.MariaDBFlavor {
		// Add BINLOG_SEND_ANNOTATE_ROWS_EVENT flag when FillZeroLogPos is enabled.
		// This ensures the server sends ANNOTATE_ROWS_EVENT events which are needed
		// for correct LogPos calculation in MariaDB 11.4+, where some events have LogPos=0.
		dumpCommandFlag |= BINLOG_SEND_ANNOTATE_ROWS_EVENT
	}
	binary.LittleEndian.PutUint16(data[pos:], dumpCommandFlag)
	pos += 2

	binary.LittleEndian.PutUint32(data[pos:], b.cfg.ServerID)
	pos += 4

	copy(data[pos:], p.Name)

	return b.c.WritePacket(data)
}

func (b *BinlogSyncer) writeBinlogDumpMysqlGTIDCommand(gset mysql.GTIDSet) error {
	p := mysql.Position{Name: "", Pos: 4}
	gtidData := gset.Encode()

	b.c.ResetSequence()

	data := make([]byte, 4+1+2+4+4+len(p.Name)+8+4+len(gtidData))
	pos := 4
	data[pos] = mysql.COM_BINLOG_DUMP_GTID
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

func (b *BinlogSyncer) writeBinlogDumpMariadbGTIDCommand(gset mysql.GTIDSet) error {
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
	return b.writeBinlogDumpCommand(mysql.Position{Name: "", Pos: 0})
}

// localHostname returns the hostname that register replica would register as.
// this gets truncated to 255 bytes.
func (b *BinlogSyncer) localHostname() string {
	h := b.cfg.Localhost
	if len(h) == 0 {
		h, _ = os.Hostname()
	}
	if len(h) <= 255 {
		return h
	}
	return h[:255]
}

func (b *BinlogSyncer) writeRegisterSlaveCommand() error {
	b.c.ResetSequence()

	hostname := b.localHostname()

	// This should be the name of slave host not the host we are connecting to.
	data := make([]byte, 4+1+4+1+len(hostname)+1+len(b.cfg.User)+1+2+4+4)
	pos := 4

	data[pos] = mysql.COM_REGISTER_SLAVE
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

	data[pos] = uint8(0)
	pos++

	binary.LittleEndian.PutUint16(data[pos:], b.cfg.Port)
	pos += 2

	// replication rank, not used
	binary.LittleEndian.PutUint32(data[pos:], 0)
	pos += 4

	// master ID, 0 is OK
	binary.LittleEndian.PutUint32(data[pos:], 0)

	return b.c.WritePacket(data)
}

func (b *BinlogSyncer) replySemiSyncACK(p mysql.Position) error {
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
	b.prevMySQLGTIDEvent = nil

	if b.prevGset != nil {
		extra := []any{slog.String("GTID Set", b.prevGset.String())}
		if b.currGset != nil {
			extra = append(extra, slog.String("last read GTID", b.currGset.String()))
		}
		b.cfg.Logger.Info("begin to re-sync", extra...)

		if err := b.prepareSyncGTID(b.prevGset); err != nil {
			return errors.Trace(err)
		}
	} else {
		b.cfg.Logger.Info("begin to re-sync", slog.String("file", b.nextPos.Name), slog.Uint64("position", uint64(b.nextPos.Pos)))
		if err := b.prepareSyncPos(b.nextPos); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (b *BinlogSyncer) prepareSyncPos(pos mysql.Position) error {
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

func (b *BinlogSyncer) prepareSyncGTID(gset mysql.GTIDSet) error {
	var err error

	// re establishing network connection here and will start getting binlog events from "gset + 1", thus until first
	// MariadbGTIDEvent/GTIDEvent event is received - we effectively do not have a "current GTID"
	b.currGset = nil

	if err = b.prepare(); err != nil {
		return errors.Trace(err)
	}

	switch b.cfg.Flavor {
	case mysql.MariaDBFlavor:
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
			s.closeWithError(fmt.Errorf("panic %v\nstack: %s", e, mysql.Pstack()))
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
			b.cfg.Logger.Error(err.Error())
			// we meet connection error, should re-connect again with
			// last nextPos or nextGTID we got.
			if len(b.nextPos.Name) == 0 && b.prevGset == nil {
				// we can't get the correct position, close.
				s.closeWithError(err)
				return
			}

			if b.cfg.DisableRetrySync {
				b.cfg.Logger.Warn("retry sync is disabled")
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
							b.cfg.Logger.Error(
								"retry sync err, exceeded max retries",
								slog.Any("error", err), slog.Int("maxAttempts", b.cfg.MaxReconnectAttempts),
							)
							s.closeWithError(err)
							return
						}

						b.cfg.Logger.Error(
							"retry sync err, wait 1s and retry again",
							slog.Any("error", err), slog.Int("retryCount", b.retryCount), slog.Int("maxAttempts", b.cfg.MaxReconnectAttempts),
						)
						continue
					}
				}

				break
			}

			// we connect the server and begin to re-sync again.
			continue
		}

		// set read timeout
		if b.cfg.ReadTimeout > 0 {
			_ = b.c.SetReadDeadline(utils.Now().Add(b.cfg.ReadTimeout))
		}

		// Reset retry count on successful packet receieve
		b.retryCount = 0

		switch data[0] {
		case mysql.OK_HEADER:
			// Parse the event
			e, needACK, err := b.parseEvent(data)
			if err != nil {
				s.closeWithError(err)
				return
			}

			// Handle the event and send ACK if necessary
			err = b.handleEventAndACK(s, e, needACK)
			if err != nil {
				s.closeWithError(err)
				return
			}
		case mysql.ERR_HEADER:
			err = b.c.HandleErrorPacket(data)
			s.closeWithError(err)
			return
		case mysql.EOF_HEADER:
			// refer to https://dev.mysql.com/doc/internals/en/com-binlog-dump.html#binlog-dump-non-block
			// when COM_BINLOG_DUMP command use BINLOG_DUMP_NON_BLOCK flag,
			// if there is no more event to send an EOF_Packet instead of blocking the connection
			b.cfg.Logger.Info("receive EOF packet, no more binlog event now.")
			continue
		default:
			b.cfg.Logger.Error("invalid stream header", slog.Int("header", int(data[0])))
			continue
		}
	}
}

// parseEvent parses the raw data into a BinlogEvent.
// It only handles parsing and does not perform any side effects.
// Returns the parsed BinlogEvent, a boolean indicating if an ACK is needed, and an error if the
// parsing fails
func (b *BinlogSyncer) parseEvent(data []byte) (event *BinlogEvent, needACK bool, err error) {
	// Skip OK byte (0x00)
	data = data[1:]

	needACK = false
	if b.cfg.SemiSyncEnabled && data[0] == SemiSyncIndicator {
		needACK = data[1] == 0x01
		// Skip semi-sync header
		data = data[2:]
	}

	// Parse the event using the BinlogParser
	event, err = b.parser.Parse(data)
	if err != nil {
		return nil, false, errors.Trace(err)
	}

	return event, needACK, nil
}

// handleEventAndACK processes an event and sends an ACK if necessary.
func (b *BinlogSyncer) handleEventAndACK(s *BinlogStreamer, e *BinlogEvent, needACK bool) error {
	// Update the next position based on the event's LogPos
	if e.Header.LogPos > 0 {
		// Some events like FormatDescriptionEvent return 0, ignore.
		b.nextPos.Pos = e.Header.LogPos
	} else if b.shouldCalculateDynamicLogPos(e) {
		calculatedPos := b.nextPos.Pos + e.Header.EventSize
		e.Header.LogPos = calculatedPos
		b.nextPos.Pos = calculatedPos
		b.cfg.Logger.Debug("MariaDB dynamic LogPos calculation",
			slog.String("eventType", e.Header.EventType.String()),
			slog.Uint64("logPos", uint64(calculatedPos)))
	}

	// Handle event types to update positions and GTID sets
	switch event := e.Event.(type) {
	case *RotateEvent:
		b.nextPos.Name = string(event.NextLogName)
		b.nextPos.Pos = uint32(event.Position)
		b.cfg.Logger.Info("rotate to next binlog", slog.String("file", b.nextPos.Name), slog.Uint64("position", uint64(b.nextPos.Pos)))

	case *GTIDEvent:
		if b.prevGset == nil {
			break
		}
		if b.currGset == nil {
			b.currGset = b.prevGset.Clone()
		}
		u, err := uuid.FromBytes(event.SID)
		if err != nil {
			return errors.Trace(err)
		}
		b.currGset.(*mysql.MysqlGTIDSet).AddGTID(u, event.GNO)
		if b.prevMySQLGTIDEvent != nil {
			u, err = uuid.FromBytes(b.prevMySQLGTIDEvent.SID)
			if err != nil {
				return errors.Trace(err)
			}
			b.prevGset.(*mysql.MysqlGTIDSet).AddGTID(u, b.prevMySQLGTIDEvent.GNO)
		}
		b.prevMySQLGTIDEvent = event
	case *GtidTaggedLogEvent:
		if b.prevGset == nil {
			break
		}
		if b.currGset == nil {
			b.currGset = b.prevGset.Clone()
		}
		u, err := uuid.FromBytes(event.SID)
		if err != nil {
			return errors.Trace(err)
		}
		b.currGset.(*mysql.MysqlGTIDSet).AddGTIDWithTag(u, event.Tag, event.GNO)
		if b.prevMySQLGTIDEvent != nil {
			u, err = uuid.FromBytes(b.prevMySQLGTIDEvent.SID)
			if err != nil {
				return errors.Trace(err)
			}
			b.prevGset.(*mysql.MysqlGTIDSet).AddGTIDWithTag(u, b.prevMySQLGTIDEvent.Tag, b.prevMySQLGTIDEvent.GNO)
		}
		b.prevMySQLGTIDEvent = &event.GTIDEvent
	case *MariadbGTIDEvent:
		if b.prevGset == nil {
			break
		}
		if b.currGset == nil {
			b.currGset = b.prevGset.Clone()
		}
		prev := b.currGset.Clone()
		err := b.currGset.(*mysql.MariadbGTIDSet).AddSet(&event.GTID)
		if err != nil {
			return errors.Trace(err)
		}
		// Right after reconnect we may see the same GTID as before; update prevGset if currGset changed
		if !b.currGset.Equal(prev) {
			b.prevGset = prev
		}

	case *XIDEvent:
		if !b.cfg.DiscardGTIDSet {
			event.GSet = b.getCurrentGtidSet()
		}

	case *QueryEvent:
		if !b.cfg.DiscardGTIDSet {
			event.GSet = b.getCurrentGtidSet()
		}
	}

	// Use SynchronousEventHandler if it's set
	if b.cfg.SynchronousEventHandler != nil {
		err := b.cfg.SynchronousEventHandler.HandleEvent(e)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		// Asynchronous mode: send the event to the streamer channel
		select {
		case s.ch <- e:
		case <-b.ctx.Done():
			return errors.New("sync is being closed")
		}
	}

	if needACK {
		err := b.replySemiSyncACK(b.nextPos)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// shouldCalculateDynamicLogPos determines if we should calculate LogPos dynamically for MariaDB events.
// This is needed for MariaDB 11.4+ when:
// 1. FillZeroLogPos is enabled
// 2. We're using MariaDB flavor
// 3. The event has LogPos=0 (indicating server didn't set it)
// 4. The event is not artificial (not marked with LOG_EVENT_ARTIFICIAL_F flag)
func (b *BinlogSyncer) shouldCalculateDynamicLogPos(e *BinlogEvent) bool {
	return b.cfg.FillZeroLogPos &&
		b.cfg.Flavor == mysql.MariaDBFlavor &&
		e.Header.LogPos == 0 &&
		(e.Header.Flags&LOG_EVENT_ARTIFICIAL_F) == 0
}

// getCurrentGtidSet returns a clone of the current GTID set.
func (b *BinlogSyncer) getCurrentGtidSet() mysql.GTIDSet {
	if b.currGset != nil {
		return b.currGset.Clone()
	}
	return nil
}

// LastConnectionID returns last connectionID.
func (b *BinlogSyncer) LastConnectionID() uint32 {
	return b.lastConnectionID
}

func (b *BinlogSyncer) newConnection(ctx context.Context) (*client.Conn, error) {
	var addr string
	if b.cfg.Port != 0 {
		addr = net.JoinHostPort(b.cfg.Host, strconv.Itoa(int(b.cfg.Port)))
	} else {
		addr = b.cfg.Host
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	return client.ConnectWithDialer(timeoutCtx, "", addr, b.cfg.User, b.cfg.Password,
		"", b.cfg.Dialer, func(c *client.Conn) error {
			c.SetTLSConfig(b.cfg.TLSConfig)
			c.SetAttributes(map[string]string{"_client_role": "binary_log_listener"})
			if b.cfg.ReadTimeout > 0 {
				c.ReadTimeout = b.cfg.ReadTimeout
			}
			return nil
		})
}

func (b *BinlogSyncer) killConnection(conn *client.Conn, id uint32) {
	cmd := fmt.Sprintf("KILL %d", id)
	if _, err := conn.Execute(cmd); err != nil {
		b.cfg.Logger.Error("kill connection", slog.Any("error", err), slog.Int64("id", int64(id)))
		// Unknown thread id
		if code := mysql.ErrorCode(err.Error()); code != mysql.ER_NO_SUCH_THREAD {
			b.cfg.Logger.Error(errors.Trace(err).Error())
		}
	}
	b.cfg.Logger.Info("kill last connection", slog.Int64("id", int64(id)))
}
