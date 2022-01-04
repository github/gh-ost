package replication

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
)

var (
	// ErrChecksumMismatch indicates binlog checksum mismatch.
	ErrChecksumMismatch = errors.New("binlog checksum mismatch, data may be corrupted")
)

type BinlogParser struct {
	// "mysql" or "mariadb", if not set, use "mysql" by default
	flavor string

	format *FormatDescriptionEvent

	tables map[uint64]*TableMapEvent

	// for rawMode, we only parse FormatDescriptionEvent and RotateEvent
	rawMode bool

	parseTime               bool
	timestampStringLocation *time.Location

	// used to start/stop processing
	stopProcessing uint32

	useDecimal          bool
	ignoreJSONDecodeErr bool
	verifyChecksum      bool
}

func NewBinlogParser() *BinlogParser {
	p := new(BinlogParser)

	p.tables = make(map[uint64]*TableMapEvent)

	return p
}

func (p *BinlogParser) Stop() {
	atomic.StoreUint32(&p.stopProcessing, 1)
}

func (p *BinlogParser) Resume() {
	atomic.StoreUint32(&p.stopProcessing, 0)
}

func (p *BinlogParser) Reset() {
	p.format = nil
}

type OnEventFunc func(*BinlogEvent) error

func (p *BinlogParser) ParseFile(name string, offset int64, onEvent OnEventFunc) error {
	f, err := os.Open(name)
	if err != nil {
		return errors.Trace(err)
	}
	defer f.Close()

	b := make([]byte, 4)
	if _, err = f.Read(b); err != nil {
		return errors.Trace(err)
	} else if !bytes.Equal(b, BinLogFileHeader) {
		return errors.Errorf("%s is not a valid binlog file, head 4 bytes must fe'bin' ", name)
	}

	if offset < 4 {
		offset = 4
	} else if offset > 4 {
		//  FORMAT_DESCRIPTION event should be read by default always (despite that fact passed offset may be higher than 4)
		if _, err = f.Seek(4, io.SeekStart); err != nil {
			return errors.Errorf("seek %s to %d error %v", name, offset, err)
		}

		if err = p.parseFormatDescriptionEvent(f, onEvent); err != nil {
			return errors.Annotatef(err, "parse FormatDescriptionEvent")
		}
	}

	if _, err = f.Seek(offset, io.SeekStart); err != nil {
		return errors.Errorf("seek %s to %d error %v", name, offset, err)
	}

	return p.ParseReader(f, onEvent)
}

func (p *BinlogParser) parseFormatDescriptionEvent(r io.Reader, onEvent OnEventFunc) error {
	_, err := p.parseSingleEvent(r, onEvent)
	return err
}

// ParseSingleEvent parses single binlog event and passes the event to onEvent function.
func (p *BinlogParser) ParseSingleEvent(r io.Reader, onEvent OnEventFunc) (bool, error) {
	return p.parseSingleEvent(r, onEvent)
}

func (p *BinlogParser) parseSingleEvent(r io.Reader, onEvent OnEventFunc) (bool, error) {
	var err error
	var n int64

	var buf bytes.Buffer
	if n, err = io.CopyN(&buf, r, EventHeaderSize); err == io.EOF {
		return true, nil
	} else if err != nil {
		return false, errors.Errorf("get event header err %v, need %d but got %d", err, EventHeaderSize, n)
	}

	var h *EventHeader
	h, err = p.parseHeader(buf.Bytes())
	if err != nil {
		return false, errors.Trace(err)
	}

	if h.EventSize < uint32(EventHeaderSize) {
		return false, errors.Errorf("invalid event header, event size is %d, too small", h.EventSize)
	}
	if n, err = io.CopyN(&buf, r, int64(h.EventSize-EventHeaderSize)); err != nil {
		return false, errors.Errorf("get event err %v, need %d but got %d", err, h.EventSize, n)
	}
	if buf.Len() != int(h.EventSize) {
		return false, errors.Errorf("invalid raw data size in event %s, need %d but got %d", h.EventType, h.EventSize, buf.Len())
	}

	rawData := buf.Bytes()
	bodyLen := int(h.EventSize) - EventHeaderSize
	body := rawData[EventHeaderSize:]
	if len(body) != bodyLen {
		return false, errors.Errorf("invalid body data size in event %s, need %d but got %d", h.EventType, bodyLen, len(body))
	}

	var e Event
	e, err = p.parseEvent(h, body, rawData)
	if err != nil {
		if err == errMissingTableMapEvent {
			return false, nil
		}
		return false, errors.Trace(err)
	}

	if err = onEvent(&BinlogEvent{RawData: rawData, Header: h, Event: e}); err != nil {
		return false, errors.Trace(err)
	}

	return false, nil
}

func (p *BinlogParser) ParseReader(r io.Reader, onEvent OnEventFunc) error {
	for {
		if atomic.LoadUint32(&p.stopProcessing) == 1 {
			break
		}

		done, err := p.parseSingleEvent(r, onEvent)
		if err != nil {
			if err == errMissingTableMapEvent {
				continue
			}
			return errors.Trace(err)
		}

		if done {
			break
		}
	}

	return nil
}

func (p *BinlogParser) SetRawMode(mode bool) {
	p.rawMode = mode
}

func (p *BinlogParser) SetParseTime(parseTime bool) {
	p.parseTime = parseTime
}

func (p *BinlogParser) SetTimestampStringLocation(timestampStringLocation *time.Location) {
	p.timestampStringLocation = timestampStringLocation
}

func (p *BinlogParser) SetUseDecimal(useDecimal bool) {
	p.useDecimal = useDecimal
}

func (p *BinlogParser) SetIgnoreJSONDecodeError(ignoreJSONDecodeErr bool) {
	p.ignoreJSONDecodeErr = ignoreJSONDecodeErr
}

func (p *BinlogParser) SetVerifyChecksum(verify bool) {
	p.verifyChecksum = verify
}

func (p *BinlogParser) SetFlavor(flavor string) {
	p.flavor = flavor
}

func (p *BinlogParser) parseHeader(data []byte) (*EventHeader, error) {
	h := new(EventHeader)
	err := h.Decode(data)
	if err != nil {
		return nil, err
	}

	return h, nil
}

func (p *BinlogParser) parseEvent(h *EventHeader, data []byte, rawData []byte) (Event, error) {
	var e Event

	if h.EventType == FORMAT_DESCRIPTION_EVENT {
		p.format = &FormatDescriptionEvent{}
		e = p.format
	} else {
		if p.format != nil && p.format.ChecksumAlgorithm == BINLOG_CHECKSUM_ALG_CRC32 {
			err := p.verifyCrc32Checksum(rawData)
			if err != nil {
				return nil, err
			}
			data = data[0 : len(data)-BinlogChecksumLength]
		}

		if h.EventType == ROTATE_EVENT {
			e = &RotateEvent{}
		} else if !p.rawMode {
			switch h.EventType {
			case QUERY_EVENT:
				e = &QueryEvent{}
			case XID_EVENT:
				e = &XIDEvent{}
			case TABLE_MAP_EVENT:
				te := &TableMapEvent{
					flavor: p.flavor,
				}
				if p.format.EventTypeHeaderLengths[TABLE_MAP_EVENT-1] == 6 {
					te.tableIDSize = 4
				} else {
					te.tableIDSize = 6
				}
				e = te
			case WRITE_ROWS_EVENTv0,
				UPDATE_ROWS_EVENTv0,
				DELETE_ROWS_EVENTv0,
				WRITE_ROWS_EVENTv1,
				DELETE_ROWS_EVENTv1,
				UPDATE_ROWS_EVENTv1,
				WRITE_ROWS_EVENTv2,
				UPDATE_ROWS_EVENTv2,
				DELETE_ROWS_EVENTv2:
				e = p.newRowsEvent(h)
			case ROWS_QUERY_EVENT:
				e = &RowsQueryEvent{}
			case GTID_EVENT:
				e = &GTIDEvent{}
			case ANONYMOUS_GTID_EVENT:
				e = &GTIDEvent{}
			case BEGIN_LOAD_QUERY_EVENT:
				e = &BeginLoadQueryEvent{}
			case EXECUTE_LOAD_QUERY_EVENT:
				e = &ExecuteLoadQueryEvent{}
			case MARIADB_ANNOTATE_ROWS_EVENT:
				e = &MariadbAnnotateRowsEvent{}
			case MARIADB_BINLOG_CHECKPOINT_EVENT:
				e = &MariadbBinlogCheckPointEvent{}
			case MARIADB_GTID_LIST_EVENT:
				e = &MariadbGTIDListEvent{}
			case MARIADB_GTID_EVENT:
				ee := &MariadbGTIDEvent{}
				ee.GTID.ServerID = h.ServerID
				e = ee
			case PREVIOUS_GTIDS_EVENT:
				e = &PreviousGTIDsEvent{}
			default:
				e = &GenericEvent{}
			}
		} else {
			e = &GenericEvent{}
		}
	}

	if err := e.Decode(data); err != nil {
		return nil, &EventError{h, err.Error(), data}
	}

	if te, ok := e.(*TableMapEvent); ok {
		p.tables[te.TableID] = te
	}

	if re, ok := e.(*RowsEvent); ok {
		if (re.Flags & RowsEventStmtEndFlag) > 0 {
			// Refer https://github.com/alibaba/canal/blob/38cc81b7dab29b51371096fb6763ca3a8432ffee/dbsync/src/main/java/com/taobao/tddl/dbsync/binlog/event/RowsLogEvent.java#L176
			p.tables = make(map[uint64]*TableMapEvent)
		}
	}

	return e, nil
}

// Parse: Given the bytes for a a binary log event: return the decoded event.
// With the exception of the FORMAT_DESCRIPTION_EVENT event type
// there must have previously been passed a FORMAT_DESCRIPTION_EVENT
// into the parser for this to work properly on any given event.
// Passing a new FORMAT_DESCRIPTION_EVENT into the parser will replace
// an existing one.
func (p *BinlogParser) Parse(data []byte) (*BinlogEvent, error) {
	rawData := data

	h, err := p.parseHeader(data)

	if err != nil {
		return nil, err
	}

	data = data[EventHeaderSize:]
	eventLen := int(h.EventSize) - EventHeaderSize

	if len(data) != eventLen {
		return nil, fmt.Errorf("invalid data size %d in event %s, less event length %d", len(data), h.EventType, eventLen)
	}

	e, err := p.parseEvent(h, data, rawData)
	if err != nil {
		return nil, err
	}

	return &BinlogEvent{RawData: rawData, Header: h, Event: e}, nil
}

func (p *BinlogParser) verifyCrc32Checksum(rawData []byte) error {
	if !p.verifyChecksum {
		return nil
	}

	calculatedPart := rawData[0 : len(rawData)-BinlogChecksumLength]
	expectedChecksum := rawData[len(rawData)-BinlogChecksumLength:]

	// mysql use zlib's CRC32 implementation, which uses polynomial 0xedb88320UL.
	// reference: https://github.com/madler/zlib/blob/master/crc32.c
	// https://github.com/madler/zlib/blob/master/doc/rfc1952.txt#L419
	checksum := crc32.ChecksumIEEE(calculatedPart)
	computed := make([]byte, BinlogChecksumLength)
	binary.LittleEndian.PutUint32(computed, checksum)
	if !bytes.Equal(expectedChecksum, computed) {
		return ErrChecksumMismatch
	}
	return nil
}

func (p *BinlogParser) newRowsEvent(h *EventHeader) *RowsEvent {
	e := &RowsEvent{}
	if p.format.EventTypeHeaderLengths[h.EventType-1] == 6 {
		e.tableIDSize = 4
	} else {
		e.tableIDSize = 6
	}

	e.needBitmap2 = false
	e.tables = p.tables
	e.parseTime = p.parseTime
	e.timestampStringLocation = p.timestampStringLocation
	e.useDecimal = p.useDecimal
	e.ignoreJSONDecodeErr = p.ignoreJSONDecodeErr

	switch h.EventType {
	case WRITE_ROWS_EVENTv0:
		e.Version = 0
	case UPDATE_ROWS_EVENTv0:
		e.Version = 0
	case DELETE_ROWS_EVENTv0:
		e.Version = 0
	case WRITE_ROWS_EVENTv1:
		e.Version = 1
	case DELETE_ROWS_EVENTv1:
		e.Version = 1
	case UPDATE_ROWS_EVENTv1:
		e.Version = 1
		e.needBitmap2 = true
	case WRITE_ROWS_EVENTv2:
		e.Version = 2
	case UPDATE_ROWS_EVENTv2:
		e.Version = 2
		e.needBitmap2 = true
	case DELETE_ROWS_EVENTv2:
		e.Version = 2
	}

	return e
}
