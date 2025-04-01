package replication

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"

	. "github.com/go-mysql-org/go-mysql/mysql"
)

// On The Wire: Field Types
// See also binary_log::codecs::binary::Transaction_payload::fields in MySQL
// https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1codecs_1_1binary_1_1Transaction__payload.html#a9fff7ac12ba064f40e9216565c53d07b
const (
	OTW_PAYLOAD_HEADER_END_MARK = iota
	OTW_PAYLOAD_SIZE_FIELD
	OTW_PAYLOAD_COMPRESSION_TYPE_FIELD
	OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD
)

// Compression Types
const (
	ZSTD = 0
	NONE = 255
)

type TransactionPayloadEvent struct {
	format           FormatDescriptionEvent
	Size             uint64
	UncompressedSize uint64
	CompressionType  uint64
	Payload          []byte
	Events           []*BinlogEvent
}

func (e *TransactionPayloadEvent) compressionType() string {
	switch e.CompressionType {
	case ZSTD:
		return "ZSTD"
	case NONE:
		return "NONE"
	default:
		return "Unknown"
	}
}

func (e *TransactionPayloadEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "Payload Size: %d\n", e.Size)
	fmt.Fprintf(w, "Payload Uncompressed Size: %d\n", e.UncompressedSize)
	fmt.Fprintf(w, "Payload CompressionType: %s\n", e.compressionType())
	fmt.Fprintf(w, "Payload Body: \n%s", hex.Dump(e.Payload))
	fmt.Fprintln(w, "=== Start of events decoded from compressed payload ===")
	for _, event := range e.Events {
		event.Dump(w)
	}
	fmt.Fprintln(w, "=== End of events decoded from compressed payload ===")
	fmt.Fprintln(w)
}

func (e *TransactionPayloadEvent) Decode(data []byte) error {
	err := e.decodeFields(data)
	if err != nil {
		return err
	}
	return e.decodePayload()
}

func (e *TransactionPayloadEvent) decodeFields(data []byte) error {
	offset := uint64(0)

	for {
		fieldType := FixedLengthInt(data[offset : offset+1])
		offset++

		if fieldType == OTW_PAYLOAD_HEADER_END_MARK {
			e.Payload = data[offset:]
			break
		} else {
			fieldLength := FixedLengthInt(data[offset : offset+1])
			offset++

			switch fieldType {
			case OTW_PAYLOAD_SIZE_FIELD:
				e.Size = FixedLengthInt(data[offset : offset+fieldLength])
			case OTW_PAYLOAD_COMPRESSION_TYPE_FIELD:
				e.CompressionType = FixedLengthInt(data[offset : offset+fieldLength])
			case OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD:
				e.UncompressedSize = FixedLengthInt(data[offset : offset+fieldLength])
			}

			offset += fieldLength
		}
	}

	return nil
}

func (e *TransactionPayloadEvent) decodePayload() error {
	if e.CompressionType != ZSTD {
		return fmt.Errorf("TransactionPayloadEvent has compression type %d (%s)",
			e.CompressionType, e.compressionType())
	}

	var decoder, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
	if err != nil {
		return err
	}
	defer decoder.Close()

	payloadUncompressed, err := decoder.DecodeAll(e.Payload, nil)
	if err != nil {
		return err
	}

	// The uncompressed data needs to be split up into individual events for Parse()
	// to work on them. We can't use e.parser directly as we need to disable checksums
	// but we still need the initialization from the FormatDescriptionEvent. We can't
	// modify e.parser as it is used elsewhere.
	parser := NewBinlogParser()
	parser.format = &FormatDescriptionEvent{
		Version:                e.format.Version,
		ServerVersion:          e.format.ServerVersion,
		CreateTimestamp:        e.format.CreateTimestamp,
		EventHeaderLength:      e.format.EventHeaderLength,
		EventTypeHeaderLengths: e.format.EventTypeHeaderLengths,
		ChecksumAlgorithm:      BINLOG_CHECKSUM_ALG_OFF,
	}

	offset := uint32(0)
	for {
		payloadUncompressedLength := uint32(len(payloadUncompressed))
		if offset+13 > payloadUncompressedLength {
			break
		}
		eventLength := binary.LittleEndian.Uint32(payloadUncompressed[offset+9 : offset+13])
		if offset+eventLength > payloadUncompressedLength {
			return fmt.Errorf("Event length of %d with offset %d in uncompressed payload exceeds payload length of %d",
				eventLength, offset, payloadUncompressedLength)
		}
		data := payloadUncompressed[offset : offset+eventLength]

		pe, err := parser.Parse(data)
		if err != nil {
			return err
		}
		e.Events = append(e.Events, pe)

		offset += eventLength
	}

	return nil
}
