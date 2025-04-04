package packet

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/x509"
	"encoding/pem"
	goErrors "errors"
	"io"
	"net"
	"time"

	"github.com/go-mysql-org/go-mysql/compress"
	. "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/utils"
	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/errors"
)

const MinCompressionLength = 50
const DefaultBufferSize = 16 * 1024

// Conn is the base class to handle MySQL protocol.
type Conn struct {
	net.Conn

	readTimeout  time.Duration
	writeTimeout time.Duration

	// Buffered reader for net.Conn in Non-TLS connection only to address replication performance issue.
	// See https://github.com/go-mysql-org/go-mysql/pull/422 for more details.
	br     *bufio.Reader
	reader io.Reader

	copyNBuf []byte

	header [4]byte

	Sequence uint8

	Compression uint8

	CompressedSequence uint8

	compressedHeader [7]byte

	compressedReader io.Reader

	compressedReaderActive bool
}

func NewConn(conn net.Conn) *Conn {
	return NewBufferedConn(conn, 65536) // 64kb
}

func NewBufferedConn(conn net.Conn, bufferSize int) *Conn {
	c := new(Conn)
	c.Conn = conn

	c.br = bufio.NewReaderSize(c, bufferSize)
	c.reader = c.br

	c.copyNBuf = make([]byte, DefaultBufferSize)

	return c
}

func NewConnWithTimeout(conn net.Conn, readTimeout, writeTimeout time.Duration, bufferSize int) *Conn {
	c := NewBufferedConn(conn, bufferSize)
	c.readTimeout = readTimeout
	c.writeTimeout = writeTimeout
	return c
}

func NewTLSConn(conn net.Conn) *Conn {
	c := new(Conn)
	c.Conn = conn

	c.reader = c

	c.copyNBuf = make([]byte, DefaultBufferSize)

	return c
}

func NewTLSConnWithTimeout(conn net.Conn, readTimeout, writeTimeout time.Duration) *Conn {
	c := NewTLSConn(conn)
	c.readTimeout = readTimeout
	c.writeTimeout = writeTimeout
	return c
}

func (c *Conn) ReadPacket() ([]byte, error) {
	return c.ReadPacketReuseMem(nil)
}

func (c *Conn) ReadPacketReuseMem(dst []byte) ([]byte, error) {
	// Here we use `sync.Pool` to avoid allocate/destroy buffers frequently.
	buf := utils.BytesBufferGet()
	defer func() {
		utils.BytesBufferPut(buf)
	}()

	if c.Compression != MYSQL_COMPRESS_NONE {
		// it's possible that we're using compression but the server response with a compressed
		// packet with uncompressed length of 0. In this case we leave compressedReader nil. The
		// compressedReaderActive flag is important to track the state of the reader, allowing
		// for the compressedReader to be reset after a packet write. Without this flag, when a
		// compressed packet with uncompressed length of 0 is read, the compressedReader would
		// be nil, and we'd incorrectly attempt to read the next packet as compressed.
		if !c.compressedReaderActive {
			var err error
			c.compressedReader, err = c.newCompressedPacketReader()
			if err != nil {
				return nil, err
			}
			c.compressedReaderActive = true
		}
	}

	if err := c.ReadPacketTo(buf); err != nil {
		return nil, errors.Trace(err)
	}

	readBytes := buf.Bytes()
	readSize := len(readBytes)
	var result []byte
	if len(dst) > 0 {
		result = append(dst, readBytes...)
		// if read block is big, do not cache buf anymore
		if readSize > utils.TooBigBlockSize {
			buf = nil
		}
	} else {
		if readSize > utils.TooBigBlockSize {
			// if read block is big, use read block as result and do not cache buf anymore
			result = readBytes
			buf = nil
		} else {
			result = append(dst, readBytes...)
		}
	}

	return result, nil
}

// newCompressedPacketReader creates a new compressed packet reader.
func (c *Conn) newCompressedPacketReader() (io.Reader, error) {
	if c.readTimeout != 0 {
		if err := c.SetReadDeadline(utils.Now().Add(c.readTimeout)); err != nil {
			return nil, err
		}
	}
	if _, err := io.ReadFull(c.reader, c.compressedHeader[:7]); err != nil {
		return nil, errors.Wrapf(ErrBadConn, "io.ReadFull(compressedHeader) failed. err %v", err)
	}

	compressedSequence := c.compressedHeader[3]
	if compressedSequence != c.CompressedSequence {
		return nil, errors.Errorf("invalid compressed sequence %d != %d",
			compressedSequence, c.CompressedSequence)
	}

	compressedLength := int(uint32(c.compressedHeader[0]) | uint32(c.compressedHeader[1])<<8 | uint32(c.compressedHeader[2])<<16)
	uncompressedLength := int(uint32(c.compressedHeader[4]) | uint32(c.compressedHeader[5])<<8 | uint32(c.compressedHeader[6])<<16)
	if uncompressedLength > 0 {
		limitedReader := io.LimitReader(c.reader, int64(compressedLength))
		switch c.Compression {
		case MYSQL_COMPRESS_ZLIB:
			return compress.GetPooledZlibReader(limitedReader)
		case MYSQL_COMPRESS_ZSTD:
			return zstd.NewReader(limitedReader)
		}
	}

	return nil, nil
}

func (c *Conn) currentPacketReader() io.Reader {
	if c.Compression == MYSQL_COMPRESS_NONE || c.compressedReader == nil {
		return c.reader
	} else {
		return c.compressedReader
	}
}

func (c *Conn) copyN(dst io.Writer, n int64) (int64, error) {
	var written int64

	for n > 0 {
		bcap := cap(c.copyNBuf)
		if int64(bcap) > n {
			bcap = int(n)
		}
		buf := c.copyNBuf[:bcap]

		// Call ReadAtLeast with the currentPacketReader as it may change on every iteration
		// of this loop.
		if c.readTimeout != 0 {
			if err := c.SetReadDeadline(utils.Now().Add(c.readTimeout)); err != nil {
				return written, err
			}
		}
		rd, err := io.ReadAtLeast(c.currentPacketReader(), buf, bcap)

		n -= int64(rd)

		// ReadAtLeast will return EOF or ErrUnexpectedEOF when fewer than the min
		// bytes are read. In this case, and when we have compression then advance
		// the sequence number and reset the compressed reader to continue reading
		// the remaining bytes in the next compressed packet.
		if c.Compression != MYSQL_COMPRESS_NONE &&
			(goErrors.Is(err, io.ErrUnexpectedEOF) || goErrors.Is(err, io.EOF)) {
			// we have read to EOF and read an incomplete uncompressed packet
			// so advance the compressed sequence number and reset the compressed reader
			// to get the remaining unread uncompressed bytes from the next compressed packet.
			c.CompressedSequence++
			if c.compressedReader, err = c.newCompressedPacketReader(); err != nil {
				return written, errors.Trace(err)
			}
		}

		if err != nil {
			return written, errors.Trace(err)
		}

		// careful to only write from the buffer the number of bytes read
		wr, err := dst.Write(buf[:rd])
		written += int64(wr)
		if err != nil {
			return written, errors.Trace(err)
		}
	}

	return written, nil
}

func (c *Conn) ReadPacketTo(w io.Writer) error {
	b := utils.BytesBufferGet()
	defer func() {
		utils.BytesBufferPut(b)
	}()

	// packets that come in a compressed packet may be partial
	// so use the copyN function to read the packet header into a
	// buffer, since copyN is capable of getting the next compressed
	// packet and updating the Conn state with a new compressedReader.
	if _, err := c.copyN(b, 4); err != nil {
		return errors.Wrapf(ErrBadConn, "io.ReadFull(header) failed. err %v", err)
	} else {
		// copy was successful so copy the 4 bytes from the buffer to the header
		copy(c.header[:4], b.Bytes()[:4])
	}

	length := int(uint32(c.header[0]) | uint32(c.header[1])<<8 | uint32(c.header[2])<<16)
	sequence := c.header[3]

	if sequence != c.Sequence {
		return errors.Errorf("invalid sequence %d != %d", sequence, c.Sequence)
	}

	c.Sequence++

	if buf, ok := w.(*bytes.Buffer); ok {
		// Allocate the buffer with expected length directly instead of call `grow` and migrate data many times.
		buf.Grow(length)
	}

	if n, err := c.copyN(w, int64(length)); err != nil {
		return errors.Wrapf(ErrBadConn, "io.CopyN failed. err %v, copied %v, expected %v", err, n, length)
	} else if n != int64(length) {
		return errors.Wrapf(ErrBadConn, "io.CopyN failed(n != int64(length)). %v bytes copied, while %v expected", n, length)
	} else {
		if length < MaxPayloadLen {
			return nil
		}

		if err = c.ReadPacketTo(w); err != nil {
			return errors.Wrap(err, "ReadPacketTo failed")
		}
	}

	return nil
}

// WritePacket data already has 4 bytes header will modify data in-place
func (c *Conn) WritePacket(data []byte) error {
	length := len(data) - 4

	for length >= MaxPayloadLen {
		data[0] = 0xff
		data[1] = 0xff
		data[2] = 0xff

		data[3] = c.Sequence

		if n, err := c.writeWithTimeout(data[:4+MaxPayloadLen]); err != nil {
			return errors.Wrapf(ErrBadConn, "Write(payload portion) failed. err %v", err)
		} else if n != (4 + MaxPayloadLen) {
			return errors.Wrapf(ErrBadConn, "Write(payload portion) failed. only %v bytes written, while %v expected", n, 4+MaxPayloadLen)
		} else {
			c.Sequence++
			length -= MaxPayloadLen
			data = data[MaxPayloadLen:]
		}
	}

	data[0] = byte(length)
	data[1] = byte(length >> 8)
	data[2] = byte(length >> 16)
	data[3] = c.Sequence

	switch c.Compression {
	case MYSQL_COMPRESS_NONE:
		if n, err := c.writeWithTimeout(data); err != nil {
			return errors.Wrapf(ErrBadConn, "Write failed. err %v", err)
		} else if n != len(data) {
			return errors.Wrapf(ErrBadConn, "Write failed. only %v bytes written, while %v expected", n, len(data))
		}
	case MYSQL_COMPRESS_ZLIB, MYSQL_COMPRESS_ZSTD:
		if n, err := c.writeCompressed(data); err != nil {
			return errors.Wrapf(ErrBadConn, "Write failed. err %v", err)
		} else if n != len(data) {
			return errors.Wrapf(ErrBadConn, "Write failed. only %v bytes written, while %v expected", n, len(data))
		}

		c.compressedReaderActive = false
		if c.compressedReader != nil {
			if _, ok := c.compressedReader.(io.ReadCloser); ok {
				_ = c.compressedReader.(io.ReadCloser).Close()
			}
			c.compressedReader = nil
		}
	default:
		return errors.Wrapf(ErrBadConn, "Write failed. Unsuppored compression algorithm set")
	}

	c.Sequence++
	return nil
}

func (c *Conn) writeWithTimeout(b []byte) (n int, err error) {
	if c.writeTimeout != 0 {
		if err := c.SetWriteDeadline(utils.Now().Add(c.writeTimeout)); err != nil {
			return n, err
		}
	}

	return c.Write(b)
}

func (c *Conn) writeCompressed(data []byte) (n int, err error) {
	var (
		compressedLength, uncompressedLength int
		payload                              *bytes.Buffer
		compressedHeader                     = make([]byte, 7)
	)

	if len(data) > MinCompressionLength {
		var w io.WriteCloser
		payload = utils.BytesBufferGet()
		defer utils.BytesBufferPut(payload)

		switch c.Compression {
		case MYSQL_COMPRESS_ZLIB:
			w, err = compress.GetPooledZlibWriter(payload)
		case MYSQL_COMPRESS_ZSTD:
			w, err = zstd.NewWriter(payload)
		default:
			return 0, errors.Wrapf(ErrBadConn, "Write failed. Unsuppored compression algorithm set")
		}
		if err != nil {
			return 0, err
		}

		uncompressedLength = len(data)
		if n, err = w.Write(data); err != nil {
			_ = w.Close()
			return 0, err
		}
		if err = w.Close(); err != nil {
			return 0, err
		}

		compressedLength = payload.Len()
	} else {
		compressedLength = len(data)
	}

	c.CompressedSequence = 0
	// write the compressed packet header
	compressedPacket := utils.BytesBufferGet()
	defer utils.BytesBufferPut(compressedPacket)

	compressedHeader[0] = byte(compressedLength)
	compressedHeader[1] = byte(compressedLength >> 8)
	compressedHeader[2] = byte(compressedLength >> 16)
	compressedHeader[3] = c.CompressedSequence
	compressedHeader[4] = byte(uncompressedLength)
	compressedHeader[5] = byte(uncompressedLength >> 8)
	compressedHeader[6] = byte(uncompressedLength >> 16)
	if _, err = compressedPacket.Write(compressedHeader); err != nil {
		return 0, err
	}
	c.CompressedSequence++

	if payload != nil {
		_, err = compressedPacket.Write(payload.Bytes())
	} else {
		n, err = compressedPacket.Write(data)
	}

	if err != nil {
		return 0, err
	}
	if _, err = c.writeWithTimeout(compressedPacket.Bytes()); err != nil {
		return 0, err
	}

	return n, nil
}

// WriteClearAuthPacket Client clear text authentication packet
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_switch_response.html
func (c *Conn) WriteClearAuthPacket(password string) error {
	// Calculate the packet length and add a tailing 0
	pktLen := len(password) + 1
	data := make([]byte, 4+pktLen)

	// Add the clear password [null terminated string]
	copy(data[4:], password)
	data[4+pktLen-1] = 0x00

	return errors.Wrap(c.WritePacket(data), "WritePacket failed")
}

// WritePublicKeyAuthPacket Caching sha2 authentication. Public key request and send encrypted password
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_switch_response.html
func (c *Conn) WritePublicKeyAuthPacket(password string, cipher []byte) error {
	// request public key
	data := make([]byte, 4+1)
	data[4] = 2 // cachingSha2PasswordRequestPublicKey
	if err := c.WritePacket(data); err != nil {
		return errors.Wrap(err, "WritePacket(single byte) failed")
	}

	data, err := c.ReadPacket()
	if err != nil {
		return errors.Wrap(err, "ReadPacket failed")
	}

	block, _ := pem.Decode(data[1:])
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return errors.Wrap(err, "x509.ParsePKIXPublicKey failed")
	}

	plain := make([]byte, len(password)+1)
	copy(plain, password)
	for i := range plain {
		j := i % len(cipher)
		plain[i] ^= cipher[j]
	}
	sha1v := sha1.New()
	enc, _ := rsa.EncryptOAEP(sha1v, rand.Reader, pub.(*rsa.PublicKey), plain, nil)
	data = make([]byte, 4+len(enc))
	copy(data[4:], enc)
	return errors.Wrap(c.WritePacket(data), "WritePacket failed")
}

func (c *Conn) WriteEncryptedPassword(password string, seed []byte, pub *rsa.PublicKey) error {
	enc, err := EncryptPassword(password, seed, pub)
	if err != nil {
		return errors.Wrap(err, "EncryptPassword failed")
	}
	return errors.Wrap(c.WriteAuthSwitchPacket(enc, false), "WriteAuthSwitchPacket failed")
}

// WriteAuthSwitchPacket see https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_switch_response.html
func (c *Conn) WriteAuthSwitchPacket(authData []byte, addNUL bool) error {
	pktLen := 4 + len(authData)
	if addNUL {
		pktLen++
	}
	data := make([]byte, pktLen)

	// Add the auth data [EOF]
	copy(data[4:], authData)
	if addNUL {
		data[pktLen-1] = 0x00
	}

	return errors.Wrap(c.WritePacket(data), "WritePacket failed")
}

func (c *Conn) ResetSequence() {
	c.Sequence = 0
}

func (c *Conn) Close() error {
	c.Sequence = 0
	if c.Conn != nil {
		return errors.Wrap(c.Conn.Close(), "Conn.Close failed")
	}
	return nil
}
