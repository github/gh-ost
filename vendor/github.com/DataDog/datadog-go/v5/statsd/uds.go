//go:build !windows
// +build !windows

package statsd

import (
	"encoding/binary"
	"net"
	"strings"
	"sync"
	"time"
)

// udsWriter is an internal class wrapping around management of UDS connection
type udsWriter struct {
	// Address to send metrics to, needed to allow reconnection on error
	addr string
	// Transport used
	transport string
	// Established connection object, or nil if not connected yet
	conn net.Conn
	// write timeout
	writeTimeout time.Duration
	// connect timeout
	connectTimeout time.Duration
	sync.RWMutex   // used to lock conn / writer can replace it
}

// newUDSWriter returns a pointer to a new udsWriter given a socket file path as addr.
func newUDSWriter(addr string, writeTimeout time.Duration, connectTimeout time.Duration, transport string) (*udsWriter, error) {
	// Defer connection to first Write
	writer := &udsWriter{addr: addr, transport: transport, conn: nil, writeTimeout: writeTimeout, connectTimeout: connectTimeout}
	return writer, nil
}

// GetTransportName returns the transport used by the writer
func (w *udsWriter) GetTransportName() string {
	w.RLock()
	defer w.RUnlock()

	if w.transport == "unix" {
		return writerNameUDSStream
	} else {
		return writerNameUDS
	}
}

func (w *udsWriter) shouldCloseConnection(err error, partialWrite bool) bool {
	if err != nil && partialWrite {
		// We can't recover from a partial write
		return true
	}
	if err, isNetworkErr := err.(net.Error); err != nil && (!isNetworkErr || !err.Timeout()) {
		// Statsd server disconnected, retry connecting at next packet
		return true
	}
	return false
}

// Write data to the UDS connection with write timeout and minimal error handling:
// create the connection if nil, and destroy it if the statsd server has disconnected
func (w *udsWriter) Write(data []byte) (int, error) {
	var n int
	partialWrite := false
	conn, err := w.ensureConnection()
	if err != nil {
		return 0, err
	}
	stream := conn.LocalAddr().Network() == "unix"

	// When using streams the deadline will only make us drop the packet if we can't write it at all,
	// once we've started writing we need to finish.
	conn.SetWriteDeadline(time.Now().Add(w.writeTimeout))

	// When using streams, we append the length of the packet to the data
	if stream {
		bs := []byte{0, 0, 0, 0}
		binary.LittleEndian.PutUint32(bs, uint32(len(data)))
		_, err = w.conn.Write(bs)

		partialWrite = true

		// W need to be able to finish to write partially written packets once we have started.
		// But we will reset the connection if we can't write anything at all for a long time.
		w.conn.SetWriteDeadline(time.Now().Add(w.connectTimeout))

		// Continue writing only if we've written the length of the packet
		if err == nil {
			n, err = w.conn.Write(data)
			if err == nil {
				partialWrite = false
			}
		}
	} else {
		n, err = w.conn.Write(data)
	}

	if w.shouldCloseConnection(err, partialWrite) {
		w.unsetConnection()
	}
	return n, err
}

func (w *udsWriter) Close() error {
	if w.conn != nil {
		return w.conn.Close()
	}
	return nil
}

func (w *udsWriter) tryToDial(network string) (net.Conn, error) {
	udsAddr, err := net.ResolveUnixAddr(network, w.addr)
	if err != nil {
		return nil, err
	}

	// Try to gracefully reconnect to the socket when we encounter "connection refused", as it's likely that the Agent
	// is restarting and the socket is not yet available.
	connectAttemptsLeft := 3
	connectDeadline := time.Now().Add(w.connectTimeout)

	// Calculate the backoff time for connection refused errors, but don't exceed one second: this means we won't waste
	// longer than 1 seconds worth of time if the socket becomes available immediately after our last connect attempt
	connRefusedBackoff := w.connectTimeout / time.Duration(connectAttemptsLeft+1)
	if connRefusedBackoff > time.Second {
		connRefusedBackoff = time.Second
	}

	for {
		connectAttemptsLeft--

		perCallTimeout := time.Until(connectDeadline)
		newConn, err := net.DialTimeout(udsAddr.Network(), udsAddr.String(), perCallTimeout)
		if err != nil {
			if strings.HasSuffix(err.Error(), "connection refused") && connectAttemptsLeft > 0 {
				// If we get a connection refused error, we need to wait a bit before trying again.
				time.Sleep(connRefusedBackoff)
				continue
			}
			return nil, err
		}
		return newConn, nil
	}
}

func (w *udsWriter) ensureConnection() (net.Conn, error) {
	// Check if we've already got a socket we can use
	w.RLock()
	currentConn := w.conn
	w.RUnlock()

	if currentConn != nil {
		return currentConn, nil
	}

	// Looks like we might need to connect - try again with write locking.
	w.Lock()
	defer w.Unlock()
	if w.conn != nil {
		return w.conn, nil
	}

	var newConn net.Conn
	var err error

	// Try to guess the transport if not specified.
	if w.transport == "" {
		newConn, err = w.tryToDial("unixgram")
		// try to connect with unixgram failed, try again with unix streams.
		if err != nil && strings.Contains(err.Error(), "protocol wrong type for socket") {
			newConn, err = w.tryToDial("unix")
		}
	} else {
		newConn, err = w.tryToDial(w.transport)
	}

	if err != nil {
		return nil, err
	}
	w.conn = newConn
	w.transport = newConn.RemoteAddr().Network()
	return newConn, nil
}

func (w *udsWriter) unsetConnection() {
	w.Lock()
	defer w.Unlock()
	_ = w.conn.Close()
	w.conn = nil
}
