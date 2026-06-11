package statsd

import (
	"net"
	"time"
)

// udpWriter is an internal class wrapping around management of UDP connection
type udpWriter struct {
	conn net.Conn
}

// New returns a pointer to a new udpWriter given an addr in the format "hostname:port".
func newUDPWriter(addr string, _ time.Duration) (*udpWriter, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return nil, err
	}
	writer := &udpWriter{conn: conn}
	return writer, nil
}

// Write data to the UDP connection with no error handling
func (w *udpWriter) Write(data []byte) (int, error) {
	return w.conn.Write(data)
}

func (w *udpWriter) Close() error {
	return w.conn.Close()
}

// GetTransportName returns the transport used by the sender
func (w *udpWriter) GetTransportName() string {
	return writerNameUDP
}
