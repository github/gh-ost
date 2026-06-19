package statsd

import (
	"io"
	"sync/atomic"
)

// senderTelemetry contains telemetry about the health of the sender
type senderTelemetry struct {
	totalPayloadsSent             uint64
	totalPayloadsDroppedQueueFull uint64
	totalPayloadsDroppedWriter    uint64
	totalBytesSent                uint64
	totalBytesDroppedQueueFull    uint64
	totalBytesDroppedWriter       uint64
}

type Transport interface {
	io.WriteCloser

	// GetTransportName returns the name of the transport
	GetTransportName() string
}

type sender struct {
	transport    Transport
	pool         *bufferPool
	queue        chan *statsdBuffer
	telemetry    *senderTelemetry
	stop         chan struct{}
	flushSignal  chan struct{}
	errorHandler ErrorHandler
}

type ErrorSenderChannelFull struct {
	LostElements int
	ChannelSize  int
	Msg          string
}

func (e *ErrorSenderChannelFull) Error() string {
	return e.Msg
}

func newSender(transport Transport, queueSize int, pool *bufferPool, errorHandler ErrorHandler) *sender {
	sender := &sender{
		transport:    transport,
		pool:         pool,
		queue:        make(chan *statsdBuffer, queueSize),
		telemetry:    &senderTelemetry{},
		stop:         make(chan struct{}),
		flushSignal:  make(chan struct{}),
		errorHandler: errorHandler,
	}

	go sender.sendLoop()
	return sender
}

func (s *sender) send(buffer *statsdBuffer) {
	select {
	case s.queue <- buffer:
	default:
		if s.errorHandler != nil {
			err := &ErrorSenderChannelFull{
				LostElements: buffer.elementCount,
				ChannelSize:  len(s.queue),
				Msg:          "Sender queue is full",
			}
			s.errorHandler(err)
		}
		atomic.AddUint64(&s.telemetry.totalPayloadsDroppedQueueFull, 1)
		atomic.AddUint64(&s.telemetry.totalBytesDroppedQueueFull, uint64(len(buffer.bytes())))
		s.pool.returnBuffer(buffer)
	}
}

func (s *sender) write(buffer *statsdBuffer) {
	_, err := s.transport.Write(buffer.bytes())
	if err != nil {
		atomic.AddUint64(&s.telemetry.totalPayloadsDroppedWriter, 1)
		atomic.AddUint64(&s.telemetry.totalBytesDroppedWriter, uint64(len(buffer.bytes())))
		if s.errorHandler != nil {
			s.errorHandler(err)
		}
	} else {
		atomic.AddUint64(&s.telemetry.totalPayloadsSent, 1)
		atomic.AddUint64(&s.telemetry.totalBytesSent, uint64(len(buffer.bytes())))
	}
	s.pool.returnBuffer(buffer)
}

func (s *sender) flushTelemetryMetrics(t *Telemetry) {
	t.TotalPayloadsSent = atomic.LoadUint64(&s.telemetry.totalPayloadsSent)
	t.TotalPayloadsDroppedQueueFull = atomic.LoadUint64(&s.telemetry.totalPayloadsDroppedQueueFull)
	t.TotalPayloadsDroppedWriter = atomic.LoadUint64(&s.telemetry.totalPayloadsDroppedWriter)

	t.TotalBytesSent = atomic.LoadUint64(&s.telemetry.totalBytesSent)
	t.TotalBytesDroppedQueueFull = atomic.LoadUint64(&s.telemetry.totalBytesDroppedQueueFull)
	t.TotalBytesDroppedWriter = atomic.LoadUint64(&s.telemetry.totalBytesDroppedWriter)
}

func (s *sender) sendLoop() {
	defer close(s.stop)
	for {
		select {
		case buffer := <-s.queue:
			s.write(buffer)
		case <-s.stop:
			return
		case <-s.flushSignal:
			// At that point we know that the workers are paused (the statsd client
			// will pause them before calling sender.flush()).
			// So we can fully flush the input queue
			s.flushInputQueue()
			s.flushSignal <- struct{}{}
		}
	}
}

func (s *sender) flushInputQueue() {
	for {
		select {
		case buffer := <-s.queue:
			s.write(buffer)
		default:
			return
		}
	}
}
func (s *sender) flush() {
	s.flushSignal <- struct{}{}
	<-s.flushSignal
}

func (s *sender) close() error {
	s.stop <- struct{}{}
	<-s.stop
	s.flushInputQueue()
	return s.transport.Close()
}

func (s *sender) getTransportName() string {
	return s.transport.GetTransportName()
}
