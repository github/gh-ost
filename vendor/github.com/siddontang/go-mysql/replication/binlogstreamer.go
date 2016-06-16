package replication

import (
	"time"

	"github.com/juju/errors"
)

var (
	ErrGetEventTimeout = errors.New("Get event timeout, try get later")
	ErrNeedSyncAgain   = errors.New("Last sync error or closed, try sync and get event again")
	ErrSyncClosed      = errors.New("Sync was closed")
)

type BinlogStreamer struct {
	ch  chan *BinlogEvent
	ech chan error
	err error
}

func (s *BinlogStreamer) GetEvent() (*BinlogEvent, error) {
	if s.err != nil {
		return nil, ErrNeedSyncAgain
	}

	select {
	case c := <-s.ch:
		return c, nil
	case s.err = <-s.ech:
		return nil, s.err
	}
}

// if timeout, ErrGetEventTimeout will returns
// timeout value won't be set too large, otherwise it may waste lots of memory
func (s *BinlogStreamer) GetEventTimeout(d time.Duration) (*BinlogEvent, error) {
	if s.err != nil {
		return nil, ErrNeedSyncAgain
	}

	select {
	case c := <-s.ch:
		return c, nil
	case s.err = <-s.ech:
		return nil, s.err
	case <-time.After(d):
		return nil, ErrGetEventTimeout
	}
}

func (s *BinlogStreamer) close() {
	s.closeWithError(ErrSyncClosed)
}

func (s *BinlogStreamer) closeWithError(err error) {
	if err == nil {
		err = ErrSyncClosed
	}
	select {
	case s.ech <- err:
	default:
	}
}

func newBinlogStreamer() *BinlogStreamer {
	s := new(BinlogStreamer)

	s.ch = make(chan *BinlogEvent, 1024)
	s.ech = make(chan error, 4)

	return s
}
