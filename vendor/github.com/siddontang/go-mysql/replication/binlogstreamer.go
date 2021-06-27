package replication

import (
	"context"
	"github.com/pingcap/errors"
	"github.com/siddontang/go-log/log"
	"time"
)

var (
	ErrNeedSyncAgain = errors.New("Last sync error or closed, try sync and get event again")
	ErrSyncClosed    = errors.New("Sync was closed")
)

// BinlogStreamer gets the streaming event.
type BinlogStreamer struct {
	ch  chan *BinlogEvent
	ech chan error
	err error
}

// GetEvent gets the binlog event one by one, it will block until Syncer receives any events from MySQL
// or meets a sync error. You can pass a context (like Cancel or Timeout) to break the block.
func (s *BinlogStreamer) GetEvent(ctx context.Context) (*BinlogEvent, error) {
	if s.err != nil {
		return nil, ErrNeedSyncAgain
	}

	select {
	case c := <-s.ch:
		return c, nil
	case s.err = <-s.ech:
		return nil, s.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// GetEventWithStartTime gets the binlog event with starttime, if current binlog event timestamp smaller than specify starttime
// return nil event
func (s *BinlogStreamer) GetEventWithStartTime(ctx context.Context, startTime time.Time) (*BinlogEvent, error) {
	if s.err != nil {
		return nil, ErrNeedSyncAgain
	}
	startUnix := startTime.Unix()
	select {
	case c := <-s.ch:
		if int64(c.Header.Timestamp) >= startUnix {
			return c, nil
		}
		return nil, nil
	case s.err = <-s.ech:
		return nil, s.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// DumpEvents dumps all left events
func (s *BinlogStreamer) DumpEvents() []*BinlogEvent {
	count := len(s.ch)
	events := make([]*BinlogEvent, 0, count)
	for i := 0; i < count; i++ {
		events = append(events, <-s.ch)
	}
	return events
}

func (s *BinlogStreamer) close() {
	s.closeWithError(nil)
}

func (s *BinlogStreamer) closeWithError(err error) {
	if err == nil {
		err = ErrSyncClosed
	} else {
		log.Errorf("close sync with err: %v", err)
	}

	select {
	case s.ech <- err:
	default:
	}
}

func newBinlogStreamer() *BinlogStreamer {
	s := new(BinlogStreamer)

	s.ch = make(chan *BinlogEvent, 10240)
	s.ech = make(chan error, 4)

	return s
}
