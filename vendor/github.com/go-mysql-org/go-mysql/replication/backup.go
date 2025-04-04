package replication

import (
	"context"
	"io"
	"os"
	"path"
	"sync"
	"time"

	. "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
)

// StartBackup starts the backup process for the binary log and writes to the backup directory.
func (b *BinlogSyncer) StartBackup(backupDir string, p Position, timeout time.Duration) error {
	err := os.MkdirAll(backupDir, 0755)
	if err != nil {
		return errors.Trace(err)
	}
	if b.cfg.SynchronousEventHandler == nil {
		return b.StartBackupWithHandler(p, timeout, func(filename string) (io.WriteCloser, error) {
			return os.OpenFile(path.Join(backupDir, filename), os.O_CREATE|os.O_WRONLY, 0644)
		})
	} else {
		return b.StartSynchronousBackup(p, timeout)
	}
}

// StartBackupWithHandler starts the backup process for the binary log using the specified position and handler.
// The process will continue until the timeout is reached or an error occurs.
// This method should not be used together with SynchronousEventHandler.
//
// Parameters:
//   - p: The starting position in the binlog from which to begin the backup.
//   - timeout: The maximum duration to wait for new binlog events before stopping the backup process.
//     If set to 0, a default very long timeout (30 days) is used instead.
//   - handler: A function that takes a binlog filename and returns an WriteCloser for writing raw events to.
func (b *BinlogSyncer) StartBackupWithHandler(p Position, timeout time.Duration,
	handler func(binlogFilename string) (io.WriteCloser, error)) (retErr error) {
	if timeout == 0 {
		// a very long timeout here
		timeout = 30 * 3600 * 24 * time.Second
	}
	if b.cfg.SynchronousEventHandler != nil {
		return errors.New("StartBackupWithHandler cannot be used when SynchronousEventHandler is set. Use StartSynchronousBackup instead.")
	}

	// Force use raw mode
	b.parser.SetRawMode(true)

	// Set up the backup event handler
	backupHandler := &BackupEventHandler{
		handler: handler,
	}

	s, err := b.StartSync(p)
	if err != nil {
		return errors.Trace(err)
	}

	defer func() {
		if backupHandler.w != nil {
			closeErr := backupHandler.w.Close()
			if retErr == nil {
				retErr = closeErr
			}
		}
	}()

	for {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		select {
		case <-ctx.Done():
			return nil
		case <-b.ctx.Done():
			return nil
		case err := <-s.ech:
			return errors.Trace(err)
		case e := <-s.ch:
			err = backupHandler.HandleEvent(e)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
}

// StartSynchronousBackup starts the backup process using the SynchronousEventHandler in the BinlogSyncerConfig.
func (b *BinlogSyncer) StartSynchronousBackup(p Position, timeout time.Duration) error {
	if b.cfg.SynchronousEventHandler == nil {
		return errors.New("SynchronousEventHandler must be set in BinlogSyncerConfig to use StartSynchronousBackup")
	}

	s, err := b.StartSync(p)
	if err != nil {
		return errors.Trace(err)
	}

	var ctx context.Context
	var cancel context.CancelFunc

	if timeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
		defer cancel()
	} else {
		ctx = context.Background()
	}

	select {
	case <-ctx.Done():
		// The timeout has been reached
		return nil
	case <-b.ctx.Done():
		// The BinlogSyncer has been closed
		return nil
	case err := <-s.ech:
		// An error occurred during streaming
		return errors.Trace(err)
	}
}

// BackupEventHandler handles writing events for backup
type BackupEventHandler struct {
	handler func(binlogFilename string) (io.WriteCloser, error)
	w       io.WriteCloser
	mutex   sync.Mutex

	filename string
}

func NewBackupEventHandler(handlerFunction func(filename string) (io.WriteCloser, error)) *BackupEventHandler {
	return &BackupEventHandler{
		handler: handlerFunction,
	}
}

// HandleEvent processes a single event for the backup.
func (h *BackupEventHandler) HandleEvent(e *BinlogEvent) error {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	var err error
	offset := e.Header.LogPos

	if e.Header.EventType == ROTATE_EVENT {
		rotateEvent := e.Event.(*RotateEvent)
		h.filename = string(rotateEvent.NextLogName)
		if e.Header.Timestamp == 0 || offset == 0 {
			// fake rotate event
			return nil
		}
	} else if e.Header.EventType == FORMAT_DESCRIPTION_EVENT {
		if h.w != nil {
			if err = h.w.Close(); err != nil {
				h.w = nil
				return errors.Trace(err)
			}
		}

		if len(h.filename) == 0 {
			return errors.Errorf("empty binlog filename for FormatDescriptionEvent")
		}

		h.w, err = h.handler(h.filename)
		if err != nil {
			return errors.Trace(err)
		}

		// Write binlog header 0xfebin
		_, err = h.w.Write(BinLogFileHeader)
		if err != nil {
			return errors.Trace(err)
		}
	}

	if h.w != nil {
		n, err := h.w.Write(e.RawData)
		if err != nil {
			return errors.Trace(err)
		}
		if n != len(e.RawData) {
			return errors.Trace(io.ErrShortWrite)
		}
	} else {
		return errors.New("writer is not initialized")
	}

	return nil
}
