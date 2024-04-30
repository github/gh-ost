package replication

import (
	"context"
	"io"
	"os"
	"path"
	"time"

	. "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
)

// StartBackup: Like mysqlbinlog remote raw backup
// Backup remote binlog from position (filename, offset) and write in backupDir
func (b *BinlogSyncer) StartBackup(backupDir string, p Position, timeout time.Duration) error {
	err := os.MkdirAll(backupDir, 0755)
	if err != nil {
		return errors.Trace(err)
	}
	return b.StartBackupWithHandler(p, timeout, func(filename string) (io.WriteCloser, error) {
		return os.OpenFile(path.Join(backupDir, filename), os.O_CREATE|os.O_WRONLY, 0644)
	})
}

// StartBackupWithHandler starts the backup process for the binary log using the specified position and handler.
// The process will continue until the timeout is reached or an error occurs.
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

	// Force use raw mode
	b.parser.SetRawMode(true)

	s, err := b.StartSync(p)
	if err != nil {
		return errors.Trace(err)
	}

	var filename string
	var offset uint32

	var w io.WriteCloser
	defer func() {
		var closeErr error
		if w != nil {
			closeErr = w.Close()
		}
		if retErr == nil {
			retErr = closeErr
		}
	}()

	for {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		e, err := s.GetEvent(ctx)
		cancel()

		if err == context.DeadlineExceeded {
			return nil
		}

		if err != nil {
			return errors.Trace(err)
		}

		offset = e.Header.LogPos

		if e.Header.EventType == ROTATE_EVENT {
			rotateEvent := e.Event.(*RotateEvent)
			filename = string(rotateEvent.NextLogName)

			if e.Header.Timestamp == 0 || offset == 0 {
				// fake rotate event
				continue
			}
		} else if e.Header.EventType == FORMAT_DESCRIPTION_EVENT {
			// FormateDescriptionEvent is the first event in binlog, we will close old one and create a new

			if w != nil {
				if err = w.Close(); err != nil {
					w = nil
					return errors.Trace(err)
				}
			}

			if len(filename) == 0 {
				return errors.Errorf("empty binlog filename for FormateDescriptionEvent")
			}

			w, err = handler(filename)
			if err != nil {
				return errors.Trace(err)
			}

			// write binlog header fe'bin'
			if _, err = w.Write(BinLogFileHeader); err != nil {
				return errors.Trace(err)
			}
		}

		if n, err := w.Write(e.RawData); err != nil {
			return errors.Trace(err)
		} else if n != len(e.RawData) {
			return errors.Trace(io.ErrShortWrite)
		}
	}
}
