package mysql

import (
	"context"
	"sync"
)

// StreamResult provides a streaming interface for sending query results row by row.
// It is designed for scenarios where the result set is too large to fit in memory
// or when results need to be sent incrementally as they are generated.
// Usage pattern:
//
//	sr := mysql.NewStreamResult(fields, bufSize, binary)
//	go func() {
//	    defer sr.Close()  // Always close when done
//	    for row := range rows {
//	        if !sr.WriteRow(ctx, row) {
//	            return  // Context canceled, exit early
//	        }
//	    }
//	}()
//	err := conn.WriteValue(sr.AsResult())
//
// IMPORTANT: The producer MUST NOT call WriteRow() after Close() has been called.
// Doing so will result in a panic (send on closed channel). Use "defer sr.Close()"
// to ensure Close() is called after all WriteRow() calls complete.
type StreamResult struct {
	// Fields contains the column metadata for the result set.
	Fields []*Field
	// Binary indicates whether to use binary protocol (true) or text protocol (false).
	Binary bool
	// rowsChan is the internal channel for streaming rows.
	rowsChan chan []any
	// done is used to signal that the stream has been closed.
	done chan struct{}

	// mu protects the close operation and err field.
	mu sync.Mutex
	// err stores any error that occurred during streaming.
	err error
}

// NewStreamResult creates a new StreamResult with the specified fields and buffer size.
// The bufSize parameter controls the channel buffer size. A bufSize of 0 creates an
// unbuffered channel for synchronous communication.
func NewStreamResult(fields []*Field, bufSize int, binary bool) *StreamResult {
	return &StreamResult{
		Fields:   fields,
		rowsChan: make(chan []any, bufSize),
		done:     make(chan struct{}),
		Binary:   binary,
	}
}

// RowsChan returns a read-only channel for consuming rows.
// The channel is closed when Close() is called on the StreamResult.
// Consumers should range over this channel to receive all rows.
func (sr *StreamResult) RowsChan() <-chan []any {
	return sr.rowsChan
}

// WriteRow sends a row to the stream. It returns true if the row was successfully
// written, or false if the context is canceled.
// This method will block if the channel buffer is full.
//
// IMPORTANT: WriteRow MUST NOT be called after Close() has been called on the same
// StreamResult. Doing so will cause a panic.
func (sr *StreamResult) WriteRow(ctx context.Context, row []any) (ok bool) {
	select {
	case <-ctx.Done():
		return false
	case <-sr.done:
		return false
	case sr.rowsChan <- row:
		return true
	}
}

// SetError records an error that occurred during streaming.
// The consumer can retrieve this error by calling Err() after the channel is closed.
// This is useful for propagating errors from the producer to the consumer.
func (sr *StreamResult) SetError(err error) {
	sr.mu.Lock()
	sr.err = err
	sr.mu.Unlock()
}

// Err returns any error that was set via SetError().
// This should typically be called after the RowsChan() channel is closed
// to check if the stream completed successfully.
func (sr *StreamResult) Err() error {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	return sr.err
}

// Close closes the stream and signals to consumers that no more rows will be sent.
// This method is idempotent and safe to call multiple times.
//
// IMPORTANT: Close MUST only be called after all WriteRow() calls have completed.
// Calling WriteRow() after Close() will cause a panic. The recommended pattern is:
//
//	go func() {
//	    defer sr.Close()
//	    // ... all WriteRow calls here ...
//	}()
func (sr *StreamResult) Close() {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if sr.IsClosed() {
		return
	}

	close(sr.done)
	close(sr.rowsChan)
}

// IsClosed returns true if the stream has been closed.
func (sr *StreamResult) IsClosed() bool {
	select {
	case <-sr.done:
		return true
	default:
		return false
	}
}

// AsResult wraps the StreamResult in a Result struct for use with the MySQL protocol.
// The returned Result will have IsStreaming() return true.
func (sr *StreamResult) AsResult() *Result {
	return &Result{
		StreamResult: sr,
	}
}
