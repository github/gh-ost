package wait

import (
	"context"
	"io"
	"regexp"
	"strings"
	"time"
)

// Implement interface
var (
	_ Strategy        = (*LogStrategy)(nil)
	_ StrategyTimeout = (*LogStrategy)(nil)
)

// LogStrategy will wait until a given log entry shows up in the docker logs
type LogStrategy struct {
	// all Strategies should have a startupTimeout to avoid waiting infinitely
	timeout *time.Duration

	// additional properties
	Log          string
	IsRegexp     bool
	Occurrence   int
	PollInterval time.Duration
}

// NewLogStrategy constructs with polling interval of 100 milliseconds and startup timeout of 60 seconds by default
func NewLogStrategy(log string) *LogStrategy {
	return &LogStrategy{
		Log:          log,
		IsRegexp:     false,
		Occurrence:   1,
		PollInterval: defaultPollInterval(),
	}
}

// fluent builders for each property
// since go has neither covariance nor generics, the return type must be the type of the concrete implementation
// this is true for all properties, even the "shared" ones like startupTimeout

// AsRegexp can be used to change the default behavior of the log strategy to use regexp instead of plain text
func (ws *LogStrategy) AsRegexp() *LogStrategy {
	ws.IsRegexp = true
	return ws
}

// WithStartupTimeout can be used to change the default startup timeout
func (ws *LogStrategy) WithStartupTimeout(timeout time.Duration) *LogStrategy {
	ws.timeout = &timeout
	return ws
}

// WithPollInterval can be used to override the default polling interval of 100 milliseconds
func (ws *LogStrategy) WithPollInterval(pollInterval time.Duration) *LogStrategy {
	ws.PollInterval = pollInterval
	return ws
}

func (ws *LogStrategy) WithOccurrence(o int) *LogStrategy {
	// the number of occurrence needs to be positive
	if o <= 0 {
		o = 1
	}
	ws.Occurrence = o
	return ws
}

// ForLog is the default construction for the fluid interface.
//
// For Example:
//
//	wait.
//		ForLog("some text").
//		WithPollInterval(1 * time.Second)
func ForLog(log string) *LogStrategy {
	return NewLogStrategy(log)
}

func (ws *LogStrategy) Timeout() *time.Duration {
	return ws.timeout
}

// WaitUntilReady implements Strategy.WaitUntilReady
func (ws *LogStrategy) WaitUntilReady(ctx context.Context, target StrategyTarget) error {
	timeout := defaultStartupTimeout()
	if ws.timeout != nil {
		timeout = *ws.timeout
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	length := 0

LOOP:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			checkErr := checkTarget(ctx, target)

			reader, err := target.Logs(ctx)
			if err != nil {
				time.Sleep(ws.PollInterval)
				continue
			}

			b, err := io.ReadAll(reader)
			if err != nil {
				time.Sleep(ws.PollInterval)
				continue
			}

			logs := string(b)

			switch {
			case length == len(logs) && checkErr != nil:
				return checkErr
			case checkLogsFn(ws, b):
				break LOOP
			default:
				length = len(logs)
				time.Sleep(ws.PollInterval)
				continue
			}
		}
	}

	return nil
}

func checkLogsFn(ws *LogStrategy, b []byte) bool {
	if ws.IsRegexp {
		re := regexp.MustCompile(ws.Log)
		occurrences := re.FindAll(b, -1)

		return len(occurrences) >= ws.Occurrence
	}

	logs := string(b)
	return strings.Count(logs, ws.Log) >= ws.Occurrence
}
