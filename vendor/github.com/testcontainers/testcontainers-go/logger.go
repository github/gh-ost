package testcontainers

import (
	"context"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/docker/docker/client"
)

// Logger is the default log instance
var Logger Logging = log.New(os.Stderr, "", log.LstdFlags)

func init() {
	for _, arg := range os.Args {
		if strings.EqualFold(arg, "-test.v=true") || strings.EqualFold(arg, "-v") {
			return
		}
	}

	// If we are not running in verbose mode, we configure a noop logger by default.
	Logger = &noopLogger{}
}

// Validate our types implement the required interfaces.
var (
	_ Logging               = (*log.Logger)(nil)
	_ ContainerCustomizer   = LoggerOption{}
	_ GenericProviderOption = LoggerOption{}
	_ DockerProviderOption  = LoggerOption{}
)

// Logging defines the Logger interface
type Logging interface {
	Printf(format string, v ...interface{})
}

type noopLogger struct{}

// Printf implements Logging.
func (n noopLogger) Printf(format string, v ...interface{}) {
	// NOOP
}

// Deprecated: this function will be removed in a future release
// LogDockerServerInfo logs the docker server info using the provided logger and Docker client
func LogDockerServerInfo(ctx context.Context, client client.APIClient, logger Logging) {
	// NOOP
}

// TestLogger returns a Logging implementation for testing.TB
// This way logs from testcontainers are part of the test output of a test suite or test case.
func TestLogger(tb testing.TB) Logging {
	tb.Helper()
	return testLogger{TB: tb}
}

// WithLogger returns a generic option that sets the logger to be used.
//
// Consider calling this before other "With functions" as these may generate logs.
//
// This can be given a TestLogger to collect the logs from testcontainers into a
// test case.
func WithLogger(logger Logging) LoggerOption {
	return LoggerOption{
		logger: logger,
	}
}

// LoggerOption is a generic option that sets the logger to be used.
//
// It can be used to set the logger for providers and containers.
type LoggerOption struct {
	logger Logging
}

// ApplyGenericTo implements GenericProviderOption.
func (o LoggerOption) ApplyGenericTo(opts *GenericProviderOptions) {
	opts.Logger = o.logger
}

// ApplyDockerTo implements DockerProviderOption.
func (o LoggerOption) ApplyDockerTo(opts *DockerProviderOptions) {
	opts.Logger = o.logger
}

// Customize implements ContainerCustomizer.
func (o LoggerOption) Customize(req *GenericContainerRequest) error {
	req.Logger = o.logger
	return nil
}

type testLogger struct {
	testing.TB
}

// Printf implements Logging.
func (t testLogger) Printf(format string, v ...interface{}) {
	t.Helper()
	t.Logf(format, v...)
}
