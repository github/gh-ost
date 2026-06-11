// Copyright 2013 Ooyala, Inc.

/*
Package statsd provides a Go dogstatsd client. Dogstatsd extends the popular statsd,
adding tags and histograms and pushing upstream to Datadog.

Refer to http://docs.datadoghq.com/guides/dogstatsd/ for information about DogStatsD.

statsd is based on go-statsd-client.
*/
package statsd

//go:generate mockgen -source=statsd.go -destination=mocks/statsd.go

import (
	"io"
	"time"
)

// ClientInterface is an interface that exposes the common client functions for the
// purpose of being able to provide a no-op client or even mocking. This can aid
// downstream users' with their testing.
type ClientInterface interface {
	// Gauge measures the value of a metric at a particular time.
	Gauge(name string, value float64, tags []string, rate float64) error

	// GaugeWithTimestamp measures the value of a metric at a given time.
	// BETA - Please contact our support team for more information to use this feature: https://www.datadoghq.com/support/
	// The value will bypass any aggregation on the client side and agent side, this is
	// useful when sending points in the past.
	//
	// Minimum Datadog Agent version: 7.40.0
	GaugeWithTimestamp(name string, value float64, tags []string, rate float64, timestamp time.Time) error

	// Count tracks how many times something happened per second.
	Count(name string, value int64, tags []string, rate float64) error

	// CountWithTimestamp tracks how many times something happened at the given second.
	// BETA - Please contact our support team for more information to use this feature: https://www.datadoghq.com/support/
	// The value will bypass any aggregation on the client side and agent side, this is
	// useful when sending points in the past.
	//
	// Minimum Datadog Agent version: 7.40.0
	CountWithTimestamp(name string, value int64, tags []string, rate float64, timestamp time.Time) error

	// Histogram tracks the statistical distribution of a set of values on each host.
	Histogram(name string, value float64, tags []string, rate float64) error

	// Distribution tracks the statistical distribution of a set of values across your infrastructure.
	//
	// It is recommended to use `WithMaxBufferedMetricsPerContext` to avoid dropping metrics at high throughput, `rate` can
	// also be used to limit the load. Both options can *not* be used together.
	Distribution(name string, value float64, tags []string, rate float64) error

	// Decr is just Count of -1
	Decr(name string, tags []string, rate float64) error

	// Incr is just Count of 1
	Incr(name string, tags []string, rate float64) error

	// Set counts the number of unique elements in a group.
	Set(name string, value string, tags []string, rate float64) error

	// Timing sends timing information, it is an alias for TimeInMilliseconds
	Timing(name string, value time.Duration, tags []string, rate float64) error

	// TimeInMilliseconds sends timing information in milliseconds.
	// It is flushed by statsd with percentiles, mean and other info (https://github.com/etsy/statsd/blob/master/docs/metric_types.md#timing)
	TimeInMilliseconds(name string, value float64, tags []string, rate float64) error

	// Event sends the provided Event.
	Event(e *Event) error

	// SimpleEvent sends an event with the provided title and text.
	SimpleEvent(title, text string) error

	// ServiceCheck sends the provided ServiceCheck.
	ServiceCheck(sc *ServiceCheck) error

	// SimpleServiceCheck sends an serviceCheck with the provided name and status.
	SimpleServiceCheck(name string, status ServiceCheckStatus) error

	// Close the client connection.
	Close() error

	// Flush forces a flush of all the queued dogstatsd payloads.
	Flush() error

	// IsClosed returns if the client has been closed.
	IsClosed() bool

	// GetTelemetry return the telemetry metrics for the client since it started.
	GetTelemetry() Telemetry
}

// A Client is a handle for sending messages to dogstatsd.  It is safe to
// use one Client from multiple goroutines simultaneously.
type Client struct {
	clientEx *ClientEx
}

// Verify that Client implements the ClientInterface.
// https://golang.org/doc/faq#guarantee_satisfies_interface
var _ ClientInterface = &Client{}

// New returns a pointer to a new Client given an addr in the format "hostname:port" for UDP,
// "unix:///path/to/socket" for UDS or "\\.\pipe\path\to\pipe" for Windows Named Pipes.
func New(addr string, options ...Option) (*Client, error) {
	clientEx, err := NewEx(addr, options...)
	if err != nil {
		return nil, err
	}

	return &Client{
		clientEx: clientEx,
	}, nil
}

// NewWithWriter creates a new Client with given writer. Writer is a
// io.WriteCloser
func NewWithWriter(w io.WriteCloser, options ...Option) (*Client, error) {
	clientEx, err := NewWithWriterEx(w, options...)
	if err != nil {
		return nil, err
	}

	return &Client{
		clientEx: clientEx,
	}, nil
}

// CloneWithExtraOptions create a new Client with extra options
func CloneWithExtraOptions(c *Client, options ...Option) (*Client, error) {
	if c == nil {
		return nil, ErrNoClient
	}

	clientEx, err := CloneWithExtraOptionsEx(c.clientEx, options...)
	if err != nil {
		return nil, err
	}

	return &Client{
		clientEx: clientEx,
	}, nil
}

// Flush forces a flush of all the queued dogstatsd payloads This method is
// blocking and will not return until everything is sent through the network.
// In mutexMode, this will also block sampling new data to the client while the
// workers and sender are flushed.
func (c *Client) Flush() error {
	if c == nil {
		return ErrNoClient
	}
	return c.clientEx.Flush()
}

// IsClosed returns if the client has been closed.
func (c *Client) IsClosed() bool {
	return c.clientEx.IsClosed()
}

// GetTelemetry return the telemetry metrics for the client since it started.
func (c *Client) GetTelemetry() Telemetry {
	return c.clientEx.GetTelemetry()
}

// GetTransport return the name of the transport used.
func (c *Client) GetTransport() string {
	return c.clientEx.GetTransport()
}

// Gauge measures the value of a metric at a particular time.
func (c *Client) Gauge(name string, value float64, tags []string, rate float64) error {
	if c == nil {
		return ErrNoClient
	}
	return c.clientEx.Gauge(name, value, tags, rate)
}

// GaugeWithTimestamp measures the value of a metric at a given time.
// BETA - Please contact our support team for more information to use this feature: https://www.datadoghq.com/support/
// The value will bypass any aggregation on the client side and agent side, this is
// useful when sending points in the past.
//
// Minimum Datadog Agent version: 7.40.0
func (c *Client) GaugeWithTimestamp(name string, value float64, tags []string, rate float64, timestamp time.Time) error {
	if c == nil {
		return ErrNoClient
	}
	return c.clientEx.GaugeWithTimestamp(name, value, tags, rate, timestamp)
}

// Count tracks how many times something happened per second.
func (c *Client) Count(name string, value int64, tags []string, rate float64) error {
	if c == nil {
		return ErrNoClient
	}
	return c.clientEx.Count(name, value, tags, rate)
}

// CountWithTimestamp tracks how many times something happened at the given second.
// BETA - Please contact our support team for more information to use this feature: https://www.datadoghq.com/support/
// The value will bypass any aggregation on the client side and agent side, this is
// useful when sending points in the past.
//
// Minimum Datadog Agent version: 7.40.0
func (c *Client) CountWithTimestamp(name string, value int64, tags []string, rate float64, timestamp time.Time) error {
	if c == nil {
		return ErrNoClient
	}
	return c.clientEx.CountWithTimestamp(name, value, tags, rate, timestamp)
}

// Histogram tracks the statistical distribution of a set of values on each host.
func (c *Client) Histogram(name string, value float64, tags []string, rate float64) error {
	if c == nil {
		return ErrNoClient
	}
	return c.clientEx.Histogram(name, value, tags, rate)
}

// Distribution tracks the statistical distribution of a set of values across your infrastructure.
func (c *Client) Distribution(name string, value float64, tags []string, rate float64) error {
	if c == nil {
		return ErrNoClient
	}
	return c.clientEx.Distribution(name, value, tags, rate)
}

// Decr is just Count of -1
func (c *Client) Decr(name string, tags []string, rate float64) error {
	if c == nil {
		return ErrNoClient
	}
	return c.clientEx.Decr(name, tags, rate)
}

// Incr is just Count of 1
func (c *Client) Incr(name string, tags []string, rate float64) error {
	if c == nil {
		return ErrNoClient
	}
	return c.clientEx.Incr(name, tags, rate)
}

// Set counts the number of unique elements in a group.
func (c *Client) Set(name string, value string, tags []string, rate float64) error {
	if c == nil {
		return ErrNoClient
	}
	return c.clientEx.Set(name, value, tags, rate)

}

// Timing sends timing information, it is an alias for TimeInMilliseconds
func (c *Client) Timing(name string, value time.Duration, tags []string, rate float64) error {
	if c == nil {
		return ErrNoClient
	}
	return c.clientEx.Timing(name, value, tags, rate)
}

// TimeInMilliseconds sends timing information in milliseconds.
// It is flushed by statsd with percentiles, mean and other info (https://github.com/etsy/statsd/blob/master/docs/metric_types.md#timing)
func (c *Client) TimeInMilliseconds(name string, value float64, tags []string, rate float64) error {
	if c == nil {
		return ErrNoClient
	}
	return c.clientEx.TimeInMilliseconds(name, value, tags, rate)
}

// Event sends the provided Event.
func (c *Client) Event(e *Event) error {
	if c == nil {
		return ErrNoClient
	}
	return c.clientEx.Event(e)
}

// SimpleEvent sends an event with the provided title and text.
func (c *Client) SimpleEvent(title, text string) error {
	if c == nil {
		return ErrNoClient
	}
	return c.clientEx.SimpleEvent(title, text)
}

// ServiceCheck sends the provided ServiceCheck.
func (c *Client) ServiceCheck(sc *ServiceCheck) error {
	if c == nil {
		return ErrNoClient
	}
	return c.clientEx.ServiceCheck(sc)
}

// SimpleServiceCheck sends an serviceCheck with the provided name and status.
func (c *Client) SimpleServiceCheck(name string, status ServiceCheckStatus) error {
	if c == nil {
		return ErrNoClient
	}
	return c.clientEx.SimpleServiceCheck(name, status)

}

// Close the client connection.
func (c *Client) Close() error {
	if c == nil {
		return ErrNoClient
	}
	return c.clientEx.Close()
}

// sendBlocking is used by the aggregator to inject aggregated metrics.
func (c *Client) sendBlocking(m metric) error {
	return c.clientEx.sendBlocking(m)
}
