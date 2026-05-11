/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package metrics

import (
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/openark/golib/log"
)

// Noop is a StatsD client that discards all metrics. NewClient("", ...) returns
// this exact pointer so callers can use `client == metrics.Noop`.
var Noop = &Client{}

// Client wraps a StatsD client with namespace and global tags (from --statsd-tags).
type Client struct {
	sd *statsd.Client
}

// NewClient connects to addr for StatsD. If addr is empty, returns Noop and nil error.
// namespace is typically "gh_ost." (metrics are named namespace + short name, e.g. gh_ost.startup).
// tags are global tags applied to every metric (repeatable --statsd-tags).
func NewClient(addr string, tags []string, namespace string) (*Client, error) {
	if addr == "" {
		return Noop, nil
	}
	sd, err := statsd.New(addr,
		statsd.WithNamespace(namespace),
		statsd.WithTags(tags),
		statsd.WithoutTelemetry(),
		statsd.WithoutOriginDetection(),
		statsd.WithClientSideAggregation(),
		statsd.WithExtendedClientSideAggregation(),
		statsd.WithMaxSamplesPerContext(1_000),
		statsd.WithMaxBytesPerPayload(8_172),
		statsd.WithAggregationInterval(5*time.Second),
	)
	if err != nil {
		return nil, err
	}
	log.Infof("metrics: DogStatsD client connected to %s (namespace: %s)", addr, namespace)
	return &Client{sd: sd}, nil
}

func (c *Client) Gauge(name string, value float64, tags ...string) {
	if c.sd == nil {
		return
	}
	_ = c.sd.Gauge(name, value, tags, 1.0)
}

func (c *Client) Count(name string, value int64, tags ...string) {
	if c.sd == nil {
		return
	}
	_ = c.sd.Count(name, value, tags, 1.0)
}

// Close flushes buffered metrics; safe for Noop.
func (c *Client) Close() error {
	if c.sd == nil {
		return nil
	}
	return c.sd.Close()
}
