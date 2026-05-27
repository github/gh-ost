package statsd

import (
	"io"
	"strings"
	"sync/atomic"
)

type ClientDirectInterface interface {
	DistributionSamples(name string, values []float64, tags []string, rate float64) error
}

// ClientDirect is an *experimental* statsd client that gives direct access to some dogstatsd features.
//
// It is not recommended to use this client in production. This client might allow you to take advantage of
// new features in the agent before they are released, but it might also break your application.
type ClientDirect struct {
	*Client
}

// NewDirect returns a pointer to a new ClientDirect given an addr in the format "hostname:port" for UDP,
// "unix:///path/to/socket" for UDS or "\\.\pipe\path\to\pipe" for Windows Named Pipes.
func NewDirect(addr string, options ...Option) (*ClientDirect, error) {
	client, err := New(addr, options...)
	if err != nil {
		return nil, err
	}
	return &ClientDirect{
		client,
	}, nil
}

func NewDirectWithWriter(writer io.WriteCloser, options ...Option) (*ClientDirect, error) {
	client, err := NewWithWriter(writer, options...)
	if err != nil {
		return nil, err
	}
	return &ClientDirect{
		client,
	}, nil
}

// DistributionSamples is similar to Distribution, but it lets the client deals with the sampling.
//
// The provided `rate` is the sampling rate applied by the client and will *not* be used to apply further
// sampling. This is recommended in high performance cases were the overhead of the statsd library might be
// significant and the sampling is already done by the client.
//
// `WithMaxBufferedMetricsPerContext` is ignored when using this method.
func (c *ClientDirect) DistributionSamples(name string, values []float64, tags []string, rate float64) error {
	if c == nil {
		return ErrNoClient
	}
	atomic.AddUint64(&c.clientEx.telemetry.totalMetricsDistribution, uint64(len(values)))
	return c.clientEx.send(metric{
		metricType: distributionAggregated,
		name:       name,
		fvalues:    values,
		tags:       tags,
		stags:      strings.Join(tags, tagSeparatorSymbol),
		rate:       rate,
		globalTags: c.clientEx.tags,
		namespace:  c.clientEx.namespace,
	})
}

// Validate that ClientDirect implements ClientDirectInterface and ClientInterface.
var _ ClientDirectInterface = (*ClientDirect)(nil)
var _ ClientInterface = (*ClientDirect)(nil)
