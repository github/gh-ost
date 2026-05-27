package statsd

import (
	"fmt"
	"math"
	"strings"
	"time"
)

var (
	defaultNamespace                    = ""
	defaultTags                         = []string{}
	defaultMaxBytesPerPayload           = 0
	defaultMaxMessagesPerPayload        = math.MaxInt32
	defaultBufferPoolSize               = 0
	defaultBufferFlushInterval          = 100 * time.Millisecond
	defaultWorkerCount                  = 32
	defaultSenderQueueSize              = 0
	defaultWriteTimeout                 = 100 * time.Millisecond
	defaultConnectTimeout               = 1000 * time.Millisecond
	defaultTelemetry                    = true
	defaultReceivingMode                = mutexMode
	defaultChannelModeBufferSize        = 4096
	defaultAggregationFlushInterval     = 2 * time.Second
	defaultAggregation                  = true
	defaultExtendedAggregation          = false
	defaultMaxBufferedSamplesPerContext = -1
	defaultOriginDetection              = true
	defaultChannelModeErrorsWhenFull    = false
	defaultErrorHandler                 = func(error) {}
	defaultAggregatorShardCount         = 1
)

// Options contains the configuration options for a client.
type Options struct {
	namespace                    string
	tags                         []string
	maxBytesPerPayload           int
	maxMessagesPerPayload        int
	bufferPoolSize               int
	bufferFlushInterval          time.Duration
	workersCount                 int
	senderQueueSize              int
	writeTimeout                 time.Duration
	connectTimeout               time.Duration
	telemetry                    bool
	receiveMode                  receivingMode
	channelModeBufferSize        int
	aggregationFlushInterval     time.Duration
	aggregation                  bool
	extendedAggregation          bool
	maxBufferedSamplesPerContext int
	aggregatorShardCount         int
	telemetryAddr                string
	originDetection              bool
	containerID                  string
	channelModeErrorsWhenFull    bool
	errorHandler                 ErrorHandler
	tagCardinality               *Cardinality
}

func resolveOptions(options []Option) (*Options, error) {
	o := &Options{
		namespace:                    defaultNamespace,
		tags:                         defaultTags,
		maxBytesPerPayload:           defaultMaxBytesPerPayload,
		maxMessagesPerPayload:        defaultMaxMessagesPerPayload,
		bufferPoolSize:               defaultBufferPoolSize,
		bufferFlushInterval:          defaultBufferFlushInterval,
		workersCount:                 defaultWorkerCount,
		senderQueueSize:              defaultSenderQueueSize,
		writeTimeout:                 defaultWriteTimeout,
		connectTimeout:               defaultConnectTimeout,
		telemetry:                    defaultTelemetry,
		receiveMode:                  defaultReceivingMode,
		channelModeBufferSize:        defaultChannelModeBufferSize,
		aggregationFlushInterval:     defaultAggregationFlushInterval,
		aggregation:                  defaultAggregation,
		extendedAggregation:          defaultExtendedAggregation,
		maxBufferedSamplesPerContext: defaultMaxBufferedSamplesPerContext,
		originDetection:              defaultOriginDetection,
		channelModeErrorsWhenFull:    defaultChannelModeErrorsWhenFull,
		errorHandler:                 defaultErrorHandler,
		aggregatorShardCount:         defaultAggregatorShardCount,
	}

	for _, option := range options {
		err := option(o)
		if err != nil {
			return nil, err
		}
	}

	return o, nil
}

// Option is a client option. Can return an error if validation fails.
type Option func(*Options) error

// WithNamespace sets a string to be prepend to all metrics, events and service checks name.
//
// A '.' will automatically be added after the namespace if needed. For example a metrics 'test' with a namespace 'prod'
// will produce a final metric named 'prod.test'.
func WithNamespace(namespace string) Option {
	return func(o *Options) error {
		if strings.HasSuffix(namespace, ".") {
			o.namespace = namespace
		} else {
			o.namespace = namespace + "."
		}
		return nil
	}
}

// WithTags sets global tags to be applied to every metrics, events and service checks.
func WithTags(tags []string) Option {
	return func(o *Options) error {
		o.tags = tags
		return nil
	}
}

// WithMaxMessagesPerPayload sets the maximum number of metrics, events and/or service checks that a single payload can
// contain.
//
// The default is 'math.MaxInt32' which will most likely let the WithMaxBytesPerPayload option take precedence. This
// option can be set to `1` to create an unbuffered client (each metrics/event/service check will be send in its own
// payload to the agent).
func WithMaxMessagesPerPayload(maxMessagesPerPayload int) Option {
	return func(o *Options) error {
		o.maxMessagesPerPayload = maxMessagesPerPayload
		return nil
	}
}

// WithMaxBytesPerPayload sets the maximum number of bytes a single payload can contain. Each sample, even and service
// check must be lower than this value once serialized or an `MessageTooLongError` is returned.
//
// The default value 0 which will set the option to the optimal size for the transport protocol used: 1432 for UDP and
// named pipe and 8192 for UDS. Those values offer the best performances.
// Be careful when changing this option, see
// https://docs.datadoghq.com/developers/dogstatsd/high_throughput/#ensure-proper-packet-sizes.
func WithMaxBytesPerPayload(MaxBytesPerPayload int) Option {
	return func(o *Options) error {
		o.maxBytesPerPayload = MaxBytesPerPayload
		return nil
	}
}

// WithBufferPoolSize sets the size of the pool of buffers used to serialized metrics, events and service_checks.
//
// The default, 0, will set the option to the optimal size for the transport protocol used: 2048 for UDP and named pipe
// and 512 for UDS.
func WithBufferPoolSize(bufferPoolSize int) Option {
	return func(o *Options) error {
		o.bufferPoolSize = bufferPoolSize
		return nil
	}
}

// WithBufferFlushInterval sets the interval after which the current buffer is flushed.
//
// A buffers are used to serialized data, they're flushed either when full (see WithMaxBytesPerPayload) or when it's
// been open for longer than this interval.
//
// With apps sending a high number of metrics/events/service_checks the interval rarely timeout. But with slow sending
// apps increasing this value will reduce the number of payload sent on the wire as more data is serialized in the same
// payload.
//
// Default is 100ms
func WithBufferFlushInterval(bufferFlushInterval time.Duration) Option {
	return func(o *Options) error {
		o.bufferFlushInterval = bufferFlushInterval
		return nil
	}
}

// WithWorkersCount sets the number of workers that will be used to serialized data.
//
// Those workers allow the use of multiple buffers at the same time (see WithBufferPoolSize) to reduce lock contention.
//
// Default is 32.
func WithWorkersCount(workersCount int) Option {
	return func(o *Options) error {
		if workersCount < 1 {
			return fmt.Errorf("workersCount must be a positive integer")
		}
		o.workersCount = workersCount
		return nil
	}
}

// WithSenderQueueSize sets the size of the sender queue in number of buffers.
//
// After data has been serialized in a buffer they're pushed to a queue that the sender will consume and then each one
// ot the agent.
//
// The default value 0 will set the option to the optimal size for the transport protocol used: 2048 for UDP and named
// pipe and 512 for UDS.
func WithSenderQueueSize(senderQueueSize int) Option {
	return func(o *Options) error {
		o.senderQueueSize = senderQueueSize
		return nil
	}
}

// WithWriteTimeout sets the timeout for network communication with the Agent, after this interval a payload is
// dropped. This is only used for UDS and named pipes connection.
func WithWriteTimeout(writeTimeout time.Duration) Option {
	return func(o *Options) error {
		o.writeTimeout = writeTimeout
		return nil
	}
}

// WithConnectTimeout sets the timeout for network connection with the Agent, after this interval the connection
// attempt is aborted. This is only used for UDS connection. This will also reset the connection if nothing can be
// written to it for this duration.
func WithConnectTimeout(connectTimeout time.Duration) Option {
	return func(o *Options) error {
		o.connectTimeout = connectTimeout
		return nil
	}
}

// WithChannelMode make the client use channels to receive metrics
//
// This determines how the client receive metrics from the app (for example when calling the `Gauge()` method).
// The client will either drop the metrics if its buffers are full (WithChannelMode option) or block the caller until the
// metric can be handled (WithMutexMode option). By default, the client use mutexes.
//
// WithChannelMode uses a channel (see WithChannelModeBufferSize to configure its size) to receive metrics and drops metrics if
// the channel is full. Sending metrics in this mode is much slower that WithMutexMode (because of the channel), but will not
// block the application. This mode is made for application using statsd directly into the application code instead of
// a separated periodic reporter. The goal is to not slow down the application at the cost of dropping metrics and having a lower max
// throughput.
func WithChannelMode() Option {
	return func(o *Options) error {
		o.receiveMode = channelMode
		return nil
	}
}

// WithMutexMode will use mutex to receive metrics from the app through the API.
//
// This determines how the client receive metrics from the app (for example when calling the `Gauge()` method).
// The client will either drop the metrics if its buffers are full (WithChannelMode option) or block the caller until the
// metric can be handled (WithMutexMode option). By default the client use mutexes.
//
// WithMutexMode uses mutexes to receive metrics which is much faster than channels but can cause some lock contention
// when used with a high number of goroutines sending the same metrics. Mutexes are sharded based on the metrics name
// which limit mutex contention when multiple goroutines send different metrics (see WithWorkersCount). This is the
// default behavior which will produce the best throughput.
func WithMutexMode() Option {
	return func(o *Options) error {
		o.receiveMode = mutexMode
		return nil
	}
}

// WithChannelModeBufferSize sets the size of the channel holding incoming metrics when WithChannelMode is used.
func WithChannelModeBufferSize(bufferSize int) Option {
	return func(o *Options) error {
		o.channelModeBufferSize = bufferSize
		return nil
	}
}

// WithChannelModeErrorsWhenFull makes the client return an error when the channel is full.
// This should be enabled if you want to be notified when the client is dropping metrics. You
// will also need to set `WithErrorHandler` to be notified of sender error. This might have
// a small performance impact.
func WithChannelModeErrorsWhenFull() Option {
	return func(o *Options) error {
		o.channelModeErrorsWhenFull = true
		return nil
	}
}

// WithoutChannelModeErrorsWhenFull makes the client not return an error when the channel is full.
func WithoutChannelModeErrorsWhenFull() Option {
	return func(o *Options) error {
		o.channelModeErrorsWhenFull = false
		return nil
	}
}

// WithErrorHandler sets a function that will be called when an error occurs.
func WithErrorHandler(errorHandler ErrorHandler) Option {
	return func(o *Options) error {
		o.errorHandler = errorHandler
		return nil
	}
}

// WithAggregationInterval sets the interval at which aggregated metrics are flushed. See WithClientSideAggregation and
// WithExtendedClientSideAggregation for more.
//
// The default interval is 2s. The interval must divide the Agent reporting period (default=10s) evenly to reduce "aliasing"
// that can cause values to appear irregular/spiky.
//
// For example a 3s aggregation interval will create spikes in the final graph: a application sending a count metric
// that increments at a constant 1000 time per second will appear noisy with an interval of 3s. This is because
// client-side aggregation would report every 3 seconds, while the agent is reporting every 10 seconds. This means in
// each agent bucket, the values are: 9000, 9000, 12000.
func WithAggregationInterval(interval time.Duration) Option {
	return func(o *Options) error {
		o.aggregationFlushInterval = interval
		return nil
	}
}

// WithClientSideAggregation enables client side aggregation for Gauges, Counts and Sets.
func WithClientSideAggregation() Option {
	return func(o *Options) error {
		o.aggregation = true
		return nil
	}
}

// WithoutClientSideAggregation disables client side aggregation.
func WithoutClientSideAggregation() Option {
	return func(o *Options) error {
		o.aggregation = false
		o.extendedAggregation = false
		return nil
	}
}

// WithExtendedClientSideAggregation enables client side aggregation for all types. This feature is only compatible with
// Agent's version >=6.25.0 && <7.0.0 or Agent's versions >=7.25.0.
// When enabled, the use of `rate` with distribution is discouraged and `WithMaxSamplesPerContext()` should be used.
// If `rate` is used with different values of `rate` the resulting rate is not guaranteed to be correct.
func WithExtendedClientSideAggregation() Option {
	return func(o *Options) error {
		o.aggregation = true
		o.extendedAggregation = true
		return nil
	}
}

// WithMaxSamplesPerContext limits the number of sample for metric types that require multiple samples to be send
// over statsd to the agent, such as distributions or timings. This limits the number of sample per
// context for a distribution to a given number. Gauges and counts will not be affected as a single sample per context
// is sent with client side aggregation.
// - This will enable client side aggregation for all metrics.
// - This feature should be used with `WithExtendedClientSideAggregation` for optimal results.
func WithMaxSamplesPerContext(maxSamplesPerDistribution int) Option {
	return func(o *Options) error {
		o.aggregation = true
		o.maxBufferedSamplesPerContext = maxSamplesPerDistribution
		return nil
	}
}

// WithoutTelemetry disables the client telemetry.
//
// More on this here: https://docs.datadoghq.com/developers/dogstatsd/high_throughput/#client-side-telemetry
func WithoutTelemetry() Option {
	return func(o *Options) error {
		o.telemetry = false
		return nil
	}
}

// WithTelemetryAddr sets a different address for telemetry metrics. By default the same address as the client is used
// for telemetry.
//
// More on this here: https://docs.datadoghq.com/developers/dogstatsd/high_throughput/#client-side-telemetry
func WithTelemetryAddr(addr string) Option {
	return func(o *Options) error {
		o.telemetryAddr = addr
		return nil
	}
}

// WithoutOriginDetection disables the client origin detection.
// When enabled, the client tries to discover its container ID and sends it to the Agent
// to enrich the metrics with container tags.
// If the container id is not found and the client is running in a private cgroup namespace, the client
// sends the base cgroup controller inode.
// Origin detection can also be disabled by configuring the environment variabe DD_ORIGIN_DETECTION_ENABLED=false
// The client tries to read the container ID by parsing the file /proc/self/cgroup, this is not supported on Windows.
//
// More on this here: https://docs.datadoghq.com/developers/dogstatsd/?tab=kubernetes#origin-detection-over-udp
func WithoutOriginDetection() Option {
	return func(o *Options) error {
		o.originDetection = false
		return nil
	}
}

// WithOriginDetection enables the client origin detection.
// This feature requires Datadog Agent version >=6.35.0 && <7.0.0 or Agent versions >=7.35.0.
// When enabled, the client tries to discover its container ID and sends it to the Agent
// to enrich the metrics with container tags.
// If the container id is not found and the client is running in a private cgroup namespace, the client
// sends the base cgroup controller inode.
// Origin detection can be disabled by configuring the environment variable DD_ORIGIN_DETECTION_ENABLED=false
//
// More on this here: https://docs.datadoghq.com/developers/dogstatsd/?tab=kubernetes#origin-detection-over-udp
func WithOriginDetection() Option {
	return func(o *Options) error {
		o.originDetection = true
		return nil
	}
}

// WithContainerID allows passing the container ID, this will be used by the Agent to enrich metrics with container tags.
// This feature requires Datadog Agent version >=6.35.0 && <7.0.0 or Agent versions >=7.35.0.
// When configured, the provided container ID is prioritized over the container ID discovered via Origin Detection.
// The client prioritizes the value passed via DD_ENTITY_ID (if set) over the container ID.
func WithContainerID(id string) Option {
	return func(o *Options) error {
		o.containerID = id
		return nil
	}
}

// WithCardinality sets the tag cardinality of the metric.
func WithCardinality(card Cardinality) Option {
	return func(o *Options) error {
		if !card.isValid() {
			return fmt.Errorf("invalid cardinality %d", card)
		}
		o.tagCardinality = &card
		return nil
	}
}

// WithAggregatorShardCount sets the number of shards used for the aggregator.
// Higher values reduce lock contention but increase memory usage.
//
// The default is 1 as to mimic current behavior.
func WithAggregatorShardCount(shardCount int) Option {
	return func(o *Options) error {
		if shardCount < 1 {
			return fmt.Errorf("shardCount must be a positive integer")
		}
		o.aggregatorShardCount = shardCount
		return nil
	}
}
