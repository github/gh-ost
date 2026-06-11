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
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

/*
OptimalUDPPayloadSize defines the optimal payload size for a UDP datagram, 1432 bytes
is optimal for regular networks with an MTU of 1500 so datagrams don't get
fragmented. It's generally recommended not to fragment UDP datagrams as losing
a single fragment will cause the entire datagram to be lost.
*/
const OptimalUDPPayloadSize = 1432

/*
MaxUDPPayloadSize defines the maximum payload size for a UDP datagram.
Its value comes from the calculation: 65535 bytes Max UDP datagram size -
8byte UDP header - 60byte max IP headers
any number greater than that will see frames being cut out.
*/
const MaxUDPPayloadSize = 65467

// DefaultUDPBufferPoolSize is the default size of the buffer pool for UDP clients.
const DefaultUDPBufferPoolSize = 2048

// DefaultUDSBufferPoolSize is the default size of the buffer pool for UDS clients.
const DefaultUDSBufferPoolSize = 512

/*
DefaultMaxAgentPayloadSize is the default maximum payload size the agent
can receive. This can be adjusted by changing dogstatsd_buffer_size in the
agent configuration file datadog.yaml. This is also used as the optimal payload size
for UDS datagrams.
*/
const DefaultMaxAgentPayloadSize = 8192

/*
UnixAddressPrefix holds the prefix to use to enable Unix Domain Socket
traffic instead of UDP. The type of the socket will be guessed.
*/
const UnixAddressPrefix = "unix://"

/*
UnixDatagramAddressPrefix holds the prefix to use to enable Unix Domain Socket
datagram traffic instead of UDP.
*/
const UnixAddressDatagramPrefix = "unixgram://"

/*
UnixAddressStreamPrefix holds the prefix to use to enable Unix Domain Socket
stream traffic instead of UDP.
*/
const UnixAddressStreamPrefix = "unixstream://"

/*
WindowsPipeAddressPrefix holds the prefix to use to enable Windows Named Pipes
traffic instead of UDP.
*/
const WindowsPipeAddressPrefix = `\\.\pipe\`

var (
	AddressPrefixes = []string{UnixAddressPrefix, UnixAddressDatagramPrefix, UnixAddressStreamPrefix, WindowsPipeAddressPrefix}
)

const (
	agentHostEnvVarName = "DD_AGENT_HOST"
	agentPortEnvVarName = "DD_DOGSTATSD_PORT"
	agentURLEnvVarName  = "DD_DOGSTATSD_URL"
	defaultUDPPort      = "8125"
)

const (
	// ddEntityID specifies client-side user-specified entity ID injection.
	// This env var can be set to the Pod UID on Kubernetes via the downward API.
	// Docs: https://docs.datadoghq.com/developers/dogstatsd/?tab=kubernetes#origin-detection-over-udp
	ddEntityID = "DD_ENTITY_ID"

	// ddEntityIDTag specifies the tag name for the client-side entity ID injection
	// The Agent expects this tag to contain a non-prefixed Kubernetes Pod UID.
	ddEntityIDTag = "dd.internal.entity_id"

	// originDetectionEnabled specifies the env var to enable/disable sending the container ID field.
	originDetectionEnabled = "DD_ORIGIN_DETECTION_ENABLED"
)

/*
ddEnvTagsMapping is a mapping of each "DD_" prefixed environment variable
to a specific tag name. We use a slice to keep the order and simplify tests.
*/
var ddEnvTagsMapping = []struct{ envName, tagName string }{
	{ddEntityID, ddEntityIDTag}, // Client-side entity ID injection for container tagging.
	{"DD_ENV", "env"},           // The name of the env in which the service runs.
	{"DD_SERVICE", "service"},   // The name of the running service.
	{"DD_VERSION", "version"},   // The current version of the running service.
}

type metricType int

const (
	gauge metricType = iota
	count
	histogram
	histogramAggregated
	distribution
	distributionAggregated
	set
	timing
	timingAggregated
	event
	serviceCheck
)

type receivingMode int

const (
	mutexMode receivingMode = iota
	channelMode
)

const (
	writerNameUDP       string = "udp"
	writerNameUDS       string = "uds"
	writerNameUDSStream string = "uds-stream"
	writerWindowsPipe   string = "pipe"
	writerNameCustom    string = "custom"
)

// noTimestamp is used as a value for metric without a given timestamp.
const noTimestamp = int64(0)

type metric struct {
	metricType      metricType
	namespace       string
	globalTags      []string
	name            string
	fvalue          float64
	fvalues         []float64
	ivalue          int64
	svalue          string
	evalue          *Event
	scvalue         *ServiceCheck
	tags            []string
	stags           string
	rate            float64
	timestamp       int64
	originDetection bool
	cardinality     Cardinality
}

type noClientErr string

// ErrNoClient is returned if statsd reporting methods are invoked on
// a nil client.
const ErrNoClient = noClientErr("statsd client is nil")

func (e noClientErr) Error() string {
	return string(e)
}

type invalidTimestampErr string

// InvalidTimestamp is returned if a provided timestamp is invalid.
const InvalidTimestamp = invalidTimestampErr("invalid timestamp")

func (e invalidTimestampErr) Error() string {
	return string(e)
}

// ClientInterfaceEx is an temporary interface  that is similar to ClientInterface
// but with the addition of a `...Parameter` for the telemetry functions. This is currently
// just used to specify the tag cardinality. We want to avoid changing ClientInterface
// at present as that would require a new major release.
// Users should avoid implementing this interface as it will be deprecated in the next version.
type ClientInterfaceEx interface {
	// Gauge measures the value of a metric at a particular time.
	Gauge(name string, value float64, tags []string, rate float64, parameters ...Parameter) error

	// GaugeWithTimestamp measures the value of a metric at a given time.
	// BETA - Please contact our support team for more information to use this feature: https://www.datadoghq.com/support/
	// The value will bypass any aggregation on the client side and agent side, this is
	// useful when sending points in the past.
	//
	// Minimum Datadog Agent version: 7.40.0
	GaugeWithTimestamp(name string, value float64, tags []string, rate float64, timestamp time.Time, parameters ...Parameter) error

	// Count tracks how many times something happened per second.
	Count(name string, value int64, tags []string, rate float64, parameters ...Parameter) error

	// CountWithTimestamp tracks how many times something happened at the given second.
	// BETA - Please contact our support team for more information to use this feature: https://www.datadoghq.com/support/
	// The value will bypass any aggregation on the client side and agent side, this is
	// useful when sending points in the past.
	//
	// Minimum Datadog Agent version: 7.40.0
	CountWithTimestamp(name string, value int64, tags []string, rate float64, timestamp time.Time, parameters ...Parameter) error

	// Histogram tracks the statistical distribution of a set of values on each host.
	Histogram(name string, value float64, tags []string, rate float64, parameters ...Parameter) error

	// Distribution tracks the statistical distribution of a set of values across your infrastructure.
	//
	// It is recommended to use `WithMaxBufferedMetricsPerContext` to avoid dropping metrics at high throughput, `rate` can
	// also be used to limit the load. Both options can *not* be used together.
	Distribution(name string, value float64, tags []string, rate float64, parameters ...Parameter) error

	// Decr is just Count of -1
	Decr(name string, tags []string, rate float64, parameters ...Parameter) error

	// Incr is just Count of 1
	Incr(name string, tags []string, rate float64, parameters ...Parameter) error

	// Set counts the number of unique elements in a group.
	Set(name string, value string, tags []string, rate float64, parameters ...Parameter) error

	// Timing sends timing information, it is an alias for TimeInMilliseconds
	Timing(name string, value time.Duration, tags []string, rate float64, parameters ...Parameter) error

	// TimeInMilliseconds sends timing information in milliseconds.
	// It is flushed by statsd with percentiles, mean and other info (https://github.com/etsy/statsd/blob/master/docs/metric_types.md#timing)
	TimeInMilliseconds(name string, value float64, tags []string, rate float64, parameters ...Parameter) error

	// Event sends the provided Event.
	Event(e *Event, parameters ...Parameter) error

	// SimpleEvent sends an event with the provided title and text.
	SimpleEvent(title, text string, parameters ...Parameter) error

	// ServiceCheck sends the provided ServiceCheck.
	ServiceCheck(sc *ServiceCheck, parameters ...Parameter) error

	// SimpleServiceCheck sends an serviceCheck with the provided name and status.
	SimpleServiceCheck(name string, status ServiceCheckStatus, parameters ...Parameter) error

	// Close the client connection.
	Close() error

	// Flush forces a flush of all the queued dogstatsd payloads.
	Flush() error

	// IsClosed returns if the client has been closed.
	IsClosed() bool

	// GetTelemetry return the telemetry metrics for the client since it started.
	GetTelemetry() Telemetry

	// Ensure this interface can't be implemented outside of this package.
	// ClientInterfaceEx is a temporary measure to allow us to release a version of the library with the
	// extra `...Parameter` parameter (currently used to specify the tag cardinality) in the metric functions
	// without having to release a new major version.
	// This interface will be deprecated with the next release.
	private()
}

type ErrorHandler func(error)

// A Client is a handle for sending messages to dogstatsd.  It is safe to
// use one Client from multiple goroutines simultaneously.
type ClientEx struct {
	// Sender handles the underlying networking protocol
	sender *sender
	// namespace to prepend to all statsd calls
	namespace string
	// tags are global tags to be added to every statsd call
	tags                  []string
	flushTime             time.Duration
	telemetry             *statsdTelemetry
	telemetryClient       *telemetryClient
	stop                  chan struct{}
	wg                    sync.WaitGroup
	workers               []*worker
	closerLock            sync.Mutex
	workersMode           receivingMode
	aggregatorMode        receivingMode
	agg                   *aggregator
	aggExtended           *aggregator
	options               []Option
	addrOption            string
	isClosed              bool
	errorOnBlockedChannel bool
	errorHandler          ErrorHandler
	originDetection       bool
	defaultCardinality    Cardinality
}

// statsdTelemetry contains telemetry metrics about the client
type statsdTelemetry struct {
	totalMetricsGauge        uint64
	totalMetricsCount        uint64
	totalMetricsHistogram    uint64
	totalMetricsDistribution uint64
	totalMetricsSet          uint64
	totalMetricsTiming       uint64
	totalEvents              uint64
	totalServiceChecks       uint64
	totalDroppedOnReceive    uint64
}

// Verify that ClientEx implements the ClientInterfaceEx interface.
// https://golang.org/doc/faq#guarantee_satisfies_interface
var _ ClientInterfaceEx = &ClientEx{}

func resolveAddr(addr string) string {
	envPort := ""

	if addr == "" {
		addr = os.Getenv(agentHostEnvVarName)
		envPort = os.Getenv(agentPortEnvVarName)
		agentURL, _ := os.LookupEnv(agentURLEnvVarName)
		agentURL = parseAgentURL(agentURL)

		// agentURLEnvVarName has priority over agentHostEnvVarName
		if agentURL != "" {
			return agentURL
		}
	}

	if addr == "" {
		return ""
	}

	for _, prefix := range AddressPrefixes {
		if strings.HasPrefix(addr, prefix) {
			return addr
		}
	}
	// TODO: How does this work for IPv6?
	if strings.Contains(addr, ":") {
		return addr
	}
	if envPort != "" {
		addr = fmt.Sprintf("%s:%s", addr, envPort)
	} else {
		addr = fmt.Sprintf("%s:%s", addr, defaultUDPPort)
	}
	return addr
}

func parseAgentURL(agentURL string) string {
	if agentURL != "" {
		if strings.HasPrefix(agentURL, WindowsPipeAddressPrefix) {
			return agentURL
		}

		parsedURL, err := url.Parse(agentURL)
		if err != nil {
			return ""
		}

		if parsedURL.Scheme == "udp" {
			if strings.Contains(parsedURL.Host, ":") {
				return parsedURL.Host
			}
			return fmt.Sprintf("%s:%s", parsedURL.Host, defaultUDPPort)
		}

		if parsedURL.Scheme == "unix" {
			return agentURL
		}
	}
	return ""
}

func createWriter(addr string, writeTimeout time.Duration, connectTimeout time.Duration) (Transport, string, error) {
	if addr == "" {
		return nil, "", errors.New("No address passed and autodetection from environment failed")
	}

	switch {
	case strings.HasPrefix(addr, WindowsPipeAddressPrefix):
		w, err := newWindowsPipeWriter(addr, writeTimeout)
		return w, writerWindowsPipe, err
	case strings.HasPrefix(addr, UnixAddressPrefix):
		w, err := newUDSWriter(addr[len(UnixAddressPrefix):], writeTimeout, connectTimeout, "")
		return w, writerNameUDS, err
	case strings.HasPrefix(addr, UnixAddressDatagramPrefix):
		w, err := newUDSWriter(addr[len(UnixAddressDatagramPrefix):], writeTimeout, connectTimeout, "unixgram")
		return w, writerNameUDS, err
	case strings.HasPrefix(addr, UnixAddressStreamPrefix):
		w, err := newUDSWriter(addr[len(UnixAddressStreamPrefix):], writeTimeout, connectTimeout, "unix")
		return w, writerNameUDS, err
	default:
		w, err := newUDPWriter(addr, writeTimeout)
		return w, writerNameUDP, err
	}
}

// New returns a pointer to a new Client given an addr in the format "hostname:port" for UDP,
// "unix:///path/to/socket" for UDS or "\\.\pipe\path\to\pipe" for Windows Named Pipes.
func NewEx(addr string, options ...Option) (*ClientEx, error) {
	o, err := resolveOptions(options)
	if err != nil {
		return nil, err
	}

	addr = resolveAddr(addr)
	w, writerType, err := createWriter(addr, o.writeTimeout, o.connectTimeout)
	if err != nil {
		return nil, err
	}

	client, err := newWithWriter(w, o, writerType)
	if err == nil {
		client.options = append(client.options, options...)
		client.addrOption = addr
	}
	return client, err
}

type customWriter struct {
	io.WriteCloser
}

func (w *customWriter) GetTransportName() string {
	return writerNameCustom
}

// NewWithWriter creates a new ClientEx with given writer. Writer is a
// io.WriteCloser
func NewWithWriterEx(w io.WriteCloser, options ...Option) (*ClientEx, error) {
	o, err := resolveOptions(options)
	if err != nil {
		return nil, err
	}
	return newWithWriter(&customWriter{w}, o, writerNameCustom)
}

// CloneWithExtraOptions create a new ClientEx with extra options
func CloneWithExtraOptionsEx(c *ClientEx, options ...Option) (*ClientEx, error) {
	if c == nil {
		return nil, ErrNoClient
	}

	if c.addrOption == "" {
		return nil, fmt.Errorf("can't clone client with no addrOption")
	}
	opt := append(c.options, options...)
	return NewEx(c.addrOption, opt...)
}

func newWithWriter(w Transport, o *Options, writerName string) (*ClientEx, error) {
	c := ClientEx{
		namespace:             o.namespace,
		tags:                  o.tags,
		telemetry:             &statsdTelemetry{},
		errorOnBlockedChannel: o.channelModeErrorsWhenFull,
		errorHandler:          o.errorHandler,
		originDetection:       isOriginDetectionEnabled(o),
	}

	// Inject values of DD_* environment variables as global tags.
	for _, mapping := range ddEnvTagsMapping {
		if value := os.Getenv(mapping.envName); value != "" {
			c.tags = append(c.tags, fmt.Sprintf("%s:%s", mapping.tagName, value))
		}
	}
	// Whether origin detection is enabled or not for this client, we need to initialize the global
	// external environment variable in case another client has enabled it and needs to access it.
	initExternalEnv()

	if o.tagCardinality != nil {
		c.defaultCardinality = *o.tagCardinality
	} else if card, ok := envTagCardinality(); ok {
		c.defaultCardinality = card
	} else {
		c.defaultCardinality = CardinalityNotSet
	}

	initContainerID(o.containerID, fillInContainerID(o), isHostCgroupNamespace())
	isUDS := writerName == writerNameUDS

	if o.maxBytesPerPayload == 0 {
		if isUDS {
			o.maxBytesPerPayload = DefaultMaxAgentPayloadSize
		} else {
			o.maxBytesPerPayload = OptimalUDPPayloadSize
		}
	}
	if o.bufferPoolSize == 0 {
		if isUDS {
			o.bufferPoolSize = DefaultUDSBufferPoolSize
		} else {
			o.bufferPoolSize = DefaultUDPBufferPoolSize
		}
	}
	if o.senderQueueSize == 0 {
		if isUDS {
			o.senderQueueSize = DefaultUDSBufferPoolSize
		} else {
			o.senderQueueSize = DefaultUDPBufferPoolSize
		}
	}

	bufferPool := newBufferPool(o.bufferPoolSize, o.maxBytesPerPayload, o.maxMessagesPerPayload)
	c.sender = newSender(w, o.senderQueueSize, bufferPool, o.errorHandler)
	c.aggregatorMode = o.receiveMode

	c.workersMode = o.receiveMode
	// channelMode mode at the worker level is not enabled when
	// ExtendedAggregation is since the user app will not directly
	// use the worker (the aggregator sit between the app and the
	// workers).
	if o.extendedAggregation {
		c.workersMode = mutexMode
	}

	if o.aggregation || o.extendedAggregation || o.maxBufferedSamplesPerContext > 0 {
		c.agg = newAggregator(&c, int64(o.maxBufferedSamplesPerContext), o.aggregatorShardCount)
		c.agg.start(o.aggregationFlushInterval)

		if o.extendedAggregation {
			c.aggExtended = c.agg

			if c.aggregatorMode == channelMode {
				c.agg.startReceivingMetric(o.channelModeBufferSize, o.workersCount)
			}
		}
	}

	for i := 0; i < o.workersCount; i++ {
		w := newWorker(bufferPool, c.sender)
		c.workers = append(c.workers, w)

		if c.workersMode == channelMode {
			w.startReceivingMetric(o.channelModeBufferSize)
		}
	}

	c.flushTime = o.bufferFlushInterval
	c.stop = make(chan struct{}, 1)

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.watch()
	}()

	if o.telemetry {
		if o.telemetryAddr == "" {
			c.telemetryClient = newTelemetryClient(&c, c.agg != nil)
		} else {
			var err error
			c.telemetryClient, err = newTelemetryClientWithCustomAddr(&c, o.telemetryAddr, c.agg != nil, bufferPool, o.writeTimeout, o.connectTimeout)
			if err != nil {
				return nil, err
			}
		}
		c.telemetryClient.run(&c.wg, c.stop)
	}

	return &c, nil
}

func (c *ClientEx) watch() {
	ticker := time.NewTicker(c.flushTime)

	for {
		select {
		case <-ticker.C:
			for _, w := range c.workers {
				w.flush()
			}
		case <-c.stop:
			ticker.Stop()
			return
		}
	}
}

// Flush forces a flush of all the queued dogstatsd payloads This method is
// blocking and will not return until everything is sent through the network.
// In mutexMode, this will also block sampling new data to the client while the
// workers and sender are flushed.
func (c *ClientEx) Flush() error {
	if c == nil {
		return ErrNoClient
	}
	if c.agg != nil {
		c.agg.flush()
	}
	for _, w := range c.workers {
		w.pause()
		defer w.unpause()
		w.flushUnsafe()
	}
	// Now that the worker are pause the sender can flush the queue between
	// worker and senders
	c.sender.flush()
	return nil
}

// IsClosed returns if the client has been closed.
func (c *ClientEx) IsClosed() bool {
	c.closerLock.Lock()
	defer c.closerLock.Unlock()
	return c.isClosed
}

func (c *ClientEx) flushTelemetryMetrics(t *Telemetry) {
	t.TotalMetricsGauge = atomic.LoadUint64(&c.telemetry.totalMetricsGauge)
	t.TotalMetricsCount = atomic.LoadUint64(&c.telemetry.totalMetricsCount)
	t.TotalMetricsSet = atomic.LoadUint64(&c.telemetry.totalMetricsSet)
	t.TotalMetricsHistogram = atomic.LoadUint64(&c.telemetry.totalMetricsHistogram)
	t.TotalMetricsDistribution = atomic.LoadUint64(&c.telemetry.totalMetricsDistribution)
	t.TotalMetricsTiming = atomic.LoadUint64(&c.telemetry.totalMetricsTiming)
	t.TotalEvents = atomic.LoadUint64(&c.telemetry.totalEvents)
	t.TotalServiceChecks = atomic.LoadUint64(&c.telemetry.totalServiceChecks)
	t.TotalDroppedOnReceive = atomic.LoadUint64(&c.telemetry.totalDroppedOnReceive)
}

// GetTelemetry return the telemetry metrics for the client since it started.
func (c *ClientEx) GetTelemetry() Telemetry {
	return c.telemetryClient.getTelemetry()
}

// GetTransport return the name of the transport used.
func (c *ClientEx) GetTransport() string {
	if c.sender == nil {
		return ""
	}
	return c.sender.getTransportName()
}

type ErrorInputChannelFull struct {
	Metric      metric
	ChannelSize int
	Msg         string
}

func (e ErrorInputChannelFull) Error() string {
	return e.Msg
}

func (c *ClientEx) send(m metric) error {
	h := hashString32(m.name)
	worker := c.workers[h%uint32(len(c.workers))]

	if c.workersMode == channelMode {
		select {
		case worker.inputMetrics <- m:
		default:
			atomic.AddUint64(&c.telemetry.totalDroppedOnReceive, 1)
			err := &ErrorInputChannelFull{m, len(worker.inputMetrics), "Worker input channel full"}
			if c.errorHandler != nil {
				c.errorHandler(err)
			}
			if c.errorOnBlockedChannel {
				return err
			}
		}
		return nil
	}
	return worker.processMetric(m)
}

// sendBlocking is used by the aggregator to inject aggregated metrics.
func (c *ClientEx) sendBlocking(m metric) error {
	m.globalTags = c.tags
	m.namespace = c.namespace

	h := hashString32(m.name)
	worker := c.workers[h%uint32(len(c.workers))]
	return worker.processMetric(m)
}

func (c *ClientEx) sendToAggregator(mType metricType, name string, value float64, tags []string, rate float64, f bufferedMetricSampleFunc, cardinality Cardinality) error {
	if c.aggregatorMode == channelMode {
		m := metric{metricType: mType, name: name, fvalue: value, tags: tags, rate: rate, cardinality: cardinality}
		select {
		case c.aggExtended.inputMetrics <- m:
		default:
			atomic.AddUint64(&c.telemetry.totalDroppedOnReceive, 1)
			err := &ErrorInputChannelFull{m, len(c.aggExtended.inputMetrics), "Aggregator input channel full"}
			if c.errorHandler != nil {
				c.errorHandler(err)
			}
			if c.errorOnBlockedChannel {
				return err
			}
		}
		return nil
	}
	return f(name, value, tags, rate, cardinality)
}

// Gauge measures the value of a metric at a particular time.
func (c *ClientEx) Gauge(name string, value float64, tags []string, rate float64, parameters ...Parameter) error {
	if c == nil {
		return ErrNoClient
	}
	atomic.AddUint64(&c.telemetry.totalMetricsGauge, 1)
	cardinality := parameterCardinality(parameters, c.defaultCardinality)
	if c.agg != nil {
		return c.agg.gauge(name, value, tags, cardinality)
	}
	return c.send(metric{metricType: gauge, name: name, fvalue: value, tags: tags, rate: rate, globalTags: c.tags, namespace: c.namespace, originDetection: c.originDetection, cardinality: cardinality})
}

// GaugeWithTimestamp measures the value of a metric at a given time.
// BETA - Please contact our support team for more information to use this feature: https://www.datadoghq.com/support/
// The value will bypass any aggregation on the client side and agent side, this is
// useful when sending points in the past.
//
// Minimum Datadog Agent version: 7.40.0
func (c *ClientEx) GaugeWithTimestamp(name string, value float64, tags []string, rate float64, timestamp time.Time, parameters ...Parameter) error {
	if c == nil {
		return ErrNoClient
	}

	if timestamp.IsZero() || timestamp.Unix() <= noTimestamp {
		return InvalidTimestamp
	}

	atomic.AddUint64(&c.telemetry.totalMetricsGauge, 1)
	cardinality := parameterCardinality(parameters, c.defaultCardinality)
	return c.send(metric{metricType: gauge, name: name, fvalue: value, tags: tags, rate: rate, globalTags: c.tags, namespace: c.namespace, timestamp: timestamp.Unix(), originDetection: c.originDetection, cardinality: cardinality})
}

// Count tracks how many times something happened per second.
func (c *ClientEx) Count(name string, value int64, tags []string, rate float64, parameters ...Parameter) error {
	if c == nil {
		return ErrNoClient
	}
	atomic.AddUint64(&c.telemetry.totalMetricsCount, 1)
	cardinality := parameterCardinality(parameters, c.defaultCardinality)
	if c.agg != nil {
		return c.agg.count(name, value, tags, cardinality)
	}
	return c.send(metric{metricType: count, name: name, ivalue: value, tags: tags, rate: rate, globalTags: c.tags, namespace: c.namespace, originDetection: c.originDetection, cardinality: cardinality})
}

// CountWithTimestamp tracks how many times something happened at the given second.
// BETA - Please contact our support team for more information to use this feature: https://www.datadoghq.com/support/
// The value will bypass any aggregation on the client side and agent side, this is
// useful when sending points in the past.
//
// Minimum Datadog Agent version: 7.40.0
func (c *ClientEx) CountWithTimestamp(name string, value int64, tags []string, rate float64, timestamp time.Time, parameters ...Parameter) error {
	if c == nil {
		return ErrNoClient
	}

	if timestamp.IsZero() || timestamp.Unix() <= noTimestamp {
		return InvalidTimestamp
	}

	atomic.AddUint64(&c.telemetry.totalMetricsCount, 1)
	cardinality := parameterCardinality(parameters, c.defaultCardinality)
	return c.send(metric{metricType: count, name: name, ivalue: value, tags: tags, rate: rate, globalTags: c.tags, namespace: c.namespace, timestamp: timestamp.Unix(), originDetection: c.originDetection, cardinality: cardinality})
}

// Histogram tracks the statistical distribution of a set of values on each host.
func (c *ClientEx) Histogram(name string, value float64, tags []string, rate float64, parameters ...Parameter) error {
	if c == nil {
		return ErrNoClient
	}
	atomic.AddUint64(&c.telemetry.totalMetricsHistogram, 1)
	cardinality := parameterCardinality(parameters, c.defaultCardinality)
	if c.aggExtended != nil {
		return c.sendToAggregator(histogram, name, value, tags, rate, c.aggExtended.histogram, cardinality)
	}
	return c.send(metric{metricType: histogram, name: name, fvalue: value, tags: tags, rate: rate, globalTags: c.tags, namespace: c.namespace, originDetection: c.originDetection, cardinality: cardinality})
}

// Distribution tracks the statistical distribution of a set of values across your infrastructure.
func (c *ClientEx) Distribution(name string, value float64, tags []string, rate float64, parameters ...Parameter) error {
	if c == nil {
		return ErrNoClient
	}
	atomic.AddUint64(&c.telemetry.totalMetricsDistribution, 1)
	cardinality := parameterCardinality(parameters, c.defaultCardinality)
	if c.aggExtended != nil {
		return c.sendToAggregator(distribution, name, value, tags, rate, c.aggExtended.distribution, cardinality)
	}
	return c.send(metric{metricType: distribution, name: name, fvalue: value, tags: tags, rate: rate, globalTags: c.tags, namespace: c.namespace, originDetection: c.originDetection, cardinality: cardinality})
}

// Decr is just Count of -1
func (c *ClientEx) Decr(name string, tags []string, rate float64, parameters ...Parameter) error {
	return c.Count(name, -1, tags, rate, parameters...)
}

// Incr is just Count of 1
func (c *ClientEx) Incr(name string, tags []string, rate float64, parameters ...Parameter) error {
	return c.Count(name, 1, tags, rate, parameters...)
}

// Set counts the number of unique elements in a group.
func (c *ClientEx) Set(name string, value string, tags []string, rate float64, parameters ...Parameter) error {
	if c == nil {
		return ErrNoClient
	}
	atomic.AddUint64(&c.telemetry.totalMetricsSet, 1)
	cardinality := parameterCardinality(parameters, c.defaultCardinality)
	if c.agg != nil {
		return c.agg.set(name, value, tags, cardinality)
	}
	return c.send(metric{metricType: set, name: name, svalue: value, tags: tags, rate: rate, globalTags: c.tags, namespace: c.namespace, originDetection: c.originDetection, cardinality: cardinality})
}

// Timing sends timing information, it is an alias for TimeInMilliseconds
func (c *ClientEx) Timing(name string, value time.Duration, tags []string, rate float64, parameters ...Parameter) error {
	return c.TimeInMilliseconds(name, value.Seconds()*1000, tags, rate, parameters...)
}

// TimeInMilliseconds sends timing information in milliseconds.
// It is flushed by statsd with percentiles, mean and other info (https://github.com/etsy/statsd/blob/master/docs/metric_types.md#timing)
func (c *ClientEx) TimeInMilliseconds(name string, value float64, tags []string, rate float64, parameters ...Parameter) error {
	if c == nil {
		return ErrNoClient
	}
	atomic.AddUint64(&c.telemetry.totalMetricsTiming, 1)
	cardinality := parameterCardinality(parameters, c.defaultCardinality)
	if c.aggExtended != nil {
		return c.sendToAggregator(timing, name, value, tags, rate, c.aggExtended.timing, cardinality)
	}
	return c.send(metric{metricType: timing, name: name, fvalue: value, tags: tags, rate: rate, globalTags: c.tags, namespace: c.namespace, originDetection: c.originDetection, cardinality: cardinality})
}

// Event sends the provided Event.
func (c *ClientEx) Event(e *Event, parameters ...Parameter) error {
	if c == nil {
		return ErrNoClient
	}
	atomic.AddUint64(&c.telemetry.totalEvents, 1)
	cardinality := parameterCardinality(parameters, c.defaultCardinality)
	return c.send(metric{metricType: event, evalue: e, rate: 1, globalTags: c.tags, namespace: c.namespace, originDetection: c.originDetection, cardinality: cardinality})
}

// SimpleEvent sends an event with the provided title and text.
func (c *ClientEx) SimpleEvent(title, text string, parameters ...Parameter) error {
	e := NewEvent(title, text)
	return c.Event(e, parameters...)
}

// ServiceCheck sends the provided ServiceCheck.
func (c *ClientEx) ServiceCheck(sc *ServiceCheck, parameters ...Parameter) error {
	if c == nil {
		return ErrNoClient
	}
	atomic.AddUint64(&c.telemetry.totalServiceChecks, 1)
	cardinality := parameterCardinality(parameters, c.defaultCardinality)
	return c.send(metric{metricType: serviceCheck, scvalue: sc, rate: 1, globalTags: c.tags, namespace: c.namespace, originDetection: c.originDetection, cardinality: cardinality})
}

// SimpleServiceCheck sends an serviceCheck with the provided name and status.
func (c *ClientEx) SimpleServiceCheck(name string, status ServiceCheckStatus, parameters ...Parameter) error {
	sc := NewServiceCheck(name, status)
	return c.ServiceCheck(sc, parameters...)
}

// Close the client connection.
func (c *ClientEx) Close() error {
	if c == nil {
		return ErrNoClient
	}

	// Acquire closer lock to ensure only one thread can close the stop channel
	c.closerLock.Lock()
	defer c.closerLock.Unlock()

	if c.isClosed {
		return nil
	}

	// Notify all other threads that they should stop
	select {
	case <-c.stop:
		return nil
	default:
	}
	close(c.stop)

	if c.workersMode == channelMode {
		for _, w := range c.workers {
			w.stopReceivingMetric()
		}
	}

	// flush the aggregator first
	if c.agg != nil {
		if c.aggExtended != nil && c.aggregatorMode == channelMode {
			c.agg.stopReceivingMetric()
		}
		c.agg.stop()
	}

	// Wait for the threads to stop
	c.wg.Wait()

	c.Flush()

	c.isClosed = true
	return c.sender.close()
}

func (*ClientEx) private() {
}

// isOriginDetectionEnabled returns whether origin detection is enabled.
//
// Disable origin detection only in one of the following cases:
// - DD_ORIGIN_DETECTION_ENABLED is explicitly set to false
// - o.originDetection is explicitly set to false, which is true by default
func isOriginDetectionEnabled(o *Options) bool {
	if !o.originDetection {
		return false
	}

	envVarValue := os.Getenv(originDetectionEnabled)
	if envVarValue == "" {
		// DD_ORIGIN_DETECTION_ENABLED is not set
		// default to true
		return true
	}

	enabled, err := strconv.ParseBool(envVarValue)
	if err != nil {
		// Error due to an unsupported DD_ORIGIN_DETECTION_ENABLED value
		// default to true
		return true
	}

	return enabled
}

// fillInContainerID returns whether the clients should fill the container field.
func fillInContainerID(o *Options) bool {
	if o.containerID != "" {
		return false
	}
	return isOriginDetectionEnabled(o)
}
