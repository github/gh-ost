package statsd

import (
	"fmt"
	"sync"
	"time"
)

/*
telemetryInterval is the interval at which telemetry will be sent by the client.
*/
const telemetryInterval = 10 * time.Second

/*
clientTelemetryTag is a tag identifying this specific client.
*/
var clientTelemetryTag = "client:go"

/*
clientVersionTelemetryTag is a tag identifying this specific client version.
*/
var clientVersionTelemetryTag = "client_version:5.8.3"

// Telemetry represents internal metrics about the client behavior since it started.
type Telemetry struct {
	//
	// Those are produced by the 'Client'
	//

	// TotalMetrics is the total number of metrics sent by the client before aggregation and sampling.
	TotalMetrics uint64
	// TotalMetricsGauge is the total number of gauges sent by the client before aggregation and sampling.
	TotalMetricsGauge uint64
	// TotalMetricsCount is the total number of counts sent by the client before aggregation and sampling.
	TotalMetricsCount uint64
	// TotalMetricsHistogram is the total number of histograms sent by the client before aggregation and sampling.
	TotalMetricsHistogram uint64
	// TotalMetricsDistribution is the total number of distributions sent by the client before aggregation and
	// sampling.
	TotalMetricsDistribution uint64
	// TotalMetricsSet is the total number of sets sent by the client before aggregation and sampling.
	TotalMetricsSet uint64
	// TotalMetricsTiming is the total number of timings sent by the client before aggregation and sampling.
	TotalMetricsTiming uint64
	// TotalEvents is the total number of events sent by the client before aggregation and sampling.
	TotalEvents uint64
	// TotalServiceChecks is the total number of service_checks sent by the client before aggregation and sampling.
	TotalServiceChecks uint64

	// TotalDroppedOnReceive is the total number metrics/event/service_checks dropped when using ChannelMode (see
	// WithChannelMode option).
	TotalDroppedOnReceive uint64

	//
	// Those are produced by the 'sender'
	//

	// TotalPayloadsSent is the total number of payload (packet on the network) succesfully sent by the client. When
	// using UDP we don't know if packet dropped or not, so all packet are considered as succesfully sent.
	TotalPayloadsSent uint64
	// TotalPayloadsDropped is the total number of payload dropped by the client. This includes all cause of dropped
	// (TotalPayloadsDroppedQueueFull and TotalPayloadsDroppedWriter). When using UDP This won't includes the
	// network dropped.
	TotalPayloadsDropped uint64
	// TotalPayloadsDroppedWriter is the total number of payload dropped by the writer (when using UDS or named
	// pipe) due to network timeout or error.
	TotalPayloadsDroppedWriter uint64
	// TotalPayloadsDroppedQueueFull is the total number of payload dropped internally because the queue of payloads
	// waiting to be sent on the wire is full. This means the client is generating more metrics than can be sent on
	// the wire. If your app sends metrics in batch look at WithSenderQueueSize option to increase the queue size.
	TotalPayloadsDroppedQueueFull uint64

	// TotalBytesSent is the total number of bytes succesfully sent by the client. When using UDP we don't know if
	// packet dropped or not, so all packet are considered as succesfully sent.
	TotalBytesSent uint64
	// TotalBytesDropped is the total number of bytes dropped by the client. This includes all cause of dropped
	// (TotalBytesDroppedQueueFull and TotalBytesDroppedWriter). When using UDP This
	// won't includes the network dropped.
	TotalBytesDropped uint64
	// TotalBytesDroppedWriter is the total number of bytes dropped by the writer (when using UDS or named pipe) due
	// to network timeout or error.
	TotalBytesDroppedWriter uint64
	// TotalBytesDroppedQueueFull is the total number of bytes dropped internally because the queue of payloads
	// waiting to be sent on the wire is full. This means the client is generating more metrics than can be sent on
	// the wire. If your app sends metrics in batch look at WithSenderQueueSize option to increase the queue size.
	TotalBytesDroppedQueueFull uint64

	//
	// Those are produced by the 'aggregator'
	//

	// AggregationNbContext is the total number of contexts flushed by the aggregator when either
	// WithClientSideAggregation or WithExtendedClientSideAggregation options are enabled.
	AggregationNbContext uint64
	// AggregationNbContextGauge is the total number of contexts for gauges flushed by the aggregator when either
	// WithClientSideAggregation or WithExtendedClientSideAggregation options are enabled.
	AggregationNbContextGauge uint64
	// AggregationNbContextCount is the total number of contexts for counts flushed by the aggregator when either
	// WithClientSideAggregation or WithExtendedClientSideAggregation options are enabled.
	AggregationNbContextCount uint64
	// AggregationNbContextSet is the total number of contexts for sets flushed by the aggregator when either
	// WithClientSideAggregation or WithExtendedClientSideAggregation options are enabled.
	AggregationNbContextSet uint64
	// AggregationNbContextHistogram is the total number of contexts for histograms flushed by the aggregator when either
	// WithClientSideAggregation or WithExtendedClientSideAggregation options are enabled.
	AggregationNbContextHistogram uint64
	// AggregationNbContextDistribution is the total number of contexts for distributions flushed by the aggregator when either
	// WithClientSideAggregation or WithExtendedClientSideAggregation options are enabled.
	AggregationNbContextDistribution uint64
	// AggregationNbContextTiming is the total number of contexts for timings flushed by the aggregator when either
	// WithClientSideAggregation or WithExtendedClientSideAggregation options are enabled.
	AggregationNbContextTiming uint64
}

type telemetryClient struct {
	sync.RWMutex // used mostly to change the transport tag.

	c                 *ClientEx
	aggEnabled        bool // is aggregation enabled and should we sent aggregation telemetry.
	transport         string
	tags              []string
	tagsByType        map[metricType][]string
	transportTagKnown bool
	sender            *sender
	worker            *worker
	lastSample        Telemetry // The previous sample of telemetry sent
}

func newTelemetryClient(c *ClientEx, aggregationEnabled bool) *telemetryClient {
	t := &telemetryClient{
		c:          c,
		aggEnabled: aggregationEnabled,
		tags:       []string{},
		tagsByType: map[metricType][]string{},
	}

	t.setTags()
	return t
}

func newTelemetryClientWithCustomAddr(c *ClientEx, telemetryAddr string, aggregationEnabled bool, pool *bufferPool,
	writeTimeout time.Duration, connectTimeout time.Duration,
) (*telemetryClient, error) {
	telemetryAddr = resolveAddr(telemetryAddr)
	telemetryWriter, _, err := createWriter(telemetryAddr, writeTimeout, connectTimeout)
	if err != nil {
		return nil, fmt.Errorf("Could not resolve telemetry address: %v", err)
	}

	t := newTelemetryClient(c, aggregationEnabled)

	// Creating a custom sender/worker with 1 worker in mutex mode for the
	// telemetry that share the same bufferPool.
	// FIXME due to performance pitfall, we're always using UDP defaults
	// even for UDS.
	t.sender = newSender(telemetryWriter, DefaultUDPBufferPoolSize, pool, c.errorHandler)
	t.worker = newWorker(pool, t.sender)
	return t, nil
}

func (t *telemetryClient) run(wg *sync.WaitGroup, stop chan struct{}) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(telemetryInterval)
		for {
			select {
			case <-ticker.C:
				t.sendTelemetry()
			case <-stop:
				ticker.Stop()
				if t.sender != nil {
					t.sender.close()
				}
				return
			}
		}
	}()
}

func (t *telemetryClient) sendTelemetry() {
	for _, m := range t.flush() {
		if t.worker != nil {
			t.worker.processMetric(m)
		} else {
			t.c.send(m)
		}
	}

	if t.worker != nil {
		t.worker.flush()
	}
}

func (t *telemetryClient) getTelemetry() Telemetry {
	if t == nil {
		// telemetry was disabled through the WithoutTelemetry option
		return Telemetry{}
	}

	tlm := Telemetry{}
	t.c.flushTelemetryMetrics(&tlm)
	t.c.sender.flushTelemetryMetrics(&tlm)
	t.c.agg.flushTelemetryMetrics(&tlm)

	tlm.TotalMetrics = tlm.TotalMetricsGauge +
		tlm.TotalMetricsCount +
		tlm.TotalMetricsSet +
		tlm.TotalMetricsHistogram +
		tlm.TotalMetricsDistribution +
		tlm.TotalMetricsTiming

	tlm.TotalPayloadsDropped = tlm.TotalPayloadsDroppedQueueFull + tlm.TotalPayloadsDroppedWriter
	tlm.TotalBytesDropped = tlm.TotalBytesDroppedQueueFull + tlm.TotalBytesDroppedWriter

	if t.aggEnabled {
		tlm.AggregationNbContext = tlm.AggregationNbContextGauge +
			tlm.AggregationNbContextCount +
			tlm.AggregationNbContextSet +
			tlm.AggregationNbContextHistogram +
			tlm.AggregationNbContextDistribution +
			tlm.AggregationNbContextTiming
	}
	return tlm
}

// setTransportTag if it was never set and is now known.
func (t *telemetryClient) setTags() {
	transport := t.c.GetTransport()
	t.RLock()
	// We need to refresh if we never set the tags or if the transport changed.
	// For example when `unix://` is used we might return `uds` until we actually connect and detect that
	// this is a UDS Stream socket and then return `uds-stream`.
	needsRefresh := len(t.tags) == len(t.c.tags) || t.transport != transport
	t.RUnlock()

	if !needsRefresh {
		return
	}

	t.Lock()
	defer t.Unlock()

	t.transport = transport
	t.tags = append(t.c.tags, clientTelemetryTag, clientVersionTelemetryTag)
	if transport != "" {
		t.tags = append(t.tags, "client_transport:"+transport)
	}
	t.tagsByType[gauge] = append(append([]string{}, t.tags...), "metrics_type:gauge")
	t.tagsByType[count] = append(append([]string{}, t.tags...), "metrics_type:count")
	t.tagsByType[set] = append(append([]string{}, t.tags...), "metrics_type:set")
	t.tagsByType[timing] = append(append([]string{}, t.tags...), "metrics_type:timing")
	t.tagsByType[histogram] = append(append([]string{}, t.tags...), "metrics_type:histogram")
	t.tagsByType[distribution] = append(append([]string{}, t.tags...), "metrics_type:distribution")
}

// flushTelemetry returns Telemetry metrics to be flushed. It's its own function to ease testing.
func (t *telemetryClient) flush() []metric {
	m := []metric{}

	// same as Count but without global namespace
	telemetryCount := func(name string, value int64, tags []string) {
		m = append(m, metric{metricType: count, name: name, ivalue: value, tags: tags, rate: 1})
	}

	tlm := t.getTelemetry()
	t.setTags()

	// We send the diff between now and the previous telemetry flush. This keep the same telemetry behavior from V4
	// so users dashboard's aren't broken when upgrading to V5. It also allow to graph on the same dashboard a mix
	// of V4 and V5 apps.
	telemetryCount("datadog.dogstatsd.client.metrics", int64(tlm.TotalMetrics-t.lastSample.TotalMetrics), t.tags)
	telemetryCount("datadog.dogstatsd.client.metrics_by_type", int64(tlm.TotalMetricsGauge-t.lastSample.TotalMetricsGauge), t.tagsByType[gauge])
	telemetryCount("datadog.dogstatsd.client.metrics_by_type", int64(tlm.TotalMetricsCount-t.lastSample.TotalMetricsCount), t.tagsByType[count])
	telemetryCount("datadog.dogstatsd.client.metrics_by_type", int64(tlm.TotalMetricsHistogram-t.lastSample.TotalMetricsHistogram), t.tagsByType[histogram])
	telemetryCount("datadog.dogstatsd.client.metrics_by_type", int64(tlm.TotalMetricsDistribution-t.lastSample.TotalMetricsDistribution), t.tagsByType[distribution])
	telemetryCount("datadog.dogstatsd.client.metrics_by_type", int64(tlm.TotalMetricsSet-t.lastSample.TotalMetricsSet), t.tagsByType[set])
	telemetryCount("datadog.dogstatsd.client.metrics_by_type", int64(tlm.TotalMetricsTiming-t.lastSample.TotalMetricsTiming), t.tagsByType[timing])
	telemetryCount("datadog.dogstatsd.client.events", int64(tlm.TotalEvents-t.lastSample.TotalEvents), t.tags)
	telemetryCount("datadog.dogstatsd.client.service_checks", int64(tlm.TotalServiceChecks-t.lastSample.TotalServiceChecks), t.tags)

	telemetryCount("datadog.dogstatsd.client.metric_dropped_on_receive", int64(tlm.TotalDroppedOnReceive-t.lastSample.TotalDroppedOnReceive), t.tags)

	telemetryCount("datadog.dogstatsd.client.packets_sent", int64(tlm.TotalPayloadsSent-t.lastSample.TotalPayloadsSent), t.tags)
	telemetryCount("datadog.dogstatsd.client.packets_dropped", int64(tlm.TotalPayloadsDropped-t.lastSample.TotalPayloadsDropped), t.tags)
	telemetryCount("datadog.dogstatsd.client.packets_dropped_queue", int64(tlm.TotalPayloadsDroppedQueueFull-t.lastSample.TotalPayloadsDroppedQueueFull), t.tags)
	telemetryCount("datadog.dogstatsd.client.packets_dropped_writer", int64(tlm.TotalPayloadsDroppedWriter-t.lastSample.TotalPayloadsDroppedWriter), t.tags)

	telemetryCount("datadog.dogstatsd.client.bytes_dropped", int64(tlm.TotalBytesDropped-t.lastSample.TotalBytesDropped), t.tags)
	telemetryCount("datadog.dogstatsd.client.bytes_sent", int64(tlm.TotalBytesSent-t.lastSample.TotalBytesSent), t.tags)
	telemetryCount("datadog.dogstatsd.client.bytes_dropped_queue", int64(tlm.TotalBytesDroppedQueueFull-t.lastSample.TotalBytesDroppedQueueFull), t.tags)
	telemetryCount("datadog.dogstatsd.client.bytes_dropped_writer", int64(tlm.TotalBytesDroppedWriter-t.lastSample.TotalBytesDroppedWriter), t.tags)

	if t.aggEnabled {
		telemetryCount("datadog.dogstatsd.client.aggregated_context", int64(tlm.AggregationNbContext-t.lastSample.AggregationNbContext), t.tags)
		telemetryCount("datadog.dogstatsd.client.aggregated_context_by_type", int64(tlm.AggregationNbContextGauge-t.lastSample.AggregationNbContextGauge), t.tagsByType[gauge])
		telemetryCount("datadog.dogstatsd.client.aggregated_context_by_type", int64(tlm.AggregationNbContextSet-t.lastSample.AggregationNbContextSet), t.tagsByType[set])
		telemetryCount("datadog.dogstatsd.client.aggregated_context_by_type", int64(tlm.AggregationNbContextCount-t.lastSample.AggregationNbContextCount), t.tagsByType[count])
		telemetryCount("datadog.dogstatsd.client.aggregated_context_by_type", int64(tlm.AggregationNbContextHistogram-t.lastSample.AggregationNbContextHistogram), t.tagsByType[histogram])
		telemetryCount("datadog.dogstatsd.client.aggregated_context_by_type", int64(tlm.AggregationNbContextDistribution-t.lastSample.AggregationNbContextDistribution), t.tagsByType[distribution])
		telemetryCount("datadog.dogstatsd.client.aggregated_context_by_type", int64(tlm.AggregationNbContextTiming-t.lastSample.AggregationNbContextTiming), t.tagsByType[timing])
	}

	t.lastSample = tlm

	return m
}
