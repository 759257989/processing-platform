package observability

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Per-tier histogram buckets. The default Prometheus buckets (5ms..10s) give
// useless p99 numbers for both ends of the spectrum:
//   - realtime (ms-range): all data lands in the 5ms bucket → p99 ≈ 5ms always
//   - bulk (min-range): all data lands in +Inf → p99 ≈ +Inf always
//
// Tailored buckets are mandatory for credible SLO claims.
var (
	realtimeBuckets = []float64{0.001, 0.005, 0.025, 0.1, 0.5, 1, 5}
	standardBuckets = []float64{0.1, 1, 5, 30, 120}
	bulkBuckets     = []float64{10, 60, 300, 1800}
)

// Metrics bundles every collector the platform exposes. Hold one per service;
// passed via Obs.Metrics.
type Metrics struct {
	// HTTP middleware (API only)
	HTTPRequestsTotal   *prometheus.CounterVec
	HTTPRequestDuration *prometheus.HistogramVec

	// Job lifecycle (workers)
	JobsProcessedTotal  *prometheus.CounterVec   // labels: type, tier, state
	JobDurationRealtime *prometheus.HistogramVec // labels: type
	JobDurationStandard *prometheus.HistogramVec
	JobDurationBulk     *prometheus.HistogramVec

	// Kafka consumer (workers + retry-router + ingestion)
	KafkaMessagesConsumed *prometheus.CounterVec // labels: topic, group
	KafkaConsumerLag      *prometheus.GaugeVec   // labels: topic, partition
	KafkaProduceTotal     *prometheus.CounterVec // labels: topic, result(success/error)

	// DLQ surface
	DLQDepth *prometheus.GaugeVec // labels: topic

	// External call surface (mock-device + mock-webhook via clients)
	ExternalCallsTotal   *prometheus.CounterVec // labels: target, result
	ExternalCallDuration *prometheus.HistogramVec
}

func newMetrics(reg *prometheus.Registry, service string) *Metrics {
	m := &Metrics{
		HTTPRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "pp",
			Subsystem: "http",
			Name:      "requests_total",
			Help:      "Total HTTP requests served, labeled by method, route, status.",
		}, []string{"method", "route", "status"}),

		HTTPRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "pp",
			Subsystem: "http",
			Name:      "request_duration_seconds",
			Help:      "HTTP request handler latency.",
			// HTTP-friendly buckets — most requests are ms-range
			Buckets: []float64{0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
		}, []string{"method", "route", "status"}),

		JobsProcessedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "pp",
			Subsystem: "jobs",
			Name:      "processed_total",
			Help:      "Total jobs processed, labeled by type / tier / final state.",
		}, []string{"type", "tier", "state"}),

		JobDurationRealtime: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "pp", Subsystem: "jobs",
			Name: "duration_realtime_seconds", Help: "Realtime tier job duration.",
			Buckets: realtimeBuckets,
		}, []string{"type"}),

		JobDurationStandard: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "pp", Subsystem: "jobs",
			Name: "duration_standard_seconds", Help: "Standard tier job duration.",
			Buckets: standardBuckets,
		}, []string{"type"}),

		JobDurationBulk: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "pp", Subsystem: "jobs",
			Name: "duration_bulk_seconds", Help: "Bulk tier job duration.",
			Buckets: bulkBuckets,
		}, []string{"type"}),

		KafkaMessagesConsumed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "pp", Subsystem: "kafka",
			Name: "messages_consumed_total", Help: "Total Kafka messages consumed.",
		}, []string{"topic", "group"}),

		KafkaConsumerLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "pp", Subsystem: "kafka",
			Name: "consumer_lag", Help: "Kafka consumer lag per partition.",
		}, []string{"topic", "partition", "group"}),

		KafkaProduceTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "pp", Subsystem: "kafka",
			Name: "produce_total", Help: "Total messages produced.",
		}, []string{"topic", "result"}),

		DLQDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "pp", Subsystem: "dlq",
			Name: "depth", Help: "Approximate DLQ message count.",
		}, []string{"topic"}),

		ExternalCallsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "pp", Subsystem: "external",
			Name: "calls_total", Help: "Calls to external (mock) services.",
		}, []string{"target", "result"}),

		ExternalCallDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "pp", Subsystem: "external",
			Name: "call_duration_seconds", Help: "External call latency.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 10},
		}, []string{"target"}),
	}

	reg.MustRegister(
		m.HTTPRequestsTotal, m.HTTPRequestDuration,
		m.JobsProcessedTotal,
		m.JobDurationRealtime, m.JobDurationStandard, m.JobDurationBulk,
		m.KafkaMessagesConsumed, m.KafkaConsumerLag, m.KafkaProduceTotal,
		m.DLQDepth,
		m.ExternalCallsTotal, m.ExternalCallDuration,
	)
	return m
}

// ObserveJob picks the right histogram by tier — callers don't have to.
func (m *Metrics) ObserveJob(typ, tier, state string, duration float64) {
	m.JobsProcessedTotal.WithLabelValues(typ, tier, state).Inc()
	switch tier {
	case "realtime":
		m.JobDurationRealtime.WithLabelValues(typ).Observe(duration)
	case "standard":
		m.JobDurationStandard.WithLabelValues(typ).Observe(duration)
	case "bulk":
		m.JobDurationBulk.WithLabelValues(typ).Observe(duration)
	}
}
