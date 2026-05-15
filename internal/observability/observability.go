// Package observability is the single entry point for metrics, structured
// logging, and tracing. Every service does the same 3 things on startup:
//
//	obs, shutdown, err := observability.Init(ctx, "service-name")
//	defer shutdown(ctx)
//
//	- obs.Log     — *slog.Logger, JSON to stdout (Promtail picks it up)
//	- obs.Tracer  — otel.Tracer, all spans go through it
//	- obs.Metrics — bundled prometheus collectors (registered globally)
//
// Why a single package: handlers and middleware import this; if they each
// reach into promauto / otel directly, you get duplicate registrations
// (panic) and inconsistent label names. One place to look = one place to fix.
package observability

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
)

// Obs is what services use day-to-day. Hold one of these, pass it around.
type Obs struct {
	Log     *slog.Logger
	Tracer  trace.Tracer
	Metrics *Metrics
}

// Init builds an Obs for `service`, starts a /metrics HTTP server on
// metricsPort, and wires the OTLP exporter to send traces to tempoEndpoint.
// Returns a shutdown func — call it from defer in main.
func Init(ctx context.Context, service string, metricsPort string, tempoEndpoint string) (*Obs, func(context.Context) error, error) {
	// 1) slog: JSON to stdout. Promtail collects stdout/stderr by default,
	// so we just write structured logs and let the infrastructure handle them.
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})).With("service", service)
	slog.SetDefault(log)

	// 2) Prometheus collectors. promauto auto-registers; we use the explicit
	// registry approach so multiple services don't fight over the global one
	// in tests.
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),                                       // Go runtime: GC pauses, goroutines, etc
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}), // RSS / FDs / CPU
	)
	metrics := newMetrics(reg, service)

	// 3) Start /metrics HTTP endpoint in a background goroutine.
	// Prometheus scrapes this via a ServiceMonitor we add per service later.
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})
	srv := &http.Server{Addr: metricsPort, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("metrics server", "err", err)
		}
	}()

	// 4) OTel: OTLP gRPC exporter to Tempo. tempoEndpoint usually "pp-tempo:4317".
	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(tempoEndpoint),
		otlptracegrpc.WithInsecure(), // in-cluster, no TLS
	)
	if err != nil {
		return nil, nil, fmt.Errorf("otlp exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp,
			sdktrace.WithBatchTimeout(5*time.Second),
		),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(service), // 这个 attribute 决定 Tempo 里按 service 筛选
			attribute.String("environment", "local"),
		)),
		// Sample 100% locally; production would use ParentBased + TraceIDRatioBased.
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)

	// 5) Critical: set propagator so trace context flows in/out of HTTP and
	// (manually) through Kafka headers. Without this, OTel won't extract
	// traceparent from incoming requests.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	obs := &Obs{
		Log:     log,
		Tracer:  tp.Tracer(service),
		Metrics: metrics,
	}

	shutdown := func(ctx context.Context) error {
		_ = srv.Shutdown(ctx)
		return tp.Shutdown(ctx)
	}
	return obs, shutdown, nil
}
