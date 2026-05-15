package observability

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// WithTrace returns a logger that includes trace_id/span_id from ctx.
// Use at the top of handlers/middleware where ctx has a span:
//
//	log := observability.WithTrace(ctx, obs.Log)
//	log.Info("processing", "job_id", ...)
func WithTrace(ctx context.Context, log *slog.Logger) *slog.Logger {
	sc := trace.SpanFromContext(ctx).SpanContext()
	if !sc.IsValid() {
		return log
	}
	return log.With(
		"trace_id", sc.TraceID().String(),
		"span_id", sc.SpanID().String(),
	)
}
