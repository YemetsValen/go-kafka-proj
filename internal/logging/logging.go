// Package logging configures the global slog logger and provides chi/grpc
// helpers that propagate request_id (and trace_id when present) into logs.
package logging

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/YemetsValen/go-kafka-proj/internal/config"
	chimw "github.com/go-chi/chi/v5/middleware"
	"go.opentelemetry.io/otel/trace"
)

// New builds an slog.Logger from the LogConfig and installs it as the global
// default so packages that don't take a logger argument still emit structured
// records.
func New(cfg config.LogConfig, service string) *slog.Logger {
	level := parseLevel(cfg.Level)
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	switch strings.ToLower(cfg.Format) {
	case "text":
		h = slog.NewTextHandler(os.Stdout, opts)
	default:
		h = slog.NewJSONHandler(os.Stdout, opts)
	}
	logger := slog.New(h).With("service", service)
	slog.SetDefault(logger)
	return logger
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// RequestIDFromContext returns the chi request id, or "".
func RequestIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(chimw.RequestIDKey).(string); ok {
		return v
	}
	return ""
}

// TraceIDFromContext returns the active OpenTelemetry trace id (hex) or "".
func TraceIDFromContext(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ""
	}
	return sc.TraceID().String()
}

// FromContext returns a logger pre-decorated with request_id + trace_id from
// the supplied context. Always returns a non-nil logger.
func FromContext(ctx context.Context) *slog.Logger {
	l := slog.Default()
	if rid := RequestIDFromContext(ctx); rid != "" {
		l = l.With("request_id", rid)
	}
	if tid := TraceIDFromContext(ctx); tid != "" {
		l = l.With("trace_id", tid)
	}
	return l
}

// HTTPMiddleware logs each HTTP request with method, path, status, latency,
// and correlation IDs.
func HTTPMiddleware(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := chimw.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)
			l := logger
			if rid := RequestIDFromContext(r.Context()); rid != "" {
				l = l.With("request_id", rid)
			}
			if tid := TraceIDFromContext(r.Context()); tid != "" {
				l = l.With("trace_id", tid)
			}
			l.LogAttrs(r.Context(), slog.LevelInfo, "http_request",
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", ww.Status()),
				slog.Int("bytes", ww.BytesWritten()),
				slog.String("latency", fmt.Sprint(time.Since(start))),
				slog.String("remote", r.RemoteAddr),
			)
		})
	}
}
