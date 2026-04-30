// Package observability wires OpenTelemetry tracing for the service. When
// OTEL_EXPORTER_OTLP_ENDPOINT is empty we install a no-op tracer provider
// so the rest of the codebase can call otel.Tracer(...) unconditionally.
package observability

import (
	"context"
	"fmt"

	"github.com/YemetsValen/go-kafka-proj/internal/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace/noop"
)

// Shutdown is the function returned by InitTracing for graceful provider
// shutdown. It is always non-nil and safe to call.
type Shutdown func(ctx context.Context) error

// InitTracing installs an OTLP HTTP tracer provider when an endpoint is
// configured; otherwise a noop provider is used so that traces are simply
// dropped.
func InitTracing(ctx context.Context, cfg config.Config) (Shutdown, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	if cfg.OTEL.OTLPEndpoint == "" {
		otel.SetTracerProvider(noop.NewTracerProvider())
		return func(context.Context) error { return nil }, nil
	}

	opts := []otlptracehttp.Option{otlptracehttp.WithEndpoint(cfg.OTEL.OTLPEndpoint)}
	if cfg.OTEL.OTLPInsecure {
		opts = append(opts, otlptracehttp.WithInsecure())
	}
	exp, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("create otlp exporter: %w", err)
	}

	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(cfg.ServiceName),
		semconv.DeploymentEnvironment(cfg.Env),
	))
	if err != nil {
		return nil, fmt.Errorf("build resource: %w", err)
	}

	sampler := sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.OTEL.SampleRatio))
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}
