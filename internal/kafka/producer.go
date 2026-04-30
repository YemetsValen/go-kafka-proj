package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/YemetsValen/go-kafka-proj/internal/logging"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/YemetsValen/go-kafka-proj/internal/kafka"

// Producer wraps a kafka-go Writer with the JSON envelope, tracing and
// metrics. It is safe for concurrent use.
type Producer struct {
	writer  *kafka.Writer
	topic   string
	publish *prometheus.CounterVec // labels: event_type, result
}

// NewProducer constructs a Producer. publishCounter may be nil — pass the
// observability.Metrics.KafkaPub counter to enable per-event-type metrics.
func NewProducer(brokers []string, topic string, publishCounter *prometheus.CounterVec) *Producer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 50 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}
	return &Producer{writer: w, topic: topic, publish: publishCounter}
}

// Event is the JSON envelope every message carries.
type Event struct {
	Type      string        `json:"type"`
	Timestamp time.Time     `json:"timestamp"`
	Metadata  EventMetadata `json:"metadata,omitempty"`
	Payload   interface{}   `json:"payload"`
}

// EventMetadata carries cross-cutting context for tracing/correlation. The
// trace_id is also written to the kafka message headers via W3C tracecontext
// so downstream consumers can resume the span.
type EventMetadata struct {
	RequestID string `json:"request_id,omitempty"`
	TraceID   string `json:"trace_id,omitempty"`
}

// Publish marshals the event, attaches trace context to message headers, and
// records a publish counter sample tagged with the result.
func (p *Producer) Publish(ctx context.Context, key string, eventType string, payload interface{}) error {
	tr := otel.Tracer(tracerName)
	ctx, span := tr.Start(ctx, "kafka.publish "+eventType,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(p.topic),
			attribute.String("messaging.kafka.message.key", key),
			attribute.String("messaging.kafka.event_type", eventType),
		),
	)
	defer span.End()

	event := Event{
		Type:      eventType,
		Timestamp: time.Now().UTC(),
		Metadata: EventMetadata{
			RequestID: logging.RequestIDFromContext(ctx),
			TraceID:   logging.TraceIDFromContext(ctx),
		},
		Payload: payload,
	}
	body, err := json.Marshal(event)
	if err != nil {
		p.recordResult(eventType, "marshal_error")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("marshal event: %w", err)
	}

	headers := otelHeadersFromContext(ctx)
	if event.Metadata.RequestID != "" {
		headers = append(headers, kafka.Header{Key: "x-request-id", Value: []byte(event.Metadata.RequestID)})
	}

	msg := kafka.Message{
		Key:     []byte(key),
		Value:   body,
		Headers: headers,
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		p.recordResult(eventType, "error")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		logging.FromContext(ctx).Error("kafka publish failed",
			"topic", p.topic, "key", key, "type", eventType, "err", err)
		return err
	}
	p.recordResult(eventType, "ok")
	logging.FromContext(ctx).Debug("kafka published",
		"topic", p.topic, "key", key, "type", eventType)
	return nil
}

func (p *Producer) recordResult(eventType, result string) {
	if p.publish != nil {
		p.publish.WithLabelValues(eventType, result).Inc()
	}
}

// Close flushes and shuts down the writer.
func (p *Producer) Close() error { return p.writer.Close() }

// ---- W3C tracecontext propagation over kafka headers ----

// otelHeadersFromContext serialises the active span context into kafka
// headers using the global propagator.
func otelHeadersFromContext(ctx context.Context) []kafka.Header {
	carrier := headerCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier.headers()
}

// ExtractContext reads tracecontext headers from a kafka message and returns
// a context with the parent span context attached.
func ExtractContext(parent context.Context, msg kafka.Message) context.Context {
	carrier := headerCarrier{}
	for _, h := range msg.Headers {
		carrier[h.Key] = string(h.Value)
	}
	return otel.GetTextMapPropagator().Extract(parent, carrier)
}

// headerCarrier implements propagation.TextMapCarrier on top of a flat map
// that we then convert to []kafka.Header.
type headerCarrier map[string]string

func (h headerCarrier) Get(key string) string        { return h[key] }
func (h headerCarrier) Set(key string, value string) { h[key] = value }
func (h headerCarrier) Keys() []string {
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	return keys
}

func (h headerCarrier) headers() []kafka.Header {
	out := make([]kafka.Header, 0, len(h))
	for k, v := range h {
		out = append(out, kafka.Header{Key: k, Value: []byte(v)})
	}
	return out
}

// Static interface check.
var _ propagation.TextMapCarrier = headerCarrier{}
