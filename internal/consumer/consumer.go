// Package consumer reads sighting events off Kafka and persists them into
// the Postgres `events` table for analytics. It supports tracing via
// kafka-header propagation and exposes per-event-type metrics.
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	pkgkafka "github.com/YemetsValen/go-kafka-proj/internal/kafka"
	"github.com/YemetsValen/go-kafka-proj/internal/logging"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/YemetsValen/go-kafka-proj/internal/consumer"

// MessageReader is the subset of *kafka.Reader the consumer needs. It exists
// so unit tests can hand in an in-memory implementation.
type MessageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// EventSink stores a parsed sighting event somewhere durable.
type EventSink interface {
	StoreEvent(ctx context.Context, ev StoredEvent) error
}

// StoredEvent is the row written to the `events` table.
type StoredEvent struct {
	Type       string
	SightingID string
	Payload    json.RawMessage
	RequestID  string
	TraceID    string
	OccurredAt time.Time
	ReceivedAt time.Time
}

// Consumer pulls one message at a time from Kafka, persists it, and commits.
type Consumer struct {
	reader  MessageReader
	sink    EventSink
	metrics *prometheus.CounterVec // labels: event_type, result
}

// New constructs a Consumer. consumeCounter may be nil.
func New(reader MessageReader, sink EventSink, consumeCounter *prometheus.CounterVec) *Consumer {
	return &Consumer{reader: reader, sink: sink, metrics: consumeCounter}
}

// Run reads until ctx is cancelled or the reader returns io.EOF. Per-message
// errors are logged and the message is committed anyway so a single poison
// pill cannot block the consumer; replace this with a dead-letter strategy
// for production.
func (c *Consumer) Run(ctx context.Context) error {
	logger := logging.FromContext(ctx)
	logger.Info("consumer starting")
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
				logger.Info("consumer stopped", "reason", err.Error())
				return nil
			}
			logger.Error("fetch failed", "err", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		c.handleMessage(ctx, msg)
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			logger.Error("commit failed", "err", err)
		}
	}
}

// handleMessage parses the envelope, persists the event, and bumps metrics.
func (c *Consumer) handleMessage(parent context.Context, msg kafka.Message) {
	ctx := pkgkafka.ExtractContext(parent, msg)
	tr := otel.Tracer(tracerName)
	ctx, span := tr.Start(ctx, "kafka.consume",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(msg.Topic),
			attribute.Int("messaging.kafka.message.partition", msg.Partition),
			attribute.Int64("messaging.kafka.message.offset", msg.Offset),
		),
	)
	defer span.End()

	logger := logging.FromContext(ctx)

	var env pkgkafka.Event
	if err := json.Unmarshal(msg.Value, &env); err != nil {
		c.recordResult("unknown", "parse_error")
		span.RecordError(err)
		span.SetStatus(codes.Error, "decode envelope")
		logger.Error("decode envelope", "err", err, "key", string(msg.Key))
		return
	}
	span.SetAttributes(attribute.String("messaging.kafka.event_type", env.Type))

	payloadBytes, err := json.Marshal(env.Payload)
	if err != nil {
		c.recordResult(env.Type, "payload_error")
		span.RecordError(err)
		span.SetStatus(codes.Error, "marshal payload")
		logger.Error("marshal payload", "err", err, "type", env.Type)
		return
	}

	stored := StoredEvent{
		Type:       env.Type,
		SightingID: string(msg.Key),
		Payload:    payloadBytes,
		RequestID:  env.Metadata.RequestID,
		TraceID:    env.Metadata.TraceID,
		OccurredAt: env.Timestamp,
		ReceivedAt: time.Now().UTC(),
	}
	if err := c.sink.StoreEvent(ctx, stored); err != nil {
		c.recordResult(env.Type, "store_error")
		span.RecordError(err)
		span.SetStatus(codes.Error, "store event")
		logger.Error("store event", "err", err, "type", env.Type)
		return
	}
	c.recordResult(env.Type, "ok")
	logger.Debug("event stored", "type", env.Type, "sighting_id", stored.SightingID)
}

func (c *Consumer) recordResult(eventType, result string) {
	if c.metrics != nil {
		c.metrics.WithLabelValues(eventType, result).Inc()
	}
}

// Close shuts the underlying reader.
func (c *Consumer) Close() error { return c.reader.Close() }

// ---- Postgres-backed EventSink ----

// PostgresEventSink writes events into the `events` table.
type PostgresEventSink struct{ pool *pgxpool.Pool }

// NewPostgresEventSink constructs a sink against the given pool.
func NewPostgresEventSink(pool *pgxpool.Pool) *PostgresEventSink {
	return &PostgresEventSink{pool: pool}
}

// StoreEvent persists one event row.
func (p *PostgresEventSink) StoreEvent(ctx context.Context, ev StoredEvent) error {
	const stmt = `
INSERT INTO events (event_type, sighting_id, payload, request_id, trace_id, occurred_at, received_at)
VALUES ($1, $2, $3, NULLIF($4, ''), NULLIF($5, ''), $6, $7)`
	_, err := p.pool.Exec(ctx, stmt, ev.Type, ev.SightingID, ev.Payload, ev.RequestID, ev.TraceID, ev.OccurredAt, ev.ReceivedAt)
	if err != nil {
		return fmt.Errorf("insert event: %w", err)
	}
	return nil
}

// NewKafkaReader builds a reader on the given consumer group.
func NewKafkaReader(brokers []string, topic, group string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        group,
		Topic:          topic,
		MinBytes:       1,
		MaxBytes:       10 << 20,
		CommitInterval: 0, // explicit commits via CommitMessages
	})
}
