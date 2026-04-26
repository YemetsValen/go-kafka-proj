package kafka

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
	topic  string
}

func NewProducer(brokers []string, topic string) *Producer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 50 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}
	return &Producer{writer: w, topic: topic}
}

type Event struct {
	Type      string      `json:"type"`
	Timestamp time.Time   `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

func (p *Producer) Publish(ctx context.Context, key string, eventType string, payload interface{}) error {
	event := Event{
		Type:      eventType,
		Timestamp: time.Now().UTC(),
		Payload:   payload,
	}
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	msg := kafka.Message{
		Key:   []byte(key),
		Value: body,
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		log.Printf("kafka publish failed (topic=%s, key=%s): %v", p.topic, key, err)
		return err
	}
	log.Printf("kafka published: topic=%s key=%s type=%s", p.topic, key, eventType)
	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
