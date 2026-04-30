package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// fakeReader feeds a fixed slice of messages then returns context.Canceled
// after they are exhausted, so Run can return cleanly.
type fakeReader struct {
	mu       sync.Mutex
	messages []kafka.Message
	idx      int
	commits  []kafka.Message
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.idx >= len(r.messages) {
		return kafka.Message{}, context.Canceled
	}
	msg := r.messages[r.idx]
	r.idx++
	return msg, nil
}

func (r *fakeReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commits = append(r.commits, msgs...)
	return nil
}

func (r *fakeReader) Close() error { return nil }

type recordingSink struct {
	mu     sync.Mutex
	stored []StoredEvent
	err    error
}

func (s *recordingSink) StoreEvent(_ context.Context, ev StoredEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.stored = append(s.stored, ev)
	return nil
}

func mustEnvelope(t *testing.T, eventType, sightingID, requestID string) []byte {
	t.Helper()
	env := map[string]any{
		"type":      eventType,
		"timestamp": time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		"metadata":  map[string]any{"request_id": requestID, "trace_id": "abc"},
		"payload":   map[string]any{"id": sightingID, "species": "Wolf"},
	}
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func TestConsumer_HappyPath(t *testing.T) {
	reader := &fakeReader{messages: []kafka.Message{
		{Topic: "wildlife.sightings", Key: []byte("s1"), Value: mustEnvelope(t, "sighting.created", "s1", "req-1")},
		{Topic: "wildlife.sightings", Key: []byte("s2"), Value: mustEnvelope(t, "sighting.verified", "s2", "req-2")},
	}}
	sink := &recordingSink{}

	c := New(reader, sink, nil)
	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(sink.stored) != 2 {
		t.Fatalf("stored count=%d, want 2", len(sink.stored))
	}
	if sink.stored[0].Type != "sighting.created" || sink.stored[0].SightingID != "s1" {
		t.Errorf("first row: %+v", sink.stored[0])
	}
	if sink.stored[0].RequestID != "req-1" || sink.stored[0].TraceID != "abc" {
		t.Errorf("first row metadata: %+v", sink.stored[0])
	}
	if len(reader.commits) != 2 {
		t.Errorf("commits=%d, want 2", len(reader.commits))
	}
}

func TestConsumer_BadEnvelopeIsCommitted(t *testing.T) {
	reader := &fakeReader{messages: []kafka.Message{
		{Key: []byte("bad"), Value: []byte("not json")},
	}}
	sink := &recordingSink{}

	c := New(reader, sink, nil)
	_ = c.Run(context.Background())
	if len(sink.stored) != 0 {
		t.Errorf("stored=%d, want 0 for bad envelope", len(sink.stored))
	}
	if len(reader.commits) != 1 {
		t.Errorf("expected the bad message to be committed (skipped), commits=%d", len(reader.commits))
	}
}

func TestConsumer_StoreErrorIsCommitted(t *testing.T) {
	reader := &fakeReader{messages: []kafka.Message{
		{Key: []byte("s1"), Value: mustEnvelope(t, "sighting.created", "s1", "")},
	}}
	sink := &recordingSink{err: errors.New("db down")}

	c := New(reader, sink, nil)
	_ = c.Run(context.Background())
	if len(sink.stored) != 0 {
		t.Errorf("stored=%d, want 0", len(sink.stored))
	}
	if len(reader.commits) != 1 {
		t.Errorf("expected commit after store error (avoid poison-pill block)")
	}
}
