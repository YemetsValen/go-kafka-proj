package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/YemetsValen/go-kafka-proj/internal/events"
	"github.com/YemetsValen/go-kafka-proj/internal/models"
	"github.com/YemetsValen/go-kafka-proj/internal/store"
)

// TestStream_EmitsEventsAsSSE drives a real chi router with the handler
// wired to a shared bus, opens a streaming GET /sightings/stream, and
// asserts the bus events are delivered as SSE frames.
func TestStream_EmitsEventsAsSSE(t *testing.T) {
	bus := events.New()
	h := New(store.NewMemory(), newMockPublisher(), bus)
	srv := httptest.NewServer(h.Routes(nil))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("Content-Type"); got != "text/event-stream" {
		t.Errorf("expected text/event-stream Content-Type, got %q", got)
	}

	// Give the handler a moment to subscribe before publishing.
	time.Sleep(50 * time.Millisecond)
	bus.Publish(events.Event{
		Type:      events.TypeCreated,
		Timestamp: time.Now().UTC(),
		Sighting:  models.Sighting{ID: "1", Species: "fox"},
	})

	buf := make([]byte, 1024)
	deadline := time.Now().Add(2 * time.Second)
	var got string
	for time.Now().Before(deadline) && !strings.Contains(got, "event: created") {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			got += string(buf[:n])
		}
		if err != nil {
			break
		}
	}
	if !strings.Contains(got, "event: created") {
		t.Fatalf("expected SSE 'event: created' frame, got: %q", got)
	}
	if !strings.Contains(got, `"id":"1"`) {
		t.Fatalf("expected sighting payload in stream, got: %q", got)
	}
}
