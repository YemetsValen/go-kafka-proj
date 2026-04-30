package events

import (
	"testing"
	"time"

	"github.com/YemetsValen/go-kafka-proj/internal/models"
)

func TestBus_FanOut(t *testing.T) {
	b := New()
	a := b.Subscribe()
	c := b.Subscribe()
	defer b.Unsubscribe(a)
	defer b.Unsubscribe(c)

	go b.Publish(Event{Type: TypeCreated, Timestamp: time.Now(), Sighting: models.Sighting{ID: "1", Species: "fox"}})

	for i, ch := range []chan Event{a, c} {
		select {
		case ev := <-ch:
			if ev.Type != TypeCreated || ev.Sighting.ID != "1" {
				t.Errorf("subscriber %d got unexpected event: %+v", i, ev)
			}
		case <-time.After(time.Second):
			t.Fatalf("subscriber %d did not receive event", i)
		}
	}
}

func TestBus_UnsubscribeIsIdempotent(t *testing.T) {
	b := New()
	ch := b.Subscribe()
	b.Unsubscribe(ch)
	b.Unsubscribe(ch) // must not panic
}

func TestBus_SlowSubscriberDoesNotBlockPublisher(t *testing.T) {
	b := New()
	ch := b.Subscribe()
	defer b.Unsubscribe(ch)

	// Fill the buffer.
	for i := 0; i < 100; i++ {
		b.Publish(Event{Type: TypeCreated, Sighting: models.Sighting{ID: "x"}})
	}
	// Publisher returned without blocking — that's the test.
}
