// Package events is an in-process pub/sub for sighting domain events. Both
// the HTTP handlers and the gRPC service publish to the same Bus so the
// SSE feed (used by the web UI) and gRPC Watch see a single, unified stream.
package events

import (
	"sync"
	"time"

	"github.com/YemetsValen/go-kafka-proj/internal/models"
)

// Type enumerates the kinds of domain events the bus carries.
type Type string

const (
	TypeCreated   Type = "created"
	TypeUpdated   Type = "updated"
	TypeNoteAdded Type = "note_added"
	TypeVerified  Type = "verified"
	TypeDeleted   Type = "deleted"
)

// Event is the domain payload distributed by the Bus.
type Event struct {
	Type      Type            `json:"type"`
	Timestamp time.Time       `json:"timestamp"`
	Sighting  models.Sighting `json:"sighting"`
	Note      *models.Note    `json:"note,omitempty"`
}

// Bus is a tiny in-process fan-out. It is not durable and does not replay
// history. Slow subscribers have events dropped rather than blocking the
// publisher.
type Bus struct {
	mu   sync.Mutex
	subs map[chan Event]struct{}
}

// New returns a fresh Bus with no subscribers.
func New() *Bus { return &Bus{subs: make(map[chan Event]struct{})} }

// Subscribe returns a buffered channel. Always pair with Unsubscribe.
func (b *Bus) Subscribe() chan Event {
	ch := make(chan Event, 16)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

// Unsubscribe removes the channel and closes it. Safe to call multiple
// times — duplicate calls are no-ops.
func (b *Bus) Unsubscribe(ch chan Event) {
	b.mu.Lock()
	if _, ok := b.subs[ch]; ok {
		delete(b.subs, ch)
		close(ch)
	}
	b.mu.Unlock()
}

// Publish fans out ev to every active subscriber. Drops the event for any
// subscriber whose buffer is full instead of blocking.
func (b *Bus) Publish(ev Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subs {
		select {
		case ch <- ev:
		default:
		}
	}
}
