package grpcsvc

import (
	"sync"

	pb "github.com/YemetsValen/go-kafka-proj/gen/go/sightings/v1"
)

// Broadcast is a tiny in-process pub/sub for fan-out of sighting events to
// concurrent gRPC stream subscribers.
//
// It is not durable and does not replay history; use the store + Watch's
// from_beginning flag for that.
type Broadcast struct {
	mu       sync.Mutex
	events   map[chan *pb.WatchResponse]struct{}
	chatSubs map[chan *pb.ChatResponse]struct{}
}

func NewBroadcast() *Broadcast {
	return &Broadcast{
		events:   make(map[chan *pb.WatchResponse]struct{}),
		chatSubs: make(map[chan *pb.ChatResponse]struct{}),
	}
}

func (b *Broadcast) Subscribe() chan *pb.WatchResponse {
	ch := make(chan *pb.WatchResponse, 16)
	b.mu.Lock()
	b.events[ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

func (b *Broadcast) Unsubscribe(ch chan *pb.WatchResponse) {
	b.mu.Lock()
	delete(b.events, ch)
	b.mu.Unlock()
	close(ch)
}

func (b *Broadcast) Publish(ev *pb.WatchResponse) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.events {
		// Drop event for slow subscribers rather than blocking other writers.
		select {
		case ch <- ev:
		default:
		}
	}
}

func (b *Broadcast) SubscribeChat() chan *pb.ChatResponse {
	ch := make(chan *pb.ChatResponse, 16)
	b.mu.Lock()
	b.chatSubs[ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

func (b *Broadcast) UnsubscribeChat(ch chan *pb.ChatResponse) {
	b.mu.Lock()
	delete(b.chatSubs, ch)
	b.mu.Unlock()
	close(ch)
}

func (b *Broadcast) PublishChat(msg *pb.ChatResponse) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.chatSubs {
		select {
		case ch <- msg:
		default:
		}
	}
}
