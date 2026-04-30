package grpcsvc

import (
	"sync"

	pb "github.com/YemetsValen/go-kafka-proj/gen/go/sightings/v1"
)

// chatBroadcast is a tiny in-process pub/sub for the bidi Chat RPC. It is
// not durable and does not replay history. Sighting domain events live on
// the shared events.Bus instead.
type chatBroadcast struct {
	mu   sync.Mutex
	subs map[chan *pb.ChatResponse]struct{}
}

func newChatBroadcast() *chatBroadcast {
	return &chatBroadcast{subs: make(map[chan *pb.ChatResponse]struct{})}
}

func (b *chatBroadcast) Subscribe() chan *pb.ChatResponse {
	ch := make(chan *pb.ChatResponse, 16)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

func (b *chatBroadcast) Unsubscribe(ch chan *pb.ChatResponse) {
	b.mu.Lock()
	if _, ok := b.subs[ch]; ok {
		delete(b.subs, ch)
		close(ch)
	}
	b.mu.Unlock()
}

func (b *chatBroadcast) Publish(msg *pb.ChatResponse) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subs {
		select {
		case ch <- msg:
		default:
		}
	}
}
