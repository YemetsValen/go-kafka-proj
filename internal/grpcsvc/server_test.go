package grpcsvc

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	pb "github.com/YemetsValen/go-kafka-proj/gen/go/sightings/v1"
	"github.com/YemetsValen/go-kafka-proj/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// mockPublisher records every call so tests can assert what was published.
type mockPublisher struct {
	mu    sync.Mutex
	calls []publishCall
	err   error
}
type publishCall struct {
	Key       string
	EventType string
	Payload   interface{}
}

func (m *mockPublisher) Publish(_ context.Context, key, eventType string, payload interface{}) error {
	m.mu.Lock()
	m.calls = append(m.calls, publishCall{Key: key, EventType: eventType, Payload: payload})
	m.mu.Unlock()
	return m.err
}
func (m *mockPublisher) eventTypes() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.calls))
	for i, c := range m.calls {
		out[i] = c.EventType
	}
	return out
}

// dial spins up a real gRPC server over an in-process bufconn and returns a
// connected client. teardown cleans everything up.
func dial(t *testing.T) (pb.SightingServiceClient, *mockPublisher, func()) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	pub := &mockPublisher{}
	srv := grpc.NewServer()
	pb.RegisterSightingServiceServer(srv, New(store.NewMemory(), pub, nil))
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return pb.NewSightingServiceClient(conn), pub, func() {
		_ = conn.Close()
		srv.GracefulStop()
		_ = lis.Close()
	}
}

func TestGRPC_CreateGetUpdateDelete(t *testing.T) {
	cli, pub, cleanup := dial(t)
	defer cleanup()
	ctx := context.Background()

	// Create
	cresp, err := cli.CreateSighting(ctx, &pb.CreateSightingRequest{
		Species:    "Red Fox",
		Location:   "Carpathians",
		Position:   &pb.GeoPoint{Latitude: 48.5, Longitude: 24.5},
		ObservedBy: "ranger",
		ObservedAt: timestamppb.New(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)),
	})
	if err != nil {
		t.Fatalf("CreateSighting: %v", err)
	}
	id := cresp.GetSighting().GetId()
	if id == "" {
		t.Fatalf("expected id")
	}

	// Get
	gresp, err := cli.GetSighting(ctx, &pb.GetSightingRequest{Id: id})
	if err != nil {
		t.Fatalf("GetSighting: %v", err)
	}
	if gresp.GetSighting().GetSpecies() != "Red Fox" {
		t.Errorf("species: got %q", gresp.GetSighting().GetSpecies())
	}

	// Update with field mask
	uresp, err := cli.UpdateSighting(ctx, &pb.UpdateSightingRequest{
		Sighting:   &pb.Sighting{Id: id, Species: "Arctic Fox"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"species"}},
	})
	if err != nil {
		t.Fatalf("UpdateSighting: %v", err)
	}
	if uresp.GetSighting().GetSpecies() != "Arctic Fox" {
		t.Errorf("after update, species=%q", uresp.GetSighting().GetSpecies())
	}

	// Delete
	if _, err := cli.DeleteSighting(ctx, &pb.DeleteSightingRequest{Id: id}); err != nil {
		t.Fatalf("DeleteSighting: %v", err)
	}
	if _, err := cli.GetSighting(ctx, &pb.GetSightingRequest{Id: id}); status.Code(err) != codes.NotFound {
		t.Errorf("expected NotFound after delete, got %v", err)
	}

	// We expect kafka events for created, updated, deleted.
	want := []string{"sighting.created", "sighting.updated", "sighting.deleted"}
	got := pub.eventTypes()
	if len(got) != len(want) {
		t.Fatalf("kafka events: got %v, want %v", got, want)
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("event[%d]: got %q, want %q", i, got[i], w)
		}
	}
}

func TestGRPC_CreateValidation(t *testing.T) {
	cli, _, cleanup := dial(t)
	defer cleanup()

	_, err := cli.CreateSighting(context.Background(), &pb.CreateSightingRequest{
		Species: "", ObservedBy: "",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
}

func TestGRPC_GetNotFound(t *testing.T) {
	cli, _, cleanup := dial(t)
	defer cleanup()

	_, err := cli.GetSighting(context.Background(), &pb.GetSightingRequest{Id: "missing"})
	if status.Code(err) != codes.NotFound {
		t.Errorf("expected NotFound, got %v", err)
	}
}

func TestGRPC_VerifyAndAddNote(t *testing.T) {
	cli, pub, cleanup := dial(t)
	defer cleanup()
	ctx := context.Background()

	cresp, err := cli.CreateSighting(ctx, &pb.CreateSightingRequest{
		Species: "Lynx", ObservedBy: "bio",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	id := cresp.GetSighting().GetId()

	if _, err := cli.VerifySighting(ctx, &pb.VerifySightingRequest{Id: id}); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if _, err := cli.AddNote(ctx, &pb.AddNoteRequest{
		SightingId: id, Note: &pb.Note{Author: "ranger", Text: "tagged"},
	}); err != nil {
		t.Fatalf("add note: %v", err)
	}

	g, _ := cli.GetSighting(ctx, &pb.GetSightingRequest{Id: id})
	if !g.GetSighting().GetVerified() {
		t.Errorf("expected verified=true")
	}
	if len(g.GetSighting().GetNotes()) != 1 {
		t.Errorf("expected 1 note, got %d", len(g.GetSighting().GetNotes()))
	}

	want := []string{"sighting.created", "sighting.verified", "sighting.note_added"}
	got := pub.eventTypes()
	if len(got) != len(want) {
		t.Fatalf("kafka events: got %v, want %v", got, want)
	}
}

func TestGRPC_BulkCreate(t *testing.T) {
	cli, _, cleanup := dial(t)
	defer cleanup()

	stream, err := cli.BulkCreate(context.Background())
	if err != nil {
		t.Fatalf("BulkCreate: %v", err)
	}
	for _, sp := range []string{"Red Fox", "Wolf", ""} { // last invalid (empty species)
		if err := stream.Send(&pb.BulkCreateRequest{
			Sighting: &pb.CreateSightingRequest{Species: sp, ObservedBy: "obs"},
		}); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}
	if resp.GetReceived() != 3 {
		t.Errorf("received: %d", resp.GetReceived())
	}
	if resp.GetCreated() != 2 {
		t.Errorf("created: %d", resp.GetCreated())
	}
	if resp.GetFailed() != 1 || len(resp.GetErrors()) != 1 {
		t.Errorf("failed: %d errors=%d", resp.GetFailed(), len(resp.GetErrors()))
	}
}

func TestGRPC_Watch(t *testing.T) {
	cli, _, cleanup := dial(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stream, err := cli.Watch(ctx, &pb.WatchRequest{})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	// Give the server a moment to register the subscriber, then create.
	go func() {
		time.Sleep(50 * time.Millisecond)
		_, _ = cli.CreateSighting(context.Background(), &pb.CreateSightingRequest{
			Species: "Owl", ObservedBy: "eye",
		})
	}()

	ev, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if ev.GetType() != pb.EventType_EVENT_TYPE_CREATED {
		t.Errorf("expected CREATED, got %v", ev.GetType())
	}
	if ev.GetSighting().GetSpecies() != "Owl" {
		t.Errorf("event sighting: %+v", ev.GetSighting())
	}
	cancel()
	// Drain any tail; ignore any context-cancelled error.
	for {
		_, err := stream.Recv()
		if err != nil {
			if !errors.Is(err, io.EOF) && status.Code(err) != codes.Canceled {
				// not fatal: just log
				t.Logf("watch end: %v", err)
			}
			return
		}
	}
}
