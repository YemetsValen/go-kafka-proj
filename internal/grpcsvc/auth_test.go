package grpcsvc

import (
	"context"
	"net"
	"testing"

	pb "github.com/YemetsValen/go-kafka-proj/gen/go/sightings/v1"
	"github.com/YemetsValen/go-kafka-proj/internal/auth"
	"github.com/YemetsValen/go-kafka-proj/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func dialWithAuth(t *testing.T, v *auth.Verifier) (pb.SightingServiceClient, func()) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(v.UnaryInterceptor()),
		grpc.StreamInterceptor(v.StreamInterceptor()),
	)
	pb.RegisterSightingServiceServer(srv, New(store.NewMemory(), &mockPublisher{}, nil))
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return pb.NewSightingServiceClient(conn), func() {
		_ = conn.Close()
		srv.GracefulStop()
		_ = lis.Close()
	}
}

func TestGRPC_AuthRequiredOnMutate(t *testing.T) {
	v := auth.New(auth.Config{APIKeys: []string{"svc"}})
	cli, cleanup := dialWithAuth(t, v)
	defer cleanup()

	// No auth metadata: rejected.
	_, err := cli.CreateSighting(context.Background(), &pb.CreateSightingRequest{
		Species: "Wolf", ObservedBy: "ranger",
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("expected Unauthenticated, got %v", err)
	}

	// Wrong key: rejected.
	ctxBad := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer wrong"))
	if _, err := cli.CreateSighting(ctxBad, &pb.CreateSightingRequest{Species: "Wolf", ObservedBy: "r"}); status.Code(err) != codes.Unauthenticated {
		t.Errorf("wrong key: %v", err)
	}

	// Correct key: passes.
	ctxOK := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer svc"))
	resp, err := cli.CreateSighting(ctxOK, &pb.CreateSightingRequest{Species: "Wolf", ObservedBy: "r"})
	if err != nil {
		t.Fatalf("authorized create: %v", err)
	}
	if resp.GetSighting().GetId() == "" {
		t.Errorf("expected sighting id")
	}
}

func TestGRPC_ReadsArePublic(t *testing.T) {
	v := auth.New(auth.Config{APIKeys: []string{"svc"}})
	cli, cleanup := dialWithAuth(t, v)
	defer cleanup()

	// List without any token must succeed.
	if _, err := cli.ListSightings(context.Background(), &pb.ListSightingsRequest{}); err != nil {
		t.Errorf("ListSightings should be public: %v", err)
	}
}
