package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	pb "github.com/YemetsValen/go-kafka-proj/gen/go/sightings/v1"
	"github.com/YemetsValen/go-kafka-proj/internal/auth"
	"github.com/YemetsValen/go-kafka-proj/internal/db"
	"github.com/YemetsValen/go-kafka-proj/internal/grpcsvc"
	"github.com/YemetsValen/go-kafka-proj/internal/handlers"
	"github.com/YemetsValen/go-kafka-proj/internal/kafka"
	"github.com/YemetsValen/go-kafka-proj/internal/store"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	addr := envOr("HTTP_ADDR", ":8080")
	grpcAddr := envOr("GRPC_ADDR", ":9090")
	brokers := strings.Split(envOr("KAFKA_BROKERS", "localhost:9092"), ",")
	topic := envOr("KAFKA_TOPIC", "wildlife.sightings")
	dsn := os.Getenv("DATABASE_URL")

	producer := kafka.NewProducer(brokers, topic)
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("kafka close error: %v", err)
		}
	}()

	st, closeStore := initStore(dsn)
	defer closeStore()

	verifier := auth.New(auth.Config{
		JWTSecret: os.Getenv("AUTH_JWT_SECRET"),
		APIKeys:   splitNonEmpty(os.Getenv("AUTH_API_KEYS")),
	})
	if !verifier.Enabled() {
		log.Printf("WARNING: auth is DISABLED — set AUTH_JWT_SECRET or AUTH_API_KEYS to enable")
	}

	httpHandler := handlers.New(st, producer)
	grpcServer := newGRPCServer(st, producer, verifier)

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(15 * time.Second))

	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	r.Mount("/sightings", httpHandler.Routes(verifier.HTTPMiddleware()))

	srv := &http.Server{
		Addr:              addr,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Start HTTP server.
	go func() {
		log.Printf("HTTP listening on %s (kafka brokers=%v topic=%s)", addr, brokers, topic)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server error: %v", err)
		}
	}()

	// Start gRPC server.
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("grpc listen %s: %v", grpcAddr, err)
	}
	go func() {
		log.Printf("gRPC listening on %s", grpcAddr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("grpc server error: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("http shutdown error: %v", err)
	}
	grpcServer.GracefulStop()
}

// initStore picks a Store implementation based on DATABASE_URL. Empty DSN
// falls back to in-memory storage so the service is runnable without Postgres.
func initStore(dsn string) (store.Store, func()) {
	if dsn == "" {
		log.Println("DATABASE_URL not set — using in-memory store")
		return store.NewMemory(), func() {}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.Migrate(ctx, dsn); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	pool, err := db.Connect(context.Background(), dsn)
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	log.Printf("postgres connected; migrations up-to-date")
	return store.NewPostgres(pool), pool.Close
}

func newGRPCServer(st store.Store, p *kafka.Producer, v *auth.Verifier) *grpc.Server {
	g := grpc.NewServer(
		grpc.UnaryInterceptor(v.UnaryInterceptor()),
		grpc.StreamInterceptor(v.StreamInterceptor()),
	)
	pb.RegisterSightingServiceServer(g, grpcsvc.New(st, p))
	reflection.Register(g)
	return g
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func splitNonEmpty(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
