package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/YemetsValen/go-kafka-proj/gen/go/sightings/v1"
	"github.com/YemetsValen/go-kafka-proj/internal/auth"
	"github.com/YemetsValen/go-kafka-proj/internal/config"
	"github.com/YemetsValen/go-kafka-proj/internal/db"
	"github.com/YemetsValen/go-kafka-proj/internal/grpcsvc"
	"github.com/YemetsValen/go-kafka-proj/internal/handlers"
	"github.com/YemetsValen/go-kafka-proj/internal/kafka"
	"github.com/YemetsValen/go-kafka-proj/internal/logging"
	"github.com/YemetsValen/go-kafka-proj/internal/observability"
	"github.com/YemetsValen/go-kafka-proj/internal/store"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	segkafka "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("load config", "err", err)
		os.Exit(1)
	}

	logger := logging.New(cfg.Log, cfg.ServiceName)

	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()

	shutdownTrace, err := observability.InitTracing(rootCtx, cfg)
	if err != nil {
		logger.Error("init tracing", "err", err)
		os.Exit(1)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTrace(ctx); err != nil {
			logger.Error("shutdown tracing", "err", err)
		}
	}()

	metrics := observability.NewMetrics()

	producer := kafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.Topic, metrics.KafkaPub)
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Error("kafka close", "err", err)
		}
	}()

	st, pool, closeStore := initStore(rootCtx, logger, cfg)
	defer closeStore()

	verifier := auth.New(auth.Config{
		JWTSecret: cfg.Auth.JWTSecret,
		APIKeys:   cfg.Auth.APIKeys,
	})
	if !verifier.Enabled() {
		logger.Warn("auth is DISABLED — set AUTH_JWT_SECRET or AUTH_API_KEYS to enable")
	}

	httpHandler := handlers.New(st, producer)
	grpcServer := newGRPCServer(st, producer, verifier)

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(otelhttp.NewMiddleware(cfg.ServiceName))
	r.Use(metrics.HTTPMiddleware())
	r.Use(logging.HTTPMiddleware(logger))
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(15 * time.Second))

	r.Get("/healthz", observability.HealthHandler())
	r.Get("/readyz", observability.ReadyHandler(readinessChecks(cfg, pool)))
	r.Mount("/sightings", httpHandler.Routes(verifier.HTTPMiddleware()))

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
	}

	metricsSrv := &http.Server{
		Addr:              cfg.MetricsAddr,
		Handler:           metrics.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("http listening", "addr", cfg.HTTPAddr,
			"kafka_brokers", cfg.Kafka.Brokers, "topic", cfg.Kafka.Topic)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server", "err", err)
			rootCancel()
		}
	}()

	go func() {
		logger.Info("metrics listening", "addr", cfg.MetricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server", "err", err)
		}
	}()

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		logger.Error("grpc listen", "addr", cfg.GRPCAddr, "err", err)
		os.Exit(1)
	}
	go func() {
		logger.Info("grpc listening", "addr", cfg.GRPCAddr)
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("grpc server", "err", err)
			rootCancel()
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-stop:
		logger.Info("shutting down", "signal", sig.String())
	case <-rootCtx.Done():
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("http shutdown", "err", err)
	}
	if err := metricsSrv.Shutdown(ctx); err != nil {
		logger.Error("metrics shutdown", "err", err)
	}
	grpcServer.GracefulStop()
}

// initStore picks a Store implementation based on cfg.DB.URL. Empty URL
// falls back to in-memory storage so the service is runnable without Postgres.
// Returns the store, the pgxpool (or nil) for readiness checks, and a closer.
func initStore(ctx context.Context, logger *slog.Logger, cfg config.Config) (store.Store, *pgxpool.Pool, func()) {
	if cfg.DB.URL == "" {
		logger.Warn("DATABASE_URL not set — using in-memory store")
		return store.NewMemory(), nil, func() {}
	}

	migCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := db.Migrate(migCtx, cfg.DB.URL); err != nil {
		logger.Error("migrate", "err", err)
		os.Exit(1)
	}
	pool, err := db.Connect(ctx, cfg.DB.URL)
	if err != nil {
		logger.Error("connect postgres", "err", err)
		os.Exit(1)
	}
	logger.Info("postgres connected; migrations up-to-date")
	return store.NewPostgres(pool), pool, pool.Close
}

func newGRPCServer(st store.Store, p *kafka.Producer, v *auth.Verifier) *grpc.Server {
	g := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.UnaryInterceptor(v.UnaryInterceptor()),
		grpc.StreamInterceptor(v.StreamInterceptor()),
	)
	pb.RegisterSightingServiceServer(g, grpcsvc.New(st, p))
	reflection.Register(g)
	return g
}

func readinessChecks(cfg config.Config, pool *pgxpool.Pool) map[string]observability.ReadinessCheck {
	checks := map[string]observability.ReadinessCheck{
		"kafka": func(ctx context.Context) error {
			d := segkafka.Dialer{Timeout: cfg.Kafka.DialTimeout}
			conn, err := d.DialContext(ctx, "tcp", cfg.Kafka.Brokers[0])
			if err != nil {
				return err
			}
			return conn.Close()
		},
	}
	if pool != nil {
		checks["postgres"] = func(ctx context.Context) error {
			pingCtx, cancel := context.WithTimeout(ctx, cfg.DB.PingTimeout)
			defer cancel()
			return pool.Ping(pingCtx)
		}
	}
	return checks
}
