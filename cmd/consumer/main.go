// Command consumer reads sighting events off Kafka and persists them into
// the Postgres `events` table for analytics.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/YemetsValen/go-kafka-proj/internal/config"
	"github.com/YemetsValen/go-kafka-proj/internal/consumer"
	"github.com/YemetsValen/go-kafka-proj/internal/db"
	"github.com/YemetsValen/go-kafka-proj/internal/logging"
	"github.com/YemetsValen/go-kafka-proj/internal/observability"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("load config", "err", err)
		os.Exit(1)
	}
	cfg.ServiceName = "wildlife-sightings-consumer"
	logger := logging.New(cfg.Log, cfg.ServiceName)

	if cfg.DB.URL == "" {
		logger.Error("DATABASE_URL is required for the consumer")
		os.Exit(1)
	}

	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shutdownTrace, err := observability.InitTracing(rootCtx, cfg)
	if err != nil {
		logger.Error("init tracing", "err", err)
		os.Exit(1)
	}
	defer func() {
		ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		if err := shutdownTrace(ctx); err != nil {
			logger.Error("shutdown tracing", "err", err)
		}
	}()

	metrics := observability.NewMetrics()

	migCtx, migCancel := context.WithTimeout(rootCtx, 30*time.Second)
	if err := db.Migrate(migCtx, cfg.DB.URL); err != nil {
		migCancel()
		logger.Error("migrate", "err", err)
		os.Exit(1)
	}
	migCancel()

	pool, err := db.Connect(rootCtx, cfg.DB.URL)
	if err != nil {
		logger.Error("connect postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()
	logger.Info("postgres connected")

	reader := consumer.NewKafkaReader(cfg.Kafka.Brokers, cfg.Kafka.Topic, cfg.Kafka.ConsumerGroup)
	defer func() {
		if err := reader.Close(); err != nil {
			logger.Error("kafka reader close", "err", err)
		}
	}()

	sink := consumer.NewPostgresEventSink(pool)
	c := consumer.New(reader, sink, metrics.KafkaCons)

	metricsSrv := &http.Server{
		Addr:              cfg.MetricsAddr,
		Handler:           metrics.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		logger.Info("metrics listening", "addr", cfg.MetricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server", "err", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-stop
		logger.Info("shutting down")
		cancel()
	}()

	logger.Info("consumer running",
		"brokers", cfg.Kafka.Brokers, "topic", cfg.Kafka.Topic, "group", cfg.Kafka.ConsumerGroup)
	if err := c.Run(rootCtx); err != nil {
		logger.Error("consumer error", "err", err)
	}

	shutdownCtx, sc := context.WithTimeout(context.Background(), 5*time.Second)
	defer sc()
	if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("metrics shutdown", "err", err)
	}
}
