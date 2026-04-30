// Package config centralises all environment-driven configuration for the
// service and the consumer. It uses kelseyhightower/envconfig so each field
// is explicit, typed, and self-documenting.
package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
)

// Config holds every tunable for the wildlife sightings service.
type Config struct {
	HTTPAddr    string `envconfig:"HTTP_ADDR" default:":8080"`
	GRPCAddr    string `envconfig:"GRPC_ADDR" default:":9090"`
	MetricsAddr string `envconfig:"METRICS_ADDR" default:":9100"`
	ServiceName string `envconfig:"SERVICE_NAME" default:"wildlife-sightings"`
	Env         string `envconfig:"ENV" default:"dev"`

	Kafka KafkaConfig
	DB    DBConfig
	Auth  AuthConfig
	Log   LogConfig
	OTEL  OTELConfig
}

type KafkaConfig struct {
	Brokers       []string      `envconfig:"KAFKA_BROKERS" default:"localhost:9092"`
	Topic         string        `envconfig:"KAFKA_TOPIC" default:"wildlife.sightings"`
	ConsumerGroup string        `envconfig:"KAFKA_CONSUMER_GROUP" default:"wildlife-sightings-consumer"`
	DialTimeout   time.Duration `envconfig:"KAFKA_DIAL_TIMEOUT" default:"5s"`
}

type DBConfig struct {
	URL         string        `envconfig:"DATABASE_URL"`
	MaxConns    int32         `envconfig:"DATABASE_MAX_CONNS" default:"10"`
	PingTimeout time.Duration `envconfig:"DATABASE_PING_TIMEOUT" default:"3s"`
}

type AuthConfig struct {
	JWTSecret string   `envconfig:"AUTH_JWT_SECRET"`
	APIKeys   []string `envconfig:"AUTH_API_KEYS"`
}

type LogConfig struct {
	Level  string `envconfig:"LOG_LEVEL" default:"info"`
	Format string `envconfig:"LOG_FORMAT" default:"json"`
}

type OTELConfig struct {
	// OTLPEndpoint follows the OpenTelemetry env conventions. When empty,
	// tracing falls back to a no-op tracer provider.
	OTLPEndpoint string  `envconfig:"OTEL_EXPORTER_OTLP_ENDPOINT"`
	OTLPInsecure bool    `envconfig:"OTEL_EXPORTER_OTLP_INSECURE" default:"true"`
	SampleRatio  float64 `envconfig:"OTEL_TRACES_SAMPLER_ARG" default:"1.0"`
}

// Load reads configuration from environment variables, applying defaults and
// then overlaying any .env file in the working directory (handy for local
// development; ignored when missing or in non-dev environments).
func Load() (Config, error) {
	if _, err := os.Stat(".env"); err == nil {
		if err := godotenv.Overload(".env"); err != nil {
			return Config{}, fmt.Errorf("read .env: %w", err)
		}
	}

	var c Config
	if err := envconfig.Process("", &c); err != nil {
		return Config{}, fmt.Errorf("parse env: %w", err)
	}
	if err := c.validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

func (c Config) validate() error {
	if c.HTTPAddr == "" {
		return errors.New("HTTP_ADDR must not be empty")
	}
	if c.GRPCAddr == "" {
		return errors.New("GRPC_ADDR must not be empty")
	}
	if len(c.Kafka.Brokers) == 0 {
		return errors.New("KAFKA_BROKERS must not be empty")
	}
	if c.Kafka.Topic == "" {
		return errors.New("KAFKA_TOPIC must not be empty")
	}
	switch c.Log.Format {
	case "json", "text":
	default:
		return fmt.Errorf("LOG_FORMAT must be json or text, got %q", c.Log.Format)
	}
	return nil
}

// AuthEnabled reports whether at least one auth method is configured.
func (c Config) AuthEnabled() bool {
	return c.Auth.JWTSecret != "" || len(c.Auth.APIKeys) > 0
}
