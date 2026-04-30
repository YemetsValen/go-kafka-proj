package config

import (
	"testing"
	"time"
)

func TestLoad_Defaults(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "localhost:9092")
	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr=%q", c.HTTPAddr)
	}
	if c.GRPCAddr != ":9090" {
		t.Errorf("GRPCAddr=%q", c.GRPCAddr)
	}
	if c.MetricsAddr != ":9100" {
		t.Errorf("MetricsAddr=%q", c.MetricsAddr)
	}
	if c.Kafka.Topic != "wildlife.sightings" {
		t.Errorf("Topic=%q", c.Kafka.Topic)
	}
	if c.Kafka.DialTimeout != 5*time.Second {
		t.Errorf("DialTimeout=%v", c.Kafka.DialTimeout)
	}
	if c.Log.Format != "json" {
		t.Errorf("LogFormat=%q", c.Log.Format)
	}
	if c.AuthEnabled() {
		t.Errorf("auth should default to disabled")
	}
}

func TestLoad_Override(t *testing.T) {
	t.Setenv("HTTP_ADDR", ":8181")
	t.Setenv("KAFKA_BROKERS", "k1:9092,k2:9092")
	t.Setenv("AUTH_API_KEYS", "alpha,beta")
	t.Setenv("LOG_FORMAT", "text")

	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.HTTPAddr != ":8181" {
		t.Errorf("HTTPAddr=%q", c.HTTPAddr)
	}
	if got, want := len(c.Kafka.Brokers), 2; got != want {
		t.Errorf("brokers count=%d want=%d", got, want)
	}
	if !c.AuthEnabled() {
		t.Errorf("auth should be enabled when AUTH_API_KEYS is set")
	}
	if got := c.Auth.APIKeys; len(got) != 2 || got[0] != "alpha" {
		t.Errorf("APIKeys=%v", got)
	}
}

func TestLoad_InvalidLogFormat(t *testing.T) {
	t.Setenv("LOG_FORMAT", "yaml")
	if _, err := Load(); err == nil {
		t.Fatalf("expected error for invalid LOG_FORMAT")
	}
}
