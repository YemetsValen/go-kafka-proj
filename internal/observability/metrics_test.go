package observability

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
)

func TestMetrics_HTTPMiddlewareRecordsCounters(t *testing.T) {
	m := NewMetrics()

	r := chi.NewRouter()
	r.Use(m.HTTPMiddleware())
	r.Get("/sightings/{id}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	for i := 0; i < 3; i++ {
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, httptest.NewRequest("GET", "/sightings/abc", nil))
	}

	mr := httptest.NewRecorder()
	m.Handler().ServeHTTP(mr, httptest.NewRequest("GET", "/metrics", nil))
	body := mr.Body.String()
	if !strings.Contains(body, `wildlife_http_requests_total{method="GET",route="/sightings/{id}",status="200"} 3`) {
		t.Errorf("expected counter for /sightings/{id}, got body:\n%s", body)
	}
	if !strings.Contains(body, "wildlife_http_request_duration_seconds_bucket") {
		t.Errorf("expected latency histogram in body")
	}
}

func TestReadyHandler(t *testing.T) {
	h := ReadyHandler(map[string]ReadinessCheck{
		"db":    func(_ context.Context) error { return nil },
		"kafka": func(_ context.Context) error { return errors.New("down") },
	})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("code=%d want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "kafka: down") {
		t.Errorf("body should include failed check name and error: %s", rec.Body.String())
	}
}

func TestHealthHandler(t *testing.T) {
	rec := httptest.NewRecorder()
	HealthHandler().ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Errorf("got code=%d body=%q", rec.Code, rec.Body.String())
	}
}
