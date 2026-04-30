package observability

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics is a registry-scoped bundle of every counter/histogram the service
// exposes. Constructing it through New(...) keeps the global registry clean
// and makes tests trivially independent of one another.
type Metrics struct {
	Registry *prometheus.Registry

	HTTPRequests *prometheus.CounterVec
	HTTPLatency  *prometheus.HistogramVec
	KafkaPub     *prometheus.CounterVec
	KafkaCons    *prometheus.CounterVec
}

// NewMetrics builds a Metrics with go runtime + process collectors registered.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	httpReq := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "wildlife",
		Subsystem: "http",
		Name:      "requests_total",
		Help:      "Total HTTP requests handled, labelled by method, route and status.",
	}, []string{"method", "route", "status"})

	httpLat := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "wildlife",
		Subsystem: "http",
		Name:      "request_duration_seconds",
		Help:      "HTTP request latency.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method", "route"})

	kPub := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "wildlife",
		Subsystem: "kafka",
		Name:      "publish_total",
		Help:      "Kafka producer publish attempts, labelled by event type and result.",
	}, []string{"event_type", "result"})

	kCons := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "wildlife",
		Subsystem: "kafka",
		Name:      "consume_total",
		Help:      "Kafka consumer messages processed, labelled by event type and result.",
	}, []string{"event_type", "result"})

	reg.MustRegister(httpReq, httpLat, kPub, kCons)
	return &Metrics{
		Registry:     reg,
		HTTPRequests: httpReq,
		HTTPLatency:  httpLat,
		KafkaPub:     kPub,
		KafkaCons:    kCons,
	}
}

// HTTPMiddleware returns chi middleware that records request count and
// latency per route pattern.
func (m *Metrics) HTTPMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := chimw.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)

			route := routePattern(r)
			m.HTTPRequests.WithLabelValues(r.Method, route, strconv.Itoa(ww.Status())).Inc()
			m.HTTPLatency.WithLabelValues(r.Method, route).Observe(time.Since(start).Seconds())
		})
	}
}

func routePattern(r *http.Request) string {
	if rc := chi.RouteContext(r.Context()); rc != nil && rc.RoutePattern() != "" {
		return rc.RoutePattern()
	}
	return r.URL.Path
}

// Handler returns the /metrics handler for the bundled registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}

// ---- /healthz and /readyz ----

// ReadinessCheck reports whether a downstream dependency is ready.
type ReadinessCheck func(ctx context.Context) error

// HealthHandler always returns 200 — process is alive.
func HealthHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}
}

// ReadyHandler returns 503 if any check fails. Each check has 2s to respond.
func ReadyHandler(checks map[string]ReadinessCheck) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		failed := 0
		body := []byte("ready\n")
		for name, check := range checks {
			if err := check(ctx); err != nil {
				failed++
				body = append(body, []byte(name+": "+err.Error()+"\n")...)
			} else {
				body = append(body, []byte(name+": ok\n")...)
			}
		}
		if failed > 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_, _ = w.Write(body)
	}
}
