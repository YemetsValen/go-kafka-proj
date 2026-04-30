package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/YemetsValen/go-kafka-proj/internal/auth"
	"github.com/YemetsValen/go-kafka-proj/internal/store"
)

func TestAuthMiddleware_Routing(t *testing.T) {
	v := auth.New(auth.Config{APIKeys: []string{"svc"}})
	h := New(store.NewMemory(), newMockPublisher(), nil).Routes(v.HTTPMiddleware())

	// GETs are public — no token, expect 200.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("GET /: code=%d, want 200 (public)", rec.Code)
	}

	// POST without token is rejected.
	rec = httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/", strings.NewReader(`{"species":"X","observed_by":"y"}`))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("POST without token: code=%d, want 401", rec.Code)
	}

	// POST with token passes auth (then handler does its work).
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("POST", "/", strings.NewReader(`{"species":"Wolf","observed_by":"r"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer svc")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted && rec.Code != http.StatusCreated {
		t.Errorf("POST with token: code=%d body=%s", rec.Code, rec.Body.String())
	}
}
