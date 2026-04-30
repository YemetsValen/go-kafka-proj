package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/YemetsValen/go-kafka-proj/internal/models"
	"github.com/YemetsValen/go-kafka-proj/internal/store"
)

// mockPublisher records every Publish call and optionally returns a preset error.
type mockPublisher struct {
	mu     sync.Mutex
	calls  []mockCall
	errOn  map[string]error // eventType -> error to return
}

type mockCall struct {
	Key       string
	EventType string
	Payload   interface{}
}

func newMockPublisher() *mockPublisher {
	return &mockPublisher{errOn: make(map[string]error)}
}

func (m *mockPublisher) Publish(_ context.Context, key string, eventType string, payload interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, mockCall{Key: key, EventType: eventType, Payload: payload})
	return m.errOn[eventType]
}

func (m *mockPublisher) lastCall() (mockCall, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.calls) == 0 {
		return mockCall{}, false
	}
	return m.calls[len(m.calls)-1], true
}

func (m *mockPublisher) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func setup(t *testing.T) (http.Handler, *store.Memory, *mockPublisher) {
	t.Helper()
	s := store.NewMemory()
	pub := newMockPublisher()
	h := New(s, pub)
	return h.Routes(), s, pub
}

func doJSON(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func decodeSighting(t *testing.T, body *bytes.Buffer) models.Sighting {
	t.Helper()
	var s models.Sighting
	if err := json.NewDecoder(body).Decode(&s); err != nil {
		t.Fatalf("decode sighting: %v", err)
	}
	return s
}

// ---------- POST /sightings ----------

func TestCreateSighting_Success(t *testing.T) {
	h, _, pub := setup(t)

	body := `{"species":"Red Fox","location":"Forest","latitude":48.6,"longitude":24.7,"observed_by":"rama","observed_at":"2026-04-26T05:30:00Z"}`
	rr := doJSON(t, h, http.MethodPost, "/", body)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rr.Code, rr.Body.String())
	}
	got := decodeSighting(t, rr.Body)
	if got.ID == "" {
		t.Errorf("expected generated ID")
	}
	if got.Species != "Red Fox" {
		t.Errorf("species = %q", got.Species)
	}

	call, ok := pub.lastCall()
	if !ok {
		t.Fatal("expected Kafka publish to be called")
	}
	if call.EventType != "sighting.created" {
		t.Errorf("event type = %q", call.EventType)
	}
	if call.Key != got.ID {
		t.Errorf("publish key = %q, want %q", call.Key, got.ID)
	}
}

func TestCreateSighting_MissingFields(t *testing.T) {
	h, _, pub := setup(t)

	rr := doJSON(t, h, http.MethodPost, "/", `{"species":"","observed_by":""}`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	if pub.callCount() != 0 {
		t.Errorf("expected no Kafka publish on validation failure, got %d", pub.callCount())
	}
}

func TestCreateSighting_InvalidJSON(t *testing.T) {
	h, _, _ := setup(t)
	rr := doJSON(t, h, http.MethodPost, "/", `{not json`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

func TestCreateSighting_KafkaFailureReturns202(t *testing.T) {
	h, _, pub := setup(t)
	pub.errOn["sighting.created"] = errBoom

	body := `{"species":"Red Fox","observed_by":"rama"}`
	rr := doJSON(t, h, http.MethodPost, "/", body)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202 when kafka fails, got %d: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "warning") {
		t.Errorf("expected warning in body, got %s", rr.Body.String())
	}
}

// ---------- GET /sightings ----------

func TestListSightings_Empty(t *testing.T) {
	h, _, _ := setup(t)
	rr := doJSON(t, h, http.MethodGet, "/", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	var list []models.Sighting
	if err := json.NewDecoder(rr.Body).Decode(&list); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(list) != 0 {
		t.Errorf("expected empty list, got %d", len(list))
	}
}

func TestListSightings_AfterCreate(t *testing.T) {
	h, s, _ := setup(t)
	s.Create(context.Background(), models.Sighting{ID: "a", Species: "Red Fox"})
	s.Create(context.Background(), models.Sighting{ID: "b", Species: "Lynx"})

	rr := doJSON(t, h, http.MethodGet, "/", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	var list []models.Sighting
	if err := json.NewDecoder(rr.Body).Decode(&list); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(list) != 2 {
		t.Errorf("expected 2 sightings, got %d", len(list))
	}
}

// ---------- GET /sightings/{id} ----------

func TestGetSighting_Found(t *testing.T) {
	h, s, _ := setup(t)
	s.Create(context.Background(), models.Sighting{ID: "abc", Species: "Red Fox"})

	rr := doJSON(t, h, http.MethodGet, "/abc", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	got := decodeSighting(t, rr.Body)
	if got.ID != "abc" {
		t.Errorf("got id %q", got.ID)
	}
}

func TestGetSighting_NotFound(t *testing.T) {
	h, _, _ := setup(t)
	rr := doJSON(t, h, http.MethodGet, "/missing", "")
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
}

// ---------- POST /sightings/{id}/notes ----------

func TestAddNote_Success(t *testing.T) {
	h, s, pub := setup(t)
	s.Create(context.Background(), models.Sighting{ID: "abc", Species: "Red Fox"})

	rr := doJSON(t, h, http.MethodPost, "/abc/notes", `{"author":"rama","text":"moving north"}`)
	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rr.Code, rr.Body.String())
	}
	got := decodeSighting(t, rr.Body)
	if len(got.Notes) != 1 || got.Notes[0].Text != "moving north" {
		t.Errorf("unexpected notes: %+v", got.Notes)
	}
	call, ok := pub.lastCall()
	if !ok || call.EventType != "sighting.note_added" {
		t.Errorf("expected sighting.note_added publish, got %+v (ok=%v)", call, ok)
	}
}

func TestAddNote_MissingFields(t *testing.T) {
	h, s, pub := setup(t)
	s.Create(context.Background(), models.Sighting{ID: "abc", Species: "Red Fox"})

	rr := doJSON(t, h, http.MethodPost, "/abc/notes", `{"author":"","text":""}`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	if pub.callCount() != 0 {
		t.Errorf("expected no publish on validation failure")
	}
}

func TestAddNote_NotFound(t *testing.T) {
	h, _, _ := setup(t)
	rr := doJSON(t, h, http.MethodPost, "/missing/notes", `{"author":"rama","text":"hi"}`)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
}

// ---------- PUT /sightings/{id} ----------

func TestUpdateSighting_PartialUpdate(t *testing.T) {
	h, s, _ := setup(t)
	s.Create(context.Background(), models.Sighting{ID: "abc", Species: "Red Fox", Location: "Forest", Latitude: 48.0, Longitude: 24.0})

	rr := doJSON(t, h, http.MethodPut, "/abc", `{"latitude": 49.1}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	got := decodeSighting(t, rr.Body)
	if got.Latitude != 49.1 {
		t.Errorf("latitude not updated: %v", got.Latitude)
	}
	if got.Species != "Red Fox" || got.Location != "Forest" {
		t.Errorf("unrelated fields changed: %+v", got)
	}
}

func TestUpdateSighting_NotFound(t *testing.T) {
	h, _, _ := setup(t)
	rr := doJSON(t, h, http.MethodPut, "/missing", `{"species":"Wolf"}`)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
}

func TestUpdateSighting_InvalidJSON(t *testing.T) {
	h, s, _ := setup(t)
	s.Create(context.Background(), models.Sighting{ID: "abc", Species: "Red Fox"})

	rr := doJSON(t, h, http.MethodPut, "/abc", `not json`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

// ---------- PUT /sightings/{id}/verify ----------

func TestVerifySighting_Success(t *testing.T) {
	h, s, pub := setup(t)
	s.Create(context.Background(), models.Sighting{ID: "abc", Species: "Red Fox"})

	rr := doJSON(t, h, http.MethodPut, "/abc/verify", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	got := decodeSighting(t, rr.Body)
	if !got.Verified {
		t.Errorf("expected Verified=true, got %+v", got)
	}
	call, ok := pub.lastCall()
	if !ok || call.EventType != "sighting.verified" {
		t.Errorf("expected sighting.verified publish, got %+v (ok=%v)", call, ok)
	}
}

func TestVerifySighting_NotFound(t *testing.T) {
	h, _, pub := setup(t)
	rr := doJSON(t, h, http.MethodPut, "/missing/verify", "")
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
	if pub.callCount() != 0 {
		t.Errorf("expected no publish on 404")
	}
}

// sentinel error for Kafka-failure test
var errBoom = &boomError{msg: "kafka down"}

type boomError struct{ msg string }

func (e *boomError) Error() string { return e.msg }
