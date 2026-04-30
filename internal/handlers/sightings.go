package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/YemetsValen/go-kafka-proj/internal/events"
	"github.com/YemetsValen/go-kafka-proj/internal/models"
	"github.com/YemetsValen/go-kafka-proj/internal/store"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

// Publisher is the minimal surface we need from a Kafka producer.
// Decoupling via an interface keeps handlers trivially testable.
type Publisher interface {
	Publish(ctx context.Context, key string, eventType string, payload interface{}) error
}

type Handler struct {
	store    store.Store
	producer Publisher
	bus      *events.Bus
}

func New(s store.Store, p Publisher, bus *events.Bus) *Handler {
	if bus == nil {
		bus = events.New()
	}
	return &Handler{store: s, producer: p, bus: bus}
}

// Routes builds the chi router for sightings. authMiddleware is wrapped only
// around mutating endpoints (POST/PUT). Pass nil to skip auth entirely (dev).
func (h *Handler) Routes(authMiddleware func(http.Handler) http.Handler) chi.Router {
	r := chi.NewRouter()
	r.Get("/", h.List)
	r.Get("/stream", h.Stream) // server-sent events: live feed for the web UI
	r.Get("/{id}", h.Get)

	r.Group(func(r chi.Router) {
		if authMiddleware != nil {
			r.Use(authMiddleware)
		}
		r.Post("/", h.Create)
		r.Post("/{id}/notes", h.AddNote)
		r.Put("/{id}", h.Update)
		r.Put("/{id}/verify", h.Verify)
	})
	return r
}

func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	res, err := h.store.List(r.Context(), store.ListFilter{
		Species:      r.URL.Query().Get("species"),
		OnlyVerified: r.URL.Query().Get("verified") == "true",
		PageToken:    r.URL.Query().Get("page_token"),
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res.Sightings)
}

func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	s, err := h.store.Get(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, s)
}

func (h *Handler) Create(w http.ResponseWriter, r *http.Request) {
	var req models.CreateSightingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	if strings.TrimSpace(req.Species) == "" || strings.TrimSpace(req.ObservedBy) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "species and observed_by are required"})
		return
	}
	if req.ObservedAt.IsZero() {
		req.ObservedAt = time.Now().UTC()
	}
	s := models.Sighting{
		ID:         uuid.NewString(),
		Species:    req.Species,
		Location:   req.Location,
		Latitude:   req.Latitude,
		Longitude:  req.Longitude,
		ObservedBy: req.ObservedBy,
		ObservedAt: req.ObservedAt,
	}
	saved, err := h.store.Create(r.Context(), s)
	if err != nil {
		writeError(w, err)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	pubErr := h.producer.Publish(ctx, saved.ID, "sighting.created", saved)
	h.bus.Publish(events.Event{Type: events.TypeCreated, Timestamp: time.Now().UTC(), Sighting: saved})
	if pubErr != nil {
		writeJSON(w, http.StatusAccepted, map[string]interface{}{
			"sighting": saved,
			"warning":  "saved locally but failed to publish to kafka: " + pubErr.Error(),
		})
		return
	}
	writeJSON(w, http.StatusCreated, saved)
}

func (h *Handler) AddNote(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	var req models.AddNoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	if strings.TrimSpace(req.Author) == "" || strings.TrimSpace(req.Text) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "author and text are required"})
		return
	}
	note := models.Note{Author: req.Author, Text: req.Text, CreatedAt: time.Now().UTC()}
	saved, err := h.store.AddNote(r.Context(), id, note)
	if err != nil {
		writeError(w, err)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	_ = h.producer.Publish(ctx, saved.ID, "sighting.note_added", map[string]interface{}{
		"sighting_id": saved.ID,
		"note":        note,
	})
	h.bus.Publish(events.Event{Type: events.TypeNoteAdded, Timestamp: time.Now().UTC(), Sighting: saved, Note: &note})
	writeJSON(w, http.StatusCreated, saved)
}

func (h *Handler) Update(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	var req models.UpdateSightingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	saved, err := h.store.Update(r.Context(), id, func(s *models.Sighting) {
		if req.Species != nil {
			s.Species = *req.Species
		}
		if req.Location != nil {
			s.Location = *req.Location
		}
		if req.Latitude != nil {
			s.Latitude = *req.Latitude
		}
		if req.Longitude != nil {
			s.Longitude = *req.Longitude
		}
		if req.ObservedBy != nil {
			s.ObservedBy = *req.ObservedBy
		}
		if req.ObservedAt != nil {
			s.ObservedAt = *req.ObservedAt
		}
	})
	if err != nil {
		writeError(w, err)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	_ = h.producer.Publish(ctx, saved.ID, "sighting.updated", saved)
	h.bus.Publish(events.Event{Type: events.TypeUpdated, Timestamp: time.Now().UTC(), Sighting: saved})
	writeJSON(w, http.StatusOK, saved)
}

func (h *Handler) Verify(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	saved, err := h.store.Update(r.Context(), id, func(s *models.Sighting) {
		s.Verified = true
	})
	if err != nil {
		writeError(w, err)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	_ = h.producer.Publish(ctx, saved.ID, "sighting.verified", saved)
	h.bus.Publish(events.Event{Type: events.TypeVerified, Timestamp: time.Now().UTC(), Sighting: saved})
	writeJSON(w, http.StatusOK, saved)
}

// Stream is a server-sent-events endpoint that emits live sighting events.
// It writes one `event:` per domain event plus a periodic `:keepalive`
// comment so intermediaries don't time out idle connections.
func (h *Handler) Stream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // disable nginx buffering
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprint(w, ":connected\n\n")
	flusher.Flush()

	sub := h.bus.Subscribe()
	defer h.bus.Unsubscribe(sub)

	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case ev, ok := <-sub:
			if !ok {
				return
			}
			payload, err := json.Marshal(ev)
			if err != nil {
				continue
			}
			_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Type, payload)
			flusher.Flush()
		case <-keepalive.C:
			_, _ = fmt.Fprint(w, ":keepalive\n\n")
			flusher.Flush()
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, err error) {
	if errors.Is(err, store.ErrNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
}
