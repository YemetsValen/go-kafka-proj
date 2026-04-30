package store

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/YemetsValen/go-kafka-proj/internal/models"
)

// Memory is an in-memory Store, useful for tests and local demo.
type Memory struct {
	mu        sync.RWMutex
	sightings map[string]*models.Sighting
}

// Compile-time check that Memory satisfies Store.
var _ Store = (*Memory)(nil)

func NewMemory() *Memory {
	return &Memory{sightings: make(map[string]*models.Sighting)}
}

func (m *Memory) List(_ context.Context, f ListFilter) (ListResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	all := make([]models.Sighting, 0, len(m.sightings))
	for _, s := range m.sightings {
		if f.Species != "" && !strings.EqualFold(s.Species, f.Species) {
			continue
		}
		if f.OnlyVerified && !s.Verified {
			continue
		}
		all = append(all, *s)
	}
	// Stable order for deterministic paging.
	sort.Slice(all, func(i, j int) bool {
		if all[i].CreatedAt.Equal(all[j].CreatedAt) {
			return all[i].ID < all[j].ID
		}
		return all[i].CreatedAt.Before(all[j].CreatedAt)
	})

	total := len(all)

	// Apply opaque page_token = "<offset>" for the in-memory impl.
	offset := 0
	if f.PageToken != "" {
		if v, err := parseOffsetToken(f.PageToken); err == nil {
			offset = v
		}
	}
	if offset > len(all) {
		offset = len(all)
	}
	all = all[offset:]

	pageSize := f.PageSize
	if pageSize <= 0 || pageSize > 200 {
		pageSize = 50
	}
	next := ""
	if len(all) > pageSize {
		all = all[:pageSize]
		next = formatOffsetToken(offset + pageSize)
	}
	return ListResult{Sightings: all, NextPageToken: next, Total: total}, nil
}

func (m *Memory) Get(_ context.Context, id string) (models.Sighting, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sightings[id]
	if !ok {
		return models.Sighting{}, ErrNotFound
	}
	return *s, nil
}

func (m *Memory) Create(_ context.Context, s models.Sighting) (models.Sighting, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s.CreatedAt = time.Now().UTC()
	s.UpdatedAt = s.CreatedAt
	m.sightings[s.ID] = &s
	return s, nil
}

func (m *Memory) Update(_ context.Context, id string, fn func(*models.Sighting)) (models.Sighting, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sightings[id]
	if !ok {
		return models.Sighting{}, ErrNotFound
	}
	fn(s)
	s.UpdatedAt = time.Now().UTC()
	return *s, nil
}

func (m *Memory) Delete(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.sightings[id]; !ok {
		return ErrNotFound
	}
	delete(m.sightings, id)
	return nil
}

func (m *Memory) AddNote(ctx context.Context, id string, note models.Note) (models.Sighting, error) {
	return m.Update(ctx, id, func(s *models.Sighting) {
		s.Notes = append(s.Notes, note)
	})
}
