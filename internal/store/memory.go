package store

import (
	"errors"
	"sync"
	"time"

	"github.com/YemetsValen/go-kafka-proj/internal/models"
)

var ErrNotFound = errors.New("sighting not found")

type Memory struct {
	mu        sync.RWMutex
	sightings map[string]*models.Sighting
}

func NewMemory() *Memory {
	return &Memory{sightings: make(map[string]*models.Sighting)}
}

func (m *Memory) List() []models.Sighting {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]models.Sighting, 0, len(m.sightings))
	for _, s := range m.sightings {
		out = append(out, *s)
	}
	return out
}

func (m *Memory) Get(id string) (models.Sighting, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sightings[id]
	if !ok {
		return models.Sighting{}, ErrNotFound
	}
	return *s, nil
}

func (m *Memory) Create(s models.Sighting) models.Sighting {
	m.mu.Lock()
	defer m.mu.Unlock()
	s.CreatedAt = time.Now().UTC()
	s.UpdatedAt = s.CreatedAt
	m.sightings[s.ID] = &s
	return s
}

func (m *Memory) Update(id string, fn func(*models.Sighting)) (models.Sighting, error) {
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

func (m *Memory) AddNote(id string, note models.Note) (models.Sighting, error) {
	return m.Update(id, func(s *models.Sighting) {
		s.Notes = append(s.Notes, note)
	})
}
