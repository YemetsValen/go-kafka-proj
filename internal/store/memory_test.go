package store

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/YemetsValen/go-kafka-proj/internal/models"
)

func newSighting(id, species string) models.Sighting {
	return models.Sighting{
		ID:         id,
		Species:    species,
		Location:   "Test Forest",
		Latitude:   48.5,
		Longitude:  24.5,
		ObservedBy: "tester",
		ObservedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
	}
}

func TestMemory_CreateAndGet(t *testing.T) {
	m := NewMemory()
	saved := m.Create(newSighting("s1", "Red Fox"))

	if saved.CreatedAt.IsZero() {
		t.Fatalf("expected CreatedAt to be set")
	}
	if !saved.UpdatedAt.Equal(saved.CreatedAt) {
		t.Errorf("expected UpdatedAt == CreatedAt on create, got %v vs %v", saved.UpdatedAt, saved.CreatedAt)
	}

	got, err := m.Get("s1")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.ID != "s1" || got.Species != "Red Fox" {
		t.Errorf("unexpected sighting: %+v", got)
	}
}

func TestMemory_GetNotFound(t *testing.T) {
	m := NewMemory()
	_, err := m.Get("missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemory_List(t *testing.T) {
	m := NewMemory()
	if got := m.List(); len(got) != 0 {
		t.Errorf("expected empty list, got %d", len(got))
	}

	m.Create(newSighting("s1", "Red Fox"))
	m.Create(newSighting("s2", "Gray Wolf"))
	m.Create(newSighting("s3", "European Lynx"))

	got := m.List()
	if len(got) != 3 {
		t.Fatalf("expected 3 sightings, got %d", len(got))
	}

	seen := map[string]bool{}
	for _, s := range got {
		seen[s.ID] = true
	}
	for _, id := range []string{"s1", "s2", "s3"} {
		if !seen[id] {
			t.Errorf("missing sighting %s in list", id)
		}
	}
}

func TestMemory_Update(t *testing.T) {
	m := NewMemory()
	m.Create(newSighting("s1", "Red Fox"))

	// Force a measurable difference between CreatedAt and UpdatedAt.
	time.Sleep(2 * time.Millisecond)

	updated, err := m.Update("s1", func(s *models.Sighting) {
		s.Species = "Arctic Fox"
		s.Verified = true
	})
	if err != nil {
		t.Fatalf("Update returned error: %v", err)
	}
	if updated.Species != "Arctic Fox" {
		t.Errorf("expected species to be updated, got %q", updated.Species)
	}
	if !updated.Verified {
		t.Errorf("expected Verified=true")
	}
	if !updated.UpdatedAt.After(updated.CreatedAt) {
		t.Errorf("expected UpdatedAt > CreatedAt, got %v vs %v", updated.UpdatedAt, updated.CreatedAt)
	}
}

func TestMemory_UpdateNotFound(t *testing.T) {
	m := NewMemory()
	_, err := m.Update("missing", func(s *models.Sighting) {})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemory_AddNote(t *testing.T) {
	m := NewMemory()
	m.Create(newSighting("s1", "Red Fox"))

	note := models.Note{Author: "ranger", Text: "looked healthy", CreatedAt: time.Now().UTC()}
	saved, err := m.AddNote("s1", note)
	if err != nil {
		t.Fatalf("AddNote returned error: %v", err)
	}
	if len(saved.Notes) != 1 {
		t.Fatalf("expected 1 note, got %d", len(saved.Notes))
	}
	if saved.Notes[0].Author != "ranger" || saved.Notes[0].Text != "looked healthy" {
		t.Errorf("unexpected note: %+v", saved.Notes[0])
	}

	// Second note should append, not overwrite.
	if _, err := m.AddNote("s1", models.Note{Author: "bio", Text: "tagged"}); err != nil {
		t.Fatalf("second AddNote failed: %v", err)
	}
	got, _ := m.Get("s1")
	if len(got.Notes) != 2 {
		t.Errorf("expected 2 notes after second add, got %d", len(got.Notes))
	}
}

func TestMemory_ConcurrentWrites(t *testing.T) {
	m := NewMemory()
	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			id := "s" + itoa(i)
			m.Create(newSighting(id, "Species"))
		}(i)
	}
	wg.Wait()
	if got := len(m.List()); got != n {
		t.Errorf("expected %d sightings after concurrent Create, got %d", n, got)
	}
}

// itoa avoids importing strconv just for this test file.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}
