package store

import (
	"context"
	"errors"
	"strconv"
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
	ctx := context.Background()
	m := NewMemory()
	saved, err := m.Create(ctx, newSighting("s1", "Red Fox"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if saved.CreatedAt.IsZero() {
		t.Fatalf("expected CreatedAt to be set")
	}
	if !saved.UpdatedAt.Equal(saved.CreatedAt) {
		t.Errorf("expected UpdatedAt == CreatedAt on create, got %v vs %v", saved.UpdatedAt, saved.CreatedAt)
	}

	got, err := m.Get(ctx, "s1")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.ID != "s1" || got.Species != "Red Fox" {
		t.Errorf("unexpected sighting: %+v", got)
	}
}

func TestMemory_GetNotFound(t *testing.T) {
	m := NewMemory()
	_, err := m.Get(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemory_List(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	res, _ := m.List(ctx, ListFilter{})
	if len(res.Sightings) != 0 {
		t.Errorf("expected empty list, got %d", len(res.Sightings))
	}

	_, _ = m.Create(ctx, newSighting("s1", "Red Fox"))
	_, _ = m.Create(ctx, newSighting("s2", "Gray Wolf"))
	_, _ = m.Create(ctx, newSighting("s3", "European Lynx"))

	res, _ = m.List(ctx, ListFilter{})
	if len(res.Sightings) != 3 || res.Total != 3 {
		t.Fatalf("expected 3 sightings, got %d (total=%d)", len(res.Sightings), res.Total)
	}

	seen := map[string]bool{}
	for _, s := range res.Sightings {
		seen[s.ID] = true
	}
	for _, id := range []string{"s1", "s2", "s3"} {
		if !seen[id] {
			t.Errorf("missing sighting %s in list", id)
		}
	}
}

func TestMemory_List_FiltersAndPaging(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	for i := 0; i < 7; i++ {
		s := newSighting("s"+strconv.Itoa(i), "Red Fox")
		if i%2 == 0 {
			s.Verified = true
		}
		_, _ = m.Create(ctx, s)
		// ensure distinct CreatedAt ordering
		time.Sleep(time.Millisecond)
	}
	_, _ = m.Create(ctx, newSighting("w1", "Gray Wolf"))

	// species filter (case-insensitive)
	res, _ := m.List(ctx, ListFilter{Species: "red fox"})
	if len(res.Sightings) != 7 {
		t.Errorf("species filter: got %d, want 7", len(res.Sightings))
	}

	// only_verified
	res, _ = m.List(ctx, ListFilter{OnlyVerified: true})
	for _, s := range res.Sightings {
		if !s.Verified {
			t.Errorf("only_verified returned non-verified id=%s", s.ID)
		}
	}

	// paging
	page1, _ := m.List(ctx, ListFilter{PageSize: 3})
	if len(page1.Sightings) != 3 {
		t.Fatalf("page 1: got %d", len(page1.Sightings))
	}
	if page1.NextPageToken == "" {
		t.Fatalf("page 1: expected next token")
	}
	page2, _ := m.List(ctx, ListFilter{PageSize: 3, PageToken: page1.NextPageToken})
	if len(page2.Sightings) != 3 {
		t.Errorf("page 2: got %d", len(page2.Sightings))
	}
	if page2.Sightings[0].ID == page1.Sightings[0].ID {
		t.Errorf("page 2 starts with same item as page 1")
	}
}

func TestMemory_Update(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	_, _ = m.Create(ctx, newSighting("s1", "Red Fox"))
	time.Sleep(2 * time.Millisecond)

	updated, err := m.Update(ctx, "s1", func(s *models.Sighting) {
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
	_, err := m.Update(context.Background(), "missing", func(s *models.Sighting) {})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemory_Delete(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	_, _ = m.Create(ctx, newSighting("s1", "Red Fox"))

	if err := m.Delete(ctx, "s1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := m.Get(ctx, "s1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound after Delete, got %v", err)
	}
	if err := m.Delete(ctx, "s1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound on second Delete, got %v", err)
	}
}

func TestMemory_AddNote(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	_, _ = m.Create(ctx, newSighting("s1", "Red Fox"))

	note := models.Note{Author: "ranger", Text: "looked healthy", CreatedAt: time.Now().UTC()}
	saved, err := m.AddNote(ctx, "s1", note)
	if err != nil {
		t.Fatalf("AddNote returned error: %v", err)
	}
	if len(saved.Notes) != 1 {
		t.Fatalf("expected 1 note, got %d", len(saved.Notes))
	}
	if saved.Notes[0].Author != "ranger" || saved.Notes[0].Text != "looked healthy" {
		t.Errorf("unexpected note: %+v", saved.Notes[0])
	}

	if _, err := m.AddNote(ctx, "s1", models.Note{Author: "bio", Text: "tagged"}); err != nil {
		t.Fatalf("second AddNote failed: %v", err)
	}
	got, _ := m.Get(ctx, "s1")
	if len(got.Notes) != 2 {
		t.Errorf("expected 2 notes after second add, got %d", len(got.Notes))
	}
}

func TestMemory_ConcurrentWrites(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			id := "s" + strconv.Itoa(i)
			_, _ = m.Create(ctx, newSighting(id, "Species"))
		}(i)
	}
	wg.Wait()
	res, _ := m.List(ctx, ListFilter{PageSize: 200})
	if got := res.Total; got != n {
		t.Errorf("expected total=%d sightings after concurrent Create, got %d", n, got)
	}
}
