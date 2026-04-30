package store

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/YemetsValen/go-kafka-proj/internal/db"
	"github.com/YemetsValen/go-kafka-proj/internal/models"
)

// TestPostgres exercises the Postgres store against a real database.
//
// It is skipped unless POSTGRES_TEST_DSN is set, so the unit-test workflow
// stays hermetic. To run locally:
//
//	docker compose up -d postgres
//	POSTGRES_TEST_DSN=postgres://wildlife:wildlife@localhost:5432/wildlife?sslmode=disable \
//	  go test ./internal/store/... -run TestPostgres
func TestPostgres(t *testing.T) {
	dsn := os.Getenv("POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_TEST_DSN not set; skipping Postgres integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := db.Migrate(ctx, dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := db.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	// Clean slate before/after the test.
	if _, err := pool.Exec(ctx, `TRUNCATE sightings`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(ctx, `TRUNCATE sightings`) })

	pg := NewPostgres(pool)

	saved, err := pg.Create(ctx, models.Sighting{
		ID: "pg-1", Species: "Red Fox", Location: "Carpathians",
		Latitude: 48.5, Longitude: 24.5, ObservedBy: "ranger",
		ObservedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if saved.CreatedAt.IsZero() || saved.UpdatedAt.IsZero() {
		t.Errorf("expected timestamps to be set")
	}

	got, err := pg.Get(ctx, "pg-1")
	if err != nil || got.Species != "Red Fox" {
		t.Fatalf("Get: %+v err=%v", got, err)
	}

	if _, err := pg.AddNote(ctx, "pg-1", models.Note{Author: "ranger", Text: "ok"}); err != nil {
		t.Fatalf("AddNote: %v", err)
	}
	got, _ = pg.Get(ctx, "pg-1")
	if len(got.Notes) != 1 {
		t.Errorf("expected 1 note, got %d", len(got.Notes))
	}

	upd, err := pg.Update(ctx, "pg-1", func(s *models.Sighting) { s.Verified = true })
	if err != nil || !upd.Verified {
		t.Fatalf("Update: %+v err=%v", upd, err)
	}

	res, err := pg.List(ctx, ListFilter{Species: "red fox", OnlyVerified: true})
	if err != nil || len(res.Sightings) != 1 {
		t.Fatalf("List filter: got %d err=%v", len(res.Sightings), err)
	}

	if err := pg.Delete(ctx, "pg-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := pg.Get(ctx, "pg-1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound after delete, got %v", err)
	}
}
