// Package store defines the persistence interface for sightings and provides
// in-memory and Postgres implementations.
package store

import (
	"context"
	"errors"

	"github.com/YemetsValen/go-kafka-proj/internal/models"
)

// ErrNotFound is returned when a sighting with the given id does not exist.
var ErrNotFound = errors.New("sighting not found")

// ListFilter narrows down a List call.
type ListFilter struct {
	Species      string
	OnlyVerified bool
	PageSize     int
	PageToken    string
}

// ListResult is the return shape for List.
type ListResult struct {
	Sightings     []models.Sighting
	NextPageToken string
	Total         int
}

// Store is the persistence contract for sightings. All implementations must be
// safe for concurrent use.
type Store interface {
	List(ctx context.Context, f ListFilter) (ListResult, error)
	Get(ctx context.Context, id string) (models.Sighting, error)
	Create(ctx context.Context, s models.Sighting) (models.Sighting, error)
	// Update fetches the row, calls fn to mutate it, then persists the result.
	Update(ctx context.Context, id string, fn func(*models.Sighting)) (models.Sighting, error)
	Delete(ctx context.Context, id string) error
	AddNote(ctx context.Context, id string, note models.Note) (models.Sighting, error)
}
