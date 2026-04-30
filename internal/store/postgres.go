package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/YemetsValen/go-kafka-proj/internal/models"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Postgres is a pgx-backed Store.
type Postgres struct {
	pool *pgxpool.Pool
}

// Compile-time check that Postgres satisfies Store.
var _ Store = (*Postgres)(nil)

func NewPostgres(pool *pgxpool.Pool) *Postgres {
	return &Postgres{pool: pool}
}

const sightingColumns = `id, species, location, latitude, longitude, observed_by,
		observed_at, notes, verified, created_at, updated_at`

func scanSighting(row pgx.Row) (models.Sighting, error) {
	var s models.Sighting
	var notes []byte
	err := row.Scan(
		&s.ID, &s.Species, &s.Location, &s.Latitude, &s.Longitude,
		&s.ObservedBy, &s.ObservedAt, &notes, &s.Verified,
		&s.CreatedAt, &s.UpdatedAt,
	)
	if err != nil {
		return models.Sighting{}, err
	}
	if len(notes) > 0 {
		if err := json.Unmarshal(notes, &s.Notes); err != nil {
			return models.Sighting{}, fmt.Errorf("unmarshal notes: %w", err)
		}
	}
	return s, nil
}

func (p *Postgres) Get(ctx context.Context, id string) (models.Sighting, error) {
	row := p.pool.QueryRow(ctx,
		`SELECT `+sightingColumns+` FROM sightings WHERE id = $1`, id)
	s, err := scanSighting(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return models.Sighting{}, ErrNotFound
	}
	return s, err
}

func (p *Postgres) List(ctx context.Context, f ListFilter) (ListResult, error) {
	pageSize := f.PageSize
	if pageSize <= 0 || pageSize > 200 {
		pageSize = 50
	}
	offset := 0
	if f.PageToken != "" {
		if v, err := parseOffsetToken(f.PageToken); err == nil {
			offset = v
		}
	}

	var (
		conds []string
		args  []any
	)
	if f.Species != "" {
		args = append(args, f.Species)
		conds = append(conds, fmt.Sprintf("LOWER(species) = LOWER($%d)", len(args)))
	}
	if f.OnlyVerified {
		conds = append(conds, "verified = TRUE")
	}
	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}

	// Count total matches for pagination metadata.
	var total int
	if err := p.pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM sightings "+where, args...,
	).Scan(&total); err != nil {
		return ListResult{}, err
	}

	args = append(args, pageSize+1, offset)
	q := fmt.Sprintf(`SELECT %s FROM sightings %s
		ORDER BY created_at ASC, id ASC
		LIMIT $%d OFFSET $%d`,
		sightingColumns, where, len(args)-1, len(args))

	rows, err := p.pool.Query(ctx, q, args...)
	if err != nil {
		return ListResult{}, err
	}
	defer rows.Close()

	out := make([]models.Sighting, 0, pageSize)
	for rows.Next() {
		s, err := scanSighting(rows)
		if err != nil {
			return ListResult{}, err
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return ListResult{}, err
	}

	next := ""
	if len(out) > pageSize {
		out = out[:pageSize]
		next = formatOffsetToken(offset + pageSize)
	}
	return ListResult{Sightings: out, NextPageToken: next, Total: total}, nil
}

func (p *Postgres) Create(ctx context.Context, s models.Sighting) (models.Sighting, error) {
	if s.Notes == nil {
		s.Notes = []models.Note{}
	}
	notes, err := json.Marshal(s.Notes)
	if err != nil {
		return models.Sighting{}, err
	}
	now := time.Now().UTC()
	s.CreatedAt = now
	s.UpdatedAt = now

	row := p.pool.QueryRow(ctx, `
		INSERT INTO sightings (`+sightingColumns+`)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		RETURNING `+sightingColumns,
		s.ID, s.Species, s.Location, s.Latitude, s.Longitude, s.ObservedBy,
		s.ObservedAt, notes, s.Verified, s.CreatedAt, s.UpdatedAt,
	)
	return scanSighting(row)
}

func (p *Postgres) Update(ctx context.Context, id string, fn func(*models.Sighting)) (models.Sighting, error) {
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return models.Sighting{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	row := tx.QueryRow(ctx,
		`SELECT `+sightingColumns+` FROM sightings WHERE id = $1 FOR UPDATE`, id)
	s, err := scanSighting(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return models.Sighting{}, ErrNotFound
	}
	if err != nil {
		return models.Sighting{}, err
	}

	fn(&s)
	s.UpdatedAt = time.Now().UTC()
	notes, err := json.Marshal(s.Notes)
	if err != nil {
		return models.Sighting{}, err
	}

	row = tx.QueryRow(ctx, `
		UPDATE sightings
		SET species = $2, location = $3, latitude = $4, longitude = $5,
		    observed_by = $6, observed_at = $7, notes = $8, verified = $9,
		    updated_at = $10
		WHERE id = $1
		RETURNING `+sightingColumns,
		s.ID, s.Species, s.Location, s.Latitude, s.Longitude, s.ObservedBy,
		s.ObservedAt, notes, s.Verified, s.UpdatedAt,
	)
	saved, err := scanSighting(row)
	if err != nil {
		return models.Sighting{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return models.Sighting{}, err
	}
	return saved, nil
}

func (p *Postgres) Delete(ctx context.Context, id string) error {
	tag, err := p.pool.Exec(ctx, `DELETE FROM sightings WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (p *Postgres) AddNote(ctx context.Context, id string, note models.Note) (models.Sighting, error) {
	return p.Update(ctx, id, func(s *models.Sighting) {
		s.Notes = append(s.Notes, note)
	})
}
