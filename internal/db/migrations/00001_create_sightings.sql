-- +goose Up
CREATE TABLE sightings (
    id           TEXT PRIMARY KEY,
    species      TEXT NOT NULL,
    location     TEXT NOT NULL DEFAULT '',
    latitude     DOUBLE PRECISION NOT NULL DEFAULT 0,
    longitude    DOUBLE PRECISION NOT NULL DEFAULT 0,
    observed_by  TEXT NOT NULL,
    observed_at  TIMESTAMPTZ NOT NULL,
    notes        JSONB NOT NULL DEFAULT '[]'::jsonb,
    verified     BOOLEAN NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX sightings_species_idx     ON sightings (LOWER(species));
CREATE INDEX sightings_observed_at_idx ON sightings (observed_at DESC);
CREATE INDEX sightings_created_at_idx  ON sightings (created_at ASC, id ASC);

-- +goose Down
DROP TABLE IF EXISTS sightings;
