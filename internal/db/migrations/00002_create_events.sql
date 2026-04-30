-- +goose Up
CREATE TABLE events (
    id          BIGSERIAL PRIMARY KEY,
    event_type  TEXT NOT NULL,
    sighting_id TEXT NOT NULL,
    payload     JSONB NOT NULL,
    request_id  TEXT,
    trace_id    TEXT,
    occurred_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX events_sighting_id_idx ON events (sighting_id);
CREATE INDEX events_event_type_idx  ON events (event_type);
CREATE INDEX events_received_at_idx ON events (received_at DESC);

-- +goose Down
DROP TABLE events;
