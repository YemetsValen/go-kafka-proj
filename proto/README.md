# Proto definitions

gRPC API for the Wildlife Sightings Log.

```
proto/
└── sightings/
    └── v1/
        └── sightings.proto    # SightingService — CRUD + streaming RPCs
```

## What's in `SightingService`

CRUD:
- `CreateSighting` / `GetSighting` / `ListSightings` (with paging + filters) / `UpdateSighting` (with `FieldMask`) / `DeleteSighting`

Domain ops (publish to Kafka):
- `VerifySighting`, `AddNote`

Streaming (the interesting bits):
- `Watch` — server-streaming feed of all sighting events (created / updated / noted / verified / deleted), backed by a Kafka consumer.
- `BulkCreate` — client-streaming bulk ingest (e.g. uploading a field journal) with a summary at the end.
- `Chat` — bidirectional collaborative annotation channel for observers viewing the same sighting; messages are also fan-out to Kafka as `sighting.chat`.

## Code generation (with [buf](https://buf.build))

From the repo root:

```bash
buf generate
```

This reads `buf.gen.yaml` and writes generated Go bindings to `gen/go/sightings/v1/`.

`gen/` is intentionally not committed yet — generate it locally or wire it into CI.
