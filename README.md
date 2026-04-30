# go-kafka-proj

Wildlife Sightings Log — a small Go service that lets observers record wildlife sightings over HTTP **and** gRPC and streams each event to Kafka for downstream processing (analytics, notifications, etc.).

## Stack

- **Go** 1.23+ (toolchain auto-bumps if needed)
- **HTTP router:** [go-chi/chi](https://github.com/go-chi/chi) on top of the standard `net/http`
- **gRPC:** [google.golang.org/grpc](https://pkg.go.dev/google.golang.org/grpc) — `SightingService` with CRUD + 3 streaming RPCs (`Watch`, `BulkCreate`, `Chat`)
- **Kafka client:** [segmentio/kafka-go](https://github.com/segmentio/kafka-go)
- **Storage:** Postgres via [pgx/v5](https://github.com/jackc/pgx) (in-memory fallback when `DATABASE_URL` is unset)
- **Migrations:** embedded SQL files run via [goose](https://github.com/pressly/goose) on startup
- **Proto tooling:** [buf](https://buf.build) (`buf lint`, `buf generate`)

## Project layout

```
.
├── cmd/
│   └── server/
│       └── main.go            # entrypoint: HTTP + gRPC servers, graceful shutdown
├── internal/
│   ├── db/
│   │   ├── db.go              # pgxpool + embedded goose migrations
│   │   └── migrations/        # *.sql, embedded into the binary
│   ├── grpcsvc/
│   │   ├── server.go          # SightingService gRPC implementation
│   │   ├── broadcast.go       # in-process pub/sub for Watch/Chat
│   │   └── server_test.go     # gRPC tests over bufconn
│   ├── handlers/              # HTTP handlers (GET/POST/PUT) + tests
│   ├── kafka/                 # Kafka producer wrapper
│   ├── models/                # domain types
│   └── store/
│       ├── store.go           # Store interface + ListFilter / ListResult
│       ├── memory.go          # in-memory implementation
│       ├── postgres.go        # pgx/v5 implementation
│       └── *_test.go          # unit + (skipped-by-default) integration tests
├── proto/sightings/v1/        # protobuf service definition
├── gen/go/sightings/v1/       # generated Go bindings (buf generate)
├── docker-compose.yml         # Kafka + Postgres for local development
├── buf.gen.yaml
├── go.mod
└── README.md
```

## Endpoints

| Method | Path                         | Description                                        | Publishes to Kafka      |
| ------ | ---------------------------- | -------------------------------------------------- | ----------------------- |
| GET    | `/sightings`                 | List all sightings                                 | —                       |
| GET    | `/sightings/{id}`            | Get one sighting by id                             | —                       |
| POST   | `/sightings`                 | Create a sighting                                  | `sighting.created`      |
| POST   | `/sightings/{id}/notes`      | Attach a note to a sighting                        | `sighting.note_added`   |
| PUT    | `/sightings/{id}`            | Update fields (species, location, coordinates, …)  | —                       |
| PUT    | `/sightings/{id}/verify`     | Mark a sighting as verified                        | `sighting.verified`     |

Plus operational endpoints:

- `GET /healthz` — liveness (always 200 if process is up).
- `GET /readyz` — readiness; pings Postgres (when configured) and dials the first Kafka broker. Returns 503 with a per-check breakdown when something is down.
- `GET /metrics` (on `METRICS_ADDR`, `:9100` by default) — Prometheus metrics for HTTP request counts/latency, Kafka publish/consume results, and runtime/process collectors.

## Configuration (env)

Config is parsed by [`kelseyhightower/envconfig`](https://github.com/kelseyhightower/envconfig). A `.env` file in the working directory is auto-loaded for local development.

### Service

| Variable        | Default                | Description                                                                       |
| --------------- | ---------------------- | --------------------------------------------------------------------------------- |
| `HTTP_ADDR`     | `:8080`                | HTTP listen address                                                               |
| `GRPC_ADDR`     | `:9090`                | gRPC listen address                                                               |
| `METRICS_ADDR`  | `:9100`                | Prometheus `/metrics` listener                                                    |
| `SERVICE_NAME`  | `wildlife-sightings`   | Service name used in logs and tracing                                             |
| `ENV`           | `dev`                  | Deployment environment label (used in trace resource attributes)                  |

### Kafka

| Variable                | Default                          | Description                                          |
| ----------------------- | -------------------------------- | ---------------------------------------------------- |
| `KAFKA_BROKERS`         | `localhost:9092`                 | Comma-separated bootstrap brokers                    |
| `KAFKA_TOPIC`           | `wildlife.sightings`             | Topic name (producer + consumer)                     |
| `KAFKA_CONSUMER_GROUP`  | `wildlife-sightings-consumer`    | Consumer group ID for `cmd/consumer`                 |
| `KAFKA_DIAL_TIMEOUT`    | `5s`                             | Timeout for the readyz Kafka dial                    |

### Postgres

| Variable                | Default        | Description                                                                            |
| ----------------------- | -------------- | -------------------------------------------------------------------------------------- |
| `DATABASE_URL`          | _(unset)_      | Postgres DSN. If empty the **server** falls back to in-memory storage; the **consumer** requires it. |
| `DATABASE_MAX_CONNS`    | `10`           | Pool max connections                                                                   |
| `DATABASE_PING_TIMEOUT` | `3s`           | Per-check timeout for `/readyz` Postgres ping                                          |

### Auth

| Variable          | Default      | Description                                                                          |
| ----------------- | ------------ | ------------------------------------------------------------------------------------ |
| `AUTH_JWT_SECRET` | _(unset)_    | HS256 shared secret used to validate `Authorization: Bearer <jwt>`                   |
| `AUTH_API_KEYS`   | _(unset)_    | Comma-separated list of static API keys accepted as `Authorization: Bearer <key>`    |

### Logging

| Variable      | Default | Description                                                              |
| ------------- | ------- | ------------------------------------------------------------------------ |
| `LOG_LEVEL`   | `info`  | One of `debug`, `info`, `warn`, `error`                                  |
| `LOG_FORMAT`  | `json`  | `json` (production) or `text` (human-readable, dev)                      |

### OpenTelemetry

| Variable                          | Default | Description                                                                                       |
| --------------------------------- | ------- | ------------------------------------------------------------------------------------------------- |
| `OTEL_EXPORTER_OTLP_ENDPOINT`     | _(unset)_ | Host:port of an OTLP/HTTP collector. When unset, a no-op tracer is used and traces are dropped. |
| `OTEL_EXPORTER_OTLP_INSECURE`     | `true`  | Skip TLS when talking to the collector                                                            |
| `OTEL_TRACES_SAMPLER_ARG`         | `1.0`   | TraceID-ratio sampler argument (0.0–1.0)                                                          |

## Running locally

There are two ways to run the stack:

### A. Everything in Docker (`app` profile)

Builds the multi-stage `Dockerfile` and brings up server, consumer, Kafka and Postgres in one shot:

```bash
docker compose --profile app up --build
```

The server is reachable on `localhost:8080` (HTTP), `localhost:9090` (gRPC), `localhost:9100` (`/metrics`); the consumer's `/metrics` is exposed on `localhost:9101`. Auth is enabled with the dev API key `dev` (`Authorization: Bearer dev`).

### B. Hot-reload Go locally, infra in Docker

Start the infrastructure only:

```bash
docker compose up -d kafka postgres
```

Then run the binaries from source:

```bash
DATABASE_URL=postgres://wildlife:wildlife@localhost:5432/wildlife?sslmode=disable \
  go run ./cmd/server

# in another shell
DATABASE_URL=postgres://wildlife:wildlife@localhost:5432/wildlife?sslmode=disable \
METRICS_ADDR=:9101 \
  go run ./cmd/consumer
```

Migrations run automatically at startup (goose, embedded SQL).

Without `DATABASE_URL` the server starts with an in-memory store — handy for quick demos, but state is lost on restart.

### Docker images

The repo ships a single multi-stage `Dockerfile` with two final targets:

```bash
docker build --target server   -t go-kafka-proj-server   .
docker build --target consumer -t go-kafka-proj-consumer .
```

Both produce ~25 MB images on top of `gcr.io/distroless/static-debian12:nonroot` (static-linked, no shell, runs as `nonroot`).

Released images are published automatically to GHCR on every `v*.*.*` tag — see [Releases](#releases) below.

### Releases

`.github/workflows/release.yml` builds and pushes both images to GHCR on tag pushes that match `v*.*.*` (also available as a manual `workflow_dispatch`). Images are multi-arch (`linux/amd64`, `linux/arm64`):

- `ghcr.io/yemetsvalen/go-kafka-proj-server:<tag>`
- `ghcr.io/yemetsvalen/go-kafka-proj-consumer:<tag>`

Tags applied per release: full SemVer (`v1.2.3`), major.minor (`1.2`), major (`1`), commit SHA (`sha-abcdef0`).

Cut a release:

```bash
git tag v0.1.0
git push origin v0.1.0
```

### gRPC quickstart

```bash
# list services (server reflection is enabled)
grpcurl -plaintext localhost:9090 list

# create a sighting
grpcurl -plaintext -d '{
  "species": "Red Fox",
  "location": "Carpathians",
  "position": {"latitude": 48.5, "longitude": 24.5},
  "observed_by": "ranger"
}' localhost:9090 sightings.v1.SightingService/CreateSighting

# tail the live event stream
grpcurl -plaintext -d '{"from_beginning": true}' \
  localhost:9090 sightings.v1.SightingService/Watch
```

## Authentication

Mutating endpoints (`POST /sightings`, `POST /sightings/{id}/notes`, `PUT /sightings/{id}`, `PUT /sightings/{id}/verify`) and the corresponding mutating gRPC RPCs (`CreateSighting`, `UpdateSighting`, `DeleteSighting`, `VerifySighting`, `AddNote`, `BulkCreate`, `Chat`) require a bearer token. Read endpoints (`GET /sightings`, `GET /sightings/{id}`, `GetSighting`, `ListSightings`, `Watch`) stay public.

Two token shapes are accepted, both via `Authorization: Bearer <token>` (HTTP) or the `authorization` metadata key (gRPC):

- **JWT (HS256)** signed with `AUTH_JWT_SECRET`. Mint dev tokens with the bundled CLI:

  ```bash
  AUTH_JWT_SECRET=devsecret go run ./cmd/mintjwt -sub rama -exp 1h
  ```

- **Static API keys** listed in `AUTH_API_KEYS` (comma-separated). Useful for service-to-service callers that don't need per-user identity.

If neither variable is set the verifier is **disabled** and a startup warning (`WARNING: auth is DISABLED`) is logged — handy for local hacking, never for production.

Example call:

```bash
TOKEN=$(AUTH_JWT_SECRET=devsecret go run ./cmd/mintjwt -sub rama)
curl -sS -X POST http://localhost:8080/sightings \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"species":"Red Fox","observed_by":"rama"}'
```

## Tests

```bash
go test ./...
```

What runs by default (no Kafka, no Postgres):

- HTTP handler tests via `net/http/httptest` with a mock `Publisher`.
- In-memory store unit tests, including a 100-goroutine concurrent-writes check.
- gRPC server tests over an in-process `bufconn` listener — exercise CRUD, Verify/AddNote, BulkCreate (client-streaming) and Watch (server-streaming).

The Postgres integration test in `internal/store/postgres_test.go` is skipped unless `POSTGRES_TEST_DSN` is set:

```bash
docker compose up -d postgres
POSTGRES_TEST_DSN=postgres://wildlife:wildlife@localhost:5432/wildlife?sslmode=disable \
  go test ./internal/store/... -run TestPostgres
```

## Example requests

Create a sighting:

```bash
curl -sS -X POST http://localhost:8080/sightings \
  -H 'Content-Type: application/json' \
  -d '{
    "species": "Red Fox",
    "location": "Carpathian foothills",
    "latitude": 48.6,
    "longitude": 24.7,
    "observed_by": "rama",
    "observed_at": "2026-04-26T05:30:00Z"
  }'
```

List all sightings:

```bash
curl -sS http://localhost:8080/sightings
```

Add a note:

```bash
curl -sS -X POST http://localhost:8080/sightings/<id>/notes \
  -H 'Content-Type: application/json' \
  -d '{"author":"rama","text":"looked healthy, moving north"}'
```

Update coordinates:

```bash
curl -sS -X PUT http://localhost:8080/sightings/<id> \
  -H 'Content-Type: application/json' \
  -d '{"latitude": 48.62, "longitude": 24.71}'
```

Mark as verified:

```bash
curl -sS -X PUT http://localhost:8080/sightings/<id>/verify
```

## Kafka event shape

```json
{
  "type": "sighting.created",
  "timestamp": "2026-04-26T05:30:01Z",
  "metadata": {
    "request_id": "f1c4...",
    "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736"
  },
  "payload": { "... full sighting ..." }
}
```

W3C `traceparent` is also written into Kafka message headers, so the
`cmd/consumer` reuses the same trace context when it persists each event.

Consume messages for debugging:

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic wildlife.sightings \
  --from-beginning
```

## Consumer

`cmd/consumer` is a separate binary that reads `wildlife.sightings`, parses the JSON envelope, restores the trace context from the message headers, and persists every event into the Postgres `events` table for analytics. Like the server it exposes Prometheus metrics on `METRICS_ADDR` and writes structured slog records.

Run locally (alongside `cmd/server`):

```bash
DATABASE_URL=postgres://wildlife:wildlife@localhost:5432/wildlife?sslmode=disable \
KAFKA_BROKERS=localhost:9092 \
KAFKA_CONSUMER_GROUP=wildlife-sightings-consumer \
METRICS_ADDR=:9101 \
  go run ./cmd/consumer
```

Rows are written to `events (id, event_type, sighting_id, payload jsonb, request_id, trace_id, occurred_at, received_at)`.

## Observability

- **Logs** — every record carries `service`, plus `request_id` (from chi `RequestID`) and `trace_id` (from the active OpenTelemetry span) when present, so a single `request_id` traces an HTTP call → Kafka publish → consumer write.
- **Metrics** — `wildlife_http_requests_total{method,route,status}`, `wildlife_http_request_duration_seconds_bucket`, `wildlife_kafka_publish_total{event_type,result}`, `wildlife_kafka_consume_total{event_type,result}`, plus the Go runtime/process collectors.
- **Tracing** — HTTP and gRPC servers are wrapped with `otelhttp` / `otelgrpc`. The Kafka producer creates a producer span and injects W3C tracecontext into headers; the consumer extracts it and creates a child span. Point `OTEL_EXPORTER_OTLP_ENDPOINT` at any OTLP/HTTP collector (Tempo, Jaeger, OTel Collector) to see the full HTTP → Kafka → consumer waterfall.
