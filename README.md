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

Plus `GET /healthz` for liveness.

## Configuration (env)

| Variable        | Default              | Description                                                                       |
| --------------- | -------------------- | --------------------------------------------------------------------------------- |
| `HTTP_ADDR`     | `:8080`              | HTTP listen address                                                               |
| `GRPC_ADDR`     | `:9090`              | gRPC listen address                                                               |
| `KAFKA_BROKERS` | `localhost:9092`     | Comma-separated Kafka bootstrap                                                   |
| `KAFKA_TOPIC`   | `wildlife.sightings` | Topic for all events                                                              |
| `DATABASE_URL`  | _(unset)_            | Postgres DSN. If empty, the service falls back to an in-memory store (no persistence). |
| `AUTH_JWT_SECRET` | _(unset)_          | HS256 shared secret used to validate `Authorization: Bearer <jwt>`. |
| `AUTH_API_KEYS` | _(unset)_            | Comma-separated list of static API keys accepted as `Authorization: Bearer <key>`. |

## Running locally

Start Kafka and Postgres:

```bash
docker compose up -d
```

Run the server with Postgres:

```bash
DATABASE_URL=postgres://wildlife:wildlife@localhost:5432/wildlife?sslmode=disable \
  go run ./cmd/server
```

Migrations run automatically at startup (goose, embedded SQL).

Without `DATABASE_URL` the server starts with an in-memory store — handy for quick demos, but state is lost on restart.

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
  "payload": { "... full sighting ..." }
}
```

Consume messages for debugging:

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic wildlife.sightings \
  --from-beginning
```
