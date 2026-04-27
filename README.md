# go-kafka-proj

Wildlife Sightings Log — a small Go service that lets observers record wildlife sightings over HTTP and streams each event to Kafka for downstream processing (analytics, notifications, etc.).

## Stack

- **Go** 1.23
- **Router:** [go-chi/chi](https://github.com/go-chi/chi) on top of the standard `net/http`
- **Kafka client:** [segmentio/kafka-go](https://github.com/segmentio/kafka-go)
- **Storage:** in-memory (for demo purposes)

## Project layout

```
.
├── cmd/
│   └── server/
│       └── main.go            # entrypoint: HTTP server + graceful shutdown
├── internal/
│   ├── handlers/
│   │   ├── sightings.go       # HTTP handlers (GET/POST/PUT)
│   │   └── sightings_test.go  # HTTP tests with httptest + mock Publisher
│   ├── kafka/
│   │   └── producer.go        # Kafka producer wrapper
│   ├── models/
│   │   └── sighting.go        # domain types
│   └── store/
│       ├── memory.go          # in-memory repository
│       └── memory_test.go     # unit tests
├── docker-compose.yml         # Zookeeper + Kafka for local development
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

| Variable        | Default              | Description                        |
| --------------- | -------------------- | ---------------------------------- |
| `HTTP_ADDR`     | `:8080`              | HTTP listen address                |
| `KAFKA_BROKERS` | `localhost:9092`     | Comma-separated Kafka bootstrap    |
| `KAFKA_TOPIC`   | `wildlife.sightings` | Topic for all events               |

## Running locally

Start Kafka:

```bash
docker compose up -d
```

Run the server:

```bash
go run ./cmd/server
```

## Tests

```bash
go test ./...
```

Handlers are tested with `net/http/httptest` against a mock `Publisher`, so the test suite does **not** require a running Kafka broker. The store is covered with unit tests including a concurrent-writes check.

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
