# syntax=docker/dockerfile:1.7

# ---------- builder ----------
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS builder
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=unknown

WORKDIR /src

# Cache dependencies first.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Copy the rest of the source.
COPY . .

# Build both binaries into /out so the final stages can copy by name.
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath \
      -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
      -o /out/server ./cmd/server && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath \
      -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
      -o /out/consumer ./cmd/consumer

# ---------- server ----------
FROM gcr.io/distroless/static-debian12:nonroot AS server
WORKDIR /
COPY --from=builder /out/server /server
EXPOSE 8080 9090 9100
USER nonroot:nonroot
ENTRYPOINT ["/server"]

# ---------- consumer ----------
FROM gcr.io/distroless/static-debian12:nonroot AS consumer
WORKDIR /
COPY --from=builder /out/consumer /consumer
EXPOSE 9100
USER nonroot:nonroot
ENTRYPOINT ["/consumer"]
