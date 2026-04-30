# syntax=docker/dockerfile:1.7

# ---------- web (frontend) ----------
FROM --platform=$BUILDPLATFORM node:22-alpine AS web
WORKDIR /web

# Cache npm install separately so Go-only changes don't invalidate it.
COPY web/package.json web/package-lock.json* ./
RUN --mount=type=cache,target=/root/.npm \
    npm ci --no-audit --no-fund

COPY web/ ./
RUN npm run build
# /web/dist now contains the built SPA.

# ---------- builder (Go) ----------
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

# Drop the built SPA into the embed directory so the Go binary serves it.
RUN rm -rf internal/web/dist
COPY --from=web /web/dist internal/web/dist

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
