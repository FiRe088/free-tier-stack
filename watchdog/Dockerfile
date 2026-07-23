# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /build

# Copy go.mod/go.sum first so dependency downloads are cached separately
# from source changes — rebuilding after a code-only change won't
# re-download the module graph.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /watchdog ./cmd/watchdog

# Runtime stage — alpine, not scratch/distroless, because we need a
# shell and basic utilities for debugging inside the 128MB-capped
# container on EC2; the image size difference (a few MB) is not worth
# losing the ability to `docker exec` in and poke around.
FROM alpine:3.20

RUN apk add --no-cache ca-certificates

WORKDIR /app
COPY --from=builder /watchdog .

ENTRYPOINT ["/app/watchdog"]
