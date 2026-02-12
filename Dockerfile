# Build stage
FROM golang:1.24-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make

WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o godis cmd/godis/main.go

# Runtime stage
FROM alpine:latest

# Install ca-certificates for HTTPS
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/godis /app/godis

# Create data directory
RUN mkdir -p /data

# Expose Redis default port
EXPOSE 6379

# Set default command
CMD ["./godis", "-c", "/app/godis.conf"]

# Health check
HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD nc -z localhost 6379 || exit 1
