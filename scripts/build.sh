#!/bin/bash
# Build script for Godis

set -e

VERSION=${VERSION:-"1.0.0"}
BUILD_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
GIT_COMMIT=${GIT_COMMIT:-$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")}

# Build flags
LDFLAGS="-X main.Version=${VERSION} \
         -X main.BuildTime=${BUILD_TIME} \
         -X main.GitCommit=${GIT_COMMIT} \
         -s -w"

echo "Building Godis v${VERSION}..."
echo "Git commit: ${GIT_COMMIT}"
echo "Build time: ${BUILD_TIME}"

# Create output directory
mkdir -p bin

# Build for current platform
echo "Building for $(go env GOOS)/$(go env GOARCH)..."
go build -ldflags "${LDFLAGS}" -o bin/godis cmd/godis/main.go

# Build for multiple platforms if requested
if [ "$1" = "all" ]; then
    echo "Building for multiple platforms..."

    # Linux amd64
    GOOS=linux GOARCH=amd64 go build -ldflags "${LDFLAGS}" -o bin/godis-linux-amd64 cmd/godis/main.go

    # Linux arm64
    GOOS=linux GOARCH=arm64 go build -ldflags "${LDFLAGS}" -o bin/godis-linux-arm64 cmd/godis/main.go

    # Darwin amd64
    GOOS=darwin GOARCH=amd64 go build -ldflags "${LDFLAGS}" -o bin/godis-darwin-amd64 cmd/godis/main.go

    # Darwin arm64
    GOOS=darwin GOARCH=arm64 go build -ldflags "${LDFLAGS}" -o bin/godis-darwin-arm64 cmd/godis/main.go

    # Windows amd64
    GOOS=windows GOARCH=amd64 go build -ldflags "${LDFLAGS}" -o bin/godis-windows-amd64.exe cmd/godis/main.go
fi

echo "Build complete! Binaries are in bin/"
