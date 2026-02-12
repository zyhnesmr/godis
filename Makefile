# Godis Makefile

# Build variables
VERSION ?= 1.0.0
BUILD_TIME := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")

LDFLAGS := -X main.Version=$(VERSION) \
            -X main.BuildTime=$(BUILD_TIME) \
            -X main.GitCommit=$(GIT_COMMIT) \
            -s -w

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod

.PHONY: all build clean test coverage run deps fmt vet lint help

all: deps fmt vet build

## build: Build the godis binary
build:
	@echo "Building godis..."
	@mkdir -p bin
	$(GOBUILD) -ldflags "$(LDFLAGS)" -o bin/godis cmd/godis/main.go
	@echo "Build complete: bin/godis"

## build-all: Build godis for all platforms
build-all:
	@echo "Building for multiple platforms..."
	@mkdir -p bin
	GOOS=linux GOARCH=amd64 $(GOBUILD) -ldflags "$(LDFLAGS)" -o bin/godis-linux-amd64 cmd/godis/main.go
	GOOS=linux GOARCH=arm64 $(GOBUILD) -ldflags "$(LDFLAGS)" -o bin/godis-linux-arm64 cmd/godis/main.go
	GOOS=darwin GOARCH=amd64 $(GOBUILD) -ldflags "$(LDFLAGS)" -o bin/godis-darwin-amd64 cmd/godis/main.go
	GOOS=darwin GOARCH=arm64 $(GOBUILD) -ldflags "$(LDFLAGS)" -o bin/godis-darwin-arm64 cmd/godis/main.go
	GOOS=windows GOARCH=amd64 $(GOBUILD) -ldflags "$(LDFLAGS)" -o bin/godis-windows-amd64.exe cmd/godis/main.go
	@echo "Build complete!"

## clean: Remove build artifacts
clean:
	@echo "Cleaning..."
	@rm -rf bin/
	@rm -f godis
	@echo "Clean complete"

## test: Run tests
test:
	$(GOTEST) -v -race ./...

## coverage: Generate test coverage report
coverage:
	$(GOTEST) -v -race -coverprofile=coverage.out -covermode=atomic ./...
	$(GOCMD) tool coverage -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

## run: Run godis server
run: build
	./bin/godis -c config/godis.conf

## deps: Download dependencies
deps:
	$(GOCMD) mod download
	$(GOCMD) mod tidy

## fmt: Format code
fmt:
	$(GOCMD) fmt ./...

## vet: Run go vet
vet:
	$(GOCMD) vet ./...

## lint: Run golangci-lint
lint:
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run; \
	else \
		echo "golangci-lint not installed. Run: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"; \
	fi

## docker-build: Build Docker image
docker-build:
	docker build -t godis:$(VERSION) .

## docker-run: Run godis in Docker
docker-run:
	docker run -p 6379:6379 -v $(PWD)/data:/data godis:$(VERSION)

## benchmark: Run benchmarks
benchmark:
	$(GOTEST) -bench=. -benchmem ./...

## help: Show this help message
help:
	@echo "Godis Makefile"
	@echo ""
	@echo "Usage:"
	@echo "  make [target]"
	@echo ""
	@echo "Targets:"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' | sed -e 's/^/ /'
