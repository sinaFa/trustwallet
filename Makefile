.DEFAULT_GOAL := help

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
BIN     := bin/etl

# Host ports for docker compose. Override when 5432 or 8080 is taken.
POSTGRES_PORT ?= 5432
ETL_PORT      ?= 8080
export POSTGRES_PORT ETL_PORT

.PHONY: help build run once export test test-integration lint fmt vet check up down logs ps clean

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'

build: ## Compile the service to bin/etl
	CGO_ENABLED=0 go build -ldflags '$(LDFLAGS)' -o $(BIN) ./cmd/etl

run: ## Run the service locally against the compose Postgres
	DATABASE_URL=postgres://etl:etl@localhost:$(POSTGRES_PORT)/etl?sslmode=disable go run -ldflags '$(LDFLAGS)' ./cmd/etl

once: ## Run a single fetch-transform-load-export cycle and exit
	DATABASE_URL=postgres://etl:etl@localhost:$(POSTGRES_PORT)/etl?sslmode=disable go run -ldflags '$(LDFLAGS)' ./cmd/etl -once

export: ## Export unexported rows to data/ without polling
	DATABASE_URL=postgres://etl:etl@localhost:$(POSTGRES_PORT)/etl?sslmode=disable go run -ldflags '$(LDFLAGS)' ./cmd/etl -export-only

test: ## Unit tests with race detector
	go test -race -count=1 ./...

test-integration: ## Integration tests against the compose Postgres (or TEST_DATABASE_URL)
	TEST_DATABASE_URL=$${TEST_DATABASE_URL:-postgres://etl:etl@localhost:$(POSTGRES_PORT)/etl?sslmode=disable} \
		go test -race -count=1 -tags=integration ./internal/store/...

vet: ## go vet
	go vet ./...

fmt: ## gofmt in place
	gofmt -l -w .

lint: vet ## gofmt check plus vet, and golangci-lint when installed (warns loudly when it is not)
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed on:"; gofmt -l .; exit 1)
	@if command -v golangci-lint >/dev/null; then golangci-lint run ./...; \
	else echo "WARNING: golangci-lint not installed, only gofmt and vet ran. Install: https://golangci-lint.run/welcome/install/" >&2; fi

check: lint test ## Everything CI runs without external services

up: ## Build the image and start Postgres plus the service
	docker compose up --build -d

down: ## Stop and remove containers (the Postgres volume is kept)
	docker compose down

logs: ## Follow service logs
	docker compose logs -f etl

ps: ## Container status
	docker compose ps

clean: ## Remove build artefacts (data/ and logs/ are kept on purpose)
	rm -rf bin
