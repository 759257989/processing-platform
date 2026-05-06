# Processing Platform — Makefile
# Run `make help` to see all targets.

# ---- variables ----
BINARIES := api ingestion worker-realtime worker-standard worker-bulk \
            retry-router reaper mock-device mock-webhook devsim chaosmonkey

GO            := go
GOFLAGS       := -trimpath
LDFLAGS       := -s -w
GOLANGCI_LINT := golangci-lint

DOCKER_REGISTRY ?= ghcr.io/yourname
DOCKER_TAG      ?= dev

BIN_DIR := bin

# ---- meta ----
.DEFAULT_GOAL := help
.PHONY: help build test test-integration lint fmt clean docker-build \
        up down seed submit-job get-job load-test dlq

help: ## Show this help.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ---- build ----

build: ## Build all binaries into ./bin/.
	@mkdir -p $(BIN_DIR)
	@for b in $(BINARIES); do \
		echo "  go build $$b"; \
		$(GO) build $(GOFLAGS) -ldflags='$(LDFLAGS) -X main.binaryName='$$b \
			-o $(BIN_DIR)/$$b ./cmd/$$b || exit 1; \
	done

# ---- test / lint ----

test: ## Run unit tests (no external deps).
	$(GO) test -race -count=1 -short ./...

test-integration: ## Run integration tests (testcontainers; requires Docker).
	$(GO) test -race -count=1 -tags=integration ./test/integration/...

lint: ## Run golangci-lint.
	$(GOLANGCI_LINT) run ./...

fmt: ## Format Go code.
	$(GO) fmt ./...

# ---- docker ----

docker-build: ## Build a Docker image for a single binary. Use BINARY=<name>.
	@if [ -z "$(BINARY)" ]; then \
		echo "Usage: make docker-build BINARY=api"; exit 1; \
	fi
	docker build \
		--build-arg BINARY=$(BINARY) \
		-t $(DOCKER_REGISTRY)/processing-platform-$(BINARY):$(DOCKER_TAG) \
		.

docker-build-all: ## Build Docker images for ALL binaries.
	@for b in $(BINARIES); do \
		echo "==> docker build $$b"; \
		$(MAKE) docker-build BINARY=$$b || exit 1; \
	done

# ---- not yet implemented ----
# These are placeholders; they print a friendly message and exit successfully
# so a recruiter typing `make up` doesn't see a confusing error.

up: ## (Stage 1) Bring up the local kind cluster + dependencies.
	@echo "Not yet implemented — see stages.md, Stage 1."

down: ## (Stage 1) Tear down the local cluster.
	@echo "Not yet implemented — see stages.md, Stage 1."

seed: ## (Stage 2) Seed sample devices into Postgres.
	@echo "Not yet implemented — see stages.md, Stage 2."

submit-job: ## (Stage 2) Submit a sample job for testing.
	@echo "Not yet implemented — see stages.md, Stage 2."

get-job: ## (Stage 2) Fetch a job by ID. Use ID=<uuid>.
	@echo "Not yet implemented — see stages.md, Stage 2."

load-test: ## (Stage 5) Run a 5-minute local load test.
	@echo "Not yet implemented — see stages.md, Stage 5."

dlq: ## (Stage 3) Print contents of the DLQ topic.
	@echo "Not yet implemented — see stages.md, Stage 3."

# ---- housekeeping ----

clean: ## Remove build artifacts.
	rm -rf $(BIN_DIR)
