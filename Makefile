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

.PHONY: up
up: ## bring up the local kind cluster + all infrastructure
	./scripts/bootstrap-local.sh
	./scripts/install-infra.sh   # we'll create this in Step 12

.PHONY: down
down: ## tear down the local kind cluster (destroys all data)
	kind delete cluster --name=processing-platform

load-test: ## (Stage 5) Run a 5-minute local load test.
	@echo "Not yet implemented — see stages.md, Stage 5."

dlq: ## (Stage 3) Print contents of the DLQ topic.
	@echo "Not yet implemented — see stages.md, Stage 3."

# ---- housekeeping ----

clean: ## Remove build artifacts.
	rm -rf $(BIN_DIR)

.PHONY: status
status: ## show cluster nodes and all pods
	@kubectl get nodes
	@echo
	@kubectl get pods -A


.PHONY: sync-migrations
sync-migrations: ## copy migrations/*.sql into the Helm chart files dir
	rm -f deploy/helm/processing-platform/files/migrations/*.sql
	cp migrations/*.sql deploy/helm/processing-platform/files/migrations/

.PHONY: psql
psql: ## open a psql shell against the cluster Postgres
	kubectl run psql --rm -it --restart=Never \
		--image=postgres:16-alpine \
		--env=PGPASSWORD=localdev \
		-- psql -h pp-postgresql -U postgres -d platform


.PHONY: logs-postgres
logs-postgres: ## tail Postgres logs
	kubectl logs -f pp-postgresql-0

.PHONY: logs-kafka
logs-kafka: ## tail Kafka logs (broker 0)
	kubectl logs -f pp-kafka-controller-0

.PHONY: logs-mosquitto
logs-mosquitto: ## tail Mosquitto logs
	kubectl logs -f -l app=mosquitto


.PHONY: docker-build-api
docker-build-api: ## build the api docker image
	docker build --build-arg BINARY=api -t processing-platform/api:dev .

.PHONY: docker-build-worker-standard
docker-build-worker-standard: ## build the worker-standard docker image
	docker build --build-arg BINARY=worker-standard -t processing-platform/worker-standard:dev .

.PHONY: kind-load
kind-load: docker-build-api docker-build-workers ## build all images and load into kind
	kind load docker-image processing-platform/api:dev              --name=processing-platform
	kind load docker-image processing-platform/worker-realtime:dev  --name=processing-platform
	kind load docker-image processing-platform/worker-standard:dev  --name=processing-platform
	kind load docker-image processing-platform/worker-bulk:dev      --name=processing-platform


.PHONY: seed
seed: ## insert a sample device into postgres
	@kubectl run psql-seed --rm -it --restart=Never \
		--image=postgres:16-alpine \
		--env=PGPASSWORD=localdev \
		-- psql -h pp-postgresql -U postgres -d platform \
		-c "INSERT INTO devices (id) VALUES ('device-001') ON CONFLICT DO NOTHING; SELECT * FROM devices;"

.PHONY: port-forward-api
port-forward-api: ## forward localhost:8080 → api service
	kubectl port-forward svc/api 8080:8080

.PHONY: submit-job
submit-job: ## submit a sample telemetry job (requires `make port-forward-api` running)
	@curl -s -X POST http://localhost:8080/jobs \
		-H "Content-Type: application/json" \
		-d '{"type":"TELEMETRY_PROCESSING","device_id":"device-001","idempotency_key":"submit-job-$(shell date +%s)","payload":{"value":42}}' | jq .

.PHONY: get-job
get-job: ## fetch a job by id; usage: make get-job ID=<uuid>
	@curl -s http://localhost:8080/jobs/$(ID) | jq .


.PHONY: docker-build-workers
docker-build-workers: ## build all three worker images
	docker build --build-arg BINARY=worker-realtime -t processing-platform/worker-realtime:dev .
	docker build --build-arg BINARY=worker-standard -t processing-platform/worker-standard:dev .
	docker build --build-arg BINARY=worker-bulk     -t processing-platform/worker-bulk:dev .

.PHONY: kind-load-workers
kind-load-workers: docker-build-workers
	kind load docker-image processing-platform/worker-realtime:dev --name=processing-platform
	kind load docker-image processing-platform/worker-standard:dev --name=processing-platform
	kind load docker-image processing-platform/worker-bulk:dev     --name=processing-platform

.PHONY: docker-build-retry-router
docker-build-retry-router:
	docker build --build-arg BINARY=retry-router -t processing-platform/retry-router:dev .

.PHONY: docker-build-reaper
docker-build-reaper:
	docker build --build-arg BINARY=reaper -t processing-platform/reaper:dev .

.PHONY: docker-build-ingestion
docker-build-ingestion:
	docker build --build-arg BINARY=ingestion -t processing-platform/ingestion:dev .

.PHONY: docker-build-mock-device
docker-build-mock-device:
	docker build --build-arg BINARY=mock-device -t processing-platform/mock-device:dev .

.PHONY: docker-build-mock-webhook
docker-build-mock-webhook:
	docker build --build-arg BINARY=mock-webhook -t processing-platform/mock-webhook:dev .