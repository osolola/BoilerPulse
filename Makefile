.PHONY: build test race lint run frontend cluster stop benchmark integration docker

GO ?= go
NODE_BIN := bin/node
GATEWAY_BIN := bin/gateway
INGEST_BIN := bin/ingest

## build: compile the node, gateway, and ingest binaries
build:
	$(GO) build -o $(NODE_BIN) ./cmd/node
	$(GO) build -o $(GATEWAY_BIN) ./cmd/gateway
	$(GO) build -o $(INGEST_BIN) ./cmd/ingest

## test: run all Go unit + integration tests
test:
	$(GO) test ./...

## race: run all Go tests with the race detector
race:
	$(GO) test -race ./...

## lint: gofmt + go vet
lint:
	@test -z "$$($(GO) fmt ./...)" || (echo "gofmt found unformatted files" && exit 1)
	$(GO) vet ./...

## run: run a single node locally (configs/node-1.yaml)
run: build
	./$(NODE_BIN)

## frontend: install deps and run the Next.js dev server
frontend:
	cd frontend && npm install && npm run dev

## integration: run only the tests/integration package
integration:
	$(GO) test ./tests/integration/...

## docker: build the kv-node and frontend images (no orchestration yet)
docker:
	docker compose build

## cluster: run a local 3-node Raft cluster, the gateway, and the event
## ingestion service in the background (configs/cluster/node-{1,2,3}.yaml,
## configs/gateway.yaml), logging to .cluster/*.log. Write/read through the
## gateway on :8090 -- it routes writes to the current leader and
## distributes reads for you. Ingest posts a synthetic campus event to it
## every 5s (see internal/events.SimulatorSource). Each node's admin/chaos
## server (internal/admin) comes up on :7080/:7081/:7082, gated by
## BOILERPULSE_ADMIN_TOKEN -- defaults to "dev-chaos-token" here so
## scripts/chaos.sh works out of the box; override it in your environment
## for anything beyond local dev. See docs/failure-testing.md.
BOILERPULSE_ADMIN_TOKEN ?= dev-chaos-token
cluster: build
	@mkdir -p .cluster
	@echo $(BOILERPULSE_ADMIN_TOKEN) > .cluster/admin.token
	@for n in 1 2 3; do \
		BOILERPULSE_CONFIG=configs/cluster/node-$$n.yaml BOILERPULSE_ADMIN_TOKEN=$(BOILERPULSE_ADMIN_TOKEN) ./$(NODE_BIN) > .cluster/node-$$n.log 2>&1 & echo $$! > .cluster/node-$$n.pid; \
	done
	@sleep 1
	@BOILERPULSE_ADMIN_TOKEN=$(BOILERPULSE_ADMIN_TOKEN) ./$(GATEWAY_BIN) > .cluster/gateway.log 2>&1 & echo $$! > .cluster/gateway.pid
	@sleep 1
	@./$(INGEST_BIN) > .cluster/ingest.log 2>&1 & echo $$! > .cluster/ingest.pid
	@echo "cluster started: node-1 :8080, node-2 :8081, node-3 :8082, gateway :8090, ingest (logs in .cluster/*.log)"
	@echo "admin/chaos: node-1 :7080, node-2 :7081, node-3 :7082, token in .cluster/admin.token (run scripts/chaos.sh)"
	@echo "run 'make stop' to shut it down"

## stop: stop the cluster started by `make cluster`
stop:
	@for f in .cluster/*.pid; do \
		[ -f "$$f" ] && kill $$(cat "$$f") 2>/dev/null; rm -f "$$f"; \
	done
	@echo "cluster stopped"

## benchmark: not yet implemented -- arrives in Milestone 10, once there's a
## cluster worth benchmarking. See benchmarks/README.md.
benchmark:
	@echo "make benchmark: not implemented yet -- see benchmarks/README.md"
	@exit 1
