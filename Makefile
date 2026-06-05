# GPS Pipeline — command shortcuts
# Usage: make <target>
# All docker commands use `sg docker` to handle group membership without logout.

PYTHON   = .venv/bin/python3
PIP      = .venv/bin/pip
PYTEST   = .venv/bin/pytest
STREAMLIT = .venv/bin/streamlit
ENV      = env $$(cat .env | grep -v '^\#' | xargs)
PYPATH   = PYTHONPATH=src:src/lambdas

.PHONY: up down status logs setup pipeline dashboard test test-unit clean help

## ── Infrastructure ──────────────────────────────────────────────────────────

up:          ## Start LocalStack (fake AWS)
	sg docker -c "docker compose up -d --remove-orphans"
	@echo "Waiting for LocalStack..."
	@sleep 5
	@sg docker -c "docker inspect --format='LocalStack: {{.State.Health.Status}}' gps-localstack"

down:        ## Stop LocalStack
	sg docker -c "docker compose down --remove-orphans"

bootstrap:   ## Create all AWS resources in LocalStack (run once after `make up`)
	AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
	AWS_DEFAULT_REGION=us-east-1 \
	AWS_ENDPOINT_URL=http://localhost:4566 \
	bash infra/scripts/bootstrap.sh

status:      ## Show pipeline status (S3, DynamoDB, SQS, Dashboard)
	$(ENV) $(PYPATH) bash scripts/status.sh

logs:        ## Show LocalStack logs
	sg docker -c "docker logs gps-localstack --tail 50"

## ── Setup ───────────────────────────────────────────────────────────────────

setup:       ## Create .venv and install dependencies
	python3 -m venv .venv
	$(PIP) install -q -r requirements-dev.txt
	cp -n .env.example .env || true
	@echo "Setup complete. Run: make up"

## ── Pipeline ────────────────────────────────────────────────────────────────

pipeline:    ## Run full pipeline (generate GPS events + batch CSV + quality + signal loss)
	$(ENV) $(PYPATH) $(PYTHON) scripts/run_pipeline.py --all

pipeline-gps: ## Run only GPS event generation and validation
	$(ENV) $(PYPATH) $(PYTHON) scripts/run_pipeline.py --generate

pipeline-batch: ## Run only CSV maintenance ingestion
	$(ENV) $(PYPATH) $(PYTHON) scripts/run_pipeline.py --batch

pipeline-quality: ## Run only quality checker
	$(ENV) $(PYPATH) $(PYTHON) scripts/run_pipeline.py --quality

## ── Dashboard ───────────────────────────────────────────────────────────────

dashboard:   ## Open Streamlit dashboard at http://localhost:8501
	$(ENV) $(PYPATH) $(STREAMLIT) run src/dashboard/app.py \
		--server.address=0.0.0.0 --server.port=8501

## ── Tests ───────────────────────────────────────────────────────────────────

test:        ## Run all tests (needs LocalStack running: make up)
	$(PYPATH) $(PYTEST) tests/ -v

test-unit:   ## Run only unit tests (no LocalStack needed)
	$(PYPATH) $(PYTEST) tests/ -v -k "not Integration"

## ── Utilities ───────────────────────────────────────────────────────────────

clean:       ## Remove Python cache files
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true

help:        ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*##"}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
