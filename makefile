.PHONY: setup ingest api test lint retention backup clean help dev-setup dev-test demo

# Default target
help:
	@echo "Available targets:"
	@echo "  setup      - Create virtual environment and install dependencies"
	@echo "  ingest     - Run governed ingestion pipeline (XLSX -> SQLite)"
	@echo "  api        - Start FastAPI development server"
	@echo "  test       - Run pytest suite"
	@echo "  lint       - Run ruff linter and formatter"
	@echo "  retention  - Run data retention/cleanup (optional PoC step)"
	@echo "  backup     - Backup database and governance files"
	@echo "  clean      - Remove generated files and caches"
	@echo "  demo       - Run full PoC (ingest + api)"
	@echo "  dev-setup  - Shortcut for setup"
	@echo "  dev-test   - Run lint + tests"

# ------------------------------------------------------------
# Setup virtual environment and install dependencies
# ------------------------------------------------------------
setup:
	python -m venv .venv
	./venv/Scripts/pip install --upgrade pip
	./venv/Scripts/pip install -e ".[dev]"
	@if [ ! -f .env ]; then cp .env.template .env || touch .env; echo "Created .env"; fi
	@mkdir -p data/trustpilot_raw data/meta data/backups
	@echo "Setup complete! Activate with 'source venv/Scripts/activate'"

# ------------------------------------------------------------
# Run governed data ingestion
# ------------------------------------------------------------
ingest:
	@echo "Running governed ingestion..."
	python -m app.ingest --glob "data/trustpilot_raw/*.xlsx"

# ------------------------------------------------------------
# Start API server
# ------------------------------------------------------------
api:
	@echo "Starting API server..."
	uvicorn app.api:app --host $${API_HOST:-0.0.0.0} --port $${API_PORT:-8000} --reload

# ------------------------------------------------------------
# Tests and lint
# ------------------------------------------------------------
test:
	pytest -v --maxfail=1 --disable-warnings

lint:
	ruff check . --fix
	ruff format .

# ------------------------------------------------------------
# Retention policy
# ------------------------------------------------------------
retention:
	@echo "Running data retention cleanup..."
	python -m app.retention

# ------------------------------------------------------------
# Backup
# ------------------------------------------------------------
backup:
	@echo "Backing up database and configs..."
	@mkdir -p data/backups
	@TIMESTAMP=$$(date +%Y%m%d_%H%M%S); \
	if [ -f data/trustpilot_poc.db ]; then \
		cp data/trustpilot_poc.db data/backups/trustpilot_poc_$${TIMESTAMP}.db; \
		echo "Database backup created: data/backups/trustpilot_poc_$${TIMESTAMP}.db"; \
	else \
		echo "No database found. Run 'make ingest' first."; \
	fi; \
	for f in config/catalog.yml config/retention.yml .env; do \
		if [ -f $$f ]; then cp $$f data/backups/$${TIMESTAMP}_$$(basename $$f); fi; \
	done
	@echo "Backup complete."

# ------------------------------------------------------------
# Clean
# ------------------------------------------------------------
clean:
	rm -rf __pycache__ .pytest_cache .ruff_cache .coverage htmlcov
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	@echo "Cleaned generated files"
