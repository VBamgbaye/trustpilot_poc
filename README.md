# Trustpilot Reviews PoC

A proof-of-concept data pipeline and CSV-first API that ingests Trustpilot review exports, normalizes them into a governed SQLite warehouse, and exposes curated datasets over FastAPI with lightweight security and auditing.

## Key Capabilities
- **Governed ingestion pipeline**: Validates XLSX drops, normalizes them to Parquet, and upserts dimension/fact tables with lineage metadata and masked PII fallbacks.
- **CSV-first API**: Streams review data from materialized SQLite views with download-friendly headers, pagination-free CSV payloads, and audit headers.
- **Security & governance controls**: Token/IP-based auth, request auditing middleware, and documentation-driven catalog/retention policies keep the PoC aligned with compliance needs.
- **Operational tooling**: Make targets cover setup, ingestion, API serving, lint/test scaffolding, backups, and future retention jobs.

## Repository Layout
```
app/
  api.py           # FastAPI service and CSV streaming helpers
  audit.py         # Middleware emitting per-request audit metadata
  db.py            # SQLite connection helpers and schema bootstrap
  dq_rules.py      # Row/batch validation for ingestion
  ingest.py        # XLSX discovery, normalization, and upsert pipeline
  models.py        # Table/view DDL for the governed warehouse
  security.py      # Bearer token + allowlist enforcement utilities
config/
  catalog.yml      # Data catalog: assets, schemas, PII controls
  retention.yml    # Declarative retention policy (CLI stub)
docs/
  data_dictionary.md  # Detailed schema and lineage documentation
tests/
  conftest.py      # Adds repo root to sys.path for direct pytest execution
  test_api.py      # FastAPI contract coverage for CSV review streams
  test_dq_rules.py # Data-quality regression tests for validation helpers
  test_ingest.py   # Ingestion happy path, quarantine, and idempotency checks
makefile           # Developer workflows (setup, ingest, api, lint, test...)
pyproject.toml     # Poetry/PEP 621 metadata with optional dev deps
.env.template      # Environment defaults for DB paths, tokens, retention, logging
```

## Getting Started
1. **Install prerequisites**
   - Python 3.9+
   - `make` (optional but recommended)
   - System libs for `pyarrow`/`openpyxl` if you plan to ingest real XLSX files
2. **Clone and enter the repo**
   ```bash
   git clone https://github.com/VBamgbaye/trustpilot_poc
   cd trustpilot_poc
   ```
3. **Create a virtual environment and install deps**
   ```bash
   make setup  # creates .venv, installs editable package with dev extras
   source .venv/bin/activate
   ```
4. **Prepare configuration**
   - Copy `.env.template` to `.env` (done automatically by `make setup` if missing).
   - Adjust tokens (`API_TOKENS_DEFAULT`, `API_TOKENS_PII`), IP allowlists, and data directories as needed.
   - Place Trustpilot XLSX exports into `data/trustpilot_raw/` (default glob `*.xlsx`).

## Running the Pipeline
1. **Ingest data**
   ```bash
   make ingest
   ```
   This command:
   - Discovers XLSX files, standardizes headers, and parses date/rating fields.
   - Applies row-level validation (`app.dq_rules`) and quarantines bad records to `data/quarantine/`.
   - Normalizes data to Parquet (`data/stage/reviews.parquet`) with a CSV fallback if PyArrow is unavailable.
   - Upserts the SQLite warehouse tables (`business`, `user`, `review`) and rebuilds `metrics_summary` aggregates.
   - Records load metadata in `load_audit` for traceability.

2. **Serve the API**
   ```bash
   make api
   ```
   - Runs `uvicorn` with autoreload (configurable via `.env`).
   - Audit middleware (`app.audit.AuditMiddleware`) logs actor, endpoint, row counts, and duration to stdout or the configured log file.

3. **Call the endpoints**
   - Include `Authorization: Bearer <token>` headers using values from `.env`.
   - Available routes:

     | Method | Path | Description |
     | ------ | ---- | ----------- |
     | GET | `/healthz` | Returns status plus the latest ingestion audit |
     | GET | `/meta/catalog` | Streams the governed data catalog text |
     | GET | `/reviews/by-business?business_id=...` | CSV stream of reviews for a business |
     | GET | `/reviews/by-user?user_id=...` | CSV stream of reviews for a reviewer |
     | GET | `/users/{user_id}` | CSV stream of a single reviewer profile |
   - Append `pii=true` to review endpoints when using a token listed in `API_TOKENS_PII` to include masked PII fields from `v_reviews_private`.

## Security & Compliance
- **Authentication**: `app.security.require_token` validates bearer tokens supplied in `.env` (`API_TOKENS_DEFAULT` for non-PII, `API_TOKENS_PII` for privileged requests). Optional CIDR allow-listing is available via `API_IP_ALLOWLIST`.
- **PII controls**: Public exports leverage masked views; private exports expose hashed or redacted fields per `config/catalog.yml` and the [data dictionary](docs/data_dictionary.md).
- **Auditing**: All requests pass through `AuditMiddleware`, emitting structured logs with request metadata and row counts for downstream retention.
- **Retention**: `config/retention.yml` and environment variables define desired cleanup rules. The `make retention` target is scaffolded to call `app.retention` (implementation pending) so you can extend it for automated pruning.

## Governance Assets
- **Data catalog** (`config/catalog.yml`): Canonical description of datasets, transformations, masking policies, and pipeline entrypoints.
- **Data dictionary** (`docs/data_dictionary.md`): Field-level schema, PII annotations, and lineage diagrams.
- **Audit trail**: Ingestion populates `load_audit`; API logging provides request-level observability.

## Testing & Development Workflow
- **Run the automated checks**:
  ```bash
  make lint   # runs ruff check + format
  make test   # runs pytest with ingestion, DQ, and API suites
  ```
  The API test exercises FastAPI’s streaming endpoints via `httpx`; install `httpx` (already listed in the dev extras) to ensure
  it is collected instead of being skipped.
- **Backups**:
  ```bash
  make backup
  ```
  Copies the SQLite DB and key config files into timestamped artifacts under `data/backups/`.
- **Full demo**: Chain ingestion and API startup with `make demo` (see makefile for the composed recipe).
- **Cleanup**: `make clean` removes caches and build artifacts.

---
This README focuses on the PoC surface area. For deeper field-level definitions, consult [docs/data_dictionary.md](docs/data_dictionary.md) and the governed catalog at `GET /meta/catalog` once the API is running.
