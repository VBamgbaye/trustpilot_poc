# Trustpilot Reviews PoC – Demo Preparation Notes

## 1. Project Overview & Problem Statement
- The PoC delivers an end-to-end pipeline that ingests Trustpilot review XLSX drops, enforces data quality rules, and loads curated tables into a governed SQLite warehouse before serving them through a CSV-first FastAPI layer.
- It targets data teams that need to operationalize review analytics quickly while ensuring PII handling and auditability, providing clear catalog and retention collateral alongside the code.
- Key capabilities showcased in the demo:
  - Governed ingestion pipeline with normalization and lineage tracking.
  - Secure streaming API exposing public vs. PII-filtered views with token/IP controls and audit middleware.
  - Developer ergonomics via Makefile targets for setup, ingestion, testing, serving, and future retention workflows.

## 2. Architecture Logic & Tooling Rationale
- **Python FastAPI stack:** FastAPI offers quick iteration for CSV streaming endpoints with async support, automatic OpenAPI docs, and compatibility with authentication middleware.
- **SQLite warehouse:** Lightweight yet SQL-compliant store suited to a PoC; WAL mode and enforced foreign keys provide transactional integrity without external dependencies.
- **Pandas/openpyxl ingestion:** These libraries simplify Excel normalization, schema enforcement, and Parquet exports, balancing speed of development with acceptable performance for medium batches.
- **Governance-first artifacts:** Shipping catalog, retention policies, and data dictionary alongside code keeps compliance in lockstep and demonstrates audit readiness from day one.
- **Testing strategy:** Pytest suites cover DQ rule regressions, ingestion idempotency, and API contracts, proving the architecture’s critical paths and enabling continuous verification.

## 3. Challenges Encountered
- Handling large XLSX files without chunking stresses memory and increases ingestion latency, requiring careful monitoring and future optimization.
- Balancing quick-win security (token/IP auth) with minimal infrastructure meant building custom middleware and audit trails instead of relying on managed gateways.
- Keeping governance artifacts synchronized with code demanded manual coordination because automated catalog drift detection is not yet implemented.

## 4. PoC Limitations & Improvement Roadmap
- **Scalability:** SQLite and in-memory ingestion will bottleneck under production volumes; migrating to Postgres or a managed warehouse and implementing chunked loads are key next steps.
- **Retention automation:** The Makefile references a retention workflow, but `app.retention` is unimplemented—building scheduled cleanup jobs is required before go-live.
- **Security hardening:** Need centralized token management, rate limiting, and possibly OAuth2 integration to strengthen access controls beyond the current environment-variable tokens.
- **Observability:** While audit logs are emitted, integration with a centralized logging/metrics platform is pending; establishing dashboards and alerts will improve operational readiness.

## 5. Anticipated Executive Q&A
The following questions (asked from a data leadership perspective) and prepared answers can anchor the discussion:
1. **Ingestion scalability & dependency resilience**  
   *Answer:* The CLI currently loads workbooks entirely into memory and falls back to CSV when Parquet writers fail, so we plan to add chunked ingestion, dependency health checks, and alerting similar to managed ETL practices.
2. **Remaining data-quality gaps & roadmap**  
   *Answer:* Validators cover IDs, rating range, select date formats, and email/IP regexes plus intra-file dedupe; roadmap items include broader locale handling, third-party validators, and cross-file dedupe using hashed keys.
3. **Monitoring the load audit trail**  
   *Answer:* Each run writes metrics to `load_audit` and surfaces them via `/healthz`; future work streams the audit log to observability tooling with rejection alerts and retention policies.
4. **Operationalizing ingestion & idempotency**  
   *Answer:* Idempotency relies on file-hash checks in `load_audit` (verified by tests); the next step is packaging the job for orchestration platforms like Airflow with retries/backoffs.
5. **Path off SQLite**  
   *Answer:* SQLite’s WAL mode helps but remains single-node; we will graduate to Postgres or a warehouse with SQLAlchemy to unlock concurrent writes and larger datasets.
6. **Retention roadmap**  
   *Answer:* Retention policies live in `config/retention.yml` yet lack an implementation; prioritizing the `app.retention` job (with tests and scheduling) closes that compliance gap.
7. **Token provisioning & audit trail**  
   *Answer:* Tokens reside in environment variables with optional IP allowlists; moving to managed secrets, short-lived credentials, and IAM-backed approvals ensures enterprise-grade access control.
8. **Rate-limiting & abuse controls**  
   *Answer:* The API streams CSVs without throttling; adopting FastAPI-compatible rate limiters, pagination, and signed URLs will mitigate abuse scenarios.
9. **Compliance alignment & review cadence**  
   *Answer:* Catalog and governed views align today, but we will institute release checklists, schema diff tooling, and compliance sign-offs to guard against drift.
10. **Observability & dashboards**  
    *Answer:* Structured audit logs include request IDs, actor metadata, and row counts; integrating with a SIEM/metrics stack enables dashboards and anomaly alerts.
11. **Pre-production checks**  
    *Answer:* Current automation covers linting and pytest suites; CI/CD expansion will add contract, load, and security tests plus governance drift validation before deployment.

## 6. Demo Flow Suggestions
- Begin with the governed data lifecycle: ingest (`make ingest`), audit trail inspection, and API retrieval, highlighting how DQ rules protect downstream consumers.
- Showcase security and governance artifacts, emphasizing token roles, masked views, and catalog alignment.
- Close with testing/operational readiness by running pytest and summarizing future roadmap investments for production.

## 7. Appendix: Key Commands for the Demo
- Environment setup: `make setup`  
- Run data ingestion: `make ingest`  
- Serve API locally: `make serve`  
- Execute tests: `make test`

Each command is documented in the Makefile for quick reference during the presentation.
