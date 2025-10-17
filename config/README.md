DPIA-lite (PoC)

Purpose & lawful basis: Respond to ad-hoc legal requests for reviews/users/business data for compliance and dispute resolution (Legitimate Interests / Legal Obligation).

Data categories: Business identifiers; review content and ratings; user identifiers and PII (name, email), IP addresses (sensitive).

Processing: Batch ingest from XLSX → SQLite; DQ checks; CSV exports via API.

Data minimisation: API defaults to v_reviews_public (no PII). PII requires privileged token and pii=true.

Storage & retention: Raw XLSXs 90 days; staged 180 days; database backups keep last 10. See config/retention.yml.

Security controls: Bearer tokens, optional IP allowlist, request audit logs with X-Request-ID.

Data subject rights: /users/{user_id} endpoint enables access/export on demand (CSV).

Transfers: None outside current environment (PoC).

Risks & mitigations: PII leakage → masked by default; IP treated as sensitive; full audit trail; limited surface area (CSV only).