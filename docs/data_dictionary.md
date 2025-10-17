# Data Dictionary

## Raw Reviews (`reviews_raw`)

Source data from Trustpilot xlsx files.

| Field Name | Data Type | Description | Constraints | PII |
|------------|-----------|-------------|-------------|-----|
| Review Id | String | Unique identifier for each review | NOT NULL, UNIQUE | No |
| Reviewer Name | String | Name of the person leaving the review | NOT NULL | **Yes** |
| Review Title | String | Short headline/summary of the review | Nullable | No |
| Review Rating | Integer | Rating given (1-5 stars) | 1-5, NOT NULL | No |
| Review Content | Text | Full text content of the review | Nullable | No |
| Review IP Address | String | IP address of reviewer | NOT NULL | **Yes** |
| Business Id | String | Unique identifier for the business being reviewed | NOT NULL | No |
| Business Name | String | Name of the business | NOT NULL | No |
| Reviewer Id | String | Unique identifier for the reviewer | NOT NULL | **Yes** |
| Email Address | String | Email address of reviewer | Valid email format | **Yes** |
| Reviewer Country | String | Country of the reviewer | ISO country code preferred | No |
| Review Date | String | Date when review was submitted | Parsable date, NOT NULL | No |

### PII Fields (GDPR Compliance)

The following fields contain Personally Identifiable Information:
- **Reviewer_Name**: Full name of reviewer
- **Email_Address**: Email contact information
- **Review_IP_Address**: Network identifier
- **Reviewer_Id**: Unique identifier that could trace back to individual

Controls applied to these fields:
- default masking on export
- audit logging for access
- subject-access/deletion supported


---

## Stage Reviews (`stage_reviews`)

Validated and cleaned data ready for database loading.

- `Location:` data/stage/reviews.parquet
- `Format` Parquet

## Transformations
- `Field name standardization:` Convert headers from Title Case to snake_case (e.g., “Review Id” → review_id).
- Parse Review Date → ISO 8601 UTC (YYYY-MM-DD or full timestamp if available).
- Validate Review Rating [1-5].
- Trim/normalize all text fields.
- Validate Email Address format.
- Detect/handle duplicate Review Id (retain first or latest per policy).
- Validate IP address format.

## Field Name Mapping (Raw → Stage)
| Raw Field Name    | Stage Field Name  |
| ----------------- | ----------------- |
| Review Id         | review_id         |
| Reviewer Name     | reviewer_name     |
| Review Title      | review_title      |
| Review Rating     | review_rating     |
| Review Content    | review_content    |
| Review IP Address | review_ip_address |
| Business Id       | business_id       |
| Business Name     | business_name     |
| Reviewer Id       | reviewer_id       |
| Email Address     | email_address     |
| Reviewer Country  | reviewer_country  |
| Review Date       | review_date       |
---

## Database Schema (SQLite)
`File:` data/trustpilot_poc.db, 
`Pragmas`: journal_mode=WAL, foreign_keys=ON

### Business - Dimension

| Column            | Type | Description                        | Constraints        |
| ----------------- | ---- | ---------------------------------- | ------------------ |
| business_id       | TEXT | Business identifier                | **PRIMARY KEY**    |
| business_name     | TEXT | Name of business                   | NOT NULL           |
| first_review_date | TEXT | First known review date (ISO 8601) |                    |
| last_review_date  | TEXT | Most recent review date (ISO 8601) |                    |
| total_reviews     | INT  | Total number of reviews            | NOT NULL DEFAULT 0 |


### User -  Dimension (`PII`)

| Column            | Type | Description                  | Constraints          | PII     |
| ----------------- | ---- | ---------------------------- | -------------------- | ------- |
| user_id           | TEXT | Reviewer identifier          | **PRIMARY KEY**      | **Yes** |
| email_address     | TEXT | Email                        | UNIQUE (best-effort) | **Yes** |
| user_name         | TEXT | Name                         |                      | **Yes** |
| reviewer_country  | TEXT | Country (ISO preferred)      |                      | No      |
| first_review_date | TEXT | First review date (ISO 8601) |                      | No      |
| total_reviews     | INT  | Total reviews by this user   | NOT NULL DEFAULT 0   | No      |

### Review - Fact

| Column            | Type | Description                   | Constraints                           | PII                 |
| ----------------- | ---- | ----------------------------- | ------------------------------------- | ------------------- |
| review_id         | TEXT | Review identifier             | **PRIMARY KEY**                       | No                  |
| user_id           | TEXT | FK → `user.user_id`           | **FOREIGN KEY** (`ON DELETE CASCADE`) | **(links to PII)**  |
| business_id       | TEXT | FK → `business.business_id`   | **FOREIGN KEY** (`ON DELETE CASCADE`) | No                  |
| review_date       | TEXT | Date/time (ISO 8601 UTC)      | NOT NULL                              | No                  |
| review_rating     | INT  | Rating (1–5)                  | CHECK 1–5 (enforced in DQ)            | No                  |
| review_title      | TEXT | Headline                      |                                       | No                  |
| review_content    | TEXT | Full text                     |                                       | No                  |
| review_ip_address | TEXT | IP address                    | Masked on public export               | **Yes (sensitive)** |
| source_file       | TEXT | Ingestion lineage: file name  |                                       | No                  |
| source_row        | INT  | Ingestion lineage: row number |                                       | No                  |

`Indexes:` review(business_id), review(user_id), review(review_date)

### Metrics Summary - Aggregate

| Column              | Type | Description                                  | Constraints                         |
| ------------------- | ---- | -------------------------------------------- | ----------------------------------- |
| business_id         | TEXT | Business identifier                          | **PRIMARY KEY** (with period_start) |
| period_start_date   | TEXT | Start of aggregation window (ISO 8601)       | **PRIMARY KEY** (composite)         |
| total_reviews       | INT  | Number of reviews in period                  | NOT NULL                            |
| avg_rating          | REAL | Average rating                               | NOT NULL                            |
| rating_distribution | TEXT | JSON/text map of rating→count (e.g. {"1":3}) |                                     |

---

### API Views (Consumption Layer)

| View Name           | Includes PII | Columns (summary)                                                                         | Intended Consumers                    |
| ------------------- | ------------ | ----------------------------------------------------------------------------------------- | ------------------------------------- |
| `v_reviews_public`  | No           | review_id, business_id, user_id, review_date, review_rating, review_title, review_content | Public CSV exports (default)          |
| `v_reviews_private` | Yes          | public columns + email_address, user_name, review_ip_address                              | Privileged CSV exports (`pii_reader`) |

`Export format:` CSV (UTF-8).
`Default masking:` PII excluded unless pii=true with a privileged token.

## Data Quality Rules

### Validation (ingestion):

1. **Review Id**: Unique across all reviews (or deterministic dedupe rule defined).
2. **Review Rating**: Must be integer 1-5
3. **Review Date**: Valid/parseable; not in the future (configurable).
4. **Email Address**: Valid format.
5. **Business Id/Reviewer Id**: NOT NULL
6. **IP address**: Valid IPv4/IPv6.

### Nullability Rules

- **Required:** Review Id, Reviewer Id, Business Id, Review Rating, Review Date
- **Optional:** Review Title, Review Content

**Freshness:** Configurable check on review_date (e.g., last N days for current feeds).
---

## Data Lineage

```
Raw XLSX Files (data/trustpilot_raw/)
    ↓  (standardize headers, type/format checks)
Stage Parquet (data/stage/reviews.parquet)
    ↓  (DQ validation, dedupe, normalization)
SQLite Database (data/trustpilot_poc.db)
    ├→ business
    ├→ user
    ├→ review
    └→ metrics_summary
        └─ API Views → v_reviews_public / v_reviews_private (CSV exports)
```

---

## Retention Policy9 (summary)

- **Raw files**: 90 days in data/trustpilot_raw/ (delete after expiry).
- **Stage files**: 180 days in data/stage/ (delete after expiry).
- **Database**: Indefinite for PoC; PII deletions honored on request.
- **Backups**: Keep last 10 under data/backups/.
- **Audit**: Request/response logging enabled with X-Request-ID.

## Notes & Assumptions

- Timestamps stored as `ISO 8601 UTC` strings for simplicity in SQLite.

- PII exposure via API requires `privileged token` and pii=true.

- Masking policies mirror the catalog: name (redact), email (hash), IP (redact), reviewer_id (hash) on public exports.