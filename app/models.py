from __future__ import annotations

import argparse
from datetime import datetime

from app.db import connect

DDL = r"""
-- ========== PRAGMAS ENFORCED IN CONNECTION (see db.py) ==========

-- ========== CORE TABLES ==========
CREATE TABLE IF NOT EXISTS business (
    business_id         TEXT PRIMARY KEY,
    business_name       TEXT NOT NULL,
    first_review_date   TEXT,
    last_review_date    TEXT,
    total_reviews       INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS "user" (
    user_id           TEXT PRIMARY KEY,
    email_address     TEXT,         -- PII (raw, internal only)  
    email_hash        TEXT,         -- masked (exposed externally, hashed)
    user_name         TEXT,         -- PII (raw, internal only)
    user_name_redacted  TEXT,       -- masked (exposed externally, hashed)
    reviewer_country  TEXT,
    first_review_date   TEXT,
    total_reviews       INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS review (
    review_id           TEXT PRIMARY KEY,
    user_id             TEXT NOT NULL,
    business_id         TEXT NOT NULL,
    review_date         TEXT NOT NULL,     -- ISO8601 UTC
    review_rating       INTEGER NOT NULL,
    review_title        TEXT,
    review_content      TEXT,
    review_ip_address   TEXT,
    review_ip_redacted  TEXT,              -- PII (sensitive)
    source_file         TEXT,              -- lineage
    source_row          INTEGER,           -- lineage
    FOREIGN KEY (business_id) REFERENCES business(business_id) ON DELETE CASCADE,
    FOREIGN KEY (user_id)     REFERENCES "user"(user_id)       ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS metrics_summary (
    business_id        TEXT NOT NULL,
    period_start_date  TEXT NOT NULL,
    total_reviews      INTEGER NOT NULL,
    avg_rating         REAL NOT NULL,
    rating_1           INTEGER NOT NULL,
    rating_2           INTEGER NOT NULL,
    rating_3           INTEGER NOT NULL,
    rating_4           INTEGER NOT NULL,
    rating_5           INTEGER NOT NULL,
    PRIMARY KEY (business_id, period_start_date),
    FOREIGN KEY (business_id) REFERENCES business(business_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS load_audit (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    file                TEXT NOT NULL,
    sha256              TEXT NOT NULL,
    rows_in             INTEGER NOT NULL DEFAULT 0,
    rows_loaded         INTEGER NOT NULL DEFAULT 0,
    rows_rejected       INTEGER NOT NULL DEFAULT 0,
    dq_pass             INTEGER NOT NULL DEFAULT 0,
    dq_fail             INTEGER NOT NULL DEFAULT 0,
    loaded_at           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
);

-- ========== INDEXES ==========
CREATE INDEX IF NOT EXISTS idx_review_business ON review(business_id);
CREATE INDEX IF NOT EXISTS idx_review_user     ON review(user_id);
CREATE INDEX IF NOT EXISTS idx_review_date     ON review(review_date);
CREATE INDEX IF NOT EXISTS idx_user_email_hash ON "user"(email_hash);
CREATE INDEX IF NOT EXISTS idx_metrics_summary_bday ON metrics_summary (business_id, period_start_date);

-- ========== PUBLIC/PRIVATE VIEWS  ==========
DROP VIEW IF EXISTS v_reviews_public;
CREATE VIEW v_reviews_public AS
SELECT
  r.review_id,
  r.business_id,
  r.user_id,
  r.review_date,
  r.review_rating,
  r.review_title,
  r.review_content
FROM review r;

DROP VIEW IF EXISTS v_reviews_private;
CREATE VIEW v_reviews_private AS
SELECT
  r.review_id,
  r.business_id,
  r.user_id,
  r.review_date,
  r.review_rating,
  r.review_title,
  r.review_content,
  u.email_hash AS email_address,
  u.user_name_redacted  AS user_name,
  r.review_ip_redacted AS review_ip_address
FROM review r
JOIN "user" u ON u.user_id = r.user_id;
"""

def init_db() -> None:
    with connect() as conn:
        conn.executescript(DDL)

def verify() -> dict:
    """Return a small health dict proving pragmas, tables, and views exist."""
    with connect() as conn:
        fk = conn.execute("PRAGMA foreign_keys;").fetchone()[0]
        journal_mode = conn.execute("PRAGMA journal_mode;").fetchone()[0]

        objs = conn.execute("""
            SELECT name, type FROM sqlite_master
            WHERE type IN ('table','view')
              AND name IN (
                'business','user','review','metrics_summary','load_audit',
                'v_reviews_public','v_reviews_private'
              )
            ORDER BY type,name;
        """).fetchall()

        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "foreign_keys_on": bool(fk),
            "journal_mode": journal_mode,
            "objects": [(r["name"], r["type"]) for r in objs],
        }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize or verify SQLite schema.")
    parser.add_argument("--init", action="store_true", help="Create tables, indexes, and views.")
    parser.add_argument("--verify", action="store_true", help="Print schema/pragma verification.")
    args = parser.parse_args()

    if args.init:
        init_db()
        print("Schema created/ensured.")
    if args.verify:
        print(verify())
