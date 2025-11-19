import importlib
import sys

import pytest

pytest.importorskip("httpx", reason="FastAPI test client requires httpx")

from fastapi.testclient import TestClient


def reload_modules(names):
    for name in names:
        if name in sys.modules:
            del sys.modules[name]
    return [importlib.import_module(name) for name in names]


def test_reviews_by_business_streams_csv(tmp_path, monkeypatch):
    db_path = tmp_path / "api.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("API_TOKENS_DEFAULT", "default-token")
    monkeypatch.setenv("API_TOKENS_PII", "pii-token")
    monkeypatch.setenv("API_IP_ALLOWLIST", "")

    reload_modules(["app.db", "app.models", "app.security", "app.api"])

    from app import models
    from app.db import connect
    from app.api import app

    models.init_db()

    with connect() as conn:
        conn.execute(
            "INSERT INTO business (business_id, business_name, first_review_date, last_review_date, total_reviews)"
            " VALUES (?, ?, ?, ?, ?)",
            ("b-1", "Widgets Inc", "2024-01-01T12:00:00Z", "2024-01-01T12:00:00Z", 1),
        )
        conn.execute(
            "INSERT INTO \"user\" (user_id, email_address, email_hash, user_name, user_name_redacted, reviewer_country, first_review_date, total_reviews)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("u-1", "user@example.com", "hash", "Alice", "A***", "GB", "2024-01-01T12:00:00Z", 1),
        )
        conn.execute(
            "INSERT INTO review (review_id, user_id, business_id, review_date, review_rating, review_title, review_content, review_ip_address, review_ip_redacted, source_file, source_row)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("r-1", "u-1", "b-1", "2024-01-01T12:00:00Z", 5, "Great", "Loved it", "1.2.3.4", "1.2.3.0", "file", 2),
        )

    client = TestClient(app)

    resp = client.get(
        "/reviews/by-business",
        params={"business_id": "b-1", "pii": "true"},
        headers={"Authorization": "Bearer default-token"},
    )

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/csv")
    assert resp.headers["X-Rows-Returned"] == "1"
    assert "X-Request-ID" in resp.headers

    lines = resp.text.strip().splitlines()
    assert lines[0] == "review_id,business_id,user_id,review_date,review_rating,review_title,review_content"
    assert lines[1].startswith("r-1,b-1,u-1,2024-01-01T12:00:00Z,5,Great,Loved it")
