import importlib
import os
import sys
import types


def reload_modules(names):
    for name in names:
        if name in sys.modules:
            del sys.modules[name]
    return [importlib.import_module(name) for name in names]


def test_process_file_handles_duplicates_and_audits(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("DB_PATH", str(db_path))

    # Provide a lightweight stub if optional dependencies are missing
    if "pyarrow" not in sys.modules:
        sys.modules["pyarrow"] = types.ModuleType("pyarrow")

    modules = reload_modules(["app.db", "app.models", "app.ingest", "app.dq_rules"])
    db, models, ingest, dq = modules

    models.init_db()

    sample_file = tmp_path / "sample.xlsx"
    sample_file.write_text("placeholder")

    header = [
        "Review Id",
        "Reviewer Id",
        "Business Id",
        "Review Rating",
        "Review Date",
        "Review Title",
        "Review Content",
        "Email Address",
        "Review IP Address",
        "Reviewer Name",
        "Reviewer Country",
        "Business Name",
    ]
    rows = [
        {
            "Review Id": "r-1",
            "Reviewer Id": "u-1",
            "Business Id": "b-1",
            "Review Rating": "5",
            "Review Date": "2024-01-01T12:00:00Z",
            "Review Title": "Great",
            "Review Content": "Loved it",
            "Email Address": "user@example.com",
            "Review IP Address": "1.2.3.4",
            "Reviewer Name": "Alice",
            "Reviewer Country": "GB",
            "Business Name": "Widgets Inc",
        },
        {
            "Review Id": "r-1",  # duplicate id
            "Reviewer Id": "u-2",
            "Business Id": "b-1",
            "Review Rating": "4",
            "Review Date": "2024-01-02",
            "Email Address": "bad-email",
            "Review IP Address": "not-an-ip",
        },
    ]

    monkeypatch.setattr(ingest, "read_xlsx_rows", lambda path: (header, rows))

    quarantined = {}

    def fake_quarantine(basename, hdr, bad_rows):
        quarantined["basename"] = basename
        quarantined["bad_rows"] = bad_rows
        return str(tmp_path / "quarantine.csv")

    monkeypatch.setattr(ingest, "write_quarantine", fake_quarantine)

    # ensure dirs use tmp path
    def fake_ensure_dirs():
        os.makedirs(tmp_path / "stage", exist_ok=True)
        os.makedirs(tmp_path / "quarantine", exist_ok=True)

    monkeypatch.setattr(ingest, "ensure_dirs", fake_ensure_dirs)

    with db.connect() as conn:
        stats = ingest.process_file(conn, str(sample_file))

    rows_in, rows_loaded, rows_rejected, dq_pass, dq_fail, normalized = stats

    assert rows_in == 2
    assert rows_loaded == 1
    assert rows_rejected == 1
    assert dq_pass == 1
    assert dq_fail == 1
    assert len(normalized) == 1
    assert quarantined["basename"] == "sample"
    assert len(quarantined["bad_rows"]) == 1

    with db.connect() as conn:
        review_rows = conn.execute("SELECT review_id, business_id, user_id FROM review;").fetchall()
        audit_rows = conn.execute("SELECT file, rows_in, rows_loaded, rows_rejected FROM load_audit;").fetchall()

    assert [(r["review_id"], r["business_id"], r["user_id"]) for r in review_rows] == [("r-1", "b-1", "u-1")]
    assert audit_rows[0]["rows_in"] == 2
    assert audit_rows[0]["rows_loaded"] == 1
    assert audit_rows[0]["rows_rejected"] == 1

    # second run with same file hash should be skipped entirely
    with db.connect() as conn:
        stats_again = ingest.process_file(conn, str(sample_file))

    assert stats_again == (0, 0, 0, 0, 0, [])