from __future__ import annotations

import argparse
import csv
import glob
import hashlib
import ipaddress
import os
from datetime import datetime
from typing import Dict, List, Tuple

import openpyxl

from app.db import connect
from app.dq_rules import validate_batch, validate_row

# Try to support either catalog path; env/CLI can override.
RAW_GLOB_DEFAULTS = "data/trustpilot_raw/*.xlsx",
STAGE_PATH_DEFAULT = "data/stage/reviews.parquet"
QUARANTINE_DIR = "data/quarantine"

# ---------- Utils ----------

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def discover_files(patterns: List[str]) -> List[str]:
    files: List[str] = []
    for pat in patterns:
        files.extend(glob.glob(pat))
    files = sorted(set(files))
    return files

def ensure_dirs():
    os.makedirs("data/stage", exist_ok=True)
    os.makedirs(QUARANTINE_DIR, exist_ok=True)

def email_hash_value(email: str | None) -> str | None:
    if not email:
        return None
    return hashlib.sha256(email.strip().lower().encode("utf-8")).hexdigest()

def redact_name(name: str | None) -> str | None:
    if not name:
        return None
    n = name.strip()
    return (n[0] + "***") if n else None

def redact_ip(value: str | None) -> str | None:
    if not value:
        return None
    v = value.strip()
    try:
        ip = ipaddress.ip_address(v)
        if ip.version == 4:
            parts = v.split(".")
            return ".".join(parts[:3] + ["0"])
        else:
            # collapse last hextet for IPv6
            parts = v.split(":")
            if len(parts) >= 1:
                parts[-1] = "0000"
            return ":".join(parts)
    except Exception:
        return "REDACTED"

def write_quarantine(basename: str, header: List[str], bad_rows: List[Tuple[int, Dict[str, str], List[str]]]) -> str:
    ensure_dirs()
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out = os.path.join(QUARANTINE_DIR, f"{basename}_{stamp}_bad_rows.csv")
    out_header = header + ["__reason__"]
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(out_header)
        for _, raw_row, errs in bad_rows:
            w.writerow([raw_row.get(col, "") for col in header] + ["; ".join(errs)])
    return out

def write_stage_parquet(normalized_rows: List[Dict[str, object]], stage_path: str) -> str:
    """
    Writes the validated+normalized dataset to Parquet.
    Requires pyarrow or fastparquet; if unavailable, writes a CSV fallback alongside.
    """
    import pandas as pd
    ensure_dirs()
    df = pd.DataFrame(normalized_rows)
    # Stable column order (nice-to-have)
    preferred = [
        "review_id","user_id","business_id","review_date","review_rating",
        "review_title","review_content","review_ip_address",
        "email_address","user_name","reviewer_country","business_name",
        "source_file","source_row",
    ]
    cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
    df = df[cols]
    try:
        df.to_parquet(stage_path, index=False)  # needs pyarrow or fastparquet
        return stage_path
    except Exception as e:
        # Fallback to CSV if parquet engine not available
        fallback = os.path.splitext(stage_path)[0] + ".csv"
        df.to_csv(fallback, index=False)
        print(f"[ingest] WARNING: parquet engine not available ({e}); wrote CSV fallback: {fallback}")
        return fallback

# ---------- DB upserts ----------

def upsert_business(conn, n: Dict[str, object]) -> None:
    conn.execute(
        """
        INSERT INTO business (business_id, business_name, first_review_date, last_review_date, total_reviews)
        VALUES (?, ?, ?, ?, 1)
        ON CONFLICT(business_id) DO UPDATE SET
            business_name = COALESCE(excluded.business_name, business.business_name),
            first_review_date = CASE
                WHEN business.first_review_date IS NULL OR excluded.first_review_date < business.first_review_date
                THEN excluded.first_review_date ELSE business.first_review_date END,
            last_review_date = CASE
                WHEN business.last_review_date IS NULL OR excluded.last_review_date > business.last_review_date
                THEN excluded.last_review_date ELSE business.last_review_date END,
            total_reviews = business.total_reviews + 1;
        """,
        (
            n["business_id"],
            n.get("business_name"),
            n.get("review_date"),
            n.get("review_date"),
        ),
    )


def upsert_user(conn, n: Dict[str, object]) -> None:
    conn.execute(
        """
        INSERT INTO "user" (user_id, email_address, email_hash, user_name, user_name_redacted, reviewer_country, first_review_date, total_reviews)
        VALUES (?, ?, ?, ?, ?, ?, ?, 1)
        ON CONFLICT(user_id) DO UPDATE SET
            email_address = COALESCE(excluded.email_address, "user".email_address),
            email_hash = COALESCE(excluded.email_hash, "user".email_hash),
            user_name = COALESCE(excluded.user_name, "user".user_name),
            user_name_redacted = COALESCE(excluded.user_name_redacted, "user".user_name_redacted),
            reviewer_country = COALESCE(excluded.reviewer_country, "user".reviewer_country),
            first_review_date = CASE
                WHEN "user".first_review_date IS NULL OR excluded.first_review_date < "user".first_review_date
                THEN excluded.first_review_date ELSE "user".first_review_date END,
            total_reviews = "user".total_reviews + 1;
        """,
        (
            n["user_id"],
            n.get("email_address"),
            email_hash_value(n.get("email_address")),
            n.get("user_name"),
            redact_name(n.get("user_name")),
            n.get("reviewer_country"),
            n.get("review_date"),
        ),
    )

def upsert_review(conn, n: Dict[str, object]) -> None:
    conn.execute(
        """
        INSERT INTO review (
            review_id, user_id, business_id, review_date, review_rating,
            review_title, review_content, review_ip_address, review_ip_redacted, source_file, source_row
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(review_id) DO UPDATE SET
            review_date = excluded.review_date,
            review_rating = excluded.review_rating,
            review_title = excluded.review_title,
            review_content = excluded.review_content,
            review_ip_address = excluded.review_ip_address,
            review_ip_redacted = excluded.review_ip_redacted,
            source_file = excluded.source_file,
            source_row = excluded.source_row;
        """,
        (
            n["review_id"],
            n["user_id"],
            n["business_id"],
            n.get("review_date"),
            n.get("review_rating"),
            n.get("review_title"),
            n.get("review_content"),
            n.get("review_ip_address"),
            redact_ip(n.get("review_ip_address")),
            n.get("source_file"),
            n.get("source_row"),
        ),
    )

# ---------- XLSX Reading ----------

def read_xlsx_rows(path: str) -> Tuple[List[str], List[Dict[str, str]]]:
    """
    Read XLSX file and return (header, rows) where rows are dicts with string values.
    Handles Excel date serials by converting them to ISO format strings.
    Preserves numeric values as strings without adding decimal points.
    """
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb.active

    rows_iter = ws.iter_rows(values_only=True)
    header_row = next(rows_iter, None)

    if not header_row:
        return [], []

    # Strip whitespace from headers
    header = [str(cell).strip() if cell is not None else "" for cell in header_row]

    raw_rows: List[Dict[str, str]] = []

    for row_values in rows_iter:
        row_dict = {}
        for col_idx, cell_value in enumerate(row_values):
            if col_idx >= len(header):
                break

            col_name = header[col_idx]

            # Convert cell value to string, handling various Excel types
            if cell_value is None:
                str_value = ""
            elif isinstance(cell_value, datetime):
                # Excel datetime objects - convert to ISO format string with 'Z' suffix
                str_value = cell_value.strftime("%Y-%m-%dT%H:%M:%SZ") if cell_value.tzinfo else cell_value.strftime("%Y-%m-%dT%H:%M:%S")
            elif isinstance(cell_value, (int, float)):
                # For numbers, convert to string without unnecessary decimals
                if isinstance(cell_value, float) and cell_value.is_integer():
                    str_value = str(int(cell_value))
                else:
                    str_value = str(cell_value)
            else:
                str_value = str(cell_value)

            row_dict[col_name] = str_value

        raw_rows.append(row_dict)

    wb.close()
    return header, raw_rows

# ---------- Ingestion ----------

def process_file(conn, path: str) -> Tuple[int, int, int, int, int, List[Dict[str, object]]]:
    """
    Returns:
      rows_in, rows_loaded, rows_rejected, dq_pass, dq_fail, normalized_good_rows (with lineage)
    """
    rows_in = rows_loaded = rows_rejected = dq_pass = dq_fail = 0
    basename = os.path.basename(path)

    # Hash + audit skip (we still return zeros + empty stage rows if already processed)
    file_hash = sha256_file(path)
    prior = conn.execute(
        "SELECT 1 FROM load_audit WHERE file=? AND sha256=? LIMIT 1;",
        (path, file_hash),
    ).fetchone()
    if prior:
        return (0, 0, 0, 0, 0, [])

    # Read XLSX file
    header, raw_rows = read_xlsx_rows(path)

    # Batch DQ
    dups, _batch_errs = validate_batch(raw_rows)

    bad_rows: List[Tuple[int, Dict[str, str], List[str]]] = []
    good_norm: List[Dict[str, object]] = []

    for xlsx_rownum, raw in enumerate(raw_rows, start=2):  # header is row 1
        rows_in += 1
        is_ok, errs, norm = validate_row(raw)

        if (xlsx_rownum - 2) in dups:
            errs = errs + ["Duplicate Review Id within file"]

        if is_ok and not errs:
            # attach lineage for stage + DB
            norm["source_file"] = basename
            norm["source_row"] = xlsx_rownum
            good_norm.append(norm)
            dq_pass += 1
        else:
            dq_fail += 1
            rows_rejected += 1
            bad_rows.append((xlsx_rownum, raw, errs))

    # quarantine per-file
    if bad_rows:
        write_quarantine(os.path.splitext(basename)[0], header, bad_rows)

    # Insert good rows directly (UPSERT); we still also collect for stage
    for n in good_norm:
        upsert_business(conn, n)
        upsert_user(conn, n)
        upsert_review(conn, n)
        rows_loaded += 1

    # Record audit
    conn.execute(
        """
        INSERT INTO load_audit (file, sha256, rows_in, rows_loaded, rows_rejected, dq_pass, dq_fail)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (path, file_hash, rows_in, rows_loaded, rows_rejected, dq_pass, dq_fail),
    )

    return rows_in, rows_loaded, rows_rejected, dq_pass, dq_fail, good_norm

def build_stage_and_load(patterns: List[str], stage_path: str) -> Tuple[int, int, int, int, int, int, str]:
    """
    Full run:
      - discover raw XLSX files
      - per-file DQ + UPSERT (idempotent via load_audit)
      - write a consolidated stage parquet/CSV of all validated, normalized rows processed in this run
    Returns:
      total_files, rows_in, loaded, rejected, dq_pass, dq_fail, written_count, stage_path_used
    """
    files = discover_files(patterns)
    total_files = len(files)
    agg = [0, 0, 0, 0, 0]  # rows_in, loaded, rejected, dq_pass, dq_fail
    stage_rows: List[Dict[str, object]] = []

    from app.models import init_db
    init_db()

    with connect() as conn:
        for path in files:
            r_in, r_loaded, r_rej, r_pass, r_fail, good_norm = process_file(conn, path)
            agg[0] += r_in
            agg[1] += r_loaded
            agg[2] += r_rej
            agg[3] += r_pass
            agg[4] += r_fail
            stage_rows.extend(good_norm)

    # Write stage file for **this run's** validated rows
    stage_path_used = ""
    written_count = 0
    if stage_rows:
        stage_path_used = write_stage_parquet(stage_rows, stage_path)
        written_count = len(stage_rows)

    return (total_files, *agg, written_count, stage_path_used)

def compute_metrics_summary(conn, date_from: str | None = None, date_to: str | None = None) -> None:
    """
    Rebuild metrics_summary for the affected date range.
    If no range provided, rebuild for the whole dataset.
    """
    conn.execute("DELETE FROM metrics_summary;")

    conn.execute("""
        INSERT INTO metrics_summary (
            business_id, period_start_date, total_reviews, avg_rating,
            rating_1, rating_2, rating_3, rating_4, rating_5
        )
        SELECT
            r.business_id,
            date(r.review_date) AS period_start_date,
            COUNT(*) AS total_reviews,
            AVG(r.review_rating) AS avg_rating,
            SUM(CASE WHEN r.review_rating=1 THEN 1 ELSE 0 END) AS rating_1,
            SUM(CASE WHEN r.review_rating=2 THEN 1 ELSE 0 END) AS rating_2,
            SUM(CASE WHEN r.review_rating=3 THEN 1 ELSE 0 END) AS rating_3,
            SUM(CASE WHEN r.review_rating=4 THEN 1 ELSE 0 END) AS rating_4,
            SUM(CASE WHEN r.review_rating=5 THEN 1 ELSE 0 END) AS rating_5
        FROM review r
        GROUP BY r.business_id, date(r.review_date)
        ORDER BY r.business_id, period_start_date;
    """)


# ---------- CLI ----------

def cli():
    """
    Usage:
      python -m app.ingest                            # tries trustpilot_raw
      python -m app.ingest --glob "data/foo/*.xlsx"    # explicit
      python -m app.ingest --stage "data/stage/reviews.parquet"
    """
    parser = argparse.ArgumentParser(description="Ingest xlsx → Stage Parquet → SQLite with DQ, audit, and lineage.")
    parser.add_argument("--glob", dest="glob", action="append", help="Glob for raw XLSX files; can repeat")
    parser.add_argument("--stage", dest="stage", default=STAGE_PATH_DEFAULT, help="Stage parquet path")
    args = parser.parse_args()

    patterns = args.glob if args.glob else RAW_GLOB_DEFAULTS
    total_files, rows_in, loaded, rejected, dq_pass, dq_fail, written_count, stage_used = build_stage_and_load(patterns, args.stage)

    msg = (
        f"files={total_files} rows_in={rows_in} loaded={loaded} "
        f"rejected={rejected} dq_pass={dq_pass} dq_fail={dq_fail} "
        f"stage_rows_written={written_count} stage_path={stage_used or '-'}"
    )
    print(msg)

if __name__ == "__main__":
    cli()
    with connect() as conn:
        compute_metrics_summary(conn)

    # with connect() as conn:
    #     for path in files:
    #         sha256 = file_sha256(path)
    #         if audit_exists(conn, sha256):
    #             skipped += 1
    #             continue
    #         stats = process_file(conn, path)
    #         audit_run(conn, path, sha256, stats)
    #         for k in total:
    #             total[k] += stats[k]
    #         processed += 1

    #     if processed > 0:
    #         compute_metrics_summary(conn)
