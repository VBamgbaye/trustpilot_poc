from __future__ import annotations
import os
import csv
from io import StringIO
from typing import Optional, Iterable
from fastapi import FastAPI, Depends, Request, HTTPException
from fastapi.responses import StreamingResponse, PlainTextResponse, JSONResponse
from dotenv import load_dotenv, find_dotenv
from datetime import datetime

from app.db import get_conn
from app.audit import AuditMiddleware
from app.security import require_token, allow_pii, AuthContext

load_dotenv(find_dotenv(), override=False)

app = FastAPI(title="Trustpilot Reviews API (PoC)")
app.add_middleware(AuditMiddleware)

CATALOG_PATH = os.getenv("CATALOG_PATH", "config/catalog.yml")

def stream_cursor_as_csv(rows: Iterable[tuple], headers: list[str]) -> Iterable[bytes]:
    # Use csv module for RFC4180 quoting; write to a StringIO chunk-by-chunk
    buf = StringIO()
    w = csv.writer(buf)
    w.writerow(headers)
    yield buf.getvalue().encode("utf-8")
    buf.seek(0); buf.truncate(0)

    for row in rows:
        w.writerow([("" if v is None else v) for v in row])
        yield buf.getvalue().encode("utf-8")
        buf.seek(0); buf.truncate(0)

def _count(conn, sql: str, params: tuple) -> int:
    q = f"SELECT COUNT(*) FROM ({sql}) t"
    return conn.execute(q, params).fetchone()[0]

def _dl_name(prefix: str) -> str:
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{ts}.csv"

@app.get("/healthz")
def healthz(_: AuthContext = Depends(require_token)):
    conn = get_conn()
    row = conn.execute("""
        SELECT id, file, rows_in, rows_loaded, rows_rejected, dq_pass, dq_fail, loaded_at
        FROM load_audit ORDER BY id DESC LIMIT 1;
    """).fetchone()
    payload = {"status": "ok", "last_load": None}
    if row:
        payload["last_load"] = dict(row)
        total = conn.execute("SELECT COUNT(*) AS c FROM review;").fetchone()["c"]
        payload["total_reviews"] = total
    return JSONResponse(payload)

@app.get("/meta/catalog")
def meta_catalog(_: AuthContext = Depends(require_token)):
    if not os.path.exists(CATALOG_PATH):
        raise HTTPException(status_code=404, detail="catalog_not_found")
    with open(CATALOG_PATH, "r", encoding="utf-8") as f:
        txt = f.read()
    return PlainTextResponse(txt, media_type="text/plain; charset=utf-8")

@app.get("/reviews/by-business")
def reviews_by_business(
    request: Request,
    business_id: str,
    from_: Optional[str] = None,
    to: Optional[str] = None,
    pii: bool = False,
    ctx: AuthContext = Depends(require_token),
):
    view = "v_reviews_public"
    if pii and allow_pii(ctx):
        view = "v_reviews_private"

    conn = get_conn()
    sql = f"""
      SELECT * FROM {view}
      WHERE business_id = ?
        AND (? IS NULL OR review_date >= ?)
        AND (? IS NULL OR review_date <= ?)
      ORDER BY review_date
    """
    params = (business_id, from_, from_, to, to)
    # compute count for audit header
    cnt = _count(conn, sql, params)
    cur = conn.execute(sql, params)
    headers = [d[0] for d in cur.description]
    resp = StreamingResponse(stream_cursor_as_csv(cur, headers), media_type="text/csv")
    resp.headers["X-Rows-Returned"] = str(cnt)
    resp.headers["Content-Disposition"] = f'attachment; filename="{_dl_name(f"reviews_business_{business_id}")}"' 
    return resp

@app.get("/reviews/by-user")
def reviews_by_user(
    request: Request,
    user_id: str,
    from_: Optional[str] = None,
    to: Optional[str] = None,
    pii: bool = False,
    ctx: AuthContext = Depends(require_token),
):
    view = "v_reviews_public"
    if pii and allow_pii(ctx):
        view = "v_reviews_private"

    conn = get_conn()
    sql = f"""
      SELECT * FROM {view}
      WHERE user_id = ?
        AND (? IS NULL OR review_date >= ?)
        AND (? IS NULL OR review_date <= ?)
      ORDER BY review_date
    """
    params = (user_id, from_, from_, to, to)
    cnt = _count(conn, sql, params)
    cur = conn.execute(sql, params)
    headers = [d[0] for d in cur.description]
    resp = StreamingResponse(stream_cursor_as_csv(cur, headers), media_type="text/csv")
    resp.headers["X-Rows-Returned"] = str(cnt)
    resp.headers["Content-Disposition"] = f'attachment; filename="{_dl_name(f"reviews_user_{user_id}")}"'
    return resp

@app.get("/users/{user_id}")
def user_account(
    request: Request,
    user_id: str,
    pii: bool = False,
    ctx: AuthContext = Depends(require_token),
):

    conn = get_conn()
    sql = """
      SELECT
        u.user_id,
        u.email_hash      AS email_address,
        u.user_name_redacted AS user_name,
        u.reviewer_country,
        u.first_review_date,
        u.total_reviews
      FROM "user" u
      WHERE u.user_id = ?
    """
    cur = conn.execute(sql, (user_id,))
    rows = cur.fetchall()
    headers = [d[0] for d in cur.description]
    # stream the (0/1 row) as CSV
    def gen():
        yield (",".join(headers) + "\n").encode("utf-8")
        for r in rows:
            yield (",".join("" if v is None else str(v) for v in r) + "\n").encode("utf-8")
    resp = StreamingResponse(gen(), media_type="text/csv")
    resp.headers["X-Rows-Returned"] = str(len(rows))
    resp.headers["Content-Disposition"] = f'attachment; filename="{_dl_name(f"user_{user_id}_account")}"'
    return resp
