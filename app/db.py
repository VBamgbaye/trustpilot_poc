from __future__ import annotations
import os
import sqlite3
from contextlib import contextmanager

DB_PATH = os.getenv("DB_PATH", "data/trustpilot_poc.db")

def _apply_pragmas(conn: sqlite3.Connection) -> None: #PRAGMA - makes sure that the database is not corrupted in case of a crash
    # WAL for better concurrency; enforce FK; predictable case/like behavior optional
    conn.execute("PRAGMA journal_mode=WAL;") #write ahead logging, for more concurrent reads/writes
    conn.execute("PRAGMA foreign_keys=ON;") # Enforce foreign key constraints
    conn.execute("PRAGMA synchronous=NORMAL;") # Balance of performance and safety

def get_conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    _apply_pragmas(conn)
    return conn

@contextmanager
def connect():
    conn = get_conn()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()
