from __future__ import annotations

import json
import logging
import os
import time
import uuid
from typing import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

LOG_LEVEL = (os.getenv("LOG_LEVEL", "INFO") or "INFO").upper()
LOG_FILE  = os.getenv("LOG_FILE", "")

_logger = logging.getLogger("audit")
if not _logger.handlers:
    _logger.setLevel(LOG_LEVEL)
    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter("%(message)s"))
    _logger.addHandler(sh)
    if LOG_FILE:
        os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
        fh = logging.FileHandler(LOG_FILE)
        fh.setFormatter(logging.Formatter("%(message)s"))
        _logger.addHandler(fh)

def _safe_params(req: Request) -> dict:
    return dict(req.query_params)

class AuditMiddleware(BaseHTTPMiddleware):
    """
    - Adds X-Request-ID to all responses
    - Logs JSON line: {ts, request_id, path, params, method, status, rows_returned, duration_ms, actor, ip}
      'rows_returned' is taken from response header X-Rows-Returned (if present).
    """
    def __init__(self, app: ASGIApp):
        super().__init__(app)

    async def dispatch(self, request: Request, call_next: Callable):
        rid = str(uuid.uuid4())
        start = time.time()
        request.state.request_id = rid

        try:
            response: Response = await call_next(request)
        except Exception as e:
            duration_ms = round((time.time() - start) * 1000, 2)
            _logger.info(json.dumps({
                "ts": int(time.time() * 1000),
                "request_id": rid,
                "path": request.url.path,
                "method": request.method,
                "params": _safe_params(request),
                "status": 500,
                "rows_returned": 0,
                "duration_ms": duration_ms,
                "actor": getattr(request.state, "actor", "unknown"),
                "ip": getattr(request.state, "ip", None),
                "error": str(e),
            }, ensure_ascii=False))
            raise

        response.headers["X-Request-ID"] = rid

        # count from header if endpoint set it
        try:
            rows = int(response.headers.get("X-Rows-Returned", "0"))
        except Exception:
            rows = 0

        duration_ms = round((time.time() - start) * 1000, 2)
        _logger.info(json.dumps({
            "ts": int(time.time() * 1000),
            "request_id": rid,
            "path": request.url.path,
            "method": request.method,
            "params": _safe_params(request),
            "status": response.status_code,
            "rows_returned": rows,
            "duration_ms": duration_ms,
            "actor": getattr(request.state, "actor", "unknown"),
            "ip": getattr(request.state, "ip", None),
        }, ensure_ascii=False))
        return response
