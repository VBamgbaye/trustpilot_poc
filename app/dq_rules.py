from __future__ import annotations
import re
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional

# Raw XLSX headers as they appear in source (verbatim)
RAW_REQUIRED_COLS = [
    "Review Id",
    "Reviewer Id",
    "Business Id",
    "Review Rating",
    "Review Date",
]

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$") # Simplistic email regex - edge cases are missing such as Unicode, subdomains, etc but good enough for PoC
IP_RE = re.compile(
    r"^((25[0-5]|2[0-4]\d|[01]?\d?\d)(\.(?!$)|$)){4}$|" # IPv4
    r"^([0-9a-fA-F]{0,4}:){2,7}[0-9a-fA-F]{0,4}$" # IPv6
)

# Common date patterns; we normalize to ISO 8601 UTC "YYYY-MM-DDTHH:MM:SSZ"
# Note: XLSX files may contain dates as Excel serial numbers or formatted strings
_DATE_PATTERNS = [
    "%Y-%m-%d",
    "%Y-%m-%d %H:%M:%S",
    "%d/%m/%Y",
    "%d/%m/%Y %H:%M:%S",
    "%m/%d/%Y",
    "%m/%d/%Y %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%d %H:%M",
]

def parse_date_to_iso_utc(s: str) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    
    # Try ISO fast-path (handles formats like "2024-10-17T10:43:39+0200")
    try:
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s[:-1]).replace(tzinfo=timezone.utc)
        else:
            # Normalize timezone format: +0700 -> +07:00
            normalized = re.sub(r'([+-]\d{2})(\d{2})$', r'\1:\2', s)
            dt = datetime.fromisoformat(normalized)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
        return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except Exception:
        pass
    
    # Try known patterns
    for pat in _DATE_PATTERNS:
        try:
            dt = datetime.strptime(s, pat).replace(tzinfo=timezone.utc)
            return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        except Exception:
            continue
    
    return None

def coerce_int(v: str) -> Optional[int]:
    if v is None:
        return None
    v = v.strip()
    if v == "":
        return None
    try:
        # Handle floats that should be integers (e.g., "3.0" -> 3)
        return int(float(v))
    except ValueError:
        return None

def trim(s: Optional[str]) -> Optional[str]:
    return s.strip() if isinstance(s, str) else s

def validate_row(raw: Dict[str, str]) -> Tuple[bool, List[str], Dict[str, object]]:
    """
    Validate + normalize a raw row from XLSX.
    Returns (is_valid, errors, normalized_row)
    normalized_row uses snake_case keys (stage/table contract).
    """
    errors: List[str] = []

    # Required columns present in the file
    for col in RAW_REQUIRED_COLS:
        if col not in raw:
            errors.append(f"missing required column: {col}")

    review_id = trim(raw.get("Review Id"))
    reviewer_id = trim(raw.get("Reviewer Id"))
    business_id = trim(raw.get("Business Id"))
    review_rating = coerce_int(raw.get("Review Rating", ""))
    review_date_iso = parse_date_to_iso_utc(trim(raw.get("Review Date") or ""))

    if not review_id:
        errors.append("Review Id is null/empty")
    if not reviewer_id:
        errors.append("Reviewer Id is null/empty")
    if not business_id:
        errors.append("Business Id is null/empty")
    if review_rating is None or review_rating not in (1, 2, 3, 4, 5):
        errors.append(f"Review Rating invalid: {raw.get('Review Rating')!r}")
    if not review_date_iso:
        errors.append(f"Review Date unparsable: {raw.get('Review Date')!r}")

    email = trim(raw.get("Email Address"))
    if email and not EMAIL_RE.match(email):
        errors.append(f"Email Address invalid: {email!r}")

    ip = trim(raw.get("Review IP Address"))
    if ip and not IP_RE.match(ip):
        errors.append(f"Review IP Address invalid: {ip!r}")

    normalized = {
        # user
        "user_id": reviewer_id,
        "email_address": email or None,
        "user_name": trim(raw.get("Reviewer Name")),
        "reviewer_country": trim(raw.get("Reviewer Country")),
        # business
        "business_id": business_id,
        "business_name": trim(raw.get("Business Name")),
        # review
        "review_id": review_id,
        "review_date": review_date_iso,
        "review_rating": review_rating if review_rating is not None else None,
        "review_title": trim(raw.get("Review Title")),
        "review_content": trim(raw.get("Review Content")),
        "review_ip_address": ip,
    }

    return (len(errors) == 0), errors, normalized

def validate_batch(rows: List[Dict[str, str]]) -> Tuple[List[int], Dict[int, List[str]]]:
    """
    File-level validations for XLSX (e.g., duplicate Review Id within the same file).
    Returns (duplicate_row_indices, row_errors_map)
    """
    seen = set()
    dups: List[int] = []
    per_row_errors: Dict[int, List[str]] = {}

    for idx, row in enumerate(rows):
        rid = trim(row.get("Review Id"))
        if rid:
            if rid in seen:
                dups.append(idx)
                per_row_errors.setdefault(idx, []).append("Duplicate Review Id within file")
            else:
                seen.add(rid)

    return dups, per_row_errors