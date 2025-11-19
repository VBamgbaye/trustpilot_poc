from __future__ import annotations

import pandas as pd
from great_expectations.dataset import PandasDataset

from app.dq_rules import EMAIL_RE, IP_RE, RAW_REQUIRED_COLS, coerce_int, parse_date_to_iso_utc, trim


def _non_empty(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _rating_allowed(value: object) -> bool:
    return coerce_int(str(value)) in {1, 2, 3, 4, 5}


def _date_parsable(value: object) -> bool:
    return bool(parse_date_to_iso_utc(trim(str(value) if value is not None else "")))


def _email_valid(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return EMAIL_RE.match(str(value)) is not None


def _ip_valid(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return IP_RE.match(str(value)) is not None


def build_raw_reviews_dataset(raw_rows: list[dict[str, object]]) -> PandasDataset:
    """
    Create a Great Expectations dataset from raw XLSX rows.
    Expectations mirror the lightweight rules in ``app.dq_rules``.
    """

    df = pd.DataFrame(raw_rows)
    dataset = PandasDataset(df)

    dataset.expect_table_columns_to_contain_set(set(RAW_REQUIRED_COLS))

    for col in ("Review Id", "Reviewer Id", "Business Id"):
        dataset.expect_column_values_to_satisfy_function(col, _non_empty)

    dataset.expect_column_values_to_satisfy_function("Review Rating", _rating_allowed)
    dataset.expect_column_values_to_satisfy_function("Review Date", _date_parsable)
    dataset.expect_column_values_to_be_unique("Review Id")

    if "Email Address" in dataset.columns:
        dataset.expect_column_values_to_satisfy_function("Email Address", _email_valid)
    if "Review IP Address" in dataset.columns:
        dataset.expect_column_values_to_satisfy_function("Review IP Address", _ip_valid)

    return dataset


def validate_raw_rows(raw_rows: list[dict[str, object]]) -> dict:
    """
    Run the Great Expectations suite for raw Trustpilot rows.
    Returns the validation result dict (``result_format='SUMMARY'``).
    """

    dataset = build_raw_reviews_dataset(raw_rows)
    return dataset.validate(result_format="SUMMARY")


def summarize_validation(result: dict) -> str:
    """
    Human-friendly summary line for logs/CLI output.
    """

    if not result:
        return "GE validation skipped"

    stats = result.get("statistics", {})
    success = result.get("success", False)
    evaluated = stats.get("evaluated_expectations", 0)
    successful = stats.get("successful_expectations", 0)
    return f"GE success={success} ({successful}/{evaluated} expectations passed)"
