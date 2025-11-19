import app.dq_rules as dq


def test_validate_row_flags_invalid_values():
    raw = {
        "Review Id": " ",
        "Reviewer Id": "u-1",
        "Business Id": "b-1",
        "Review Rating": "7",
        "Review Date": "not-a-date",
        "Email Address": "not-an-email",
        "Review IP Address": "999.999.0.1",
    }

    is_valid, errors, normalized = dq.validate_row(raw)

    assert not is_valid
    assert "Review Id is null/empty" in errors
    assert "Review Rating invalid" in " ".join(errors)
    assert "Review Date unparsable" in " ".join(errors)
    assert "Email Address invalid" in " ".join(errors)
    assert "Review IP Address invalid" in " ".join(errors)
    # ensure normalized has snake_case keys even on failure
    assert set(normalized.keys()) >= {"review_id", "review_rating", "review_date"}


def test_validate_batch_detects_duplicate_review_ids():
    rows = [
        {"Review Id": "dup-1"},
        {"Review Id": "dup-1"},
        {"Review Id": "unique"},
        {"Review Id": "dup-1"},
    ]

    dups, per_row_errors = dq.validate_batch(rows)

    # rows 1 and 3 (0-indexed) are marked as duplicates
    assert dups == [1, 3]
    assert per_row_errors[1] == ["Duplicate Review Id within file"]
    assert per_row_errors[3] == ["Duplicate Review Id within file"]
