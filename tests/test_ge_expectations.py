import pytest

pytest.importorskip("great_expectations")

from app.ge_expectations import validate_raw_rows


def _valid_row() -> dict[str, str]:
    return {
        "Review Id": "r1",
        "Reviewer Id": "u1",
        "Business Id": "b1",
        "Review Rating": "5",
        "Review Date": "2024-05-01T12:00:00Z",
        "Email Address": "user@example.com",
        "Review IP Address": "192.168.0.1",
    }


def test_ge_expectations_pass_on_clean_rows():
    result = validate_raw_rows([_valid_row()])

    assert result["success"] is True
    assert result["statistics"]["successful_expectations"] == result["statistics"][
        "evaluated_expectations"
    ]


def test_ge_expectations_flag_invalid_rows():
    bad_row = {
        "Review Id": "",  # blank
        "Reviewer Id": "u2",
        "Business Id": "b2",
        "Review Rating": "9",  # out of range
        "Review Date": "not-a-date",
        "Email Address": "bad@",
        "Review IP Address": "999.1.1.1",
    }

    result = validate_raw_rows([bad_row])

    assert result["success"] is False
    failed = [r for r in result["results"] if r.get("success") is False]
    assert len(failed) >= 4
