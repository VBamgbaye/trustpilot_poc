from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List

import pandas as pd


@dataclass
class _Expectation:
    expectation_type: str
    kwargs: Dict[str, Any]


class PandasDataset:
    def __init__(self, df: pd.DataFrame):
        self._df = df.copy()
        self._expectations: List[_Expectation] = []

    @property
    def columns(self):
        return list(self._df.columns)

    def expect_table_columns_to_contain_set(self, column_set):
        self._expectations.append(
            _Expectation(
                expectation_type="expect_table_columns_to_contain_set",
                kwargs={"column_set": set(column_set)},
            )
        )

    def expect_column_values_to_satisfy_function(self, column: str, function: Callable[[Any], bool]):
        self._expectations.append(
            _Expectation(
                expectation_type="expect_column_values_to_satisfy_function",
                kwargs={"column": column, "function": function},
            )
        )

    def expect_column_values_to_be_unique(self, column: str):
        self._expectations.append(
            _Expectation(
                expectation_type="expect_column_values_to_be_unique",
                kwargs={"column": column},
            )
        )

    def _run_expectation(self, expectation: _Expectation) -> Dict[str, Any]:
        etype = expectation.expectation_type
        kwargs = expectation.kwargs

        if etype == "expect_table_columns_to_contain_set":
            expected = kwargs["column_set"]
            success = expected.issubset(set(self.columns))
            return {
                "success": success,
                "expectation_config": {"expectation_type": etype, "kwargs": kwargs},
            }

        if etype == "expect_column_values_to_satisfy_function":
            column = kwargs["column"]
            func = kwargs["function"]
            if column not in self._df.columns:
                success = False
                unexpected_count = 1
            else:
                series = self._df[column]
                checks = series.map(func)
                success = bool(checks.all()) if len(checks) else True
                unexpected_count = int((~checks).sum())
            return {
                "success": success,
                "result": {"unexpected_count": unexpected_count},
                "expectation_config": {"expectation_type": etype, "kwargs": kwargs},
            }

        if etype == "expect_column_values_to_be_unique":
            column = kwargs["column"]
            if column not in self._df.columns:
                success = False
                unexpected_count = 1
            else:
                series = self._df[column]
                success = bool(series.is_unique)
                unexpected_count = int(series.duplicated().sum())
            return {
                "success": success,
                "result": {"unexpected_count": unexpected_count},
                "expectation_config": {"expectation_type": etype, "kwargs": kwargs},
            }

        return {"success": False, "expectation_config": {"expectation_type": etype, "kwargs": kwargs}}

    def validate(self, result_format: str = "SUMMARY") -> Dict[str, Any]:
        results = [self._run_expectation(exp) for exp in self._expectations]
        successes = [r.get("success", False) for r in results]
        successful_count = sum(1 for s in successes if s)
        evaluated = len(results)
        unsuccessful = evaluated - successful_count

        return {
            "success": all(successes) if results else True,
            "results": results,
            "statistics": {
                "evaluated_expectations": evaluated,
                "successful_expectations": successful_count,
                "unsuccessful_expectations": unsuccessful,
            },
        }
