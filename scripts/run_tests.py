"""Run the declarative tests defined in models/marts/schema.yml.

This is the `dbt test` equivalent. Supported generic tests:
    not_null, unique, accepted_values, positive_value, non_negative,
    unique_combination (model level)
plus free-form `data_tests` where any returned row is a failure.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import yaml

from ingestion.config import PROJECT_ROOT, load_settings
from ingestion.storage import Warehouse

SCHEMA_FILE = PROJECT_ROOT / "models" / "marts" / "schema.yml"


class TestResult:
    def __init__(self, name: str, passed: bool, detail: str = "") -> None:
        self.name, self.passed, self.detail = name, passed, detail


def _count(con: Any, sql: str) -> int:
    return con.execute(f"SELECT COUNT(*) FROM ({sql})").fetchone()[0]


def run_column_test(con: Any, model: str, column: str, test: Any) -> TestResult:
    if isinstance(test, str):
        kind, args = test, {}
    else:
        kind, args = next(iter(test.items()))

    if kind == "not_null":
        sql = f"SELECT 1 FROM {model} WHERE {column} IS NULL"
    elif kind == "unique":
        sql = (f"SELECT {column} FROM {model} WHERE {column} IS NOT NULL "
               f"GROUP BY {column} HAVING COUNT(*) > 1")
    elif kind == "accepted_values":
        vals = ", ".join(f"'{v}'" for v in args["values"])
        sql = f"SELECT 1 FROM {model} WHERE {column} IS NOT NULL AND {column} NOT IN ({vals})"
    elif kind == "positive_value":
        sql = f"SELECT 1 FROM {model} WHERE {column} <= 0"
    elif kind == "non_negative":
        sql = f"SELECT 1 FROM {model} WHERE {column} < 0"
    else:
        return TestResult(f"{model}.{column}.{kind}", False, "unknown test type")

    failures = _count(con, sql)
    return TestResult(f"{model}.{column}.{kind}", failures == 0,
                      f"{failures} failing row(s)" if failures else "")


def main() -> int:
    settings = load_settings()
    spec = yaml.safe_load(SCHEMA_FILE.read_text(encoding="utf-8"))
    results: list[TestResult] = []

    with Warehouse(settings.duckdb_path) as wh:
        con = wh.con

        for model in spec.get("models", []):
            name = model["name"]

            for mtest in model.get("tests", []):
                kind, args = next(iter(mtest.items()))
                if kind == "unique_combination":
                    cols = ", ".join(args["columns"])
                    sql = f"SELECT {cols} FROM {name} GROUP BY {cols} HAVING COUNT(*) > 1"
                    n = _count(con, sql)
                    results.append(TestResult(f"{name}.unique_combination({cols})", n == 0,
                                              f"{n} duplicate key(s)" if n else ""))

            for col in model.get("columns", []):
                for t in col.get("tests", []):
                    results.append(run_column_test(con, name, col["name"], t))

        for dtest in spec.get("data_tests", []):
            n = _count(con, dtest["sql"])
            results.append(TestResult(f"data_test.{dtest['name']}", n == 0,
                                      f"{n} failing row(s)" if n else ""))

    passed = sum(r.passed for r in results)
    print("\n" + "=" * 62)
    print(f"  DATA TESTS  |  {passed}/{len(results)} passed")
    print("=" * 62)
    for r in results:
        print(f"  {'PASS' if r.passed else 'FAIL'}  {r.name}"
              + (f"  -> {r.detail}" if r.detail else ""))
    print("=" * 62 + "\n")

    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())