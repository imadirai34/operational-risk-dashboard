"""
etl/validate.py
Data validation module — runs quality checks and flags anomalies.
"""

import pandas as pd
import sqlite3
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = "data/processed/risk_dashboard.db"


def get_connection():
    return sqlite3.connect(DB_PATH)


class ValidationResult:
    def __init__(self):
        self.checks = []
        self.passed = 0
        self.failed = 0

    def add(self, check_name: str, passed: bool, detail: str = ""):
        status = "PASS" if passed else "FAIL"
        self.checks.append({"check": check_name, "status": status, "detail": detail})
        if passed:
            self.passed += 1
        else:
            self.failed += 1
        logger.info(f"  [{status}] {check_name} {('— ' + detail) if detail else ''}")

    def summary(self):
        total = self.passed + self.failed
        score = round((self.passed / total) * 100, 1) if total > 0 else 0
        return {
            "total_checks": total,
            "passed": self.passed,
            "failed": self.failed,
            "data_quality_score": score,
        }


def validate_incidents(conn: sqlite3.Connection) -> ValidationResult:
    """Run validation checks on incidents table."""
    logger.info("Validating incidents table...")
    result = ValidationResult()
    df = pd.read_sql("SELECT * FROM incidents", conn)

    # Null checks
    result.add("No null incident_ids", df["incident_id"].isnull().sum() == 0)
    result.add("No null created_dates", df["created_date"].isnull().sum() == 0)
    result.add("No null departments", df["department"].isnull().sum() == 0)

    # Value range checks
    result.add("Severity values valid",
               df["severity"].isin(["Low", "Medium", "High", "Critical"]).all())
    result.add("Status values valid",
               df["status"].isin(["Open", "In Progress", "Escalated", "Resolved", "Closed"]).all())

    # Business logic checks
    null_resolved = df[df["resolved_date"].isnull()]
    result.add("Open incidents have no resolved_date",
               null_resolved["status"].isin(["Open", "In Progress", "Escalated"]).all()
               if len(null_resolved) > 0 else True)

    result.add("Estimated costs are positive",
               (df["estimated_cost_usd"] >= 0).all())

    result.add("Duplicate incident IDs",
               df["incident_id"].duplicated().sum() == 0,
               f"{df['incident_id'].duplicated().sum()} duplicates found")

    return result


def validate_compliance(conn: sqlite3.Connection) -> ValidationResult:
    """Run validation checks on compliance_checks table."""
    logger.info("Validating compliance_checks table...")
    result = ValidationResult()
    df = pd.read_sql("SELECT * FROM compliance_checks", conn)

    result.add("No null check_ids", df["check_id"].isnull().sum() == 0)
    result.add("Scores within 0–100 range",
               ((df["score"] >= 0) & (df["score"] <= 100)).all())
    result.add("Result values valid",
               df["result"].isin(["Pass", "Fail"]).all())
    result.add("No duplicate check_ids",
               df["check_id"].duplicated().sum() == 0)

    return result


def validate_all():
    """Run all validation checks and print summary."""
    conn = get_connection()
    all_results = []

    for validate_fn in [validate_incidents, validate_compliance]:
        result = validate_fn(conn)
        summary = result.summary()
        all_results.append(summary)
        logger.info(f"  → Data Quality Score: {summary['data_quality_score']}%\n")

    conn.close()

    overall_passed = sum(r["passed"] for r in all_results)
    overall_total = sum(r["total_checks"] for r in all_results)
    overall_score = round((overall_passed / overall_total) * 100, 1) if overall_total > 0 else 0

    logger.info(f"✅ Overall Data Quality Score: {overall_score}% ({overall_passed}/{overall_total} checks passed)")
    return overall_score


if __name__ == "__main__":
    validate_all()
