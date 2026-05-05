"""
tests/test_pipeline.py
Unit tests for ETL pipeline modules.
Run with: pytest tests/
"""

import pytest
import pandas as pd
import sqlite3
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from etl.validate import ValidationResult


# ---------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------

@pytest.fixture
def sample_incidents_df():
    """Sample incidents DataFrame for testing."""
    return pd.DataFrame({
        "incident_id": ["INC-001", "INC-002", "INC-003"],
        "created_date": ["2024-01-15", "2024-02-10", "2024-03-05"],
        "sla_deadline": ["2024-01-22", "2024-02-17", "2024-03-12"],
        "resolved_date": ["2024-01-20", None, "2024-03-20"],
        "incident_type": ["Policy Violation", "SLA Breach", "Data Anomaly"],
        "severity": ["High", "Critical", "Medium"],
        "status": ["Resolved", "Open", "Closed"],
        "department": ["Risk", "Legal", "Operations"],
        "region": ["North", "South", "East"],
        "assigned_to": ["Alice", "Bob", "Charlie"],
        "escalated": [0, 1, 0],
        "repeat_incident": [0, 0, 1],
        "estimated_cost_usd": [5000.0, 15000.0, 2500.0],
        "description": ["Test incident 1", "Test incident 2", "Test incident 3"],
    })


@pytest.fixture
def in_memory_db(sample_incidents_df):
    """Create an in-memory SQLite DB with test data."""
    conn = sqlite3.connect(":memory:")
    df = sample_incidents_df.copy()
    df["created_date"] = pd.to_datetime(df["created_date"])
    df["sla_deadline"] = pd.to_datetime(df["sla_deadline"])
    df["resolved_date"] = pd.to_datetime(df["resolved_date"], errors="coerce")

    # Add derived fields
    from datetime import datetime
    df["sla_breached"] = df.apply(
        lambda r: 1 if pd.notnull(r["resolved_date"]) and r["resolved_date"] > r["sla_deadline"]
        else (1 if pd.isnull(r["resolved_date"]) and datetime.now() > r["sla_deadline"] else 0),
        axis=1
    )
    df["days_to_resolve"] = (df["resolved_date"] - df["created_date"]).dt.days
    df["is_open"] = df["status"].isin(["Open", "In Progress", "Escalated"]).astype(int)
    df["severity_score"] = df["severity"].map({"Low": 1, "Medium": 2, "High": 3, "Critical": 4})
    df["month"] = df["created_date"].dt.to_period("M").astype(str)
    df["quarter"] = df["created_date"].dt.to_period("Q").astype(str)
    df["ingested_at"] = "2024-01-01T00:00:00"

    df.to_sql("incidents", conn, if_exists="replace", index=False)
    yield conn
    conn.close()


# ---------------------------------------------------------------
# Tests: Data Shape
# ---------------------------------------------------------------

class TestDataIngestion:

    def test_incidents_loaded(self, in_memory_db):
        df = pd.read_sql("SELECT * FROM incidents", in_memory_db)
        assert len(df) == 3

    def test_incident_columns_present(self, in_memory_db):
        df = pd.read_sql("SELECT * FROM incidents", in_memory_db)
        required_cols = ["incident_id", "severity", "status", "department", "sla_breached"]
        for col in required_cols:
            assert col in df.columns, f"Missing column: {col}"

    def test_no_duplicate_incident_ids(self, in_memory_db):
        df = pd.read_sql("SELECT incident_id FROM incidents", in_memory_db)
        assert df["incident_id"].duplicated().sum() == 0


# ---------------------------------------------------------------
# Tests: Transformations
# ---------------------------------------------------------------

class TestTransformations:

    def test_severity_score_mapping(self, in_memory_db):
        df = pd.read_sql("SELECT severity, severity_score FROM incidents", in_memory_db)
        mapping = {"Low": 1, "Medium": 2, "High": 3, "Critical": 4}
        for _, row in df.iterrows():
            assert row["severity_score"] == mapping[row["severity"]]

    def test_is_open_flag(self, in_memory_db):
        df = pd.read_sql("SELECT status, is_open FROM incidents", in_memory_db)
        open_statuses = {"Open", "In Progress", "Escalated"}
        for _, row in df.iterrows():
            expected = 1 if row["status"] in open_statuses else 0
            assert row["is_open"] == expected

    def test_days_to_resolve_positive(self, in_memory_db):
        df = pd.read_sql(
            "SELECT days_to_resolve FROM incidents WHERE days_to_resolve IS NOT NULL", in_memory_db)
        assert (df["days_to_resolve"] >= 0).all()


# ---------------------------------------------------------------
# Tests: KPI Queries
# ---------------------------------------------------------------

class TestKPIQueries:

    def test_sla_breach_rate_in_range(self, in_memory_db):
        result = pd.read_sql(
            "SELECT ROUND(100.0*SUM(sla_breached)/COUNT(*),2) AS v FROM incidents", in_memory_db)
        rate = result["v"].iloc[0]
        assert 0 <= rate <= 100

    def test_department_count_matches(self, in_memory_db):
        result = pd.read_sql(
            "SELECT COUNT(DISTINCT department) AS v FROM incidents", in_memory_db)
        assert result["v"].iloc[0] == 3

    def test_escalation_rate_in_range(self, in_memory_db):
        result = pd.read_sql(
            "SELECT ROUND(100.0*SUM(escalated)/COUNT(*),2) AS v FROM incidents", in_memory_db)
        rate = result["v"].iloc[0]
        assert 0 <= rate <= 100


# ---------------------------------------------------------------
# Tests: Validation Framework
# ---------------------------------------------------------------

class TestValidationResult:

    def test_passed_checks_counted(self):
        vr = ValidationResult()
        vr.add("Check 1", True)
        vr.add("Check 2", True)
        vr.add("Check 3", False)
        assert vr.passed == 2
        assert vr.failed == 1

    def test_quality_score_calculation(self):
        vr = ValidationResult()
        for i in range(8):
            vr.add(f"Check {i}", True)
        for i in range(2):
            vr.add(f"Check fail {i}", False)
        summary = vr.summary()
        assert summary["data_quality_score"] == 80.0

    def test_empty_validation_result(self):
        vr = ValidationResult()
        summary = vr.summary()
        assert summary["data_quality_score"] == 0
        assert summary["total_checks"] == 0
