"""
etl/transform.py
Data transformation module — cleans, normalizes, and enriches raw data.
"""

import pandas as pd
import sqlite3
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = "data/processed/risk_dashboard.db"


def get_connection():
    return sqlite3.connect(DB_PATH)


def transform_incidents(conn: sqlite3.Connection):
    """Clean and enrich incident data."""
    logger.info("Transforming incidents...")
    df = pd.read_sql("SELECT * FROM raw_incidents", conn)

    # Parse dates
    df["created_date"] = pd.to_datetime(df["created_date"])
    df["sla_deadline"] = pd.to_datetime(df["sla_deadline"])
    df["resolved_date"] = pd.to_datetime(df["resolved_date"], errors="coerce")

    # Derived fields
    df["sla_breached"] = df.apply(
        lambda r: 1 if pd.notnull(r["resolved_date"]) and r["resolved_date"] > r["sla_deadline"]
        else (1 if pd.isnull(r["resolved_date"]) and datetime.now() > r["sla_deadline"] else 0),
        axis=1
    )
    df["days_to_resolve"] = (df["resolved_date"] - df["created_date"]).dt.days
    df["is_open"] = df["status"].isin(["Open", "In Progress", "Escalated"]).astype(int)
    df["month"] = df["created_date"].dt.to_period("M").astype(str)
    df["quarter"] = df["created_date"].dt.to_period("Q").astype(str)
    df["severity_score"] = df["severity"].map({"Low": 1, "Medium": 2, "High": 3, "Critical": 4})

    df.to_sql("incidents", conn, if_exists="replace", index=False)
    logger.info(f"✅ Transformed {len(df)} incident records → table 'incidents'")
    return df


def transform_compliance(conn: sqlite3.Connection):
    """Clean and enrich compliance check data."""
    logger.info("Transforming compliance checks...")
    df = pd.read_sql("SELECT * FROM raw_compliance", conn)

    df["check_date"] = pd.to_datetime(df["check_date"])
    df["month"] = df["check_date"].dt.to_period("M").astype(str)
    df["passed"] = (df["result"] == "Pass").astype(int)
    df["normalized_score"] = df["score"] / 100

    df.to_sql("compliance_checks", conn, if_exists="replace", index=False)
    logger.info(f"✅ Transformed {len(df)} compliance records → table 'compliance_checks'")
    return df


def transform_pipeline_runs(conn: sqlite3.Connection):
    """Clean pipeline run logs."""
    logger.info("Transforming pipeline run logs...")
    df = pd.read_sql("SELECT * FROM raw_pipeline_runs", conn)

    df["run_date"] = pd.to_datetime(df["run_date"])
    df["month"] = df["run_date"].dt.to_period("M").astype(str)
    df["success"] = (df["status"] == "Success").astype(int)

    df.to_sql("pipeline_runs", conn, if_exists="replace", index=False)
    logger.info(f"✅ Transformed {len(df)} pipeline run records → table 'pipeline_runs'")
    return df


def transform_all():
    """Run all transformation steps."""
    conn = get_connection()
    transform_incidents(conn)
    transform_compliance(conn)
    transform_pipeline_runs(conn)
    conn.close()
    logger.info("All transformations complete.")


if __name__ == "__main__":
    transform_all()
