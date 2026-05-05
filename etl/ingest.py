"""
etl/ingest.py
Data ingestion module — loads raw CSV files into SQLite database.
"""

import pandas as pd
import sqlite3
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = "data/processed/risk_dashboard.db"
RAW_DIR = "data/raw"


def get_connection():
    """Create and return a SQLite database connection."""
    os.makedirs("data/processed", exist_ok=True)
    return sqlite3.connect(DB_PATH)


def ingest_csv(file_name: str, table_name: str, conn: sqlite3.Connection) -> int:
    """
    Load a CSV file from data/raw into a SQLite table.
    Returns the number of rows ingested.
    """
    file_path = os.path.join(RAW_DIR, file_name)
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        raise FileNotFoundError(f"Raw data file missing: {file_path}")

    df = pd.read_csv(file_path)
    df["ingested_at"] = datetime.now().isoformat()

    df.to_sql(table_name, conn, if_exists="replace", index=False)
    logger.info(f"✅ Ingested {len(df)} rows into table '{table_name}' from {file_name}")
    return len(df)


def ingest_all():
    """Run ingestion for all raw data files."""
    logger.info("Starting data ingestion...")
    conn = get_connection()
    total = 0

    sources = [
        ("incidents.csv", "raw_incidents"),
        ("compliance_checks.csv", "raw_compliance"),
        ("pipeline_runs.csv", "raw_pipeline_runs"),
    ]

    for file_name, table_name in sources:
        try:
            rows = ingest_csv(file_name, table_name, conn)
            total += rows
        except FileNotFoundError as e:
            logger.warning(str(e))

    conn.close()
    logger.info(f"Ingestion complete. Total rows loaded: {total}")
    return total


if __name__ == "__main__":
    ingest_all()
