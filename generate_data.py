"""
generate_data.py
Generates realistic synthetic risk & compliance data for the dashboard pipeline.
Run this first before main.py
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import os

fake = Faker()
random.seed(42)
np.random.seed(42)

os.makedirs("data/raw", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)

DEPARTMENTS = ["Risk", "Legal", "Operations", "Finance", "Compliance", "Technology", "HR"]
INCIDENT_TYPES = ["Policy Violation", "SLA Breach", "Data Anomaly", "Audit Finding",
                  "Process Failure", "Vendor Risk", "Regulatory Breach", "Fraud Alert"]
SEVERITY = ["Low", "Medium", "High", "Critical"]
STATUS = ["Open", "In Progress", "Escalated", "Resolved", "Closed"]
REGIONS = ["North", "South", "East", "West", "Central"]


def generate_incidents(n=500):
    """Generate risk incident records."""
    records = []
    start_date = datetime(2024, 1, 1)

    for i in range(n):
        created = start_date + timedelta(days=random.randint(0, 365))
        sla_days = random.choice([3, 5, 7, 10, 14])
        sla_deadline = created + timedelta(days=sla_days)
        resolved_days = random.randint(1, 20)
        resolved_date = created + timedelta(days=resolved_days) if random.random() > 0.2 else None

        severity = random.choices(SEVERITY, weights=[30, 40, 20, 10])[0]
        status = random.choices(STATUS, weights=[15, 20, 10, 35, 20])[0]
        if resolved_date:
            status = random.choice(["Resolved", "Closed"])

        records.append({
            "incident_id": f"INC-{10000 + i}",
            "created_date": created.strftime("%Y-%m-%d"),
            "sla_deadline": sla_deadline.strftime("%Y-%m-%d"),
            "resolved_date": resolved_date.strftime("%Y-%m-%d") if resolved_date else None,
            "incident_type": random.choice(INCIDENT_TYPES),
            "severity": severity,
            "status": status,
            "department": random.choice(DEPARTMENTS),
            "region": random.choice(REGIONS),
            "assigned_to": fake.name(),
            "escalated": random.random() < 0.15,
            "repeat_incident": random.random() < 0.12,
            "estimated_cost_usd": round(random.uniform(500, 50000), 2),
            "description": fake.sentence(nb_words=10),
        })

    df = pd.DataFrame(records)
    df.to_csv("data/raw/incidents.csv", index=False)
    print(f"✅ Generated {n} incident records → data/raw/incidents.csv")
    return df


def generate_compliance_checks(n=300):
    """Generate compliance audit check records."""
    records = []
    start_date = datetime(2024, 1, 1)

    for i in range(n):
        check_date = start_date + timedelta(days=random.randint(0, 365))
        passed = random.random() > 0.18

        records.append({
            "check_id": f"CHK-{20000 + i}",
            "check_date": check_date.strftime("%Y-%m-%d"),
            "department": random.choice(DEPARTMENTS),
            "policy_area": random.choice(["Data Privacy", "Financial Controls", "Vendor Management",
                                          "Access Control", "Reporting Standards", "AML", "KYC"]),
            "result": "Pass" if passed else "Fail",
            "score": round(random.uniform(60, 100) if passed else random.uniform(20, 59), 1),
            "auditor": fake.name(),
            "findings_count": 0 if passed else random.randint(1, 5),
            "repeat_finding": random.random() < 0.1,
        })

    df = pd.DataFrame(records)
    df.to_csv("data/raw/compliance_checks.csv", index=False)
    print(f"✅ Generated {n} compliance check records → data/raw/compliance_checks.csv")
    return df


def generate_pipeline_runs(n=200):
    """Generate ETL pipeline run logs."""
    records = []
    start_date = datetime(2024, 1, 1)

    for i in range(n):
        run_date = start_date + timedelta(days=random.randint(0, 365))
        success = random.random() > 0.08
        records_processed = random.randint(500, 25000)

        records.append({
            "run_id": f"RUN-{30000 + i}",
            "run_date": run_date.strftime("%Y-%m-%d"),
            "pipeline_name": random.choice(["incidents_etl", "compliance_etl", "kpi_aggregation",
                                            "report_generation", "data_validation"]),
            "status": "Success" if success else "Failed",
            "records_processed": records_processed if success else random.randint(0, records_processed),
            "duration_seconds": round(random.uniform(5, 300), 2),
            "error_message": None if success else random.choice([
                "Connection timeout", "Schema mismatch", "Null constraint violation",
                "Memory overflow", "Source file not found"
            ]),
        })

    df = pd.DataFrame(records)
    df.to_csv("data/raw/pipeline_runs.csv", index=False)
    print(f"✅ Generated {n} pipeline run records → data/raw/pipeline_runs.csv")
    return df


if __name__ == "__main__":
    print("🔄 Generating synthetic risk & compliance data...\n")
    generate_incidents(500)
    generate_compliance_checks(300)
    generate_pipeline_runs(200)
    print("\n✅ All data generated successfully. Run `python main.py` next.")
