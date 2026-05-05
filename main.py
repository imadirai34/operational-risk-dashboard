"""
main.py
Entry point — runs the full Operational Risk Intelligence Dashboard pipeline.

Usage:
    python main.py
"""

import sys
import os
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def check_data_exists():
    """Check if raw data files exist, prompt user to generate if not."""
    required = ["data/raw/incidents.csv", "data/raw/compliance_checks.csv", "data/raw/pipeline_runs.csv"]
    missing = [f for f in required if not os.path.exists(f)]
    if missing:
        logger.warning("Raw data files not found. Generating synthetic data first...")
        import generate_data
        generate_data.generate_incidents()
        generate_data.generate_compliance_checks()
        generate_data.generate_pipeline_runs()
    return True


def main():
    logger.info("=" * 60)
    logger.info("  OPERATIONAL RISK INTELLIGENCE DASHBOARD")
    logger.info("  Author: Aditya Rai | Risk & Compliance Specialist")
    logger.info("=" * 60)

    # Step 0: Ensure data exists
    check_data_exists()

    # Step 1–3: Run ETL pipeline
    from etl.pipeline import run_pipeline
    pipeline_result = run_pipeline()

    # Step 4: Generate reports and charts
    logger.info("\n📊 STEP 4: REPORT GENERATION")
    from reports.report_generator import generate_all
    report_path = generate_all()

    logger.info("\n" + "=" * 60)
    logger.info("✅ ALL STEPS COMPLETE")
    logger.info(f"   Database  : data/processed/risk_dashboard.db")
    logger.info(f"   Report    : {report_path}")
    logger.info(f"   Charts    : reports/risk_dashboard_charts.png")
    logger.info("=" * 60)
    logger.info("\n📌 Next Steps:")
    logger.info("   1. Open the Excel report for KPI details")
    logger.info("   2. View risk_dashboard_charts.png for visualizations")
    logger.info("   3. Connect data/processed/risk_dashboard.db to Power BI")
    logger.info("      → Use ODBC/SQLite connector in Power BI Desktop")
    logger.info("   4. See dashboard/powerbi_template.md for DAX measures")


if __name__ == "__main__":
    main()
