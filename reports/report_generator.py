"""
reports/report_generator.py
Automated report generation — outputs Excel + chart reports from KPI data.
"""

import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = "data/processed/risk_dashboard.db"
REPORT_DIR = "reports"
os.makedirs(REPORT_DIR, exist_ok=True)

sns.set_theme(style="whitegrid", palette="muted")


def get_connection():
    return sqlite3.connect(DB_PATH)


def compute_kpis(conn) -> dict:
    """Compute all KPI values and return as a dictionary."""
    kpis = {}

    # KPI 1: SLA Breach Rate
    r = pd.read_sql("SELECT ROUND(100.0*SUM(sla_breached)/COUNT(*),2) AS v FROM incidents", conn)
    kpis["sla_breach_rate_pct"] = r["v"].iloc[0]

    # KPI 2: Total incidents
    r = pd.read_sql("SELECT COUNT(*) AS v FROM incidents", conn)
    kpis["total_incidents"] = int(r["v"].iloc[0])

    # KPI 3: Compliance score
    r = pd.read_sql("SELECT ROUND(AVG(score),2) AS v FROM compliance_checks", conn)
    kpis["avg_compliance_score"] = r["v"].iloc[0]

    # KPI 4: Pipeline success rate
    r = pd.read_sql("SELECT ROUND(100.0*SUM(success)/COUNT(*),2) AS v FROM pipeline_runs", conn)
    kpis["pipeline_success_rate_pct"] = r["v"].iloc[0]

    # KPI 5: MTTR
    r = pd.read_sql("SELECT ROUND(AVG(days_to_resolve),1) AS v FROM incidents WHERE days_to_resolve IS NOT NULL", conn)
    kpis["mean_time_to_resolve_days"] = r["v"].iloc[0]

    # KPI 6: Escalation rate
    r = pd.read_sql("SELECT ROUND(100.0*SUM(escalated)/COUNT(*),2) AS v FROM incidents", conn)
    kpis["escalation_rate_pct"] = r["v"].iloc[0]

    # KPI 7: Policy violation rate
    r = pd.read_sql("SELECT ROUND(100.0*SUM(CASE WHEN result='Fail' THEN 1 ELSE 0 END)/COUNT(*),2) AS v FROM compliance_checks", conn)
    kpis["policy_violation_rate_pct"] = r["v"].iloc[0]

    # KPI 8: Repeat incident rate
    r = pd.read_sql("SELECT ROUND(100.0*SUM(repeat_incident)/COUNT(*),2) AS v FROM incidents", conn)
    kpis["repeat_incident_rate_pct"] = r["v"].iloc[0]

    # KPI 9: Avg resolution cost
    r = pd.read_sql("SELECT ROUND(AVG(estimated_cost_usd),2) AS v FROM incidents WHERE status IN ('Resolved','Closed')", conn)
    kpis["avg_resolution_cost_usd"] = r["v"].iloc[0]

    return kpis


def generate_charts(conn):
    """Generate and save visualization charts."""
    logger.info("Generating charts...")

    fig, axes = plt.subplots(2, 3, figsize=(18, 10))
    fig.suptitle("Operational Risk Intelligence Dashboard", fontsize=16, fontweight="bold")

    # Chart 1: Incidents by Month
    df_monthly = pd.read_sql(
        "SELECT month, COUNT(*) as count FROM incidents GROUP BY month ORDER BY month", conn)
    axes[0, 0].bar(df_monthly["month"], df_monthly["count"], color="steelblue")
    axes[0, 0].set_title("Monthly Incident Volume")
    axes[0, 0].set_xlabel("Month")
    axes[0, 0].set_ylabel("Incident Count")
    axes[0, 0].tick_params(axis="x", rotation=45)

    # Chart 2: Severity Distribution
    df_sev = pd.read_sql(
        "SELECT severity, COUNT(*) as count FROM incidents GROUP BY severity", conn)
    colors = {"Low": "#2ecc71", "Medium": "#f39c12", "High": "#e74c3c", "Critical": "#8e44ad"}
    bar_colors = [colors.get(s, "gray") for s in df_sev["severity"]]
    axes[0, 1].bar(df_sev["severity"], df_sev["count"], color=bar_colors)
    axes[0, 1].set_title("Incidents by Severity")
    axes[0, 1].set_ylabel("Count")

    # Chart 3: Department Risk Index
    df_dept = pd.read_sql("""
        SELECT department,
            ROUND((AVG(severity_score)*0.4 + 100.0*SUM(sla_breached)/COUNT(*)*0.3 +
                   100.0*SUM(escalated)/COUNT(*)*0.3), 2) AS risk_index
        FROM incidents GROUP BY department ORDER BY risk_index DESC
    """, conn)
    axes[0, 2].barh(df_dept["department"], df_dept["risk_index"], color="coral")
    axes[0, 2].set_title("Department Risk Index")
    axes[0, 2].set_xlabel("Risk Score")

    # Chart 4: Compliance Pass Rate by Department
    df_comp = pd.read_sql("""
        SELECT department, ROUND(100.0*SUM(passed)/COUNT(*),1) AS pass_rate
        FROM compliance_checks GROUP BY department ORDER BY pass_rate
    """, conn)
    axes[1, 0].barh(df_comp["department"], df_comp["pass_rate"], color="mediumseagreen")
    axes[1, 0].set_title("Compliance Pass Rate by Department")
    axes[1, 0].set_xlabel("Pass Rate (%)")
    axes[1, 0].set_xlim(0, 100)

    # Chart 5: Incident Type Breakdown
    df_type = pd.read_sql(
        "SELECT incident_type, COUNT(*) as count FROM incidents GROUP BY incident_type ORDER BY count DESC LIMIT 6", conn)
    axes[1, 1].pie(df_type["count"], labels=df_type["incident_type"],
                   autopct="%1.1f%%", startangle=90)
    axes[1, 1].set_title("Incident Type Distribution")

    # Chart 6: SLA Breach by Department
    df_sla = pd.read_sql("""
        SELECT department, ROUND(100.0*SUM(sla_breached)/COUNT(*),1) AS breach_rate
        FROM incidents GROUP BY department ORDER BY breach_rate DESC
    """, conn)
    axes[1, 2].bar(df_sla["department"], df_sla["breach_rate"], color="tomato")
    axes[1, 2].set_title("SLA Breach Rate by Department")
    axes[1, 2].set_ylabel("Breach Rate (%)")
    axes[1, 2].tick_params(axis="x", rotation=45)

    plt.tight_layout()
    chart_path = os.path.join(REPORT_DIR, "risk_dashboard_charts.png")
    plt.savefig(chart_path, dpi=150, bbox_inches="tight")
    plt.close()
    logger.info(f"✅ Charts saved → {chart_path}")
    return chart_path


def generate_excel_report(conn, kpis: dict):
    """Generate a multi-sheet Excel report."""
    logger.info("Generating Excel report...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    output_path = os.path.join(REPORT_DIR, f"risk_report_{timestamp}.xlsx")

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        # Sheet 1: KPI Summary
        kpi_df = pd.DataFrame([
            {"KPI": "SLA Breach Rate", "Value": f"{kpis['sla_breach_rate_pct']}%"},
            {"KPI": "Total Incidents", "Value": kpis["total_incidents"]},
            {"KPI": "Avg Compliance Score", "Value": kpis["avg_compliance_score"]},
            {"KPI": "Pipeline Success Rate", "Value": f"{kpis['pipeline_success_rate_pct']}%"},
            {"KPI": "Mean Time to Resolve", "Value": f"{kpis['mean_time_to_resolve_days']} days"},
            {"KPI": "Escalation Rate", "Value": f"{kpis['escalation_rate_pct']}%"},
            {"KPI": "Policy Violation Rate", "Value": f"{kpis['policy_violation_rate_pct']}%"},
            {"KPI": "Repeat Incident Rate", "Value": f"{kpis['repeat_incident_rate_pct']}%"},
            {"KPI": "Avg Resolution Cost", "Value": f"${kpis['avg_resolution_cost_usd']:,.2f}"},
        ])
        kpi_df.to_excel(writer, sheet_name="KPI Summary", index=False)

        # Sheet 2: Incidents
        incidents_df = pd.read_sql("SELECT * FROM incidents", conn)
        incidents_df.to_excel(writer, sheet_name="Incidents", index=False)

        # Sheet 3: Compliance
        compliance_df = pd.read_sql("SELECT * FROM compliance_checks", conn)
        compliance_df.to_excel(writer, sheet_name="Compliance Checks", index=False)

        # Sheet 4: Department Risk
        dept_df = pd.read_sql("""
            SELECT department,
                COUNT(*) AS total_incidents,
                ROUND(AVG(severity_score),2) AS avg_severity,
                SUM(sla_breached) AS sla_breaches,
                SUM(escalated) AS escalations,
                ROUND((AVG(severity_score)*0.4 + 100.0*SUM(sla_breached)/COUNT(*)*0.3 +
                       100.0*SUM(escalated)/COUNT(*)*0.3), 2) AS risk_index
            FROM incidents GROUP BY department ORDER BY risk_index DESC
        """, conn)
        dept_df.to_excel(writer, sheet_name="Department Risk", index=False)

    logger.info(f"✅ Excel report saved → {output_path}")
    return output_path


def generate_all():
    """Run all report generation steps."""
    conn = get_connection()
    kpis = compute_kpis(conn)
    generate_charts(conn)
    report_path = generate_excel_report(conn, kpis)
    conn.close()

    logger.info("\n📊 KPI SUMMARY:")
    for k, v in kpis.items():
        logger.info(f"  {k}: {v}")

    return report_path


if __name__ == "__main__":
    generate_all()
