# Power BI Dashboard Setup Guide

This guide explains how to connect the SQLite database to Power BI and recreate the dashboard with the correct DAX measures.

---

## Step 1: Connect Power BI to SQLite

1. Open **Power BI Desktop**
2. Click **Get Data → More → ODBC**
3. Install SQLite ODBC driver if needed: https://www.ch-werner.de/sqliteodbc/
4. Point to: `data/processed/risk_dashboard.db`
5. Import tables: `incidents`, `compliance_checks`, `pipeline_runs`

---

## Step 2: Data Model

Set relationships:
- `incidents[department]` ↔ `compliance_checks[department]` (Many-to-Many)
- `incidents[month]` ↔ `compliance_checks[month]` (Many-to-Many)

---

## Step 3: DAX Measures

Paste these DAX measures into Power BI's **New Measure** dialog:

### KPI Cards

```dax
-- Total Incidents
Total Incidents = COUNTROWS(incidents)

-- SLA Breach Rate
SLA Breach Rate % =
DIVIDE(
    COUNTROWS(FILTER(incidents, incidents[sla_breached] = 1)),
    COUNTROWS(incidents),
    0
) * 100

-- Compliance Score
Avg Compliance Score =
AVERAGE(compliance_checks[score])

-- Mean Time to Resolve
MTTR Days =
AVERAGEX(
    FILTER(incidents, NOT ISBLANK(incidents[days_to_resolve])),
    incidents[days_to_resolve]
)

-- Escalation Rate
Escalation Rate % =
DIVIDE(
    COUNTROWS(FILTER(incidents, incidents[escalated] = 1)),
    COUNTROWS(incidents),
    0
) * 100

-- Pipeline Success Rate
Pipeline Success Rate % =
DIVIDE(
    COUNTROWS(FILTER(pipeline_runs, pipeline_runs[success] = 1)),
    COUNTROWS(pipeline_runs),
    0
) * 100

-- Policy Violation Rate
Policy Violation Rate % =
DIVIDE(
    COUNTROWS(FILTER(compliance_checks, compliance_checks[result] = "Fail")),
    COUNTROWS(compliance_checks),
    0
) * 100

-- Repeat Incident Rate
Repeat Incident Rate % =
DIVIDE(
    COUNTROWS(FILTER(incidents, incidents[repeat_incident] = 1)),
    COUNTROWS(incidents),
    0
) * 100

-- Avg Resolution Cost
Avg Resolution Cost =
AVERAGEX(
    FILTER(incidents, incidents[status] IN {"Resolved", "Closed"}),
    incidents[estimated_cost_usd]
)

-- Department Risk Index
Dept Risk Index =
VAR AvgSeverity = AVERAGE(incidents[severity_score])
VAR SLABreachRate = DIVIDE(COUNTROWS(FILTER(incidents, incidents[sla_breached]=1)), COUNTROWS(incidents), 0)
VAR EscalationRate = DIVIDE(COUNTROWS(FILTER(incidents, incidents[escalated]=1)), COUNTROWS(incidents), 0)
RETURN
    (AvgSeverity * 0.4) + (SLABreachRate * 100 * 0.3) + (EscalationRate * 100 * 0.3)

-- MoM Change
MoM Incident Change % =
VAR CurrentMonth = MAX(incidents[month])
VAR CurrentCount = CALCULATE(COUNTROWS(incidents), incidents[month] = CurrentMonth)
VAR PrevCount = CALCULATE(COUNTROWS(incidents), incidents[month] = PREVIOUSMONTH(MAX(incidents[created_date])))
RETURN
    DIVIDE(CurrentCount - PrevCount, PrevCount, 0) * 100
```

---

## Step 4: Dashboard Pages

### Page 1 — Risk Overview
- KPI Cards: Total Incidents, SLA Breach Rate, MTTR, Escalation Rate
- Line Chart: Monthly Incident Volume (month on X-axis, count on Y)
- Bar Chart: Incidents by Severity (color-coded: green/orange/red/purple)
- Slicer: Department, Date Range

### Page 2 — Compliance Analytics
- KPI Cards: Avg Compliance Score, Policy Violation Rate, Audit Finding Rate
- Bar Chart: Pass Rate by Department (horizontal)
- Heatmap Matrix: Department × Policy Area → avg score
- Slicer: Month, Policy Area

### Page 3 — Incident Tracker
- Table: Full incidents list with conditional formatting on severity
- Bar Chart: Incident Type Distribution
- Scatter Plot: Days to Resolve vs Estimated Cost (by severity color)
- Slicer: Status, Region, Department

### Page 4 — Executive Summary
- KPI Cards: All 15 KPIs in card layout
- Line Chart: MoM Risk Trend
- Gauge: Overall Compliance Score (0–100)
- Bar Chart: Department Risk Index ranking

---

## Step 5: Formatting Tips

- Use **Red-Yellow-Green** conditional formatting for KPI cards
- Set SLA Breach Rate threshold: Green < 10%, Yellow 10–20%, Red > 20%
- Set Compliance Score threshold: Green > 80, Yellow 60–80, Red < 60
- Add company logo and report date to header
- Enable **Auto-refresh** if connecting to live database
