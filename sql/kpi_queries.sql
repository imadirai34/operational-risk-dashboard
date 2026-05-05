-- =============================================================
-- kpi_queries.sql
-- 15+ KPI queries for the Operational Risk Intelligence Dashboard
-- Run against: SQLite / PostgreSQL (risk_dashboard.db)
-- =============================================================


-- ---------------------------------------------------------------
-- KPI 1: SLA Breach Rate
-- % of all incidents that breached their SLA deadline
-- ---------------------------------------------------------------
SELECT
    ROUND(100.0 * SUM(sla_breached) / COUNT(*), 2) AS sla_breach_rate_pct
FROM incidents;


-- ---------------------------------------------------------------
-- KPI 2: Risk Incident Count (by month)
-- Total flagged risk events per month
-- ---------------------------------------------------------------
SELECT
    month,
    COUNT(*) AS incident_count
FROM incidents
GROUP BY month
ORDER BY month;


-- ---------------------------------------------------------------
-- KPI 3: Compliance Score (overall)
-- Weighted average compliance score across all checks
-- ---------------------------------------------------------------
SELECT
    ROUND(AVG(score), 2) AS overall_compliance_score,
    ROUND(100.0 * SUM(passed) / COUNT(*), 2) AS pass_rate_pct
FROM compliance_checks;


-- ---------------------------------------------------------------
-- KPI 4: Data Quality Score
-- % of pipeline runs completing without errors
-- ---------------------------------------------------------------
SELECT
    ROUND(100.0 * SUM(success) / COUNT(*), 2) AS pipeline_success_rate_pct,
    COUNT(*) AS total_runs,
    SUM(success) AS successful_runs,
    COUNT(*) - SUM(success) AS failed_runs
FROM pipeline_runs;


-- ---------------------------------------------------------------
-- KPI 5: Mean Time to Resolve (MTTR)
-- Average days to resolve an incident (resolved only)
-- ---------------------------------------------------------------
SELECT
    ROUND(AVG(days_to_resolve), 1) AS mean_time_to_resolve_days,
    MIN(days_to_resolve) AS min_days,
    MAX(days_to_resolve) AS max_days
FROM incidents
WHERE days_to_resolve IS NOT NULL;


-- ---------------------------------------------------------------
-- KPI 6: High Risk Case Volume
-- Count of Critical + High severity incidents
-- ---------------------------------------------------------------
SELECT
    severity,
    COUNT(*) AS case_count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM incidents), 2) AS pct_of_total
FROM incidents
WHERE severity IN ('High', 'Critical')
GROUP BY severity;


-- ---------------------------------------------------------------
-- KPI 7: Open vs Closed Ratio
-- Ratio of unresolved to resolved cases
-- ---------------------------------------------------------------
SELECT
    SUM(is_open) AS open_cases,
    COUNT(*) - SUM(is_open) AS closed_cases,
    ROUND(1.0 * SUM(is_open) / NULLIF(COUNT(*) - SUM(is_open), 0), 2) AS open_closed_ratio
FROM incidents;


-- ---------------------------------------------------------------
-- KPI 8: Escalation Rate
-- % of incidents that were escalated
-- ---------------------------------------------------------------
SELECT
    ROUND(100.0 * SUM(escalated) / COUNT(*), 2) AS escalation_rate_pct,
    SUM(escalated) AS escalated_count,
    COUNT(*) AS total_incidents
FROM incidents;


-- ---------------------------------------------------------------
-- KPI 9: Policy Violation Rate
-- % of compliance checks that failed (policy violations)
-- ---------------------------------------------------------------
SELECT
    ROUND(100.0 * SUM(CASE WHEN result = 'Fail' THEN 1 ELSE 0 END) / COUNT(*), 2) AS violation_rate_pct,
    COUNT(*) AS total_checks,
    SUM(CASE WHEN result = 'Fail' THEN 1 ELSE 0 END) AS violations
FROM compliance_checks;


-- ---------------------------------------------------------------
-- KPI 10: Audit Finding Rate
-- Audit issues per 100 reviews
-- ---------------------------------------------------------------
SELECT
    ROUND(100.0 * SUM(findings_count) / COUNT(*), 2) AS findings_per_100_audits,
    SUM(findings_count) AS total_findings,
    COUNT(*) AS total_audits
FROM compliance_checks;


-- ---------------------------------------------------------------
-- KPI 11: Repeat Incident Rate
-- % of incidents recurring within 30 days
-- ---------------------------------------------------------------
SELECT
    ROUND(100.0 * SUM(repeat_incident) / COUNT(*), 2) AS repeat_incident_rate_pct,
    SUM(repeat_incident) AS repeat_incidents,
    COUNT(*) AS total_incidents
FROM incidents;


-- ---------------------------------------------------------------
-- KPI 12: Average Resolution Cost
-- Estimated cost per resolved incident
-- ---------------------------------------------------------------
SELECT
    ROUND(AVG(estimated_cost_usd), 2) AS avg_resolution_cost_usd,
    ROUND(SUM(estimated_cost_usd), 2) AS total_cost_usd,
    COUNT(*) AS resolved_incidents
FROM incidents
WHERE status IN ('Resolved', 'Closed');


-- ---------------------------------------------------------------
-- KPI 13: Department Risk Index
-- Composite risk score per department (weighted by severity)
-- ---------------------------------------------------------------
SELECT
    department,
    COUNT(*) AS total_incidents,
    ROUND(AVG(severity_score), 2) AS avg_severity_score,
    SUM(sla_breached) AS sla_breaches,
    SUM(escalated) AS escalations,
    ROUND(
        (AVG(severity_score) * 0.4) +
        (100.0 * SUM(sla_breached) / COUNT(*) * 0.3) +
        (100.0 * SUM(escalated) / COUNT(*) * 0.3),
        2
    ) AS department_risk_index
FROM incidents
GROUP BY department
ORDER BY department_risk_index DESC;


-- ---------------------------------------------------------------
-- KPI 14: Monthly Trend Score
-- Month-over-month change in incident volume
-- ---------------------------------------------------------------
SELECT
    month,
    COUNT(*) AS incident_count,
    COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY month) AS mom_change,
    ROUND(
        100.0 * (COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY month))
        / NULLIF(LAG(COUNT(*)) OVER (ORDER BY month), 0),
        1
    ) AS mom_change_pct
FROM incidents
GROUP BY month
ORDER BY month;


-- ---------------------------------------------------------------
-- KPI 15: Compliance by Department
-- Pass rate and average score per department
-- ---------------------------------------------------------------
SELECT
    department,
    COUNT(*) AS total_checks,
    SUM(passed) AS passed_checks,
    ROUND(100.0 * SUM(passed) / COUNT(*), 2) AS pass_rate_pct,
    ROUND(AVG(score), 2) AS avg_score
FROM compliance_checks
GROUP BY department
ORDER BY pass_rate_pct ASC;


-- ---------------------------------------------------------------
-- KPI 16: Incident Breakdown by Type
-- ---------------------------------------------------------------
SELECT
    incident_type,
    COUNT(*) AS count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM incidents), 2) AS pct_share,
    ROUND(AVG(estimated_cost_usd), 2) AS avg_cost_usd
FROM incidents
GROUP BY incident_type
ORDER BY count DESC;


-- ---------------------------------------------------------------
-- KPI 17: Regional Risk Distribution
-- ---------------------------------------------------------------
SELECT
    region,
    COUNT(*) AS incident_count,
    SUM(sla_breached) AS sla_breaches,
    ROUND(AVG(severity_score), 2) AS avg_severity
FROM incidents
GROUP BY region
ORDER BY incident_count DESC;
