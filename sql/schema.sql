-- =============================================================
-- schema.sql
-- Database schema for Operational Risk Intelligence Dashboard
-- Compatible with: SQLite, PostgreSQL
-- =============================================================

-- ---------------------------------------------------------------
-- INCIDENTS TABLE
-- Core risk incident records
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS incidents (
    incident_id         TEXT PRIMARY KEY,
    created_date        DATE NOT NULL,
    sla_deadline        DATE NOT NULL,
    resolved_date       DATE,
    incident_type       TEXT NOT NULL,
    severity            TEXT NOT NULL CHECK (severity IN ('Low', 'Medium', 'High', 'Critical')),
    status              TEXT NOT NULL CHECK (status IN ('Open', 'In Progress', 'Escalated', 'Resolved', 'Closed')),
    department          TEXT NOT NULL,
    region              TEXT,
    assigned_to         TEXT,
    escalated           INTEGER DEFAULT 0,       -- 0/1 boolean
    repeat_incident     INTEGER DEFAULT 0,       -- 0/1 boolean
    estimated_cost_usd  REAL,
    description         TEXT,
    sla_breached        INTEGER DEFAULT 0,       -- derived: 0/1
    days_to_resolve     INTEGER,                 -- derived
    is_open             INTEGER DEFAULT 0,       -- derived: 0/1
    severity_score      INTEGER,                 -- derived: 1-4
    month               TEXT,                    -- YYYY-MM
    quarter             TEXT,                    -- YYYY-Q#
    ingested_at         TEXT
);

-- ---------------------------------------------------------------
-- COMPLIANCE CHECKS TABLE
-- Audit and compliance review records
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS compliance_checks (
    check_id            TEXT PRIMARY KEY,
    check_date          DATE NOT NULL,
    department          TEXT NOT NULL,
    policy_area         TEXT NOT NULL,
    result              TEXT NOT NULL CHECK (result IN ('Pass', 'Fail')),
    score               REAL CHECK (score >= 0 AND score <= 100),
    auditor             TEXT,
    findings_count      INTEGER DEFAULT 0,
    repeat_finding      INTEGER DEFAULT 0,
    month               TEXT,
    passed              INTEGER DEFAULT 0,       -- derived: 0/1
    normalized_score    REAL,                    -- derived: score/100
    ingested_at         TEXT
);

-- ---------------------------------------------------------------
-- PIPELINE RUNS TABLE
-- ETL execution logs
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id              TEXT PRIMARY KEY,
    run_date            DATE NOT NULL,
    pipeline_name       TEXT NOT NULL,
    status              TEXT NOT NULL CHECK (status IN ('Success', 'Failed')),
    records_processed   INTEGER DEFAULT 0,
    duration_seconds    REAL,
    error_message       TEXT,
    month               TEXT,
    success             INTEGER DEFAULT 0,       -- derived: 0/1
    ingested_at         TEXT
);

-- ---------------------------------------------------------------
-- INDEXES for query performance
-- ---------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_incidents_department   ON incidents(department);
CREATE INDEX IF NOT EXISTS idx_incidents_month        ON incidents(month);
CREATE INDEX IF NOT EXISTS idx_incidents_severity     ON incidents(severity);
CREATE INDEX IF NOT EXISTS idx_incidents_status       ON incidents(status);
CREATE INDEX IF NOT EXISTS idx_compliance_department  ON compliance_checks(department);
CREATE INDEX IF NOT EXISTS idx_compliance_month       ON compliance_checks(month);
CREATE INDEX IF NOT EXISTS idx_pipeline_name          ON pipeline_runs(pipeline_name);
CREATE INDEX IF NOT EXISTS idx_pipeline_month         ON pipeline_runs(month);
