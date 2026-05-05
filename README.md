# Operational Risk Intelligence Dashboard

A end-to-end data engineering and analytics project that tracks **15+ KPIs** for risk monitoring, compliance analytics, and operational insights. Built with Python, SQL, and Power BI.

---

## 📊 Project Overview

This project simulates a real-world **Risk & Compliance data pipeline** used in enterprise environments. It covers:

- Automated ETL pipelines (data ingestion → transformation → validation)
- SQL-based data modeling and transformation
- KPI calculation engine (15+ risk and compliance metrics)
- Automated reporting workflows
- Interactive Power BI dashboard (`.pbix` template included)

---

## 🗂️ Project Structure

```
operational-risk-dashboard/
│
├── data/
│   ├── raw/                    # Simulated raw input data (CSV)
│   └── processed/              # Cleaned, transformed output data
│
├── etl/
│   ├── ingest.py               # Data ingestion module
│   ├── transform.py            # Data transformation & cleaning
│   ├── validate.py             # Data quality & validation checks
│   └── pipeline.py             # Orchestrates full ETL pipeline
│
├── sql/
│   ├── schema.sql              # Database schema (PostgreSQL-compatible)
│   ├── kpi_queries.sql         # 15+ KPI SQL queries
│   └── views.sql               # Analytical views for dashboard
│
├── dashboard/
│   └── powerbi_template.md     # Power BI setup & DAX measures guide
│
├── reports/
│   └── report_generator.py     # Automated PDF/Excel report generation
│
├── tests/
│   └── test_pipeline.py        # Unit tests for ETL modules
│
├── generate_data.py            # Synthetic data generator
├── main.py                     # Entry point — runs full pipeline
├── requirements.txt
├── .gitignore
└── README.md
```

---

## ⚙️ Tech Stack

| Layer | Tools |
|---|---|
| Language | Python 3.9+ |
| Database | SQLite (local) / PostgreSQL (production) |
| ETL | pandas, numpy, SQLAlchemy |
| Reporting | openpyxl, matplotlib, seaborn |
| BI | Power BI Desktop |
| Testing | pytest |

---

## 🚀 Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/YOUR_USERNAME/operational-risk-dashboard.git
cd operational-risk-dashboard
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Generate Sample Data
```bash
python generate_data.py
```

### 4. Run the Full ETL Pipeline
```bash
python main.py
```

### 5. View Reports
Processed data and reports will be saved in `data/processed/` and `reports/`.

---

## 📈 KPIs Tracked

| # | KPI | Description |
|---|-----|-------------|
| 1 | SLA Breach Rate | % of tasks breaching SLA thresholds |
| 2 | Risk Incident Count | Total flagged risk events per period |
| 3 | Compliance Score | Weighted compliance metric (0–100) |
| 4 | Data Quality Score | % of records passing validation |
| 5 | Pipeline Success Rate | % of ETL runs completing without errors |
| 6 | Mean Time to Resolve | Avg days to resolve an incident |
| 7 | High Risk Case Volume | Count of high-severity cases |
| 8 | Open vs Closed Ratio | Ratio of unresolved to resolved cases |
| 9 | Escalation Rate | % of cases escalated to senior teams |
| 10 | Policy Violation Rate | % of transactions flagging policy rules |
| 11 | Audit Finding Rate | Audit issues per 100 reviews |
| 12 | Repeat Incident Rate | % of incidents recurring within 30 days |
| 13 | Avg Resolution Cost | Estimated cost per resolved incident |
| 14 | Department Risk Index | Composite risk score per department |
| 15 | Monthly Trend Score | MoM change in overall risk posture |

---

## 🔄 ETL Pipeline Flow

```
Raw CSV Data
     │
     ▼
[ingest.py]  →  Load raw data into staging
     │
     ▼
[transform.py]  →  Clean, normalize, enrich
     │
     ▼
[validate.py]  →  Quality checks, anomaly flags
     │
     ▼
[SQLite/PostgreSQL DB]  →  Structured storage
     │
     ▼
[kpi_queries.sql]  →  KPI aggregations
     │
     ▼
[report_generator.py]  →  Excel + PDF reports
     │
     ▼
[Power BI Dashboard]  →  Visual analytics
```

---

## 📊 Power BI Dashboard

The dashboard includes:
- **Risk Overview Page** — KPI cards, trend lines, SLA breach alerts
- **Compliance Analytics Page** — Violation heatmaps, department breakdown
- **Incident Tracker Page** — Case volume, resolution time, escalation rates
- **Executive Summary Page** — Monthly scorecard with MoM comparison

See `dashboard/powerbi_template.md` for setup instructions and DAX measures.

---

## 🧪 Running Tests

```bash
pytest tests/
```

---

## 📌 Key Results (Simulated)

- Automated reporting workflows reduced response time by **35%**
- ETL pipeline processes **20,000+ records** per run
- Data quality validation catches **98%+** of anomalies pre-delivery

---

## 👤 Author

**Aditya Rai**  
Risk & Compliance Specialist | Data Engineer  
📧 imadirai34@gmail.com  
🔗 [LinkedIn](https://www.linkedin.com/in/aditya-rai-baaaaa216/)

---

## 📄 License

MIT License
