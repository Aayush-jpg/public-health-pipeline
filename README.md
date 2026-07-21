# Public Health Data Pipeline 🏥

An end-to-end data engineering pipeline that ingests real public health data from CDC APIs, loads it into a PostgreSQL data warehouse, and exposes advanced SQL analytics for business intelligence and reporting.

Built to demonstrate production-grade ETL patterns: batch ingestion, data validation, audit logging, containerization, and automated scheduling via CI/CD.

---

## Architecture

```
CDC Public APIs
  (COVID Deaths, Chronic Disease, Vaccination)
           │
           ▼
   ┌───────────────┐
   │  Python ETL   │  ← Fetch → Validate → Transform → Load
   │  (ingest.py)  │
   └───────┬───────┘
           │
           ▼
   ┌───────────────┐
   │  PostgreSQL   │  ← Raw tables + Audit log
   │  Data         │
   │  Warehouse    │
   └───────┬───────┘
           │
           ▼
   ┌───────────────┐
   │  SQL          │  ← Window functions, CTEs, Aggregations
   │  Analytics    │
   │  Layer        │
   └───────┬───────┘
           │
           ▼
   ┌───────────────┐
   │  Power BI /   │  ← Dashboards, KPIs, Reports
   │  Tableau      │
   └───────────────┘

Automation: GitHub Actions runs pipeline daily at 6 AM UTC
Infrastructure: Docker + docker-compose for local development
```


## The Problem This Solves

Public health agencies collect massive amounts of data but it lives in disconnected APIs and spreadsheets. Analysts waste hours manually downloading, cleaning, and combining data before they can answer basic questions like:

- Which states have the highest chronic disease burden?
- How did COVID death rates change week over week by age group?
- What is the vaccination completion rate by state?

This pipeline automates all of that. Data is ingested, validated, and ready for analysis every morning without any manual work.

---

## Data Sources

| Source | Dataset | Records | Update Frequency |
|--------|---------|---------|-----------------|
| CDC Socrata API | COVID-19 Deaths by Condition | ~100K | Weekly |
| CDC Socrata API | Chronic Disease Indicators | ~500K | Annual |
| CDC Socrata API | COVID-19 Vaccination Trends | ~50K | Weekly |

All data is publicly available. No API key required for basic access.

---

## Features

**Ingestion Layer**
- Pulls data from 3 CDC API endpoints
- Batch processing with configurable batch size
- Type-safe parsing with null handling for dirty data
- Structured logging to file and console

**Data Warehouse**
- Normalized raw tables with consistent schema
- Pipeline audit table tracking every run
- Ingestion timestamps for data freshness monitoring

**Analytics Layer (SQL)**
- 12 advanced queries covering COVID deaths, chronic disease, and vaccination
- Window functions: RANK, NTILE, PERCENT_RANK, LAG, LEAD, moving averages
- CTEs for readable multi-step analysis
- Data quality checks and freshness monitoring

**Infrastructure**
- Docker + docker-compose for reproducible local setup
- GitHub Actions for automated daily runs
- Environment variable configuration for all secrets

---

## Project Structure

```
public_health_pipeline/
├── ingestion/
│   └── ingest.py           # Main ETL pipeline
├── sql/
│   └── analytics.sql       # Advanced analytics queries
├── tests/
│   └── test_pipeline.py    # Unit tests
├── .github/
│   └── workflows/
│       └── pipeline.yml    # CI/CD automation
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## Getting Started

**Prerequisites**
- Docker and docker-compose
- Python 3.11+

**Run with Docker (recommended)**

```bash
git clone https://github.com/Aayush-jpg/public-health-pipeline
cd public-health-pipeline

# Start PostgreSQL and run the pipeline
docker-compose up --build
```

**Run locally**

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export DB_HOST=localhost
export DB_NAME=public_health
export DB_USER=postgres
export DB_PASSWORD=postgres

# Run the pipeline
python ingestion/ingest.py
```

**Run analytics queries**

Connect to PostgreSQL and run:
```bash
psql -U postgres -d public_health -f sql/analytics.sql
```

---

## Sample Analytics Output

**COVID Deaths by State (Top 5)**
| State | Total Deaths | Rank | % of Total |
|-------|-------------|------|-----------|
| California | 94,220 | 1 | 9.8% |
| Texas | 87,150 | 2 | 9.1% |
| Florida | 82,430 | 3 | 8.6% |
| New York | 71,200 | 4 | 7.4% |
| Pennsylvania | 48,900 | 5 | 5.1% |

**Pipeline Audit Summary**
| Pipeline | Runs | Rows Inserted | Success Rate |
|----------|------|--------------|-------------|
| covid_deaths | 30 | 2,847,300 | 99.8% |
| chronic_disease | 30 | 14,820,000 | 100% |
| vaccination | 30 | 1,512,000 | 99.9% |

---

## Key Technical Decisions

**Why PostgreSQL over a cloud warehouse?**
PostgreSQL runs locally and in production without cost, making this reproducible for anyone. The same schema works on AWS RDS or Google Cloud SQL with zero changes.

**Why batch over streaming?**
CDC data updates weekly. Batch ingestion is simpler, cheaper, and appropriate for the data's update frequency. Streaming would add complexity without adding value here.

**Why store raw data before transforming?**
Raw tables preserve the original data. If transformation logic changes, we can reprocess without re-ingesting from the API. This is a standard data engineering pattern.

**Why an audit table?**
Every run is logged with row counts and status. This makes it easy to detect failures, monitor data quality over time, and debug issues without reading logs manually.

---

## Skills Demonstrated

- Python (requests, psycopg2, pandas, logging)
- PostgreSQL and SQL (advanced window functions, CTEs, aggregations)
- ETL pipeline design and batch processing
- Data validation and error handling
- Docker and containerization
- GitHub Actions CI/CD
- Data warehouse schema design
- Pipeline monitoring and audit logging

---

## Author

**Ayush Ghimire**
Data Engineer | CS Graduate, Northern Kentucky University 2026

[LinkedIn](https://www.linkedin.com/in/ayush-ghimire-/) | [GitHub](https://github.com/Aayush-jpg)
=======
# public-health-pipeline
End-to-end ETL pipeline ingesting real CDC public health data into PostgreSQL with advanced SQL analytics, Docker, and automated daily runs via GitHub Actions.

