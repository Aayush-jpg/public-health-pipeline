"""
Public Health Data Pipeline - Ingestion Layer
Pulls real data from CDC and WHO public APIs
Author: Ayush Ghimire
"""

import requests
import psycopg2
import pandas as pd
import logging
import os
import json
from datetime import datetime
from typing import Optional

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ── Config ────────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     os.getenv("DB_PORT", 5432),
    "dbname":   os.getenv("DB_NAME", "public_health"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}

# CDC Socrata Open Data API endpoints (no API key required for basic access)
CDC_ENDPOINTS = {
    "covid_deaths":     "https://data.cdc.gov/resource/r8kw-7aab.json",
    "chronic_disease":  "https://data.cdc.gov/resource/g4ie-h725.json",
    "vaccination":      "https://data.cdc.gov/resource/unsk-b7fc.json",
}

BATCH_SIZE = 1000


# ── Database Connection ───────────────────────────────────────────────────────
def get_connection():
    """Create and return a PostgreSQL connection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Database connection established.")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


# ── Schema Setup ──────────────────────────────────────────────────────────────
def create_schema(conn):
    """Create all tables if they don't exist."""
    ddl = """
    CREATE TABLE IF NOT EXISTS raw_covid_deaths (
        id                  SERIAL PRIMARY KEY,
        data_as_of          DATE,
        start_week          DATE,
        end_week            DATE,
        state               VARCHAR(100),
        condition_group     VARCHAR(255),
        condition           VARCHAR(255),
        icd10_codes         VARCHAR(100),
        age_group           VARCHAR(50),
        covid_19_deaths     INTEGER,
        number_of_mentions  INTEGER,
        flag                VARCHAR(10),
        ingested_at         TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS raw_chronic_disease (
        id                  SERIAL PRIMARY KEY,
        year_start          INTEGER,
        year_end            INTEGER,
        location_abbr       VARCHAR(10),
        location_desc       VARCHAR(100),
        category            VARCHAR(255),
        topic               VARCHAR(255),
        question            TEXT,
        data_value_type     VARCHAR(100),
        data_value          NUMERIC,
        data_value_unit     VARCHAR(50),
        stratification1     VARCHAR(100),
        ingested_at         TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS raw_vaccination (
        id                  SERIAL PRIMARY KEY,
        date                DATE,
        location            VARCHAR(10),
        mmwr_week           INTEGER,
        administered_dose1  BIGINT,
        series_complete     BIGINT,
        additional_doses    BIGINT,
        ingested_at         TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS pipeline_audit (
        id              SERIAL PRIMARY KEY,
        pipeline_name   VARCHAR(100),
        source_endpoint VARCHAR(500),
        rows_fetched    INTEGER,
        rows_inserted   INTEGER,
        rows_failed     INTEGER,
        status          VARCHAR(20),
        started_at      TIMESTAMP,
        completed_at    TIMESTAMP,
        error_message   TEXT
    );
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    logger.info("Schema created / verified.")


# ── Fetch from CDC API ────────────────────────────────────────────────────────
def fetch_cdc_data(endpoint: str, limit: int = 5000) -> list[dict]:
    """Fetch data from a CDC Socrata endpoint."""
    params = {"$limit": limit, "$order": ":id"}
    try:
        resp = requests.get(endpoint, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        logger.info(f"Fetched {len(data)} records from {endpoint}")
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        return []


# ── Safe type helpers ─────────────────────────────────────────────────────────
def safe_int(val) -> Optional[int]:
    try:
        return int(float(str(val).replace(",", "")))
    except (TypeError, ValueError):
        return None


def safe_float(val) -> Optional[float]:
    try:
        return float(str(val).replace(",", ""))
    except (TypeError, ValueError):
        return None


def safe_date(val) -> Optional[str]:
    if not val:
        return None
    try:
        return str(val)[:10]
    except Exception:
        return None


# ── Loaders ───────────────────────────────────────────────────────────────────
def load_covid_deaths(conn, records: list[dict]) -> tuple[int, int]:
    """Insert COVID deaths records into the raw table."""
    inserted, failed = 0, 0
    sql = """
        INSERT INTO raw_covid_deaths (
            data_as_of, start_week, end_week, state,
            condition_group, condition, icd10_codes,
            age_group, covid_19_deaths, number_of_mentions, flag
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    with conn.cursor() as cur:
        for batch_start in range(0, len(records), BATCH_SIZE):
            batch = records[batch_start: batch_start + BATCH_SIZE]
            rows = []
            for r in batch:
                try:
                    rows.append((
                        safe_date(r.get("data_as_of")),
                        safe_date(r.get("start_week")),
                        safe_date(r.get("end_week")),
                        r.get("state"),
                        r.get("condition_group"),
                        r.get("condition"),
                        r.get("icd10_codes"),
                        r.get("age_group"),
                        safe_int(r.get("covid_19_deaths")),
                        safe_int(r.get("number_of_mentions")),
                        r.get("flag"),
                    ))
                except Exception as e:
                    logger.warning(f"Row parse error: {e}")
                    failed += 1
            try:
                cur.executemany(sql, rows)
                conn.commit()
                inserted += len(rows)
            except Exception as e:
                conn.rollback()
                logger.error(f"Batch insert failed: {e}")
                failed += len(rows)
    return inserted, failed


def load_chronic_disease(conn, records: list[dict]) -> tuple[int, int]:
    """Insert chronic disease records."""
    inserted, failed = 0, 0
    sql = """
        INSERT INTO raw_chronic_disease (
            year_start, year_end, location_abbr, location_desc,
            category, topic, question, data_value_type,
            data_value, data_value_unit, stratification1
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    with conn.cursor() as cur:
        for batch_start in range(0, len(records), BATCH_SIZE):
            batch = records[batch_start: batch_start + BATCH_SIZE]
            rows = []
            for r in batch:
                try:
                    rows.append((
                        safe_int(r.get("yearstart")),
                        safe_int(r.get("yearend")),
                        r.get("locationabbr"),
                        r.get("locationdesc"),
                        r.get("category"),
                        r.get("topic"),
                        r.get("question"),
                        r.get("datavaluetype"),
                        safe_float(r.get("datavalue")),
                        r.get("datavaluunit"),
                        r.get("stratificationcategory1"),
                    ))
                except Exception as e:
                    logger.warning(f"Row parse error: {e}")
                    failed += 1
            try:
                cur.executemany(sql, rows)
                conn.commit()
                inserted += len(rows)
            except Exception as e:
                conn.rollback()
                logger.error(f"Batch insert failed: {e}")
                failed += len(rows)
    return inserted, failed


def load_vaccination(conn, records: list[dict]) -> tuple[int, int]:
    """Insert vaccination records."""
    inserted, failed = 0, 0
    sql = """
        INSERT INTO raw_vaccination (
            date, location, mmwr_week,
            administered_dose1, series_complete, additional_doses
        ) VALUES (%s,%s,%s,%s,%s,%s)
    """
    with conn.cursor() as cur:
        for batch_start in range(0, len(records), BATCH_SIZE):
            batch = records[batch_start: batch_start + BATCH_SIZE]
            rows = []
            for r in batch:
                try:
                    rows.append((
                        safe_date(r.get("date")),
                        r.get("location"),
                        safe_int(r.get("mmwr_week")),
                        safe_int(r.get("administered_dose1_recip")),
                        safe_int(r.get("series_complete_yes")),
                        safe_int(r.get("additional_doses")),
                    ))
                except Exception as e:
                    logger.warning(f"Row parse error: {e}")
                    failed += 1
            try:
                cur.executemany(sql, rows)
                conn.commit()
                inserted += len(rows)
            except Exception as e:
                conn.rollback()
                logger.error(f"Batch insert failed: {e}")
                failed += len(rows)
    return inserted, failed


# ── Audit Logger ──────────────────────────────────────────────────────────────
def log_audit(conn, pipeline_name, endpoint, fetched,
               inserted, failed, status, started_at, error=None):
    sql = """
        INSERT INTO pipeline_audit (
            pipeline_name, source_endpoint, rows_fetched,
            rows_inserted, rows_failed, status,
            started_at, completed_at, error_message
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (
            pipeline_name, endpoint, fetched,
            inserted, failed, status,
            started_at, datetime.now(), error
        ))
    conn.commit()


# ── Main Pipeline ─────────────────────────────────────────────────────────────
def run_pipeline():
    logger.info("=" * 60)
    logger.info("Public Health Data Pipeline — Starting")
    logger.info("=" * 60)

    conn = get_connection()
    create_schema(conn)

    pipelines = [
        ("covid_deaths",    CDC_ENDPOINTS["covid_deaths"],    load_covid_deaths),
        ("chronic_disease", CDC_ENDPOINTS["chronic_disease"], load_chronic_disease),
        ("vaccination",     CDC_ENDPOINTS["vaccination"],     load_vaccination),
    ]

    for name, endpoint, loader in pipelines:
        started_at = datetime.now()
        logger.info(f"\n▶ Running pipeline: {name}")
        try:
            records = fetch_cdc_data(endpoint)
            if not records:
                log_audit(conn, name, endpoint, 0, 0, 0,
                          "NO_DATA", started_at, "No records returned from API")
                continue
            inserted, failed = loader(conn, records)
            status = "SUCCESS" if failed == 0 else "PARTIAL"
            log_audit(conn, name, endpoint, len(records),
                      inserted, failed, status, started_at)
            logger.info(f"✓ {name}: {inserted} inserted, {failed} failed")
        except Exception as e:
            logger.error(f"Pipeline {name} failed: {e}")
            log_audit(conn, name, endpoint, 0, 0, 0,
                      "FAILED", started_at, str(e))

    conn.close()
    logger.info("\nPipeline complete.")


if __name__ == "__main__":
    run_pipeline()
