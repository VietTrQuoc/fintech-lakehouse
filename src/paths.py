"""Shared paths for the Financial Transaction Data Lakehouse.

All modules import paths from here instead of computing Path(__file__).resolve().parents[2] independently.
"""
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# --- data layers ---
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
QUARANTINE_DIR = DATA_DIR / "quarantine"
QUALITY_DIR = DATA_DIR / "quality"
BAD_DATA_DIR = DATA_DIR / "bad_data_samples"
EXTERNAL_DIR = DATA_DIR / "external"

# --- derived ---
CLEAN_DIR = SILVER_DIR / "clean_transactions"
GOLD_DB = GOLD_DIR / "gold.duckdb"
MARTS_SQL = PROJECT_ROOT / "sql" / "marts"
MARTS_OUT = GOLD_DIR / "marts"

# --- reference ---
SQL_DIR = PROJECT_ROOT / "sql"
DOCS_DIR = PROJECT_ROOT / "docs"
LOG_DIR = PROJECT_ROOT / "logs"
CALIBRATION_DIR = PROJECT_ROOT / "src" / "generate" / "calibration"
