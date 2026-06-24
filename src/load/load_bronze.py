"""Bronze loader — land raw CSV/JSON as Parquet with lineage metadata."""

import json
import logging
from datetime import datetime, timezone

import pandas as pd

from src.paths import BRONZE_DIR, LOG_DIR, RAW_DIR

LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / 'load_bronze.log', encoding='utf-8')
    ]
)
log = logging.getLogger("bronze")

CSV_SOURCES = [
    ("customers.csv", "raw_customers", "profile_csv"),
    ("accounts.csv", "raw_accounts", "core_banking_csv"),
    ("merchants.csv", "raw_merchants", "merchant_csv"),
    ("devices.csv", "raw_devices", "device_registry_csv"),
    ("cards.csv", "raw_cards", "card_csv"),
    ("locations.csv", "raw_locations", "reference_csv"),
    ("customer_scd_events.csv", "raw_customer_scd_events", "customer_profile_service"),
]

def make_batch_id() -> str:
    """Generate a unique batch id for this load run."""
    return "bronze_" + datetime.now().strftime("%Y%m%d_%H%M%S")

def add_metadata(df: pd.DataFrame, source_file: str, source_system: str, batch: str) -> pd.DataFrame:
    """Attach lineage columns: _ingested_at, _source_file, _source_system, _batch_id."""
    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    df["_source_file"] = source_file
    df["_source_system"] = source_system
    df["_batch_id"] = batch
    return df

def write_parquet(df: pd.DataFrame, table: str, file_stem: str) -> None:
    """Write DataFrame to data/bronze/<table>/<file_stem>.parquet."""
    out = BRONZE_DIR / table
    out.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out / f"{file_stem}.parquet", index=False)

def load_csv_source(file_name: str, table: str, source_system: str, batch: str) -> int:
    """Load one CSV source into Bronze. Returns row count."""
    # dtype=str, keep_default_na=False, na_filter=False → preserve raw strings exactly
    df = pd.read_csv(RAW_DIR / file_name, dtype=str, keep_default_na=False, na_filter=False)
    df = add_metadata(df, file_name, source_system, batch)
    write_parquet(df, table, table)
    log.info("loaded %-26s rows=%7d", table, len(df))
    return len(df)

def load_transactions(batch: str) -> int:
    """Load transactions (2M rows) into Bronze, preserving monthly partitions."""
    files = sorted((RAW_DIR / "transactions").glob("txn_*.csv"))
    total = 0
    for f in files:
        month = f.stem.replace("txn_", "")
        df = pd.read_csv(f, dtype=str, keep_default_na=False, na_filter=False)
        # Transactions already have event-level lineage from generator; only stamp Bronze load info.
        df["_source_file"] = f.name
        df["_batch_id"] = batch
        df["_bronze_ingested_at"] = datetime.now(timezone.utc).isoformat()
        # Partition by file name (reliable), not by transaction_time (may be corrupt).
        write_parquet(df, "raw_transactions", f"raw_transactions_{month}")
        log.info("loaded raw_transactions[%s] rows=%7d", month, len(df))
        total += len(df)
    return total

def load_exchange_rates(batch: str) -> int:
    payload = json.loads((RAW_DIR / "exchange_rates.json").read_text(encoding="utf-8"))
    rows = [
        {
            "date": r["date"], "base_currency": r["base_currency"],
            "rate_vnd": r["rates"]["VND"], "rate_usd": r["rates"]["USD"], "rate_eur": r["rates"]["EUR"],
            "source_system": r["source_system"],
        }
        for r in payload["records"]
    ]
    df = add_metadata(pd.DataFrame(rows).astype(str), "exchange_rates.json", "mock_fx_api", batch)
    write_parquet(df, "raw_exchange_rates", "raw_exchange_rates")
    log.info("loaded %-26s rows=%7d", "raw_exchange_rates", len(df))
    return len(df)


def load_customer_events(batch: str) -> int:
    payload = json.loads((RAW_DIR / "customer_events.json").read_text(encoding="utf-8"))
    recs = payload["records"]
    for r in recs:
        # Serialize nested metadata dict → JSON string for Bronze (Silver will parse later).
        r["metadata"] = json.dumps(r.get("metadata", {}), ensure_ascii=False)
    df = add_metadata(pd.DataFrame(recs).astype(str), "customer_events.json", "customer_activity_api", batch)
    write_parquet(df, "raw_login_events", "raw_login_events")
    log.info("loaded %-26s rows=%7d", "raw_login_events", len(df))
    return len(df)


def main() -> None:
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    batch = make_batch_id()
    log.info("bronze load start batch=%s", batch)
    counts = {"raw_transactions": load_transactions(batch)}
    for file_name, table, source_system in CSV_SOURCES:
        counts[table] = load_csv_source(file_name, table, source_system, batch)
    counts["raw_exchange_rates"] = load_exchange_rates(batch)
    counts["raw_login_events"] = load_customer_events(batch)
    log.info("bronze load done batch=%s totals=%s", batch, counts)


if __name__ == "__main__":
    main()
