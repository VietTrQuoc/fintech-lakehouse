"""Bronze → Silver: cast types, normalize values, attach per-row DQ flags."""

import logging
import shutil
from collections import Counter
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from src.generate.config import CHANNELS, CURRENCIES, STATUSES, TRANSACTION_TYPES
from src.paths import BRONZE_DIR, LOG_DIR, SILVER_DIR

LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "bronze_to_silver.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("silver")

FUTURE_YEAR_CUTOFF = 2099  # timestamps with year >= 2099 are injected junk


def make_run_id() -> str:
    return "silver_" + datetime.now().strftime("%Y%m%d_%H%M%S")


def clean_silver_dir(table: str) -> Path:
    """Remove + recreate Silver directory for idempotent re-runs."""
    out = SILVER_DIR / table
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True, exist_ok=True)
    return out


def read_id_set(table: str, column: str) -> set[str]:
    """Read one Bronze dimension column into a Python set for O(1) FK lookup."""
    df = pd.read_parquet(BRONZE_DIR / table / f"{table}.parquet", columns=[column])
    return set(df[column].astype(str).str.strip())


def build_rate_lookup() -> dict[str, float]:
    """Build (date|currency) → VND rate lookup from raw_exchange_rates."""
    rates = pd.read_parquet(BRONZE_DIR / "raw_exchange_rates" / "raw_exchange_rates.parquet")
    lookup: dict[str, float] = {}
    for _, r in rates.iterrows():
        d = str(r["date"])
        lookup[f"{d}|VND"] = float(r["rate_vnd"])
        lookup[f"{d}|USD"] = float(r["rate_usd"])
        lookup[f"{d}|EUR"] = float(r["rate_eur"])
    return lookup


def build_duplicate_index() -> tuple[set[str], dict[str, int]]:
    """Global pass across all 7 partitions → (dup_ids, id→min_source_row_number).

    Global because duplicates can span different month files. Survivor = row with smallest
    source_row_number. Only reads 2 columns (lightweight).
    """
    parts = sorted((BRONZE_DIR / "raw_transactions").glob("raw_transactions_*.parquet"))
    frames = [pd.read_parquet(p, columns=["transaction_id", "source_row_number"]) for p in parts]
    ids = pd.concat(frames, ignore_index=True)
    ids["transaction_id"] = ids["transaction_id"].astype(str).str.strip()
    ids["source_row_number"] = pd.to_numeric(ids["source_row_number"], errors="coerce")
    ids = ids[ids["transaction_id"] != ""]  # null-id rows already flagged separately
    grp = ids.groupby("transaction_id")["source_row_number"]
    sizes = grp.size()
    dup_ids = set(sizes[sizes >= 2].index)
    survivor_srn = grp.min().loc[list(dup_ids)].to_dict()
    return dup_ids, survivor_srn


def transform_transactions_partition(
    df: pd.DataFrame,
    *,
    rate_lookup: dict[str, float],
    dup_ids: set[str],
    survivor_srn: dict[str, int],
    cust_ids: set[str],
    acc_ids: set[str],
    mer_ids: set[str],
    loc_ids: set[str],
    run_id: str,
) -> pd.DataFrame:
    """Cast + flag one transactions partition. Pure DataFrame→DataFrame for testability."""
    idx = df.index

    # ---- 1) Cast types + normalize ----
    txn_id = df["transaction_id"].astype(str).str.strip()
    customer_id = df["customer_id"].astype(str).str.strip()
    account_id = df["account_id"].astype(str).str.strip()
    merchant_id = df["merchant_id"].astype(str).str.strip()
    device_id = df["device_id"].astype(str).str.strip()
    card_id = df["card_id"].astype(str).str.strip()

    amount = pd.to_numeric(df["amount"], errors="coerce")
    currency = df["currency"].astype(str).str.strip().str.upper()
    txn_type = df["transaction_type"].astype(str).str.strip().str.lower()
    channel = df["channel"].astype(str).str.strip().str.lower()
    status = df["status"].astype(str).str.strip().str.lower()
    country = df["country"].astype(str).str.strip().str.upper()
    city = df["city"].astype(str).str.strip()
    location_id = df["location_id"].astype(str).str.strip()

    event_ts = pd.to_datetime(df["transaction_time"], errors="coerce")
    ingest_ts = pd.to_datetime(df["ingestion_time"], errors="coerce")
    source_row_number = pd.to_numeric(df["source_row_number"], errors="coerce").astype("Int64")

    # ---- 2) Compute DQ flags ----
    f_null_id = (txn_id == "") | txn_id.isna()
    # TRAP: 'NaN' casts to float NaN (not NULL), and NaN <= 0 is False → must use isfinite().
    f_amount = pd.Series(~np.isfinite(amount), index=idx) | (amount <= 0)
    # TRAP: "2099-12-31" parses as valid datetime → need explicit year check.
    f_ts = event_ts.isna() | (event_ts.dt.year.fillna(0) >= FUTURE_YEAR_CUTOFF)
    f_currency = ~currency.isin(CURRENCIES)
    f_channel = ~channel.isin(CHANNELS)
    f_type = ~txn_type.isin(TRANSACTION_TYPES)
    f_status = ~status.isin(STATUSES)
    f_location = ~location_id.isin(loc_ids)

    # Duplicate: global check; only survivor is valid.
    is_dup = txn_id.isin(dup_ids) & ~f_null_id
    min_srn = txn_id.map(survivor_srn)
    is_survivor = is_dup & (source_row_number == min_srn.astype("Int64"))
    f_dup_hard = is_dup & ~is_survivor

    # FK orphan = SOFT flag: doesn't invalidate row (Day 9 maps to Unknown sk=-1).
    # Empty merchant_id is valid (transfer/withdrawal/topup have no merchant).
    f_fk_customer = ~customer_id.isin(cust_ids)
    f_fk_account = ~account_id.isin(acc_ids)
    f_fk_merchant = (merchant_id != "") & ~merchant_id.isin(mer_ids)

    # ---- 3) Derived columns ----
    event_date = event_ts.dt.date
    event_date_str = event_ts.dt.strftime("%Y-%m-%d")
    rate_key = event_date_str + "|" + currency
    exchange_rate = rate_key.map(rate_lookup)
    amount_vnd = amount * exchange_rate
    lag = (ingest_ts - event_ts).dt.total_seconds().round().astype("Int64")

    # ---- 4) Aggregate flags: bucket (priority-ordered) + is_valid + error codes ----
    bucket = np.select(
        [
            f_dup_hard,
            f_ts,
            f_amount,
            f_null_id | f_currency | f_channel | f_type | f_status | f_location,
        ],
        [
            "quarantine_duplicate_transactions",
            "quarantine_invalid_timestamp",
            "quarantine_invalid_amount",
            "quarantine_bad_records",
        ],
        default="",
    )
    bucket = pd.Series(bucket, index=idx)
    is_valid = bucket == ""

    # Build CSV error codes using np.where (vectorized, not apply for 2M rows).
    errors = pd.Series("", index=idx)
    for flag, code in [
        (f_null_id, "null_transaction_id"),
        (f_dup_hard, "duplicate_transaction_id"),
        (f_amount, "invalid_amount"),
        (f_ts, "invalid_timestamp"),
        (f_currency, "invalid_currency"),
        (f_channel, "invalid_channel"),
        (f_type, "invalid_type"),
        (f_status, "invalid_status"),
        (f_location, "invalid_location"),
        (f_fk_customer, "fk_customer_orphan"),
        (f_fk_account, "fk_account_orphan"),
        (f_fk_merchant, "fk_merchant_orphan"),
    ]:
        errors = errors + np.where(flag.fillna(False), code + ",", "")
    errors = errors.str.rstrip(",")

    # ---- 5) Assemble Silver DataFrame ----
    return pd.DataFrame(
        {
            "transaction_id": txn_id,
            "customer_id": customer_id,
            "account_id": account_id,
            "merchant_id": merchant_id,
            "device_id": device_id,
            "card_id": card_id,
            "transaction_type": txn_type,
            "channel": channel,
            "status": status,
            "currency": currency,
            "amount_original": amount,
            "exchange_rate": exchange_rate,
            "amount_vnd": amount_vnd,
            "country": country,
            "city": city,
            "location_id": location_id,
            "event_time": event_ts,
            "event_date": event_date,
            "ingestion_time": ingest_ts,
            "ingestion_lag_seconds": lag,
            "source_system": df["source_system"].astype(str).str.strip(),
            "batch_id": df["batch_id"].astype(str).str.strip(),
            "file_name": df["file_name"].astype(str).str.strip(),
            "source_row_number": source_row_number,
            # --- cờ DQ per-rule (bool) ---
            "_dq_null_transaction_id": f_null_id.fillna(False),
            "_dq_duplicate": f_dup_hard.fillna(False),
            "_dq_invalid_amount": f_amount.fillna(False),
            "_dq_invalid_timestamp": f_ts.fillna(False),
            "_dq_invalid_currency": f_currency.fillna(False),
            "_dq_invalid_channel": f_channel.fillna(False),
            "_dq_invalid_type": f_type.fillna(False),
            "_dq_invalid_status": f_status.fillna(False),
            "_dq_invalid_location": f_location.fillna(False),
            "_dq_fk_customer": f_fk_customer.fillna(False),   # soft
            "_dq_fk_account": f_fk_account.fillna(False),     # soft
            "_dq_fk_merchant": f_fk_merchant.fillna(False),
            # --- aggregate ---
            "_dq_errors": errors,
            "_dq_bucket": bucket,
            "_is_valid": is_valid,
            "_is_duplicate_survivor": is_survivor.fillna(False),
            "_dq_run_id": run_id,
        },
        index=idx,
    )


def transform_transactions(run_id: str) -> dict:
    """Orchestrate: prepare shared lookups → transform 7 partitions → write Silver."""
    out = clean_silver_dir("transactions")
    rate_lookup = build_rate_lookup()
    dup_ids, survivor_srn = build_duplicate_index()
    log.info("duplicate transaction_id: %d id trung", len(dup_ids))
    cust_ids = read_id_set("raw_customers", "customer_id")
    acc_ids = read_id_set("raw_accounts", "account_id")
    mer_ids = read_id_set("raw_merchants", "merchant_id")
    loc_ids = read_id_set("raw_locations", "location_id")

    parts = sorted((BRONZE_DIR / "raw_transactions").glob("raw_transactions_*.parquet"))
    total = 0
    valid_total = 0
    bucket_counts: Counter = Counter()
    for p in parts:
        month = p.stem.replace("raw_transactions_", "")
        raw = pd.read_parquet(p)
        silver = transform_transactions_partition(
            raw,
            rate_lookup=rate_lookup,
            dup_ids=dup_ids,
            survivor_srn=survivor_srn,
            cust_ids=cust_ids,
            acc_ids=acc_ids,
            mer_ids=mer_ids,
            loc_ids=loc_ids,
            run_id=run_id,
        )
        silver.to_parquet(out / f"silver_transactions_{month}.parquet", index=False)
        valid = int(silver["_is_valid"].sum())
        bucket_counts.update(silver.loc[silver["_dq_bucket"] != "", "_dq_bucket"])
        total += len(silver)
        valid_total += valid
        log.info("silver_transactions[%s] rows=%7d valid=%7d", month, len(silver), valid)
    return {
        "transaction_rows": total,
        "valid_rows": valid_total,
        "invalid_rows": total - valid_total,
        "bucket_counts": dict(sorted(bucket_counts.items())),
    }


def transform_simple_dim(table_in: str, table_out: str, *, date_cols=(), float_cols=(), bool_cols=()) -> int:
    """Cast-only for one dimension. SCD2/surrogate-key/dedup deferred to Day 9."""
    df = pd.read_parquet(BRONZE_DIR / table_in / f"{table_in}.parquet")
    for c in date_cols:
        df[c] = pd.to_datetime(df[c], errors="coerce").dt.date
    for c in float_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in bool_cols:
        df[c] = df[c].astype(str).str.strip().str.lower() == "true"
    out = clean_silver_dir(table_out)
    df.to_parquet(out / f"{table_out}.parquet", index=False)
    log.info("loaded %-22s rows=%7d", table_out, len(df))
    return len(df)


def transform_dims() -> dict:
    """Cast-only 5 dimensions. SCD2 + joins deferred to Day 9."""
    counts = {}
    counts["customers"] = transform_simple_dim("raw_customers", "customers", date_cols=("dob", "signup_date"))
    counts["accounts"] = transform_simple_dim("raw_accounts", "accounts", date_cols=("opened_date",), float_cols=("current_balance",))
    counts["merchants"] = transform_simple_dim("raw_merchants", "merchants", date_cols=("onboarded_date",), float_cols=("risk_score", "base_failure_rate"))
    counts["devices"] = transform_simple_dim("raw_devices", "devices", date_cols=("first_seen",), bool_cols=("is_trusted",))
    counts["locations"] = transform_simple_dim("raw_locations", "locations", float_cols=("lat", "lon"))
    return counts


def main() -> None:
    run_id = make_run_id()
    log.info("bronze->silver start run_id=%s", run_id)
    summary = transform_transactions(run_id)
    summary["dims"] = transform_dims()
    log.info("bronze->silver done run_id=%s summary=%s", run_id, summary)


if __name__ == "__main__":
    main()
