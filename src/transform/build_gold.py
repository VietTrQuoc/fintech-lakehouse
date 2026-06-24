"""Build Gold star schema: 6 dims + fact_transaction from clean Silver."""

import json
import logging
import shutil
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from src.paths import BRONZE_DIR, GOLD_DIR, LOG_DIR, RAW_DIR, SILVER_DIR, SQL_DIR

LOG_DIR.mkdir(parents=True, exist_ok=True)
SQL_FILE = SQL_DIR / "star_schema.sql"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_DIR / "build_gold.log", encoding="utf-8")],
)
log = logging.getLogger("gold")

UNKNOWN_SK = -1
DIM_DATE_START = date(2025, 12, 1)
DIM_DATE_END = date(2026, 7, 31)
PAYDAY_DAYS = {1, 2, 14, 15}
DROP_LINEAGE = ["_ingested_at", "_source_file", "_source_system", "_batch_id"]
VF_START = np.datetime64("1900-01-01T00:00:00", "us")
VF_END = np.datetime64("9999-12-31T00:00:00", "us")

FRAUD_TOTAL_EXPECTED = 14000
CLEAN_ROWS_EXPECTED = 1_976_026


def make_run_id() -> str:
    return "gold_" + datetime.now().strftime("%Y%m%d_%H%M%S")


def reset_gold_dir(table: str) -> Path:
    out = GOLD_DIR / table
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True, exist_ok=True)
    return out


def read_silver_dim(name: str) -> pd.DataFrame:
    """Read Silver dimension, dropping Bronze lineage columns."""
    df = pd.read_parquet(SILVER_DIR / name / f"{name}.parquet")
    return df.drop(columns=[c for c in DROP_LINEAGE if c in df.columns])


def add_surrogate_key(df: pd.DataFrame, sk_col: str, sort_cols: list[str]) -> pd.DataFrame:
    """Sort by sort_cols, then assign sequential int keys 1..n."""
    df = df.sort_values(sort_cols).reset_index(drop=True)
    df.insert(0, sk_col, np.arange(1, len(df) + 1, dtype="int64"))
    return df


def prepend_unknown(df: pd.DataFrame, sk_col: str, bkey_col: str, extra: dict | None = None) -> pd.DataFrame:
    """Prepend an Unknown row with sk=-1."""
    row = {c: pd.NA for c in df.columns}
    row[sk_col] = UNKNOWN_SK
    row[bkey_col] = "UNKNOWN"
    if extra:
        row.update(extra)
    return pd.concat([pd.DataFrame([row]), df], ignore_index=True)


def write_gold(df: pd.DataFrame, table: str, file_stem: str) -> Path:
    """Write DataFrame to data/gold/<table>/<file_stem>.parquet."""
    out = reset_gold_dir(table)
    path = out / f"{file_stem}.parquet"
    df.to_parquet(path, index=False)
    return path


# -- dim_date --
def build_dim_date() -> pd.DataFrame:
    idx = pd.date_range(DIM_DATE_START, DIM_DATE_END, freq="D")
    df = pd.DataFrame({"full_date": idx.date})
    df["date_key"] = idx.strftime("%Y%m%d").astype("int32")
    df["year"] = idx.year.astype("int16")
    df["quarter"] = idx.quarter.astype("int8")
    df["month"] = idx.month.astype("int8")
    df["month_name"] = idx.strftime("%B")
    df["day"] = idx.day.astype("int8")
    df["day_of_week"] = (idx.dayofweek + 1).astype("int8")  # 1=Mon..7=Sun
    df["day_name"] = idx.strftime("%A")
    df["is_weekend"] = idx.dayofweek >= 5
    df["is_payday"] = idx.day.isin(PAYDAY_DAYS)
    return df[["date_key", "full_date", "year", "quarter", "month", "month_name",
               "day", "day_of_week", "day_name", "is_weekend", "is_payday"]]


# -- dim SCD1 (account / merchant / device / location) --
def build_dim_scd1(name: str, sk_col: str, bkey_col: str, keep_cols: list[str]) -> pd.DataFrame:
    df = read_silver_dim(name)[keep_cols].drop_duplicates(subset=[bkey_col], keep="first")
    df = add_surrogate_key(df, sk_col, [bkey_col])
    return prepend_unknown(df, sk_col, bkey_col)


# -- dim_customer (SCD2 — backward undo from latest snapshot) --
def load_scd_events() -> dict[str, list[dict]]:
    ev = pd.read_parquet(BRONZE_DIR / "raw_customer_scd_events" / "raw_customer_scd_events.parquet")
    ev = ev.drop(columns=[c for c in DROP_LINEAGE if c in ev.columns])
    ev["change_time"] = pd.to_datetime(ev["change_time"], errors="coerce").astype("datetime64[us]")
    ev = ev.sort_values(["customer_id", "change_time", "event_id"])
    out: dict[str, list[dict]] = {}
    for r in ev.itertuples(index=False):
        out.setdefault(r.customer_id, []).append(
            {"change_time": r.change_time, "attribute_name": r.attribute_name, "old_value": r.old_value}
        )
    return out


SCD_ATTR_COLS = ["full_name", "email", "phone", "dob", "signup_date", "home_city", "home_country", "risk_tier", "kyc_level"]


def reconstruct_customer_versions(customers: pd.DataFrame, events: dict[str, list[dict]], city2country: dict[str, str]) -> pd.DataFrame:
    versions: list[dict] = []
    for snap in customers.itertuples(index=False):
        cid = snap.customer_id
        base = {c: getattr(snap, c) for c in SCD_ATTR_COLS}
        evs = events.get(cid, [])
        if not evs:
            versions.append({"customer_id": cid, **base, "valid_from": VF_START, "valid_to": VF_END, "is_current": True})
            continue
        # version hiện hành = snapshot, hiệu lực từ change_time event cuối
        versions.append({"customer_id": cid, **base, "valid_from": evs[-1]["change_time"], "valid_to": VF_END, "is_current": True})
        state = dict(base)
        # undo từ event mới nhất -> cũ nhất
        for i in range(len(evs) - 1, -1, -1):
            e = evs[i]
            state[e["attribute_name"]] = e["old_value"]
            if e["attribute_name"] == "home_city":
                state["home_country"] = city2country.get(e["old_value"], state["home_country"])
            valid_from = evs[i - 1]["change_time"] if i > 0 else VF_START
            versions.append({"customer_id": cid, **state, "valid_from": valid_from, "valid_to": e["change_time"], "is_current": False})
    return pd.DataFrame(versions)


def build_dim_customer() -> pd.DataFrame:
    customers = read_silver_dim("customers")
    locations = read_silver_dim("locations")
    city2country = dict(zip(locations["city"], locations["country"]))
    events = load_scd_events()
    df = reconstruct_customer_versions(customers, events, city2country)
    df = add_surrogate_key(df, "customer_sk", ["customer_id", "valid_from"])
    df = prepend_unknown(df, "customer_sk", "customer_id",
                         extra={"valid_from": VF_START, "valid_to": VF_END, "is_current": True})
    df["valid_from"] = df["valid_from"].astype("datetime64[us]")
    df["valid_to"] = df["valid_to"].astype("datetime64[us]")
    df["is_current"] = df["is_current"].astype(bool)
    return df[["customer_sk", "customer_id", "full_name", "email", "phone", "dob", "signup_date",
               "home_city", "home_country", "risk_tier", "kyc_level", "valid_from", "valid_to", "is_current"]]


# -- fact_transaction --
FACT_COLS = [
    "transaction_id", "date_key", "customer_sk", "account_sk", "merchant_sk", "device_sk", "location_sk",
    "channel", "transaction_type", "status", "currency", "fraud_pattern",
    "amount_original", "exchange_rate", "amount_vnd", "is_fraud",
    "event_time", "ingestion_time", "ingestion_lag_seconds",
]


def sk_lookup(dim: pd.DataFrame, sk_col: str, bkey_col: str) -> dict:
    real = dim[dim[sk_col] != UNKNOWN_SK]
    return dict(zip(real[bkey_col], real[sk_col]))


def load_fraud_labels() -> pd.DataFrame:
    fl = pd.read_csv(RAW_DIR / "fraud_labels.csv", usecols=["transaction_id", "is_fraud", "fraud_pattern"])
    fl["is_fraud"] = pd.to_numeric(fl["is_fraud"], errors="coerce").fillna(0).astype("int8")
    # Dedupe: duplicate txns share the same id; keep is_fraud=1 first.
    fl = fl.sort_values("is_fraud", ascending=False).drop_duplicates("transaction_id", keep="first")
    return fl


def build_fact_partition(clean: pd.DataFrame, *, cust_index: pd.DataFrame, acc_lk, mer_lk, dev_lk, loc_lk, fraud: pd.DataFrame) -> pd.DataFrame:
    part = clean.copy()
    part["date_key"] = pd.to_datetime(part["event_date"]).dt.strftime("%Y%m%d").astype("int32")
    part["account_sk"] = part["account_id"].map(acc_lk).fillna(UNKNOWN_SK).astype("int64")
    part["merchant_sk"] = part["merchant_id"].map(mer_lk).fillna(UNKNOWN_SK).astype("int64")
    part["device_sk"] = part["device_id"].map(dev_lk).fillna(UNKNOWN_SK).astype("int64")
    part["location_sk"] = part["location_id"].map(loc_lk).fillna(UNKNOWN_SK).astype("int64")

    # Point-in-time SCD2 join: version with valid_from <= event_time
    part = part.sort_values("event_time")
    merged = pd.merge_asof(part, cust_index, left_on="event_time", right_on="valid_from",
                           by="customer_id", direction="backward")
    merged["customer_sk"] = merged["customer_sk"].fillna(UNKNOWN_SK).astype("int64")

    # is_fraud / fraud_pattern
    merged = merged.merge(fraud, on="transaction_id", how="left")
    merged["is_fraud"] = merged["is_fraud"].fillna(0).astype("int8")

    return merged[FACT_COLS]


def build_fact(dim_customer: pd.DataFrame, dims: dict, run_id: str) -> dict:
    out = reset_gold_dir("fact_transaction")
    cust_index = dim_customer.loc[dim_customer["customer_sk"] != UNKNOWN_SK, ["customer_id", "valid_from", "customer_sk"]].sort_values("valid_from").reset_index(drop=True)
    acc_lk = sk_lookup(dims["account"], "account_sk", "account_id")
    mer_lk = sk_lookup(dims["merchant"], "merchant_sk", "merchant_id")
    dev_lk = sk_lookup(dims["device"], "device_sk", "device_id")
    loc_lk = sk_lookup(dims["location"], "location_sk", "location_id")
    fraud = load_fraud_labels()

    parts = sorted((SILVER_DIR / "clean_transactions").glob("clean_transactions_*.parquet"))
    total = 0
    for p in parts:
        month = p.stem.replace("clean_transactions_", "")
        clean = pd.read_parquet(p)
        fact = build_fact_partition(clean, cust_index=cust_index, acc_lk=acc_lk, mer_lk=mer_lk, dev_lk=dev_lk, loc_lk=loc_lk, fraud=fraud)
        fact.to_parquet(out / f"fact_transaction_{month}.parquet", index=False)
        total += len(fact)
        log.info("fact_transaction[%s] rows=%7d", month, len(fact))
    return {"fact_rows": total}


# -- Verify + DuckDB load --
def _gold_count(table: str, stem_glob: str) -> int:
    return sum(pq.read_metadata(p).num_rows for p in (GOLD_DIR / table).glob(stem_glob))


def verify(dim_customer: pd.DataFrame, dims: dict, dim_date: pd.DataFrame) -> list[dict]:
    checks: list[dict] = []

    def add(name, passed, detail):
        checks.append({"name": name, "passed": bool(passed), "detail": detail})

    fact_rows = _gold_count("fact_transaction", "fact_transaction_*.parquet")
    add("fact_rowcount_eq_clean", fact_rows == CLEAN_ROWS_EXPECTED, {"fact": fact_rows, "expected": CLEAN_ROWS_EXPECTED})

    fact_cols = ["transaction_id", "date_key", "customer_sk", "account_sk", "merchant_sk", "device_sk", "location_sk", "is_fraud", "fraud_pattern", "amount_vnd"]
    fact = pd.concat(
        [pd.read_parquet(p, columns=fact_cols) for p in (GOLD_DIR / "fact_transaction").glob("*.parquet")],
        ignore_index=True,
    )

    sk_map = {"customer_sk": dim_customer, "account_sk": dims["account"], "merchant_sk": dims["merchant"], "device_sk": dims["device"], "location_sk": dims["location"]}
    null_sk = {c: int(fact[c].isna().sum()) for c in [*sk_map, "date_key"]}
    add("no_null_sk", all(v == 0 for v in null_sk.values()), null_sk)
    unresolved = {}
    for sk_col, dim in sk_map.items():
        valid = set(dim[sk_col].tolist())
        bad = int((~fact[sk_col].isin(valid)).sum())
        if bad:
            unresolved[sk_col] = bad
    add("all_sk_resolve_to_dim", len(unresolved) == 0, unresolved)
    add("date_key_in_dim_date", int((~fact["date_key"].isin(set(dim_date["date_key"]))).sum()) == 0, {})

    # is_fraud total + fraud_pattern consistency
    fraud_sum = int(fact["is_fraud"].sum())
    add("is_fraud_total", fraud_sum == FRAUD_TOTAL_EXPECTED, {"sum": fraud_sum, "expected": FRAUD_TOTAL_EXPECTED})
    inconsistent = int(((fact["is_fraud"] == 1) != fact["fraud_pattern"].notna()).sum())
    add("fraud_pattern_consistency", inconsistent == 0, {"inconsistent": inconsistent})

    # transaction_id unique
    add("transaction_id_unique", int((fact["transaction_id"].value_counts() > 1).sum()) == 0, {})

    # 1 is_current per customer
    cur = dim_customer[dim_customer["customer_sk"] != UNKNOWN_SK].groupby("customer_id")["is_current"].sum()
    add("one_is_current_per_customer", int((cur != 1).sum()) == 0, {"violations": int((cur != 1).sum())})

    # amount_vnd sum bảo toàn so với clean
    clean_sum = sum(pd.read_parquet(p, columns=["amount_vnd"])["amount_vnd"].sum() for p in (SILVER_DIR / "clean_transactions").glob("*.parquet"))
    add("amount_vnd_sum_conserved", abs(fact["amount_vnd"].sum() - clean_sum) < 1.0, {"fact": float(fact["amount_vnd"].sum()), "clean": float(clean_sum)})

    add("unknown_usage", True, {sk: int((fact[sk] == UNKNOWN_SK).sum()) for sk in sk_map})

    return checks


def load_into_duckdb() -> dict:
    """Load Gold Parquet into DuckDB via star_schema.sql → FK enforcement."""
    import duckdb
    db = GOLD_DIR / "gold.duckdb"
    if db.exists():
        db.unlink()
    con = duckdb.connect(str(db))
    con.execute(SQL_FILE.read_text(encoding="utf-8"))
    dim_specs = [
        ("dim_date", "dim_date/dim_date.parquet", None),
        ("dim_customer", "dim_customer/dim_customer.parquet", "customer_sk"),
        ("dim_account", "dim_account/dim_account.parquet", "account_sk"),
        ("dim_merchant", "dim_merchant/dim_merchant.parquet", "merchant_sk"),
        ("dim_device", "dim_device/dim_device.parquet", "device_sk"),
        ("dim_location", "dim_location/dim_location.parquet", "location_sk"),
    ]
    for table, rel, sk in dim_specs:
        where = f"WHERE {sk} <> {UNKNOWN_SK}" if sk else ""
        con.execute(f"INSERT INTO {table} SELECT * FROM read_parquet('{(GOLD_DIR / rel).as_posix()}') {where}")
    con.execute(f"INSERT INTO fact_transaction SELECT * FROM read_parquet('{(GOLD_DIR / 'fact_transaction' / '*.parquet').as_posix()}')")
    counts = {t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t, _, _ in dim_specs}
    counts["fact_transaction"] = con.execute("SELECT COUNT(*) FROM fact_transaction").fetchone()[0]
    con.close()
    return counts


def main() -> None:
    run_id = make_run_id()
    log.info("build gold start run_id=%s", run_id)
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    dim_date = build_dim_date()
    write_gold(dim_date, "dim_date", "dim_date")
    log.info("dim_date rows=%d", len(dim_date))

    dims = {
        "account": build_dim_scd1("accounts", "account_sk", "account_id", ["account_id", "customer_id", "account_type", "opened_date", "status", "currency"]),
        "merchant": build_dim_scd1("merchants", "merchant_sk", "merchant_id", ["merchant_id", "merchant_name", "category", "city", "country", "onboarded_date", "risk_score"]),
        "device": build_dim_scd1("devices", "device_sk", "device_id", ["device_id", "customer_id", "device_type", "os", "app_version", "first_seen", "is_trusted"]),
        "location": build_dim_scd1("locations", "location_sk", "location_id", ["location_id", "country", "city", "region", "lat", "lon", "timezone"]),
    }
    for name, df in dims.items():
        write_gold(df, f"dim_{name}", f"dim_{name}")
        log.info("dim_%s rows=%d", name, len(df))

    dim_customer = build_dim_customer()
    write_gold(dim_customer, "dim_customer", "dim_customer")
    log.info("dim_customer rows=%d (versions)", len(dim_customer))

    summary = {"run_id": run_id, "generated_at": datetime.now(timezone.utc).isoformat()}
    summary.update(build_fact(dim_customer, dims, run_id))

    checks = verify(dim_customer, dims, dim_date)
    try:
        summary["duckdb_load"] = load_into_duckdb()
        checks.append({"name": "duckdb_fk_enforce", "passed": True, "detail": summary["duckdb_load"]})
    except Exception as e:
        checks.append({"name": "duckdb_fk_enforce", "passed": False, "detail": str(e).splitlines()[0][:200]})

    summary["verification"] = checks
    summary["overall_passed"] = all(c["passed"] for c in checks)
    with (GOLD_DIR / "gold_summary.json").open("w", encoding="utf-8") as h:
        json.dump(summary, h, ensure_ascii=False, indent=2, default=str)

    for c in checks:
        (log.info if c["passed"] else log.error)("check %-28s %s %s", c["name"], "PASS" if c["passed"] else "FAIL", c["detail"])
    log.info("build gold done passed=%s", summary["overall_passed"])
    print(json.dumps({"overall_passed": summary["overall_passed"], "fact_rows": summary.get("fact_rows"),
                      "checks": [{c["name"]: c["passed"]} for c in checks]}, indent=2, default=str))
    raise SystemExit(0 if summary["overall_passed"] else 1)


if __name__ == "__main__":
    main()
