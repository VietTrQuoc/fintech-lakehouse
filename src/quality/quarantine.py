"""Quarantine split: route Silver rows → clean or quarantine buckets."""

import json
import logging
import shutil
from collections import Counter
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from src.paths import CLEAN_DIR, LOG_DIR, QUALITY_DIR, QUARANTINE_DIR, SILVER_DIR

QUALITY_REPORT = QUALITY_DIR / "quality_report.json"
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "quarantine.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("quarantine")

BUCKETS = [
    "quarantine_invalid_amount",
    "quarantine_invalid_timestamp",
    "quarantine_duplicate_transactions",
    "quarantine_bad_records",
]

# 28 cols = 24 business/derived + 3 SOFT FK flags + lineage. Omit hard DQ flags (all False in valid).
CLEAN_KEEP_COLS = [
    "transaction_id", "customer_id", "account_id", "merchant_id", "device_id", "card_id",
    "transaction_type", "channel", "status", "currency",
    "amount_original", "exchange_rate", "amount_vnd",
    "country", "city", "location_id",
    "event_time", "event_date", "ingestion_time", "ingestion_lag_seconds",
    "source_system", "batch_id", "file_name", "source_row_number",
    "_dq_fk_customer", "_dq_fk_account", "_dq_fk_merchant", "_dq_run_id",
]


def make_run_id() -> str:
    return "quarantine_" + datetime.now().strftime("%Y%m%d_%H%M%S")


def reset_output_dirs() -> None:
    """Remove + recreate clean and quarantine dirs for idempotent re-runs."""
    for d in (CLEAN_DIR, QUARANTINE_DIR):
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True, exist_ok=True)


def split_partition(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    """Split one Silver partition → (clean_df, {bucket: rows_df}). Routes by _is_valid/_dq_bucket only."""
    clean_df = df.loc[df["_is_valid"], CLEAN_KEEP_COLS].copy()
    bucket_frames = {b: df.loc[df["_dq_bucket"] == b].copy() for b in BUCKETS}
    return clean_df, bucket_frames


def build_quarantine_frame(df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    """Add reason/lineage columns at front for auditability."""
    out = df.copy()
    out["quarantine_reason"] = out["_dq_errors"]
    out["quarantine_bucket"] = out["_dq_bucket"]
    out["quarantined_at"] = datetime.now(timezone.utc).isoformat()
    out["quarantine_run_id"] = run_id
    lead = ["quarantine_bucket", "quarantine_reason", "quarantined_at", "quarantine_run_id"]
    return out[lead + [c for c in out.columns if c not in lead]]


def run_quarantine_split() -> dict:
    """Orchestrate: reset → process each partition → write clean + accumulate quarantine → verify."""
    run_id = make_run_id()
    log.info("quarantine split start run_id=%s", run_id)
    reset_output_dirs()

    parts = sorted((SILVER_DIR / "transactions").glob("silver_transactions_*.parquet"))
    clean_counts: dict[str, int] = {}
    # Accumulate quarantine across months (1 file/bucket), write clean immediately per month.
    bucket_acc: dict[str, list[pd.DataFrame]] = {b: [] for b in BUCKETS}
    silver_cols: list[str] = []

    for p in parts:
        month = p.stem.replace("silver_transactions_", "")
        df = pd.read_parquet(p)
        silver_cols = list(df.columns)
        clean_df, bucket_frames = split_partition(df)
        clean_df.to_parquet(CLEAN_DIR / f"clean_transactions_{month}.parquet", index=False)
        clean_counts[month] = len(clean_df)
        quarantined = 0
        for b, sub in bucket_frames.items():
            if len(sub):
                bucket_acc[b].append(build_quarantine_frame(sub, run_id))
                quarantined += len(sub)
        log.info("clean_transactions[%s] rows=%7d clean=%7d quarantined=%6d", month, len(df), len(clean_df), quarantined)

    # Write 1 file/bucket. Empty buckets still get a schema-only file so glob always finds the dataset.
    quarantine_counts: dict[str, int] = {}
    empty_cols = ["quarantine_bucket", "quarantine_reason", "quarantined_at", "quarantine_run_id"] + silver_cols
    for b in BUCKETS:
        out_dir = QUARANTINE_DIR / b
        out_dir.mkdir(parents=True, exist_ok=True)
        frame = pd.concat(bucket_acc[b], ignore_index=True) if bucket_acc[b] else pd.DataFrame(columns=empty_cols)
        frame.to_parquet(out_dir / f"{b}.parquet", index=False)
        quarantine_counts[b] = len(frame)
        log.info("quarantine[%-34s] rows=%7d", b, len(frame))

    summary = _build_summary(run_id, clean_counts, quarantine_counts)
    checks = verify(summary)
    summary["verification"] = checks
    summary["overall_passed"] = all(c["passed"] for c in checks)

    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    with (QUARANTINE_DIR / "quarantine_summary.json").open("w", encoding="utf-8") as h:
        json.dump(summary, h, ensure_ascii=False, indent=2, default=str)
    log.info("quarantine split done passed=%s", summary["overall_passed"])
    return summary


def _build_summary(run_id: str, clean_counts: dict, quarantine_counts: dict) -> dict:
    """Aggregate counts. silver_total via metadata (zero-data read)."""
    silver_total = sum(pq.read_metadata(p).num_rows for p in (SILVER_DIR / "transactions").glob("silver_transactions_*.parquet"))
    clean_total = sum(clean_counts.values())
    q_total = sum(quarantine_counts.values())
    return {
        "run_id": run_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "totals": {
            "silver_rows": silver_total,
            "clean_rows": clean_total,
            "quarantine_rows": q_total,
            "conserved": clean_total + q_total == silver_total,  # tổng phải bảo toàn
        },
        "clean_by_month": clean_counts,
        "bucket_counts": quarantine_counts,
    }


def verify(summary: dict) -> list[dict]:
    """Post-split checks: conservation, reconciliation, purity, uniqueness."""
    checks: list[dict] = []

    def add(name: str, passed: bool, detail: Any) -> None:
        checks.append({"name": name, "passed": bool(passed), "detail": detail})

    # 1. Bảo toàn: clean + quarantine == silver (không mất/nhân dòng khi split).
    t = summary["totals"]
    add("conservation_sum_eq_silver", t["clean_rows"] + t["quarantine_rows"] == t["silver_rows"],
        {"clean": t["clean_rows"], "quarantine": t["quarantine_rows"], "silver": t["silver_rows"]})

    # 2-3. Đối chiếu Day 7 (nếu có quality_report.json): clean == valid, bucket khớp report.
    if QUALITY_REPORT.exists():
        rep = json.loads(QUALITY_REPORT.read_text(encoding="utf-8"))
        add("clean_eq_valid_from_report", t["clean_rows"] == rep["totals"]["valid_rows"],
            {"clean": t["clean_rows"], "report_valid": rep["totals"]["valid_rows"]})
        add("buckets_match_report", summary["bucket_counts"] == {k: v for k, v in rep["bucket_counts"].items()},
            {"split": summary["bucket_counts"], "report": rep["bucket_counts"]})

    # 4-6. Đọc lại từng file quarantine: không lẫn dòng valid, reason không rỗng, mỗi file đúng 1 bucket.
    valid_in_q = 0
    empty_reason = 0
    impure = []
    for b in BUCKETS:
        path = QUARANTINE_DIR / b / f"{b}.parquet"
        qdf = pd.read_parquet(path, columns=["_is_valid", "quarantine_reason", "quarantine_bucket"])
        if len(qdf) == 0:
            continue
        valid_in_q += int(qdf["_is_valid"].sum())                                   # phải = 0
        empty_reason += int((qdf["quarantine_reason"].fillna("") == "").sum())      # phải = 0
        uniq = qdf["quarantine_bucket"].unique().tolist()
        if uniq != [b]:                                                             # purity: chỉ 1 bucket/file
            impure.append({b: uniq})
    add("no_valid_in_quarantine", valid_in_q == 0, {"valid_rows_found": valid_in_q})
    add("reason_non_empty", empty_reason == 0, {"empty_reason_rows": empty_reason})
    add("bucket_purity", len(impure) == 0, {"impure": impure})

    # 7. transaction_id duy nhất trong clean (degenerate dim phải unique cho fact sạch — grain §5).
    ids = pd.concat(
        [pd.read_parquet(p, columns=["transaction_id"]) for p in CLEAN_DIR.glob("clean_transactions_*.parquet")],
        ignore_index=True,
    )["transaction_id"]
    dup_ids = int((ids.value_counts() > 1).sum())
    add("transaction_id_unique_in_clean", dup_ids == 0, {"duplicated_ids": dup_ids})

    return checks


def main() -> None:
    summary = run_quarantine_split()
    # In gọn ra stdout (giống validate.py) + exit code cho Airflow/CI.
    print(json.dumps({
        "overall_passed": summary["overall_passed"],
        "totals": summary["totals"],
        "bucket_counts": summary["bucket_counts"],
        "verification": [{"name": c["name"], "passed": c["passed"]} for c in summary["verification"]],
    }, indent=2, default=str))
    raise SystemExit(0 if summary["overall_passed"] else 1)


if __name__ == "__main__":
    main()
