from __future__ import annotations

import argparse
import csv
import json
import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np

from .config import DEFAULT_PAYSIM_PARAMS_PATH, RAW_DIR, BAD_DATA_DIR, TRANSACTION_TYPES, load_paysim_params


@dataclass
class OnlineStats:
    count: int = 0
    mean: float = 0.0
    m2: float = 0.0

    def add(self, value: float) -> None:
        self.count += 1
        delta = value - self.mean
        self.mean += delta / self.count
        delta2 = value - self.mean
        self.m2 += delta * delta2

    @property
    def std(self) -> float:
        return math.sqrt(self.m2 / max(1, self.count - 1))


def main() -> None:
    args = _parse_args()
    result = validate(args.raw_dir, args.bad_data_dir, args.paysim_params)
    print(json.dumps(result, indent=2, default=str))
    if result["unexpected_issue_count"] > 0:
        raise SystemExit(1)


def validate(raw_dir: Path, bad_data_dir: Path, paysim_params_path: Path) -> dict[str, Any]:
    generation_manifest = _load_json_if_exists(raw_dir / "generation_manifest.json")
    expected_txn = int(generation_manifest.get("transaction_rows", 0)) if generation_manifest else 0
    expected_customers = int(generation_manifest.get("customers", 0)) if generation_manifest else 0

    customers = _read_id_set(raw_dir / "customers.csv", "customer_id")
    accounts = _read_id_set(raw_dir / "accounts.csv", "account_id")
    merchants = _read_id_set(raw_dir / "merchants.csv", "merchant_id")
    error_rows, error_type_by_row, error_bucket_counts = _read_error_manifest(bad_data_dir / "error_manifest.csv")

    txn_summary = _scan_transactions(raw_dir / "transactions", customers, accounts, merchants, error_type_by_row)
    fraud_summary = _scan_fraud_labels(raw_dir / "fraud_labels.csv")
    api_summary = _scan_json_sources(raw_dir)
    distribution_summary = _compare_distribution(txn_summary["log_amount_stats"], paysim_params_path)

    issues: list[str] = []
    if expected_txn and txn_summary["transaction_rows"] != expected_txn:
        issues.append(f"transaction row count {txn_summary['transaction_rows']} != expected {expected_txn}")
    if expected_customers and len(customers) != expected_customers:
        issues.append(f"customer row count {len(customers)} != expected {expected_customers}")
    issues.extend(txn_summary["issues"])
    if fraud_summary["fraud_positive_rows"] == 0:
        issues.append("fraud_labels.csv has no positive labels")

    fraud_rate = fraud_summary["fraud_positive_rows"] / max(1, fraud_summary["fraud_label_rows"])
    result = {
        "counts": {
            "customers": len(customers),
            "accounts": len(accounts),
            "merchants": len(merchants),
            "transactions": txn_summary["transaction_rows"],
            "transaction_partitions": txn_summary["partition_files"],
            "fraud_label_rows": fraud_summary["fraud_label_rows"],
            "fraud_positive_rows": fraud_summary["fraud_positive_rows"],
            "error_manifest_rows": len(error_rows),
        },
        "fraud_rate": round(fraud_rate, 6),
        "error_bucket_counts": dict(error_bucket_counts),
        "ingestion_lag_seconds": txn_summary["lag_stats"],
        "distribution": distribution_summary,
        "json_sources": api_summary,
        "unexpected_issues": issues,
        "unexpected_issue_count": len(issues),
    }
    return result


def _scan_transactions(
    transactions_dir: Path,
    customers: set[str],
    accounts: set[str],
    merchants: set[str],
    error_type_by_row: dict[int, set[str]],
) -> dict[str, Any]:
    issues: list[str] = []
    txn_files = sorted(transactions_dir.glob("txn_*.csv"))
    if not txn_files:
        return {
            "transaction_rows": 0,
            "partition_files": 0,
            "issues": ["no transaction partition files found"],
            "lag_stats": {},
            "log_amount_stats": {},
        }

    seen_ids: dict[str, int] = {}
    duplicate_groups: dict[str, list[int]] = defaultdict(list)
    lag_values: list[float] = []
    log_amount_stats = {name: OnlineStats() for name in TRANSACTION_TYPES}
    row_count = 0
    unexpected_fk = Counter()
    unexpected_quality = Counter()
    manifest_row_hits = Counter()

    for path in txn_files:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                row_count += 1
                source_row_number = int(row.get("source_row_number", row_count - 1))
                row_errors = error_type_by_row.get(source_row_number, set())
                for error in row_errors:
                    manifest_row_hits[error] += 1

                transaction_id = row["transaction_id"]
                if transaction_id:
                    if transaction_id in seen_ids:
                        if not duplicate_groups[transaction_id]:
                            duplicate_groups[transaction_id].append(seen_ids[transaction_id])
                        duplicate_groups[transaction_id].append(source_row_number)
                    else:
                        seen_ids[transaction_id] = source_row_number
                elif "null_transaction_id" not in row_errors:
                    unexpected_quality["null_transaction_id"] += 1

                if row["customer_id"] not in customers and "orphan_customer" not in row_errors:
                    unexpected_fk["customer_id"] += 1
                if row["account_id"] not in accounts and "orphan_account" not in row_errors:
                    unexpected_fk["account_id"] += 1
                if row["merchant_id"] and row["merchant_id"] not in merchants and "orphan_merchant" not in row_errors:
                    unexpected_fk["merchant_id"] += 1

                amount = _parse_float(row["amount"])
                if amount is None or not math.isfinite(amount) or amount <= 0:
                    if "invalid_amount" not in row_errors:
                        unexpected_quality["invalid_amount"] += 1
                elif row["transaction_type"] in log_amount_stats and row["currency"] in {"VND", "USD", "EUR"}:
                    vnd_amount = _amount_to_vnd(amount, row["currency"])
                    log_amount_stats[row["transaction_type"]].add(math.log(vnd_amount + 1.0))

                tx_time = _parse_datetime(row["transaction_time"])
                ingest_time = _parse_datetime(row["ingestion_time"])
                if tx_time is None or tx_time.year >= 2099:
                    if "invalid_timestamp" not in row_errors:
                        unexpected_quality["invalid_timestamp"] += 1
                elif ingest_time is not None:
                    lag_values.append((ingest_time - tx_time).total_seconds())

                if row["currency"] not in {"VND", "USD", "EUR"} and "invalid_currency" not in row_errors:
                    unexpected_quality["invalid_currency"] += 1
                if row["channel"] not in {"mobile", "qr", "pos", "web", "atm"} and "invalid_channel" not in row_errors:
                    unexpected_quality["invalid_channel"] += 1
                if row["country"] == "ZZ" and "invalid_location" not in row_errors:
                    unexpected_quality["invalid_location"] += 1

    duplicate_manifest_rows = {row for row, errors in error_type_by_row.items() if "duplicate_transaction_id" in errors}
    unexpected_duplicate_groups = 0
    for rows in duplicate_groups.values():
        if not any(row in duplicate_manifest_rows for row in rows):
            unexpected_duplicate_groups += 1
    if unexpected_duplicate_groups:
        unexpected_quality["duplicate_transaction_id"] += unexpected_duplicate_groups

    if unexpected_fk:
        issues.append(f"unexpected FK failures outside manifest: {dict(unexpected_fk)}")
    if unexpected_quality:
        issues.append(f"unexpected DQ failures outside manifest: {dict(unexpected_quality)}")

    lag_array = np.array(lag_values, dtype=np.float64) if lag_values else np.array([], dtype=np.float64)
    lag_stats = {
        "p50": int(np.percentile(lag_array, 50)) if len(lag_array) else None,
        "p95": int(np.percentile(lag_array, 95)) if len(lag_array) else None,
        "p99": int(np.percentile(lag_array, 99)) if len(lag_array) else None,
        "max": int(np.max(lag_array)) if len(lag_array) else None,
        "late_gt_1h_pct": round(float(np.mean(lag_array > 3600) * 100), 4) if len(lag_array) else None,
    }
    log_stats_payload = {
        name: {"count": stats.count, "mu": round(stats.mean, 4), "sigma": round(stats.std, 4)}
        for name, stats in log_amount_stats.items()
        if stats.count
    }
    return {
        "transaction_rows": row_count,
        "partition_files": len(txn_files),
        "issues": issues,
        "lag_stats": lag_stats,
        "log_amount_stats": log_stats_payload,
        "manifest_row_hits": dict(manifest_row_hits),
    }


def _compare_distribution(stats: dict[str, Any], paysim_params_path: Path) -> dict[str, Any]:
    params = load_paysim_params(paysim_params_path)
    expected = params.get("amount_lognormal", {})
    comparison = {}
    for txn_type, actual in stats.items():
        if txn_type not in expected:
            continue
        comparison[txn_type] = {
            "actual_mu": actual["mu"],
            "expected_mu": round(float(expected[txn_type]["mu"]), 4),
            "actual_sigma": actual["sigma"],
            "expected_sigma": round(float(expected[txn_type]["sigma"]), 4),
            "count": actual["count"],
        }
    return comparison


def _read_id_set(path: Path, column: str) -> set[str]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return {row[column] for row in reader}


def _read_error_manifest(path: Path) -> tuple[list[dict[str, str]], dict[int, set[str]], Counter]:
    if not path.exists():
        return [], {}, Counter()
    rows: list[dict[str, str]] = []
    by_row: dict[int, set[str]] = defaultdict(set)
    buckets: Counter = Counter()
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append(row)
            row_idx = int(row["row_index"])
            by_row[row_idx].add(row["error_type"])
            buckets[row["expected_bucket"]] += 1
    return rows, by_row, buckets


def _scan_fraud_labels(path: Path) -> dict[str, int]:
    if not path.exists():
        return {"fraud_label_rows": 0, "fraud_positive_rows": 0}
    rows = 0
    positives = 0
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows += 1
            positives += int(row.get("is_fraud", "0") == "1")
    return {"fraud_label_rows": rows, "fraud_positive_rows": positives}


def _scan_json_sources(raw_dir: Path) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for file_name in ("exchange_rates.json", "customer_events.json"):
        path = raw_dir / file_name
        if not path.exists():
            result[file_name] = {"exists": False, "records": 0}
            continue
        payload = _load_json_if_exists(path)
        result[file_name] = {"exists": True, "records": len(payload.get("records", []))}
    return result


def _load_json_if_exists(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _parse_float(value: str) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _amount_to_vnd(amount: float, currency: str) -> float:
    if currency == "USD":
        return amount * 25_400.0
    if currency == "EUR":
        return amount * 27_300.0
    return amount


def _parse_datetime(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate generated synthetic raw data.")
    parser.add_argument("--raw-dir", type=Path, default=RAW_DIR)
    parser.add_argument("--bad-data-dir", type=Path, default=BAD_DATA_DIR)
    parser.add_argument("--paysim-params", type=Path, default=DEFAULT_PAYSIM_PARAMS_PATH)
    return parser.parse_args()


if __name__ == "__main__":
    main()
