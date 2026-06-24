from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path
from typing import Any

import numpy as np

from .config import CHANNELS, CURRENCIES, TRANSACTION_TYPES, GeneratorConfig
from .dimensions import Dimensions
from .transactions import Transactions, transaction_id_for_row


ERROR_MANIFEST_FIELDS = [
    "row_index",
    "original_transaction_id",
    "error_type",
    "expected_bucket",
    "corrupted_field",
    "corrupted_value",
    "notes",
]

BUCKETS = {
    "null_transaction_id": "quarantine_bad_records",
    "duplicate_transaction_id": "quarantine_duplicate_transactions",
    "invalid_amount": "quarantine_invalid_amount",
    "invalid_timestamp": "quarantine_invalid_timestamp",
    "orphan_customer": "quarantine_bad_records",
    "orphan_account": "quarantine_bad_records",
    "orphan_merchant": "quarantine_bad_records",
    "invalid_currency": "quarantine_bad_records",
    "invalid_channel": "quarantine_bad_records",
    "invalid_location": "quarantine_bad_records",
}


def inject_errors(
    txn: Transactions,
    dims: Dimensions,
    cfg: GeneratorConfig,
    rng: np.random.Generator,
    protected_rows: set[int] | None = None,
) -> list[dict[str, Any]]:
    protected = np.array(sorted(protected_rows or set()), dtype=np.int64)
    all_rows = np.arange(txn.n, dtype=np.int64)
    pool = np.setdiff1d(all_rows, protected, assume_unique=True)
    manifest: list[dict[str, Any]] = []

    for error_type, rate in cfg.error_rates.items():
        requested = int(round(txn.n * rate))
        if requested <= 0 or len(pool) == 0:
            continue
        count = min(requested, len(pool))
        rows = rng.choice(pool, size=count, replace=False)
        pool = np.setdiff1d(pool, rows, assume_unique=True)
        originals = {int(row): transaction_id_for_row(txn, int(row)) for row in rows}

        _DISPATCH = {
            "null_transaction_id":      lambda: _null_transaction_id(txn, rows, originals, manifest),
            "duplicate_transaction_id": lambda: _duplicate_transaction_id(txn, rows, originals, manifest, rng),
            "invalid_amount":           lambda: _invalid_amount(txn, rows, originals, manifest),
            "invalid_timestamp":        lambda: _invalid_timestamp(txn, rows, originals, manifest),
            "orphan_customer":          lambda: _orphan_customer(txn, dims, rows, originals, manifest),
            "orphan_account":           lambda: _orphan_account(txn, dims, rows, originals, manifest),
            "orphan_merchant":          lambda: _orphan_merchant(txn, dims, rows, originals, manifest),
            "invalid_currency":         lambda: _invalid_currency(txn, rows, originals, manifest),
            "invalid_channel":          lambda: _invalid_channel(txn, rows, originals, manifest),
            "invalid_location":         lambda: _invalid_location(txn, rows, originals, manifest),
        }
        handler = _DISPATCH.get(error_type)
        if handler:
            handler()

    manifest.sort(key=lambda row: int(row["row_index"]))
    return manifest


def write_error_manifest(manifest: list[dict[str, Any]], cfg: GeneratorConfig) -> dict[str, Any]:
    path = cfg.bad_data_dir / "error_manifest.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=ERROR_MANIFEST_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(manifest)
    bucket_counts = Counter(row["expected_bucket"] for row in manifest)
    type_counts = Counter(row["error_type"] for row in manifest)
    return {
        "error_manifest_rows": len(manifest),
        "error_bucket_counts": dict(sorted(bucket_counts.items())),
        "error_type_counts": dict(sorted(type_counts.items())),
    }


def _manifest_row(
    row_idx: int,
    original_id: str,
    error_type: str,
    field: str,
    value: str,
    notes: str,
) -> dict[str, Any]:
    return {
        "row_index": row_idx,
        "original_transaction_id": original_id,
        "error_type": error_type,
        "expected_bucket": BUCKETS[error_type],
        "corrupted_field": field,
        "corrupted_value": value,
        "notes": notes,
    }


def _null_transaction_id(txn: Transactions, rows: np.ndarray, originals: dict[int, str], manifest: list[dict[str, Any]]) -> None:
    txn.transaction_id_is_null[rows] = True
    for row in rows:
        row_idx = int(row)
        manifest.append(_manifest_row(row_idx, originals[row_idx], "null_transaction_id", "transaction_id", "", "transaction_id should be not null"))


def _duplicate_transaction_id(
    txn: Transactions,
    rows: np.ndarray,
    originals: dict[int, str],
    manifest: list[dict[str, Any]],
    rng: np.random.Generator,
) -> None:
    valid_sources = np.flatnonzero(~txn.transaction_id_is_null)
    if len(valid_sources) == 0:
        valid_sources = np.arange(txn.n)
    source_rows = rng.choice(valid_sources, size=len(rows), replace=True)
    same = source_rows == rows
    source_rows[same] = (source_rows[same] + 1) % txn.n
    for target, source in zip(rows, source_rows):
        target_idx = int(target)
        source_idx = int(source)
        txn.transaction_source_idx[target_idx] = txn.transaction_source_idx[source_idx]
        manifest.append(
            _manifest_row(
                target_idx,
                originals[target_idx],
                "duplicate_transaction_id",
                "transaction_id",
                transaction_id_for_row(txn, source_idx),
                "transaction_id intentionally duplicates another raw row",
            )
        )


def _invalid_amount(txn: Transactions, rows: np.ndarray, originals: dict[int, str], manifest: list[dict[str, Any]]) -> None:
    groups = np.array_split(rows, 3)
    for row in groups[0]:
        row_idx = int(row)
        txn.amount[row_idx] = -abs(float(txn.amount[row_idx]))
        manifest.append(_manifest_row(row_idx, originals[row_idx], "invalid_amount", "amount", f"{txn.amount[row_idx]:.2f}", "negative amount"))
    for row in groups[1]:
        row_idx = int(row)
        txn.amount[row_idx] = np.nan
        manifest.append(_manifest_row(row_idx, originals[row_idx], "invalid_amount", "amount", "NaN", "amount cannot be NaN"))
    for row in groups[2]:
        row_idx = int(row)
        txn.amount_text_override[row_idx] = "amount_unknown"
        manifest.append(_manifest_row(row_idx, originals[row_idx], "invalid_amount", "amount", "amount_unknown", "amount cannot be parsed as numeric"))


def _invalid_timestamp(txn: Transactions, rows: np.ndarray, originals: dict[int, str], manifest: list[dict[str, Any]]) -> None:
    groups = np.array_split(rows, 2)
    for row in groups[0]:
        row_idx = int(row)
        value = "2099-12-31T23:59:59"
        txn.timestamp_text_override[row_idx] = value
        manifest.append(_manifest_row(row_idx, originals[row_idx], "invalid_timestamp", "transaction_time", value, "timestamp is unrealistically far in the future"))
    for row in groups[1]:
        row_idx = int(row)
        value = "not_a_timestamp"
        txn.timestamp_text_override[row_idx] = value
        manifest.append(_manifest_row(row_idx, originals[row_idx], "invalid_timestamp", "transaction_time", value, "timestamp cannot be parsed"))


def _orphan_customer(txn: Transactions, dims: Dimensions, rows: np.ndarray, originals: dict[int, str], manifest: list[dict[str, Any]]) -> None:
    txn.customer_idx[rows] = len(dims.customers) + 999
    for row in rows:
        row_idx = int(row)
        manifest.append(_manifest_row(row_idx, originals[row_idx], "orphan_customer", "customer_id", "cus_999999", "customer_id does not exist in customers.csv"))


def _orphan_account(txn: Transactions, dims: Dimensions, rows: np.ndarray, originals: dict[int, str], manifest: list[dict[str, Any]]) -> None:
    txn.account_idx[rows] = len(dims.accounts) + 999
    for row in rows:
        row_idx = int(row)
        manifest.append(_manifest_row(row_idx, originals[row_idx], "orphan_account", "account_id", "acc_9999999", "account_id does not exist in accounts.csv"))


def _orphan_merchant(txn: Transactions, dims: Dimensions, rows: np.ndarray, originals: dict[int, str], manifest: list[dict[str, Any]]) -> None:
    txn.type_idx[rows] = TRANSACTION_TYPES.index("payment")
    txn.merchant_idx[rows] = len(dims.merchants) + 999
    for row in rows:
        row_idx = int(row)
        manifest.append(_manifest_row(row_idx, originals[row_idx], "orphan_merchant", "merchant_id", "mer_999999", "merchant_id does not exist in merchants.csv"))


def _invalid_currency(txn: Transactions, rows: np.ndarray, originals: dict[int, str], manifest: list[dict[str, Any]]) -> None:
    txn.currency_idx[rows] = len(CURRENCIES)
    for row in rows:
        row_idx = int(row)
        manifest.append(_manifest_row(row_idx, originals[row_idx], "invalid_currency", "currency", "XXX", "currency should be one of VND, USD, EUR"))


def _invalid_channel(txn: Transactions, rows: np.ndarray, originals: dict[int, str], manifest: list[dict[str, Any]]) -> None:
    txn.channel_idx[rows] = len(CHANNELS)
    for row in rows:
        row_idx = int(row)
        manifest.append(_manifest_row(row_idx, originals[row_idx], "invalid_channel", "channel", "foo", "channel should be one of configured payment channels"))


def _invalid_location(txn: Transactions, rows: np.ndarray, originals: dict[int, str], manifest: list[dict[str, Any]]) -> None:
    for row in rows:
        row_idx = int(row)
        txn.location_text_override[row_idx] = ("loc_bad_record", "ZZ", "Unknown City")
        manifest.append(_manifest_row(row_idx, originals[row_idx], "invalid_location", "country/city", "ZZ/Unknown City", "country and city are outside the reference list"))
