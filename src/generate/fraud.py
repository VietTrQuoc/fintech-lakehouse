from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import numpy as np

from .config import CHANNELS, CURRENCIES, STATUSES, TRANSACTION_TYPES, GeneratorConfig, load_paysim_params
from .dimensions import Dimensions
from .transactions import Transactions, transaction_id_for_row


FRAUD_LABEL_FIELDS = ["transaction_id", "is_fraud", "fraud_pattern", "label_source", "label_time"]


def inject_fraud(txn: Transactions, dims: Dimensions, cfg: GeneratorConfig, rng: np.random.Generator) -> set[int]:
    target_count = int(round(txn.n * cfg.fraud_rate))
    if target_count <= 0:
        return set()

    fraud_params = load_paysim_params(cfg.paysim_params_path).get("fraud_amount_lognormal", {})
    fraud_rows = rng.choice(txn.n, size=min(target_count, txn.n), replace=False)
    pattern_counts = _pattern_counts(len(fraud_rows), cfg.fraud_pattern_mix)
    cursor = 0
    for pattern, count in pattern_counts.items():
        rows = np.array(fraud_rows[cursor : cursor + count], dtype=np.int64)
        cursor += count
        if count == 0:
            continue
        _FRAUD_DISPATCH = {
            "velocity_burst":      lambda: _velocity_burst(txn, dims, rows, rng, fraud_params),
            "amount_spike":        lambda: _amount_spike(txn, rows, rng, fraud_params),
            "new_device_location": lambda: _new_device_location(txn, dims, rows, rng, fraud_params),
            "cross_country":       lambda: _cross_country(txn, dims, rows, rng, fraud_params),
            "night_high_amount":   lambda: _night_high_amount(txn, rows, rng, fraud_params),
            "card_testing":        lambda: _card_testing(txn, dims, rows, rng),
        }
        handler = _FRAUD_DISPATCH.get(pattern)
        if handler:
            handler()
        for row in rows:
            txn.fraud_patterns[int(row)] = pattern

    return {int(row) for row in fraud_rows}


def write_fraud_labels(txn: Transactions, cfg: GeneratorConfig) -> dict[str, int]:
    path = cfg.raw_dir / "fraud_labels.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    fraud_count = len(txn.fraud_patterns)
    total_rows = txn.n if cfg.label_all_transactions else fraud_count
    rows_iter = ((i, txn.fraud_patterns.get(i, "")) for i in range(txn.n)) if cfg.label_all_transactions else sorted(txn.fraud_patterns.items())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(FRAUD_LABEL_FIELDS)
        for row_idx, pattern in rows_iter:
            writer.writerow([transaction_id_for_row(txn, row_idx), 1 if pattern else 0, pattern, "synthetic_rule_engine", str(txn.ingestion_time[row_idx].astype("datetime64[s]"))])
    return {"fraud_label_rows": total_rows, "fraud_positive_rows": fraud_count}


def _pattern_counts(total: int, mix: dict[str, float]) -> dict[str, int]:
    patterns = list(mix.keys())
    weights = np.array([mix[name] for name in patterns], dtype=np.float64)
    weights = weights / weights.sum()
    counts = np.floor(weights * total).astype(int)
    remainder = total - int(counts.sum())
    for idx in np.argsort(-(weights * total - counts))[:remainder]:
        counts[idx] += 1
    return dict(zip(patterns, counts.tolist()))


def _set_customer_context(txn: Transactions, dims: Dimensions, rows: np.ndarray, customer_idx: int) -> None:
    txn.customer_idx[rows] = customer_idx
    txn.account_idx[rows] = dims.primary_account_idx[customer_idx]
    txn.device_idx[rows] = dims.primary_device_idx[customer_idx]
    txn.location_idx[rows] = dims.customer_home_location_idx[customer_idx]


def _set_ingestion_after_event(txn: Transactions, rows: np.ndarray, rng: np.random.Generator) -> None:
    lags = rng.integers(30, 1_800, size=len(rows)).astype("timedelta64[s]")
    txn.ingestion_time[rows] = txn.transaction_time[rows] + lags


def _fraud_vnd_finish(txn: Transactions, rows: np.ndarray, rng: np.random.Generator) -> None:
    """Set VND currency + ingestion lag — common tail for all fraud patterns."""
    txn.currency_idx[rows] = CURRENCIES.index("VND")
    _set_ingestion_after_event(txn, rows, rng)


def _fraud_amount_floor(type_idx: np.ndarray, fraud_params: dict[str, Any], rng: np.random.Generator) -> np.ndarray:
    """Draw a per-row fraud amount (VND) from the PaySim-calibrated fraud amount distribution for
    each row's transaction type. Falls back to a wide default when a type is absent from the
    calibration file. Patterns take max(spike_multiple_of_history, this_floor) so the amount stays
    both anomalous vs the customer's own history and consistent with real fraud magnitudes."""
    n = len(type_idx)
    mu = np.full(n, 14.0, dtype=np.float64)
    sigma = np.full(n, 0.85, dtype=np.float64)
    for idx, name in enumerate(TRANSACTION_TYPES):
        mask = type_idx == idx
        if not mask.any():
            continue
        params = fraud_params.get(name)
        if params:
            mu[mask] = float(params["mu"])
            sigma[mask] = float(params["sigma"])
    return rng.lognormal(mean=mu, sigma=sigma)


def _velocity_burst(txn: Transactions, dims: Dimensions, rows: np.ndarray, rng: np.random.Generator, fraud_params: dict[str, Any]) -> None:
    if len(rows) == 0:
        return
    groups = np.array_split(rows, max(1, len(rows) // 7))
    for group in groups:
        if len(group) == 0:
            continue
        customer_idx = int(rng.integers(0, len(dims.customers)))
        _set_customer_context(txn, dims, group, customer_idx)
        base_hour = txn.transaction_time[int(group[0])].astype("datetime64[h]")
        offsets = np.sort(rng.integers(0, 3_600, size=len(group))).astype("timedelta64[s]")
        txn.transaction_time[group] = base_hour + offsets
        txn.type_idx[group] = rng.choice(
            [TRANSACTION_TYPES.index("transfer"), TRANSACTION_TYPES.index("payment")],
            size=len(group),
            p=[0.65, 0.35],
        )
        txn.channel_idx[group] = CHANNELS.index("mobile")
        txn.amount[group] = np.maximum(txn.amount[group] * rng.uniform(3.0, 7.0, size=len(group)), _fraud_amount_floor(txn.type_idx[group], fraud_params, rng))
        _fraud_vnd_finish(txn, group, rng)


def _amount_spike(txn: Transactions, rows: np.ndarray, rng: np.random.Generator, fraud_params: dict[str, Any]) -> None:
    txn.type_idx[rows] = rng.choice(
        [TRANSACTION_TYPES.index("transfer"), TRANSACTION_TYPES.index("withdrawal"), TRANSACTION_TYPES.index("payment")],
        size=len(rows),
        p=[0.52, 0.28, 0.20],
    )
    txn.amount[rows] = np.maximum(txn.amount[rows] * rng.uniform(9.0, 24.0, size=len(rows)), _fraud_amount_floor(txn.type_idx[rows], fraud_params, rng))
    txn.amount[rows] = np.minimum(txn.amount[rows], 400_000_000)
    _fraud_vnd_finish(txn, rows, rng)


def _new_device_location(txn: Transactions, dims: Dimensions, rows: np.ndarray, rng: np.random.Generator, fraud_params: dict[str, Any]) -> None:
    foreign_start = max(0, len(dims.locations) - 5)
    txn.device_idx[rows] = rng.integers(0, len(dims.devices), size=len(rows))
    txn.location_idx[rows] = rng.integers(foreign_start, len(dims.locations), size=len(rows))
    txn.channel_idx[rows] = rng.choice([CHANNELS.index("mobile"), CHANNELS.index("web")], size=len(rows), p=[0.82, 0.18])
    txn.type_idx[rows] = rng.choice([TRANSACTION_TYPES.index("transfer"), TRANSACTION_TYPES.index("payment")], size=len(rows), p=[0.58, 0.42])
    txn.amount[rows] = np.maximum(txn.amount[rows] * rng.uniform(4.0, 12.0, size=len(rows)), _fraud_amount_floor(txn.type_idx[rows], fraud_params, rng))
    _fraud_vnd_finish(txn, rows, rng)


def _cross_country(txn: Transactions, dims: Dimensions, rows: np.ndarray, rng: np.random.Generator, fraud_params: dict[str, Any]) -> None:
    if len(rows) == 0:
        return
    foreign_start = max(0, len(dims.locations) - 5)
    groups = np.array_split(rows, max(1, len(rows) // 2))
    for group in groups:
        customer_idx = int(rng.integers(0, len(dims.customers)))
        _set_customer_context(txn, dims, group, customer_idx)
        base = txn.transaction_time[int(group[0])].astype("datetime64[h]")
        if len(group) == 1:
            txn.location_idx[group] = rng.integers(foreign_start, len(dims.locations), size=1)
            txn.transaction_time[group] = base + np.timedelta64(int(rng.integers(900, 3_600)), "s")
        else:
            txn.location_idx[group[0]] = dims.customer_home_location_idx[customer_idx]
            txn.location_idx[group[1:]] = rng.integers(foreign_start, len(dims.locations), size=len(group) - 1)
            offsets = np.linspace(0, 2_400, len(group)).astype(np.int64).astype("timedelta64[s]")
            txn.transaction_time[group] = base + offsets
        txn.channel_idx[group] = CHANNELS.index("mobile")
        txn.type_idx[group] = rng.choice([TRANSACTION_TYPES.index("transfer"), TRANSACTION_TYPES.index("payment")], size=len(group), p=[0.62, 0.38])
        txn.amount[group] = np.maximum(txn.amount[group] * rng.uniform(3.0, 9.0, size=len(group)), _fraud_amount_floor(txn.type_idx[group], fraud_params, rng))
        _fraud_vnd_finish(txn, group, rng)


def _night_high_amount(txn: Transactions, rows: np.ndarray, rng: np.random.Generator, fraud_params: dict[str, Any]) -> None:
    days = txn.transaction_time[rows].astype("datetime64[D]")
    seconds = rng.integers(2 * 3_600, 4 * 3_600 + 1_800, size=len(rows)).astype("timedelta64[s]")
    txn.transaction_time[rows] = days + seconds
    txn.channel_idx[rows] = rng.choice([CHANNELS.index("mobile"), CHANNELS.index("web")], size=len(rows), p=[0.72, 0.28])
    txn.type_idx[rows] = rng.choice([TRANSACTION_TYPES.index("transfer"), TRANSACTION_TYPES.index("withdrawal")], size=len(rows), p=[0.72, 0.28])
    txn.amount[rows] = np.maximum(txn.amount[rows] * rng.uniform(6.0, 18.0, size=len(rows)), _fraud_amount_floor(txn.type_idx[rows], fraud_params, rng))
    _fraud_vnd_finish(txn, rows, rng)


def _card_testing(txn: Transactions, dims: Dimensions, rows: np.ndarray, rng: np.random.Generator) -> None:
    if len(rows) == 0:
        return
    groups = np.array_split(rows, max(1, len(rows) // 6))
    for group in groups:
        if len(group) == 0:
            continue
        customer_idx = int(rng.integers(0, len(dims.customers)))
        merchant_idx = int(rng.integers(0, len(dims.merchants)))
        _set_customer_context(txn, dims, group, customer_idx)
        txn.merchant_idx[group] = merchant_idx
        txn.type_idx[group] = TRANSACTION_TYPES.index("payment")
        txn.channel_idx[group] = rng.choice([CHANNELS.index("pos"), CHANNELS.index("web")], size=len(group), p=[0.35, 0.65])
        txn.amount[group] = rng.uniform(5_000, 95_000, size=len(group))
        txn.status_idx[group] = STATUSES.index("failed")
        txn.status_idx[group[-1]] = STATUSES.index("success")
        base_minute = txn.transaction_time[int(group[0])].astype("datetime64[m]")
        offsets = np.sort(rng.integers(0, 1_500, size=len(group))).astype("timedelta64[s]")
        txn.transaction_time[group] = base_minute + offsets
        _fraud_vnd_finish(txn, group, rng)
