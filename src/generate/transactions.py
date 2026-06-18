from __future__ import annotations

import csv
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any

import numpy as np

from .config import (
    CHANNELS,
    CURRENCIES,
    SOURCE_SYSTEMS,
    STATUSES,
    TRANSACTION_TYPES,
    GeneratorConfig,
    load_paysim_params,
)
from .dimensions import Dimensions, account_id, card_id, customer_id, device_id, merchant_id


TXN_FIELDS = [
    "transaction_id",
    "customer_id",
    "account_id",
    "merchant_id",
    "device_id",
    "card_id",
    "amount",
    "currency",
    "transaction_type",
    "channel",
    "transaction_time",
    "country",
    "city",
    "status",
    "location_id",
    "ingestion_time",
    "source_system",
    "batch_id",
    "file_name",
    "source_row_number",
]


@dataclass
class Transactions:
    n: int
    transaction_source_idx: np.ndarray
    transaction_id_is_null: np.ndarray
    customer_idx: np.ndarray
    account_idx: np.ndarray
    merchant_idx: np.ndarray
    device_idx: np.ndarray
    location_idx: np.ndarray
    amount: np.ndarray
    currency_idx: np.ndarray
    type_idx: np.ndarray
    channel_idx: np.ndarray
    status_idx: np.ndarray
    source_idx: np.ndarray
    transaction_time: np.ndarray
    ingestion_time: np.ndarray
    amount_text_override: dict[int, str] = field(default_factory=dict)
    timestamp_text_override: dict[int, str] = field(default_factory=dict)
    location_text_override: dict[int, tuple[str, str, str]] = field(default_factory=dict)
    fraud_patterns: dict[int, str] = field(default_factory=dict)


def transaction_id_for_source(source_idx: int) -> str:
    return f"txn_{source_idx + 1:012d}"


def transaction_id_for_row(txn: Transactions, row_idx: int) -> str:
    if bool(txn.transaction_id_is_null[row_idx]):
        return ""
    return transaction_id_for_source(int(txn.transaction_source_idx[row_idx]))


def generate_transactions(cfg: GeneratorConfig, dims: Dimensions, rng: np.random.Generator) -> Transactions:
    params = load_paysim_params(cfg.paysim_params_path)
    n = cfg.n_transactions

    type_probs = _ordered_probs(params.get("type_ratios", {}), TRANSACTION_TYPES)
    type_idx = rng.choice(len(TRANSACTION_TYPES), size=n, p=type_probs).astype(np.int16)

    customer_idx = _sample_customers(n, cfg.n_customers, rng)
    account_idx = dims.primary_account_idx[customer_idx].copy()
    has_secondary_account = dims.secondary_account_idx[customer_idx] >= 0
    use_secondary_account = has_secondary_account & (rng.random(n) < 0.24)
    account_idx[use_secondary_account] = dims.secondary_account_idx[customer_idx[use_secondary_account]]

    channel_idx = _sample_channels(type_idx, cfg, rng)
    currency_idx = rng.choice(len(CURRENCIES), size=n, p=_ordered_probs(cfg.currency_mix, CURRENCIES)).astype(np.int16)

    merchant_idx = np.full(n, -1, dtype=np.int32)
    merchant_mask = np.isin(type_idx, [TRANSACTION_TYPES.index("payment"), TRANSACTION_TYPES.index("debit")])
    merchant_count = int(merchant_mask.sum())
    if merchant_count:
        merchant_idx[merchant_mask] = rng.choice(
            len(dims.merchants),
            size=merchant_count,
            p=dims.merchant_popularity,
        ).astype(np.int32)

    device_idx = dims.primary_device_idx[customer_idx].copy()
    has_secondary_device = dims.secondary_device_idx[customer_idx] >= 0
    use_secondary_device = has_secondary_device & (rng.random(n) < 0.09)
    device_idx[use_secondary_device] = dims.secondary_device_idx[customer_idx[use_secondary_device]]

    location_idx = dims.customer_home_location_idx[customer_idx].astype(np.int16).copy()
    move_mask = rng.random(n) < 0.037
    if int(move_mask.sum()):
        vn_location_count = max(1, len(dims.locations) - 5)
        location_idx[move_mask] = rng.integers(0, vn_location_count, size=int(move_mask.sum()))
    foreign_mask = rng.random(n) < 0.006
    if int(foreign_mask.sum()):
        first_foreign = len(dims.locations) - 5
        location_idx[foreign_mask] = rng.integers(first_foreign, len(dims.locations), size=int(foreign_mask.sum()))

    amount = _sample_amounts(type_idx, currency_idx, params, rng)
    transaction_time = _sample_event_times(cfg, n, rng)
    ingestion_time = transaction_time + _sample_ingestion_lags(n, rng)
    status_idx = _sample_statuses(merchant_idx, channel_idx, dims, n, rng)
    source_idx = _source_by_channel(channel_idx, type_idx, rng)

    return Transactions(
        n=n,
        transaction_source_idx=np.arange(n, dtype=np.int64),
        transaction_id_is_null=np.zeros(n, dtype=bool),
        customer_idx=customer_idx.astype(np.int32),
        account_idx=account_idx.astype(np.int32),
        merchant_idx=merchant_idx.astype(np.int32),
        device_idx=device_idx.astype(np.int32),
        location_idx=location_idx.astype(np.int16),
        amount=amount.astype(np.float64),
        currency_idx=currency_idx,
        type_idx=type_idx,
        channel_idx=channel_idx,
        status_idx=status_idx,
        source_idx=source_idx.astype(np.int16),
        transaction_time=transaction_time,
        ingestion_time=ingestion_time,
    )


def write_transactions(txn: Transactions, dims: Dimensions, cfg: GeneratorConfig) -> dict[str, Any]:
    out_dir = cfg.raw_transactions_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    month_keys = np.datetime_as_string(txn.transaction_time.astype("datetime64[M]"), unit="M")
    partition_counts: dict[str, int] = {}

    for month in sorted(np.unique(month_keys)):
        idxs = np.flatnonzero(month_keys == month)
        partition_counts[str(month)] = int(len(idxs))
        file_name = f"txn_{month}.csv"
        path = out_dir / file_name
        event_strings = np.datetime_as_string(txn.transaction_time[idxs], unit="s")
        ingestion_strings = np.datetime_as_string(txn.ingestion_time[idxs], unit="s")
        ingestion_days = np.datetime_as_string(txn.ingestion_time[idxs].astype("datetime64[D]"), unit="D")
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(TXN_FIELDS)
            for local_pos, row_idx_np in enumerate(idxs):
                row_idx = int(row_idx_np)
                writer.writerow(
                    _transaction_row(
                        txn=txn,
                        dims=dims,
                        cfg=cfg,
                        row_idx=row_idx,
                        event_time=event_strings[local_pos],
                        ingestion_time=ingestion_strings[local_pos],
                        ingestion_day=ingestion_days[local_pos],
                        file_name=file_name,
                    )
                )
    return {
        "transaction_rows": txn.n,
        "transaction_partitions": partition_counts,
    }


def _transaction_row(
    txn: Transactions,
    dims: Dimensions,
    cfg: GeneratorConfig,
    row_idx: int,
    event_time: str,
    ingestion_time: str,
    ingestion_day: str,
    file_name: str,
) -> list[Any]:
    cust_idx = int(txn.customer_idx[row_idx])
    acc_idx = int(txn.account_idx[row_idx])
    mer_idx = int(txn.merchant_idx[row_idx])
    dev_idx = int(txn.device_idx[row_idx])
    loc_idx = int(txn.location_idx[row_idx])

    if row_idx in txn.location_text_override:
        loc_id, country, city = txn.location_text_override[row_idx]
    elif 0 <= loc_idx < len(dims.locations):
        loc = dims.locations[loc_idx]
        loc_id, country, city = str(loc["location_id"]), str(loc["country"]), str(loc["city"])
    else:
        loc_id, country, city = "loc_unknown", "??", "Unknown"

    amount = txn.amount_text_override.get(row_idx)
    if amount is None:
        value = float(txn.amount[row_idx])
        amount = "NaN" if np.isnan(value) else f"{value:.2f}"

    channel = "foo" if int(txn.channel_idx[row_idx]) >= len(CHANNELS) else CHANNELS[int(txn.channel_idx[row_idx])]
    currency = "XXX" if int(txn.currency_idx[row_idx]) >= len(CURRENCIES) else CURRENCIES[int(txn.currency_idx[row_idx])]
    transaction_type = TRANSACTION_TYPES[int(txn.type_idx[row_idx])]
    status = STATUSES[int(txn.status_idx[row_idx])]
    source_system = SOURCE_SYSTEMS[int(txn.source_idx[row_idx])]

    if 0 <= cust_idx < len(dims.customers):
        cust_id = customer_id(cust_idx)
    else:
        cust_id = "cus_999999"

    if 0 <= acc_idx < len(dims.accounts):
        acc_id = account_id(acc_idx)
        card_idx = int(dims.account_card_idx[acc_idx])
        card = card_id(card_idx) if card_idx >= 0 else ""
    else:
        acc_id = "acc_9999999"
        card = ""

    merchant = ""
    if mer_idx >= 0:
        merchant = merchant_id(mer_idx) if mer_idx < len(dims.merchants) else "mer_999999"

    device = device_id(dev_idx) if 0 <= dev_idx < len(dims.devices) else "dev_9999999"
    event_value = txn.timestamp_text_override.get(row_idx, event_time)

    return [
        transaction_id_for_row(txn, row_idx),
        cust_id,
        acc_id,
        merchant,
        device,
        card,
        amount,
        currency,
        transaction_type,
        channel,
        event_value,
        country,
        city,
        status,
        loc_id,
        ingestion_time,
        source_system,
        f"batch_{ingestion_day.replace('-', '')}",
        file_name,
        row_idx,
    ]


def _ordered_probs(values: dict[str, float], keys: tuple[str, ...]) -> np.ndarray:
    probs = np.array([float(values.get(key, 0.0)) for key in keys], dtype=np.float64)
    if probs.sum() <= 0:
        probs = np.ones(len(keys), dtype=np.float64)
    return probs / probs.sum()


def _sample_customers(n: int, n_customers: int, rng: np.random.Generator) -> np.ndarray:
    ranks = np.arange(1, n_customers + 1, dtype=np.float64)
    weights = 1.0 / np.power(ranks, 1.10)
    weights = weights / weights.sum()
    customer_order = rng.permutation(n_customers)
    rank_idx = rng.choice(n_customers, size=n, p=weights)
    return customer_order[rank_idx].astype(np.int32)


def _sample_channels(type_idx: np.ndarray, cfg: GeneratorConfig, rng: np.random.Generator) -> np.ndarray:
    n = len(type_idx)
    base = rng.choice(len(CHANNELS), size=n, p=_ordered_probs(cfg.channel_mix, CHANNELS)).astype(np.int16)
    withdrawal_idx = TRANSACTION_TYPES.index("withdrawal")
    topup_idx = TRANSACTION_TYPES.index("topup")
    transfer_idx = TRANSACTION_TYPES.index("transfer")
    payment_idx = TRANSACTION_TYPES.index("payment")
    debit_idx = TRANSACTION_TYPES.index("debit")

    withdrawal = type_idx == withdrawal_idx
    if int(withdrawal.sum()):
        base[withdrawal] = rng.choice([CHANNELS.index("atm"), CHANNELS.index("mobile")], size=int(withdrawal.sum()), p=[0.82, 0.18])

    topup = type_idx == topup_idx
    if int(topup.sum()):
        base[topup] = rng.choice([CHANNELS.index("mobile"), CHANNELS.index("atm"), CHANNELS.index("web")], size=int(topup.sum()), p=[0.72, 0.18, 0.10])

    transfer = type_idx == transfer_idx
    if int(transfer.sum()):
        base[transfer] = rng.choice([CHANNELS.index("mobile"), CHANNELS.index("web")], size=int(transfer.sum()), p=[0.86, 0.14])

    payment_like = np.isin(type_idx, [payment_idx, debit_idx])
    if int(payment_like.sum()):
        base[payment_like] = rng.choice(
            [CHANNELS.index("mobile"), CHANNELS.index("qr"), CHANNELS.index("pos"), CHANNELS.index("web")],
            size=int(payment_like.sum()),
            p=[0.42, 0.30, 0.20, 0.08],
        )
    return base


def _sample_amounts(type_idx: np.ndarray, currency_idx: np.ndarray, params: dict[str, Any], rng: np.random.Generator) -> np.ndarray:
    amount_params = params.get("amount_lognormal", {})
    amount = np.empty(len(type_idx), dtype=np.float64)
    for idx, name in enumerate(TRANSACTION_TYPES):
        mask = type_idx == idx
        count = int(mask.sum())
        if not count:
            continue
        p = amount_params.get(name, {"mu": 12.0, "sigma": 1.0, "cap_vnd": 100_000_000})
        values = rng.lognormal(mean=float(p["mu"]), sigma=float(p["sigma"]), size=count)
        values = np.minimum(values, float(p.get("cap_vnd", 250_000_000)))
        amount[mask] = values

    usd = currency_idx == CURRENCIES.index("USD")
    eur = currency_idx == CURRENCIES.index("EUR")
    amount[usd] = amount[usd] / 25_400.0
    amount[eur] = amount[eur] / 27_300.0
    return np.round(amount, 2)


def _sample_event_times(cfg: GeneratorConfig, n: int, rng: np.random.Generator) -> np.ndarray:
    days = np.arange(cfg.date_count)
    weekday = np.array([(cfg.date_start + timedelta(days=int(day))).weekday() for day in days])
    weekend_factor = np.where(weekday >= 5, 0.78, 1.0)
    month_wave = 1.0 + 0.10 * np.sin(2 * np.pi * days / 30.5)
    payday_bump = np.where(np.isin(days % 30, [0, 1, 14, 15]), 1.16, 1.0)
    day_probs = weekend_factor * month_wave * payday_bump
    day_probs = day_probs / day_probs.sum()
    day_offsets = rng.choice(days, size=n, p=day_probs)

    hour_probs = np.array(
        [0.010, 0.006, 0.004, 0.004, 0.005, 0.008, 0.018, 0.040, 0.058, 0.067, 0.070, 0.071,
         0.072, 0.070, 0.068, 0.066, 0.065, 0.067, 0.069, 0.065, 0.052, 0.036, 0.024, 0.015],
        dtype=np.float64,
    )
    hour_probs = hour_probs / hour_probs.sum()
    hours = rng.choice(np.arange(24), size=n, p=hour_probs)
    seconds = hours * 3600 + rng.integers(0, 3600, size=n)
    start = np.datetime64(cfg.date_start.isoformat(), "s")
    return start + day_offsets.astype("timedelta64[D]") + seconds.astype("timedelta64[s]")


def _sample_ingestion_lags(n: int, rng: np.random.Generator) -> np.ndarray:
    mode = rng.choice(3, size=n, p=[0.962, 0.030, 0.008])
    lag = np.empty(n, dtype=np.int64)
    fast = mode == 0
    medium = mode == 1
    late = mode == 2
    lag[fast] = np.maximum(5, rng.gamma(shape=2.2, scale=80.0, size=int(fast.sum())).astype(np.int64))
    lag[medium] = rng.integers(3600, 12 * 3600, size=int(medium.sum()))
    lag[late] = rng.integers(12 * 3600, 5 * 86_400, size=int(late.sum()))
    return lag.astype("timedelta64[s]")


def _sample_statuses(merchant_idx: np.ndarray, channel_idx: np.ndarray, dims: Dimensions, n: int, rng: np.random.Generator) -> np.ndarray:
    fail_prob = np.full(n, 0.010, dtype=np.float32)
    valid_merchants = merchant_idx >= 0
    if int(valid_merchants.sum()):
        fail_prob[valid_merchants] += dims.merchant_failure_rates[merchant_idx[valid_merchants]]
    fail_prob[channel_idx == CHANNELS.index("web")] += 0.006
    fail_prob[channel_idx == CHANNELS.index("atm")] += 0.004
    pending_prob = np.full(n, 0.006, dtype=np.float32)
    draw = rng.random(n)
    status_idx = np.full(n, STATUSES.index("success"), dtype=np.int16)
    status_idx[draw < fail_prob] = STATUSES.index("failed")
    pending_mask = (draw >= fail_prob) & (draw < fail_prob + pending_prob)
    status_idx[pending_mask] = STATUSES.index("pending")
    return status_idx


def _source_by_channel(channel_idx: np.ndarray, type_idx: np.ndarray, rng: np.random.Generator) -> np.ndarray:
    source_idx = np.empty(len(channel_idx), dtype=np.int16)
    source_idx[channel_idx == CHANNELS.index("mobile")] = SOURCE_SYSTEMS.index("mobile_wallet")
    source_idx[channel_idx == CHANNELS.index("qr")] = SOURCE_SYSTEMS.index("merchant_gateway")
    source_idx[channel_idx == CHANNELS.index("pos")] = SOURCE_SYSTEMS.index("card_processor")
    source_idx[channel_idx == CHANNELS.index("web")] = SOURCE_SYSTEMS.index("merchant_gateway")
    source_idx[channel_idx == CHANNELS.index("atm")] = SOURCE_SYSTEMS.index("atm_switch")
    core_mask = np.isin(type_idx, [TRANSACTION_TYPES.index("transfer"), TRANSACTION_TYPES.index("topup")]) & (rng.random(len(type_idx)) < 0.22)
    source_idx[core_mask] = SOURCE_SYSTEMS.index("core_banking")
    return source_idx
