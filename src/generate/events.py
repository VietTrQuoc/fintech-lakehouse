from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

import numpy as np

from .config import GeneratorConfig
from .dimensions import Dimensions, customer_id, device_id


def write_exchange_rates(cfg: GeneratorConfig, rng: np.random.Generator) -> dict[str, int]:
    rates = []
    usd = 25_350.0
    eur = 27_450.0
    for day_offset in range(cfg.date_count):
        current_date = cfg.date_start + timedelta(days=day_offset)
        usd *= float(1.0 + rng.normal(0.0, 0.0018))
        eur *= float(1.0 + rng.normal(0.0, 0.0022))
        rates.append(
            {
                "date": current_date.isoformat(),
                "base_currency": "VND",
                "quote": "VND per 1 unit of currency",
                "rates": {
                    "VND": 1.0,
                    "USD": round(usd, 4),
                    "EUR": round(eur, 4),
                },
                "source_system": "mock_fx_api",
            }
        )

    payload = {
        "api_name": "exchange_rates",
        "generated_at": f"{cfg.date_end.isoformat()}T00:00:00",
        "records": rates,
    }
    path = cfg.raw_dir / "exchange_rates.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    return {"exchange_rate_days": len(rates)}


def write_customer_events(cfg: GeneratorConfig, dims: Dimensions, rng: np.random.Generator) -> dict[str, int]:
    n_events = max(cfg.n_customers, int(round(cfg.n_customers * 2.5)))
    event_types = rng.choice(
        ["login", "add_device", "password_change", "add_beneficiary"],
        size=n_events,
        p=[0.72, 0.10, 0.08, 0.10],
    )
    customer_idx = _sample_event_customers(n_events, cfg.n_customers, rng)
    event_times = _sample_event_times(cfg, n_events, rng)
    ingestion_times = event_times + _sample_event_lags(n_events, rng)

    events: list[dict[str, Any]] = []
    foreign_start = max(0, len(dims.locations) - 5)
    for idx in range(n_events):
        cust_idx = int(customer_idx[idx])
        event_type = str(event_types[idx])
        if event_type == "add_device":
            dev_idx = int(rng.integers(0, len(dims.devices)))
        else:
            secondary = int(dims.secondary_device_idx[cust_idx])
            dev_idx = secondary if secondary >= 0 and rng.random() < 0.12 else int(dims.primary_device_idx[cust_idx])

        if event_type in ("login", "add_device") and rng.random() < 0.035:
            loc = dims.locations[int(rng.integers(foreign_start, len(dims.locations)))]
        else:
            loc = dims.locations[int(dims.customer_home_location_idx[cust_idx])]

        events.append(
            {
                "event_id": f"evt_{idx + 1:010d}",
                "customer_id": customer_id(cust_idx),
                "event_type": event_type,
                "event_time": str(event_times[idx].astype("datetime64[s]")),
                "ingestion_time": str(ingestion_times[idx].astype("datetime64[s]")),
                "device_id": device_id(dev_idx),
                "country": loc["country"],
                "city": loc["city"],
                "source_system": "customer_activity_api",
                "metadata": _event_metadata(event_type, rng),
            }
        )

    payload = {
        "api_name": "customer_events",
        "generated_at": f"{cfg.date_end.isoformat()}T00:00:00",
        "records": events,
    }
    path = cfg.raw_dir / "customer_events.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    return {"customer_event_rows": len(events)}


def _sample_event_customers(n_events: int, n_customers: int, rng: np.random.Generator) -> np.ndarray:
    ranks = np.arange(1, n_customers + 1, dtype=np.float64)
    weights = 1.0 / np.power(ranks, 1.05)
    weights = weights / weights.sum()
    order = rng.permutation(n_customers)
    return order[rng.choice(n_customers, size=n_events, p=weights)]


def _sample_event_times(cfg: GeneratorConfig, n_events: int, rng: np.random.Generator) -> np.ndarray:
    days = rng.integers(0, cfg.date_count, size=n_events)
    hour_probs = np.array(
        [0.012, 0.006, 0.004, 0.004, 0.006, 0.010, 0.025, 0.048, 0.062, 0.066, 0.066, 0.066,
         0.066, 0.066, 0.066, 0.066, 0.067, 0.071, 0.075, 0.068, 0.055, 0.041, 0.026, 0.014],
        dtype=np.float64,
    )
    hour_probs = hour_probs / hour_probs.sum()
    hours = rng.choice(np.arange(24), size=n_events, p=hour_probs)
    seconds = hours * 3600 + rng.integers(0, 3600, size=n_events)
    start = np.datetime64(cfg.date_start.isoformat(), "s")
    return start + days.astype("timedelta64[D]") + seconds.astype("timedelta64[s]")


def _sample_event_lags(n_events: int, rng: np.random.Generator) -> np.ndarray:
    mode = rng.choice(3, size=n_events, p=[0.92, 0.06, 0.02])
    lag = np.empty(n_events, dtype=np.int64)
    fast = mode == 0
    medium = mode == 1
    late = mode == 2
    lag[fast] = rng.integers(3, 420, size=int(fast.sum()))
    lag[medium] = rng.integers(900, 8 * 3600, size=int(medium.sum()))
    lag[late] = rng.integers(12 * 3600, 4 * 86_400, size=int(late.sum()))
    return lag.astype("timedelta64[s]")


def _event_metadata(event_type: str, rng: np.random.Generator) -> dict[str, Any]:
    if event_type == "login":
        return {"auth_result": str(rng.choice(["success", "failed"], p=[0.965, 0.035]))}
    if event_type == "add_device":
        return {"trust_decision": str(rng.choice(["approved", "review"], p=[0.90, 0.10]))}
    if event_type == "password_change":
        return {"initiated_by": str(rng.choice(["customer", "risk_engine"], p=[0.94, 0.06]))}
    return {"beneficiary_bank": str(rng.choice(["VCB", "TCB", "ACB", "BIDV", "MB", "VPB"]))}
