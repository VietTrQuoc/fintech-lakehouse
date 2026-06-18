from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np

from .config import GeneratorConfig, get_config
from .dimensions import generate_dimensions, write_dimensions
from .errors import inject_errors, write_error_manifest
from .events import write_customer_events, write_exchange_rates
from .fraud import inject_fraud, write_fraud_labels
from .transactions import generate_transactions, write_transactions


def main() -> None:
    args = _parse_args()
    cfg = _config_from_args(args)
    summary = generate_all(cfg)
    print(json.dumps(summary, indent=2, default=str))


def generate_all(cfg: GeneratorConfig) -> dict[str, Any]:
    cfg.raw_dir.mkdir(parents=True, exist_ok=True)
    cfg.raw_transactions_dir.mkdir(parents=True, exist_ok=True)
    cfg.bad_data_dir.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(cfg.seed)
    summary: dict[str, Any] = {
        "seed": cfg.seed,
        "date_start": cfg.date_start.isoformat(),
        "date_end": cfg.date_end.isoformat(),
        "date_count": cfg.date_count,
        "raw_dir": str(cfg.raw_dir),
        "bad_data_dir": str(cfg.bad_data_dir),
    }

    dims = generate_dimensions(cfg, rng)
    write_dimensions(dims, cfg)
    summary.update(
        {
            "customers": len(dims.customers),
            "customer_scd_events": len(dims.scd_events),
            "accounts": len(dims.accounts),
            "merchants": len(dims.merchants),
            "devices": len(dims.devices),
            "cards": len(dims.cards),
            "locations": len(dims.locations),
        }
    )

    txn = generate_transactions(cfg, dims, rng)
    fraud_rows = inject_fraud(txn, dims, cfg, rng)
    error_manifest = inject_errors(txn, dims, cfg, rng, protected_rows=fraud_rows)
    summary.update(write_transactions(txn, dims, cfg))
    summary.update(write_fraud_labels(txn, cfg))
    summary.update(write_error_manifest(error_manifest, cfg))
    summary.update(write_exchange_rates(cfg, rng))
    summary.update(write_customer_events(cfg, dims, rng))
    summary.update(_lag_stats(txn.transaction_time, txn.ingestion_time))
    summary["fraud_rate"] = round(summary["fraud_positive_rows"] / max(1, cfg.n_transactions), 6)
    summary["bad_row_rate"] = round(summary["error_manifest_rows"] / max(1, cfg.n_transactions), 6)

    manifest_path = cfg.raw_dir / "generation_manifest.json"
    with manifest_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2, default=str)
    summary["generation_manifest"] = str(manifest_path)
    return summary


def _lag_stats(transaction_time: np.ndarray, ingestion_time: np.ndarray) -> dict[str, Any]:
    lag_seconds = ((ingestion_time - transaction_time) / np.timedelta64(1, "s")).astype(np.float64)
    return {
        "ingestion_lag_seconds": {
            "min": int(np.min(lag_seconds)),
            "p50": int(np.percentile(lag_seconds, 50)),
            "p95": int(np.percentile(lag_seconds, 95)),
            "p99": int(np.percentile(lag_seconds, 99)),
            "max": int(np.max(lag_seconds)),
            "late_gt_1h_pct": round(float(np.mean(lag_seconds > 3600) * 100), 4),
            "late_gt_24h_pct": round(float(np.mean(lag_seconds > 86400) * 100), 4),
        }
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic raw finance data.")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--n-customers", type=int, default=None)
    parser.add_argument("--n-txn", type=int, default=None)
    parser.add_argument("--n-merchants", type=int, default=None)
    parser.add_argument("--date-start", type=str, default=None)
    parser.add_argument("--date-end", type=str, default=None)
    parser.add_argument("--raw-dir", type=Path, default=None)
    parser.add_argument("--bad-data-dir", type=Path, default=None)
    parser.add_argument("--paysim-params", type=Path, default=None)
    parser.add_argument("--fraud-rate", type=float, default=None)
    parser.add_argument("--label-positives-only", action="store_true")
    return parser.parse_args()


def _config_from_args(args: argparse.Namespace) -> GeneratorConfig:
    overrides: dict[str, Any] = {
        "seed": args.seed,
        "n_customers": args.n_customers,
        "n_transactions": args.n_txn,
        "n_merchants": args.n_merchants,
        "raw_dir": args.raw_dir,
        "bad_data_dir": args.bad_data_dir,
        "paysim_params_path": args.paysim_params,
        "fraud_rate": args.fraud_rate,
    }
    if args.date_start:
        overrides["date_start"] = date.fromisoformat(args.date_start)
    if args.date_end:
        overrides["date_end"] = date.fromisoformat(args.date_end)
    if args.label_positives_only:
        overrides["label_all_transactions"] = False
    return get_config(**overrides)


if __name__ == "__main__":
    main()
