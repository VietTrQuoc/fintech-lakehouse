from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
BAD_DATA_DIR = DATA_DIR / "bad_data_samples"
CALIBRATION_DIR = Path(__file__).resolve().parent / "calibration"
DEFAULT_PAYSIM_PARAMS_PATH = CALIBRATION_DIR / "paysim_params.json"


TRANSACTION_TYPES = ("payment", "transfer", "withdrawal", "topup", "debit")
CHANNELS = ("mobile", "qr", "pos", "web", "atm")
CURRENCIES = ("VND", "USD", "EUR")
STATUSES = ("success", "failed", "pending")
SOURCE_SYSTEMS = (
    "mobile_wallet",
    "merchant_gateway",
    "card_processor",
    "atm_switch",
    "core_banking",
)


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value.replace("_", "")) if value else default


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    return float(value) if value else default


def _env_date(name: str, default: str) -> date:
    value = os.getenv(name, default)
    return date.fromisoformat(value)


def _env_path(name: str, default: Path) -> Path:
    value = os.getenv(name)
    return Path(value) if value else default


def load_paysim_params(path: Path = DEFAULT_PAYSIM_PARAMS_PATH) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


@dataclass(frozen=True)
class GeneratorConfig:
    seed: int = field(default_factory=lambda: _env_int("GEN_SEED", 20260610))
    n_customers: int = field(default_factory=lambda: _env_int("GEN_N_CUSTOMERS", 20_000))
    n_transactions: int = field(default_factory=lambda: _env_int("GEN_N_TXN", 2_000_000))
    n_merchants: int = field(default_factory=lambda: _env_int("GEN_N_MERCHANTS", 2_000))
    n_days_hint: int = 180
    date_start: date = field(default_factory=lambda: _env_date("GEN_DATE_START", "2025-12-13"))
    date_end: date = field(default_factory=lambda: _env_date("GEN_DATE_END", "2026-06-10"))
    raw_dir: Path = field(default_factory=lambda: _env_path("GEN_RAW_DIR", RAW_DIR))
    bad_data_dir: Path = field(default_factory=lambda: _env_path("GEN_BAD_DATA_DIR", BAD_DATA_DIR))
    paysim_params_path: Path = field(
        default_factory=lambda: _env_path("GEN_PAYSIM_PARAMS", DEFAULT_PAYSIM_PARAMS_PATH)
    )
    fraud_rate: float = field(default_factory=lambda: _env_float("GEN_FRAUD_RATE", 0.007))
    label_all_transactions: bool = True

    channel_mix: dict[str, float] = field(
        default_factory=lambda: {
            "mobile": 0.50,
            "qr": 0.20,
            "pos": 0.15,
            "web": 0.10,
            "atm": 0.05,
        }
    )
    currency_mix: dict[str, float] = field(
        default_factory=lambda: {
            "VND": 0.960,
            "USD": 0.025,
            "EUR": 0.015,
        }
    )
    error_rates: dict[str, float] = field(
        default_factory=lambda: {
            "null_transaction_id": 0.0010,
            "duplicate_transaction_id": 0.0020,
            "invalid_amount": 0.0035,
            "invalid_timestamp": 0.0025,
            "orphan_customer": 0.0015,
            "orphan_account": 0.0015,
            "orphan_merchant": 0.0010,
            "invalid_currency": 0.0010,
            "invalid_channel": 0.0010,
            "invalid_location": 0.0010,
        }
    )
    fraud_pattern_mix: dict[str, float] = field(
        default_factory=lambda: {
            "velocity_burst": 0.25,
            "amount_spike": 0.20,
            "new_device_location": 0.20,
            "cross_country": 0.15,
            "night_high_amount": 0.12,
            "card_testing": 0.08,
        }
    )

    @property
    def date_count(self) -> int:
        return (self.date_end - self.date_start).days + 1

    @property
    def raw_transactions_dir(self) -> Path:
        return self.raw_dir / "transactions"


def get_config(**overrides: Any) -> GeneratorConfig:
    base = GeneratorConfig()
    if not overrides:
        return base
    data = {**base.__dict__, **{key: value for key, value in overrides.items() if value is not None}}
    return GeneratorConfig(**data)
