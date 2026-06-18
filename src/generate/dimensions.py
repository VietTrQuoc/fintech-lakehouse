from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np

from .config import GeneratorConfig


try:
    from faker import Faker
except ImportError:  # pragma: no cover - exercised only in minimal local smoke environments.
    Faker = None


VN_LOCATIONS: tuple[dict[str, Any], ...] = (
    {"location_id": "loc_vn_hcm", "country": "VN", "city": "Ho Chi Minh City", "region": "South", "lat": 10.7769, "lon": 106.7009, "timezone": "Asia/Ho_Chi_Minh"},
    {"location_id": "loc_vn_hn", "country": "VN", "city": "Hanoi", "region": "North", "lat": 21.0278, "lon": 105.8342, "timezone": "Asia/Ho_Chi_Minh"},
    {"location_id": "loc_vn_dn", "country": "VN", "city": "Da Nang", "region": "Central", "lat": 16.0544, "lon": 108.2022, "timezone": "Asia/Ho_Chi_Minh"},
    {"location_id": "loc_vn_hp", "country": "VN", "city": "Hai Phong", "region": "North", "lat": 20.8449, "lon": 106.6881, "timezone": "Asia/Ho_Chi_Minh"},
    {"location_id": "loc_vn_ct", "country": "VN", "city": "Can Tho", "region": "Mekong Delta", "lat": 10.0452, "lon": 105.7469, "timezone": "Asia/Ho_Chi_Minh"},
    {"location_id": "loc_vn_bd", "country": "VN", "city": "Binh Duong", "region": "South", "lat": 11.3254, "lon": 106.4770, "timezone": "Asia/Ho_Chi_Minh"},
    {"location_id": "loc_vn_dnai", "country": "VN", "city": "Dong Nai", "region": "South", "lat": 11.0686, "lon": 107.1676, "timezone": "Asia/Ho_Chi_Minh"},
    {"location_id": "loc_vn_qn", "country": "VN", "city": "Quang Ninh", "region": "North", "lat": 21.0064, "lon": 107.2925, "timezone": "Asia/Ho_Chi_Minh"},
    {"location_id": "loc_vn_kh", "country": "VN", "city": "Khanh Hoa", "region": "Central", "lat": 12.2585, "lon": 109.0526, "timezone": "Asia/Ho_Chi_Minh"},
    {"location_id": "loc_vn_lam_dong", "country": "VN", "city": "Lam Dong", "region": "Central Highlands", "lat": 11.9404, "lon": 108.4583, "timezone": "Asia/Ho_Chi_Minh"},
    {"location_id": "loc_vn_nghe_an", "country": "VN", "city": "Nghe An", "region": "North Central", "lat": 19.2342, "lon": 104.9200, "timezone": "Asia/Ho_Chi_Minh"},
    {"location_id": "loc_vn_thua_thien_hue", "country": "VN", "city": "Hue", "region": "Central", "lat": 16.4637, "lon": 107.5909, "timezone": "Asia/Ho_Chi_Minh"},
)

FOREIGN_LOCATIONS: tuple[dict[str, Any], ...] = (
    {"location_id": "loc_sg_singapore", "country": "SG", "city": "Singapore", "region": "Singapore", "lat": 1.3521, "lon": 103.8198, "timezone": "Asia/Singapore"},
    {"location_id": "loc_th_bangkok", "country": "TH", "city": "Bangkok", "region": "Bangkok", "lat": 13.7563, "lon": 100.5018, "timezone": "Asia/Bangkok"},
    {"location_id": "loc_us_san_francisco", "country": "US", "city": "San Francisco", "region": "California", "lat": 37.7749, "lon": -122.4194, "timezone": "America/Los_Angeles"},
    {"location_id": "loc_jp_tokyo", "country": "JP", "city": "Tokyo", "region": "Kanto", "lat": 35.6762, "lon": 139.6503, "timezone": "Asia/Tokyo"},
    {"location_id": "loc_au_sydney", "country": "AU", "city": "Sydney", "region": "New South Wales", "lat": -33.8688, "lon": 151.2093, "timezone": "Australia/Sydney"},
)

LOCATION_RECORDS = VN_LOCATIONS + FOREIGN_LOCATIONS

RISK_TIERS = ("low", "medium", "high")
KYC_LEVELS = ("basic", "standard", "enhanced")
ACCOUNT_TYPES = ("wallet", "checking", "savings")
MERCHANT_CATEGORIES = ("grocery", "food", "transport", "ecommerce", "utilities", "travel", "gaming", "fashion", "electronics", "education")
DEVICE_TYPES = ("ios_phone", "android_phone", "tablet", "desktop", "atm_terminal")
DEVICE_OS = ("iOS", "Android", "Windows", "macOS", "Linux", "ATM_OS")
CARD_NETWORKS = ("visa", "mastercard", "napas")
CARD_TYPES = ("debit", "credit", "prepaid")

FIRST_NAMES = ("An", "Binh", "Chi", "Dung", "Giang", "Ha", "Hieu", "Khanh", "Lan", "Linh", "Minh", "Nam", "Phong", "Quang", "Trang", "Vy")
LAST_NAMES = ("Nguyen", "Tran", "Le", "Pham", "Hoang", "Huynh", "Phan", "Vu", "Vo", "Dang", "Bui", "Do", "Ho", "Ngo", "Duong")


@dataclass
class Dimensions:
    customers: list[dict[str, Any]]
    accounts: list[dict[str, Any]]
    merchants: list[dict[str, Any]]
    devices: list[dict[str, Any]]
    cards: list[dict[str, Any]]
    locations: list[dict[str, Any]]
    scd_events: list[dict[str, Any]]
    customer_home_location_idx: np.ndarray
    primary_account_idx: np.ndarray
    secondary_account_idx: np.ndarray
    account_customer_idx: np.ndarray
    account_card_idx: np.ndarray
    primary_device_idx: np.ndarray
    secondary_device_idx: np.ndarray
    device_customer_idx: np.ndarray
    merchant_failure_rates: np.ndarray
    merchant_popularity: np.ndarray


def customer_id(idx: int) -> str:
    return f"cus_{idx + 1:06d}"


def account_id(idx: int) -> str:
    return f"acc_{idx + 1:07d}"


def merchant_id(idx: int) -> str:
    return f"mer_{idx + 1:06d}"


def device_id(idx: int) -> str:
    return f"dev_{idx + 1:07d}"


def card_id(idx: int) -> str:
    return f"card_{idx + 1:07d}"


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _make_fake(seed: int):
    if Faker is None:
        return None
    fake = Faker("vi_VN")
    Faker.seed(seed)
    return fake


def _fallback_name(rng: np.random.Generator) -> str:
    return f"{rng.choice(LAST_NAMES)} {rng.choice(FIRST_NAMES)}"


def _fallback_email(name: str, idx: int) -> str:
    compact = "".join(ch for ch in name.lower() if ch.isalnum())
    return f"{compact}.{idx + 1}@example.vn"


def _fallback_phone(rng: np.random.Generator) -> str:
    prefix = rng.choice(("032", "033", "034", "035", "036", "037", "038", "039", "090", "091", "092", "093"))
    return f"{prefix}{int(rng.integers(1_000_000, 9_999_999)):07d}"


def _random_date(rng: np.random.Generator, start: date, end: date) -> date:
    span = max((end - start).days, 1)
    return start + timedelta(days=int(rng.integers(0, span + 1)))


def _location_weights() -> np.ndarray:
    vn_weights = np.array([0.24, 0.19, 0.11, 0.06, 0.06, 0.08, 0.07, 0.04, 0.04, 0.03, 0.04, 0.04])
    vn_weights = vn_weights / vn_weights.sum() * 0.985
    foreign_weights = np.ones(len(FOREIGN_LOCATIONS)) / len(FOREIGN_LOCATIONS) * 0.015
    return np.concatenate([vn_weights, foreign_weights])


def generate_dimensions(cfg: GeneratorConfig, rng: np.random.Generator) -> Dimensions:
    fake = _make_fake(cfg.seed)
    locations = [dict(record) for record in LOCATION_RECORDS]
    location_probs = _location_weights()

    home_location_idx = rng.choice(len(locations), size=cfg.n_customers, p=location_probs)
    signup_start = date(cfg.date_start.year - 3, 1, 1)
    customers: list[dict[str, Any]] = []
    customer_risk = rng.choice(RISK_TIERS, size=cfg.n_customers, p=[0.78, 0.18, 0.04])
    customer_kyc = rng.choice(KYC_LEVELS, size=cfg.n_customers, p=[0.22, 0.62, 0.16])

    for idx in range(cfg.n_customers):
        name = fake.name() if fake else _fallback_name(rng)
        email = fake.email() if fake else _fallback_email(name, idx)
        phone = fake.phone_number() if fake else _fallback_phone(rng)
        dob_year = int(rng.integers(cfg.date_end.year - 70, cfg.date_end.year - 18))
        dob = date(dob_year, int(rng.integers(1, 13)), int(rng.integers(1, 29)))
        signup_date = _random_date(rng, signup_start, min(cfg.date_start, cfg.date_end))
        loc = locations[int(home_location_idx[idx])]
        customers.append(
            {
                "customer_id": customer_id(idx),
                "full_name": name,
                "email": email,
                "phone": phone,
                "dob": dob.isoformat(),
                "signup_date": signup_date.isoformat(),
                "home_city": loc["city"],
                "home_country": loc["country"],
                "risk_tier": str(customer_risk[idx]),
                "kyc_level": str(customer_kyc[idx]),
            }
        )

    scd_events = _generate_scd_events(customers, home_location_idx, locations, cfg, rng, fake)

    n_accounts = max(cfg.n_customers, int(round(cfg.n_customers * 1.2)))
    account_customer_idx = np.empty(n_accounts, dtype=np.int32)
    account_customer_idx[: cfg.n_customers] = np.arange(cfg.n_customers, dtype=np.int32)
    if n_accounts > cfg.n_customers:
        account_customer_idx[cfg.n_customers :] = rng.integers(0, cfg.n_customers, size=n_accounts - cfg.n_customers)

    primary_account_idx = np.arange(cfg.n_customers, dtype=np.int32)
    secondary_account_idx = np.full(cfg.n_customers, -1, dtype=np.int32)
    for acc_idx in range(cfg.n_customers, n_accounts):
        cust_idx = int(account_customer_idx[acc_idx])
        if secondary_account_idx[cust_idx] == -1:
            secondary_account_idx[cust_idx] = acc_idx

    account_types = rng.choice(ACCOUNT_TYPES, size=n_accounts, p=[0.45, 0.40, 0.15])
    account_statuses = rng.choice(("active", "frozen", "closed"), size=n_accounts, p=[0.965, 0.025, 0.010])
    balances = np.minimum(rng.lognormal(mean=14.2, sigma=1.35, size=n_accounts), 2_000_000_000)
    accounts: list[dict[str, Any]] = []
    for idx in range(n_accounts):
        opened_date = _random_date(rng, signup_start, min(cfg.date_start, cfg.date_end))
        accounts.append(
            {
                "account_id": account_id(idx),
                "customer_id": customer_id(int(account_customer_idx[idx])),
                "account_type": str(account_types[idx]),
                "opened_date": opened_date.isoformat(),
                "status": str(account_statuses[idx]),
                "currency": "VND",
                "current_balance": f"{balances[idx]:.2f}",
            }
        )

    merchants, merchant_failure_rates, merchant_popularity = _generate_merchants(cfg, rng, fake)

    n_devices = max(cfg.n_customers, int(round(cfg.n_customers * 1.5)))
    device_customer_idx = np.empty(n_devices, dtype=np.int32)
    device_customer_idx[: cfg.n_customers] = np.arange(cfg.n_customers, dtype=np.int32)
    if n_devices > cfg.n_customers:
        device_customer_idx[cfg.n_customers :] = rng.integers(0, cfg.n_customers, size=n_devices - cfg.n_customers)

    primary_device_idx = np.arange(cfg.n_customers, dtype=np.int32)
    secondary_device_idx = np.full(cfg.n_customers, -1, dtype=np.int32)
    for dev_idx in range(cfg.n_customers, n_devices):
        cust_idx = int(device_customer_idx[dev_idx])
        if secondary_device_idx[cust_idx] == -1:
            secondary_device_idx[cust_idx] = dev_idx

    devices = _generate_devices(device_customer_idx, cfg, rng)
    cards, account_card_idx = _generate_cards(account_customer_idx, cfg, rng)

    return Dimensions(
        customers=customers,
        accounts=accounts,
        merchants=merchants,
        devices=devices,
        cards=cards,
        locations=locations,
        scd_events=scd_events,
        customer_home_location_idx=home_location_idx.astype(np.int16),
        primary_account_idx=primary_account_idx,
        secondary_account_idx=secondary_account_idx,
        account_customer_idx=account_customer_idx,
        account_card_idx=account_card_idx,
        primary_device_idx=primary_device_idx,
        secondary_device_idx=secondary_device_idx,
        device_customer_idx=device_customer_idx,
        merchant_failure_rates=merchant_failure_rates,
        merchant_popularity=merchant_popularity,
    )


def _generate_scd_events(
    customers: list[dict[str, Any]],
    home_location_idx: np.ndarray,
    locations: list[dict[str, Any]],
    cfg: GeneratorConfig,
    rng: np.random.Generator,
    fake: Any,
) -> list[dict[str, Any]]:
    event_customer_count = int(round(cfg.n_customers * 0.10))
    if event_customer_count == 0:
        return []
    event_customers = rng.choice(cfg.n_customers, size=event_customer_count, replace=False)
    scd_events: list[dict[str, Any]] = []
    event_idx = 1
    for cust_idx in event_customers:
        event_count = int(rng.choice([1, 2], p=[0.82, 0.18]))
        # Draw all change times up front and apply changes in chronological order. This guarantees
        # each event's old_value chains from the previous event's new_value, so a downstream SCD2
        # build never sees a broken version history (later-in-time row contradicting an earlier one).
        change_times = sorted(_random_datetime_iso(rng, cfg.date_start, cfg.date_end) for _ in range(event_count))
        for change_time in change_times:
            field = str(rng.choice(("risk_tier", "home_city", "phone"), p=[0.45, 0.25, 0.30]))
            customer = customers[int(cust_idx)]
            old_value = str(customer[field])
            if field == "risk_tier":
                choices = [tier for tier in RISK_TIERS if tier != old_value]
                new_value = str(rng.choice(choices))
            elif field == "home_city":
                new_loc_idx = int(rng.choice(len(VN_LOCATIONS)))
                home_location_idx[int(cust_idx)] = new_loc_idx
                new_value = str(locations[new_loc_idx]["city"])
                customer["home_country"] = str(locations[new_loc_idx]["country"])
            else:
                new_value = fake.phone_number() if fake else _fallback_phone(rng)

            customer[field] = new_value
            scd_events.append(
                {
                    "event_id": f"scd_{event_idx:07d}",
                    "customer_id": customer_id(int(cust_idx)),
                    "change_time": change_time,
                    "attribute_name": field,
                    "old_value": old_value,
                    "new_value": new_value,
                    "source_system": "customer_profile_service",
                }
            )
            event_idx += 1
    scd_events.sort(key=lambda row: (row["customer_id"], row["change_time"], row["event_id"]))
    return scd_events


def _random_datetime_iso(rng: np.random.Generator, start: date, end: date) -> str:
    days = (end - start).days + 1
    day_offset = int(rng.integers(0, max(days, 1)))
    second_offset = int(rng.integers(0, 86_400))
    value = np.datetime64(start.isoformat(), "s") + np.timedelta64(day_offset, "D") + np.timedelta64(second_offset, "s")
    return str(value)


def _generate_merchants(cfg: GeneratorConfig, rng: np.random.Generator, fake: Any) -> tuple[list[dict[str, Any]], np.ndarray, np.ndarray]:
    categories = rng.choice(MERCHANT_CATEGORIES, size=cfg.n_merchants, p=[0.18, 0.15, 0.12, 0.16, 0.09, 0.07, 0.06, 0.06, 0.06, 0.05])
    base_failure_by_category = {
        "grocery": 0.010,
        "food": 0.014,
        "transport": 0.012,
        "ecommerce": 0.026,
        "utilities": 0.008,
        "travel": 0.032,
        "gaming": 0.042,
        "fashion": 0.022,
        "electronics": 0.028,
        "education": 0.010,
    }
    failure_rates = np.array(
        [base_failure_by_category[str(category)] + float(rng.uniform(0.0, 0.015)) for category in categories],
        dtype=np.float32,
    )
    ranks = np.arange(1, cfg.n_merchants + 1, dtype=np.float64)
    popularity = 1.0 / np.power(ranks, 1.08)
    popularity = popularity / popularity.sum()
    merchant_order = rng.permutation(cfg.n_merchants)
    popularity = popularity[merchant_order]

    merchants: list[dict[str, Any]] = []
    for idx in range(cfg.n_merchants):
        name = fake.company() if fake else f"{rng.choice(('Lotus', 'Mekong', 'Saigon', 'Red River', 'Nova'))} {rng.choice(('Mart', 'Pay', 'Store', 'Cafe', 'Travel'))}"
        loc_idx = int(rng.choice(len(VN_LOCATIONS), p=_location_weights()[: len(VN_LOCATIONS)] / _location_weights()[: len(VN_LOCATIONS)].sum()))
        loc = VN_LOCATIONS[loc_idx]
        merchants.append(
            {
                "merchant_id": merchant_id(idx),
                "merchant_name": name,
                "category": str(categories[idx]),
                "city": loc["city"],
                "country": loc["country"],
                "onboarded_date": _random_date(rng, date(cfg.date_start.year - 2, 1, 1), min(cfg.date_start, cfg.date_end)).isoformat(),
                "risk_score": f"{min(0.99, failure_rates[idx] * 8 + rng.uniform(0.01, 0.10)):.4f}",
                "base_failure_rate": f"{failure_rates[idx]:.5f}",
            }
        )
    return merchants, failure_rates, popularity.astype(np.float64)


def _generate_devices(device_customer_idx: np.ndarray, cfg: GeneratorConfig, rng: np.random.Generator) -> list[dict[str, Any]]:
    n_devices = len(device_customer_idx)
    device_types = rng.choice(DEVICE_TYPES[:-1], size=n_devices, p=[0.43, 0.47, 0.03, 0.07])
    os_by_device = {
        "ios_phone": "iOS",
        "android_phone": "Android",
        "tablet": str(rng.choice(("iOS", "Android"))),
        "desktop": str(rng.choice(("Windows", "macOS", "Linux"))),
    }
    devices: list[dict[str, Any]] = []
    for idx in range(n_devices):
        dev_type = str(device_types[idx])
        os_name = os_by_device.get(dev_type, "Android")
        first_seen = _random_date(rng, date(cfg.date_start.year - 2, 1, 1), min(cfg.date_start, cfg.date_end))
        devices.append(
            {
                "device_id": device_id(idx),
                "customer_id": customer_id(int(device_customer_idx[idx])),
                "device_type": dev_type,
                "os": os_name,
                "app_version": f"{int(rng.integers(4, 7))}.{int(rng.integers(0, 9))}.{int(rng.integers(0, 20))}",
                "first_seen": first_seen.isoformat(),
                "is_trusted": str(bool(rng.random() < 0.88)).lower(),
            }
        )
    return devices


def _generate_cards(account_customer_idx: np.ndarray, cfg: GeneratorConfig, rng: np.random.Generator) -> tuple[list[dict[str, Any]], np.ndarray]:
    n_cards = len(account_customer_idx)
    networks = rng.choice(CARD_NETWORKS, size=n_cards, p=[0.38, 0.30, 0.32])
    card_types = rng.choice(CARD_TYPES, size=n_cards, p=[0.68, 0.24, 0.08])
    statuses = rng.choice(("active", "blocked", "expired"), size=n_cards, p=[0.955, 0.025, 0.020])
    cards: list[dict[str, Any]] = []
    for idx in range(n_cards):
        issue_date = _random_date(rng, date(cfg.date_start.year - 3, 1, 1), min(cfg.date_start, cfg.date_end))
        expiry_year = issue_date.year + int(rng.integers(3, 6))
        expiry_month = int(rng.integers(1, 13))
        cards.append(
            {
                "card_id": card_id(idx),
                "account_id": account_id(idx),
                "customer_id": customer_id(int(account_customer_idx[idx])),
                "network": str(networks[idx]),
                "card_type": str(card_types[idx]),
                "last4": f"{int(rng.integers(0, 10_000)):04d}",
                "issue_date": issue_date.isoformat(),
                "expiry_year_month": f"{expiry_year}-{expiry_month:02d}",
                "status": str(statuses[idx]),
            }
        )
    return cards, np.arange(n_cards, dtype=np.int32)


def write_dimensions(dims: Dimensions, cfg: GeneratorConfig) -> None:
    raw_dir = cfg.raw_dir
    _write_csv(
        raw_dir / "customers.csv",
        ["customer_id", "full_name", "email", "phone", "dob", "signup_date", "home_city", "home_country", "risk_tier", "kyc_level"],
        dims.customers,
    )
    _write_csv(
        raw_dir / "customer_scd_events.csv",
        ["event_id", "customer_id", "change_time", "attribute_name", "old_value", "new_value", "source_system"],
        dims.scd_events,
    )
    _write_csv(
        raw_dir / "accounts.csv",
        ["account_id", "customer_id", "account_type", "opened_date", "status", "currency", "current_balance"],
        dims.accounts,
    )
    _write_csv(
        raw_dir / "merchants.csv",
        ["merchant_id", "merchant_name", "category", "city", "country", "onboarded_date", "risk_score", "base_failure_rate"],
        dims.merchants,
    )
    _write_csv(
        raw_dir / "devices.csv",
        ["device_id", "customer_id", "device_type", "os", "app_version", "first_seen", "is_trusted"],
        dims.devices,
    )
    _write_csv(
        raw_dir / "cards.csv",
        ["card_id", "account_id", "customer_id", "network", "card_type", "last4", "issue_date", "expiry_year_month", "status"],
        dims.cards,
    )
    _write_csv(
        raw_dir / "locations.csv",
        ["location_id", "country", "city", "region", "lat", "lon", "timezone"],
        dims.locations,
    )
