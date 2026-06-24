"""Data Quality checks: aggregate Silver DQ flags → table-level checks + report."""

import argparse
import json
import logging
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from src.generate.config import GeneratorConfig
from src.paths import BAD_DATA_DIR, BRONZE_DIR, DOCS_DIR, LOG_DIR, QUALITY_DIR, SILVER_DIR

LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "quality_checks.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("quality")

SEVERITY_ERROR = "ERROR"
SEVERITY_WARNING = "WARNING"

# Row-flag → (check name, max rate). Thresholds ~2-3x expected from config.error_rates.
RULE_THRESHOLDS: dict[str, tuple[str, float]] = {
    "_dq_null_transaction_id": ("null_transaction_id", 0.003),
    "_dq_invalid_amount": ("invalid_amount", 0.010),
    "_dq_invalid_timestamp": ("invalid_timestamp", 0.0075),
    "_dq_invalid_currency": ("invalid_currency", 0.003),
    "_dq_invalid_channel": ("invalid_channel", 0.003),
    "_dq_invalid_type": ("invalid_type", 0.001),
    "_dq_invalid_status": ("invalid_status", 0.001),
    "_dq_invalid_location": ("invalid_location", 0.003),
    "_dq_duplicate": ("duplicate", 0.005),
    "_dq_fk_customer": ("fk_customer_orphan", 0.010),  # SOFT
    "_dq_fk_account": ("fk_account_orphan", 0.010),    # SOFT
    "_dq_fk_merchant": ("fk_merchant_orphan", 0.010),  # SOFT
}

REQUIRED_COLUMNS = [
    "transaction_id", "amount_original", "amount_vnd", "exchange_rate", "event_time", "event_date",
    "source_row_number", "_dq_errors", "_dq_bucket", "_is_valid", "_is_duplicate_survivor", "_dq_run_id",
    *RULE_THRESHOLDS.keys(),
]

MANIFEST_FLAG_MAP = {
    "null_transaction_id": "_dq_null_transaction_id",
    "invalid_amount": "_dq_invalid_amount",
    "invalid_timestamp": "_dq_invalid_timestamp",
    "invalid_currency": "_dq_invalid_currency",
    "invalid_channel": "_dq_invalid_channel",
    "invalid_location": "_dq_invalid_location",
    "orphan_customer": "_dq_fk_customer",
    "orphan_account": "_dq_fk_account",
    "orphan_merchant": "_dq_fk_merchant",
    # "duplicate_transaction_id" xử riêng (membership dup-group, không chỉ non-survivor)
}


@dataclass
class Check:
    name: str
    group: str
    severity: str
    description: str
    observed: float
    threshold: float
    comparator: str
    passed: bool
    detail: dict = field(default_factory=dict)


_EVAL = {"<=": lambda o, t: o <= t, ">=": lambda o, t: o >= t, "==": lambda o, t: o == t}


def _make_check(name: str, group: str, severity: str, desc: str, observed: float, threshold: float, comparator: str, detail: dict | None = None) -> Check:
    return Check(name, group, severity, desc, round(observed, 6), threshold, comparator, _EVAL[comparator](observed, threshold), detail or {})


def make_run_id() -> str:
    return "quality_" + datetime.now().strftime("%Y%m%d_%H%M%S")


def load_silver_transactions() -> pd.DataFrame:
    """Read 7 Silver partitions (needed columns only) + attach _partition_month from filename."""
    parts = sorted((SILVER_DIR / "transactions").glob("silver_transactions_*.parquet"))
    cols = [
        "transaction_id", "customer_id", "account_id", "source_row_number",
        "amount_vnd", "exchange_rate", "event_date",
        "_is_valid", "_is_duplicate_survivor", "_dq_bucket", "_dq_errors", "_dq_run_id",
        *RULE_THRESHOLDS.keys(),
    ]
    frames = []
    for p in parts:
        df = pd.read_parquet(p, columns=cols)
        df["_partition_month"] = p.stem.replace("silver_transactions_", "")
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def partition_rowcounts(directory: Path, pattern: str) -> dict[str, int]:
    """Count rows per partition via Parquet metadata (zero-data read)."""
    out = {}
    for p in sorted(directory.glob(pattern)):
        out[p.stem] = pq.read_metadata(p).num_rows
    return out


# -- Row-flag checks (WARNING) --
def build_row_flag_checks(df: pd.DataFrame) -> list[Check]:
    n = len(df)
    checks: list[Check] = []
    for flag, (name, threshold) in RULE_THRESHOLDS.items():
        failing = int(df[flag].sum())
        rate = failing / n
        checks.append(_make_check(f"rate_{name}", "row_flag", SEVERITY_WARNING,
            f"Rate of {name}", rate, threshold, "<=",
            {"failing": failing, "total": n, "by_month": (df.groupby("_partition_month")[flag].mean()).round(6).to_dict()}))
    valid_rate = float(df["_is_valid"].mean())
    checks.append(_make_check("valid_rate", "row_flag", SEVERITY_WARNING,
        "Valid row rate", valid_rate, 0.95, ">=",
        {"valid_rows": int(df["_is_valid"].sum()), "total": n}))
    return checks


# -- Table-level integrity checks (ERROR) --
def build_integrity_checks(df: pd.DataFrame, bronze_counts: dict, silver_counts: dict, schema_names: set[str], cfg: GeneratorConfig) -> list[Check]:
    n = len(df)
    c: list[Check] = []
    _add = lambda name, obs, thr, comp, detail: c.append(_make_check(name, "integrity", SEVERITY_ERROR, "", obs, thr, comp, detail))

    bronze_total = sum(bronze_counts.values())
    silver_total = sum(silver_counts.values())
    _add("rowcount_silver_eq_bronze", abs(silver_total - bronze_total), 0, "==", {"bronze_total": bronze_total, "silver_total": silver_total})

    missing = [col for col in REQUIRED_COLUMNS if col not in schema_names]
    _add("schema_conformance", len(missing), 0, "==", {"missing_columns": missing})

    _add("amount_vnd_notnull_when_valid", int((df["_is_valid"] & df["amount_vnd"].isna()).sum()), 0, "==", {})
    _add("exchange_rate_notnull_when_valid", int((df["_is_valid"] & df["exchange_rate"].isna()).sum()), 0, "==", {})

    dup_members = df[df["_dq_duplicate"] | df["_is_duplicate_survivor"]]
    surv_per_group = dup_members.groupby("transaction_id")["_is_duplicate_survivor"].sum()
    bad_groups = int((surv_per_group != 1).sum())
    _add("exactly_one_survivor_per_group", bad_groups, 0, "==", {"dup_groups": int(len(surv_per_group)), "violating_groups": bad_groups})

    valid_ids = df.loc[df["_is_valid"] & (df["transaction_id"] != ""), "transaction_id"]
    _add("no_dup_among_valid", int((valid_ids.value_counts() > 1).sum()), 0, "==", {})

    empty_bucket = df["_dq_bucket"].fillna("") == ""
    _add("bucket_consistency", int(((df["_is_valid"] & ~empty_bucket) | (~df["_is_valid"] & empty_bucket)).sum()), 0, "==", {})

    start, end = pd.Timestamp(cfg.date_start), pd.Timestamp(cfg.date_end)
    ed = pd.to_datetime(df["event_date"], errors="coerce")
    _add("event_date_in_window", int((df["_is_valid"] & (ed.isna() | (ed < start) | (ed > end))).sum()), 0, "==", {})
    return c


# -- Manifest reconciliation (WARNING) --
def reconcile_manifest(df: pd.DataFrame) -> tuple[Check, list[dict]]:
    path = BAD_DATA_DIR / "error_manifest.csv"
    if not path.exists():
        return Check("manifest_reconciliation", "manifest", SEVERITY_WARNING,
                     "Không tìm thấy error_manifest.csv", observed=0, threshold=0, comparator="==",
                     passed=True, detail={"manifest_found": False}), []
    man = pd.read_csv(path)
    indexed = df.set_index("source_row_number")
    per_type: list[dict] = []
    worst_coverage = 1.0
    for et, flag in MANIFEST_FLAG_MAP.items():
        rows = man.loc[man["error_type"] == et, "row_index"].values
        if len(rows) == 0:
            continue
        sub = indexed.reindex(rows)
        caught = int(sub[flag].fillna(False).sum())
        cov = caught / len(rows)
        worst_coverage = min(worst_coverage, cov)
        per_type.append({"error_type": et, "manifest": int(len(rows)), "silver_flagged": caught, "coverage": round(cov, 4)})
    # duplicate: dùng membership (dup OR survivor), không chỉ non-survivor
    dup_rows = man.loc[man["error_type"] == "duplicate_transaction_id", "row_index"].values
    if len(dup_rows):
        sub = indexed.reindex(dup_rows)
        caught = int((sub["_dq_duplicate"].fillna(False) | sub["_is_duplicate_survivor"].fillna(False)).sum())
        cov = caught / len(dup_rows)
        worst_coverage = min(worst_coverage, cov)
        per_type.append({"error_type": "duplicate_transaction_id", "manifest": int(len(dup_rows)), "silver_flagged": caught, "coverage": round(cov, 4)})
    check = Check(
        name="manifest_coverage", group="manifest", severity=SEVERITY_WARNING,
        description="DQ bắt được các lỗi đã inject trong manifest (coverage tối thiểu)",
        observed=round(worst_coverage, 4), threshold=0.99, comparator=">=",
        passed=_EVAL[">="](worst_coverage, 0.99),
        detail={"per_type": per_type},
    )
    return check, per_type


# -- Run + report --
def run_checks(checks: list[Check]) -> dict:
    errors_failed = [c for c in checks if not c.passed and c.severity == SEVERITY_ERROR]
    warnings_failed = [c for c in checks if not c.passed and c.severity == SEVERITY_WARNING]
    overall_passed = len(errors_failed) == 0
    status = "PASS" if (overall_passed and not warnings_failed) else ("FAIL" if not overall_passed else "PASS_WITH_WARNINGS")
    return {
        "overall_passed": overall_passed,
        "overall_status": status,
        "total_checks": len(checks),
        "passed": sum(c.passed for c in checks),
        "failed_errors": len(errors_failed),
        "failed_warnings": len(warnings_failed),
    }


def _bucket_by_month(df: pd.DataFrame) -> dict:
    sub = df[df["_dq_bucket"].fillna("") != ""]
    if sub.empty:
        return {}
    pivot = sub.groupby(["_partition_month", "_dq_bucket"]).size().unstack(fill_value=0)
    return {m: {k: int(v) for k, v in row.items()} for m, row in pivot.iterrows()}


def _top_error_codes(df: pd.DataFrame) -> list[dict]:
    codes = df.loc[df["_dq_errors"].fillna("") != "", "_dq_errors"].str.split(",").explode()
    return [{"code": c, "count": int(n)} for c, n in codes.value_counts().items()]


def build_report(df: pd.DataFrame, checks: list[Check], verdict: dict, manifest_per_type: list[dict], run_id: str, bronze_total: int) -> dict:
    n = len(df)
    valid = int(df["_is_valid"].sum())
    buckets = {k: int(v) for k, v in df.loc[df["_dq_bucket"].fillna("") != "", "_dq_bucket"].value_counts().items()}
    return {
        "run_id": run_id,
        "silver_run_id": str(df["_dq_run_id"].iloc[0]) if n else None,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "overall_status": verdict["overall_status"],
        "overall_passed": verdict["overall_passed"],
        "totals": {"bronze_rows": bronze_total, "silver_rows": n, "valid_rows": valid,
                   "invalid_rows": n - valid, "valid_rate": round(valid / n, 6) if n else None,
                   "soft_orphan_rows": int(sum(df[c].sum() for c in ("_dq_fk_customer", "_dq_fk_account", "_dq_fk_merchant")))},
        "summary": {k: verdict[k] for k in ("total_checks", "passed", "failed_errors", "failed_warnings")},
        "bucket_counts": dict(sorted(buckets.items())),
        "bucket_by_month": _bucket_by_month(df),
        "top_error_codes": _top_error_codes(df),
        "soft_orphans": {c: int(df[c].sum()) for c in ("_dq_fk_customer", "_dq_fk_account", "_dq_fk_merchant")},
        "manifest_reconciliation": manifest_per_type,
        "checks": [asdict(c) for c in checks],
    }


def write_json_report(report: dict) -> Path:
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    path = QUALITY_DIR / "quality_report.json"
    with path.open("w", encoding="utf-8") as h:
        json.dump(report, h, ensure_ascii=False, indent=2, default=str)
    return path



def write_markdown_report(report: dict) -> Path:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    t, s = report["totals"], report["summary"]
    row_ok = "no row loss ✓" if t["bronze_rows"] == t["silver_rows"] else "ROW LOSS ✗"
    go = "CÓ" if report["overall_passed"] else "CHƯA — sửa ERROR trước"

    def _tbl(rows, headers): return "\n".join(["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"] + ["| " + " | ".join(str(x) for x in r) + " |" for r in rows])

    integrity = [[c["name"], c["observed"], f"{c['comparator']} {c['threshold']}", "PASS" if c["passed"] else "**FAIL**"] for c in report["checks"] if c["group"] == "integrity"]
    row_flags = [[c["name"], f"{c['observed']*100:.3f}%", f"{c['comparator']} {c['threshold']*100:.2f}%" if c["comparator"] in ("<=", ">=") else f"== {c['threshold']}", "PASS" if c["passed"] else "**WARN**"] for c in report["checks"] if c["group"] == "row_flag"]
    buckets = [[k, f"{v:,}"] for k, v in report["bucket_counts"].items()]
    errors = [[r["code"], f"{r['count']:,}"] for r in report["top_error_codes"]]
    manifest = [[r["error_type"], f"{r['manifest']:,}", f"{r['silver_flagged']:,}", f"{r['coverage']*100:.1f}%"] for r in report["manifest_reconciliation"]]

    md = f"""# Data Quality Report — Silver Transactions (Day 7)

- run_id: `{report['run_id']}` · silver_run_id: `{report['silver_run_id']}` · generated_at: {report['generated_at']}
- **Overall: {report['overall_status']}** — {s['passed']}/{s['total_checks']} check PASS · {s['failed_errors']} ERROR · {s['failed_warnings']} WARNING

## 1. Summary
- Bronze rows: {t['bronze_rows']:,} → Silver rows: {t['silver_rows']:,} ({row_ok})
- Valid: {t['valid_rows']:,} ({t['valid_rate']*100:.2f}%) · Invalid (quarantine-bound): {t['invalid_rows']:,}
- Soft orphan FK: {t['soft_orphan_rows']:,} → Unknown member (Day 9)

## 2. Integrity checks (ERROR)
{_tbl(integrity, ["check", "observed", "threshold", "status"])}

## 3. Row-flag rates (WARNING)
{_tbl(row_flags, ["check", "observed", "threshold", "status"])}

## 4. Quarantine buckets
{_tbl(buckets, ["bucket", "count"])}

## 5. Top error codes (`_dq_errors`)
{_tbl(errors, ["error_code", "count"])}

## 6. Manifest reconciliation
{_tbl(manifest, ["error_type", "manifest", "silver_flagged", "coverage"])}

## 7. Kết luận
Đủ điều kiện sang Day 8 (quarantine split): **{go}**.
"""
    path = DOCS_DIR / "data_quality_report.md"
    path.write_text(md, encoding="utf-8")
    return path


def run_quality_checks() -> dict:
    """Entrypoint (Airflow-callable): load Silver → build checks → write reports."""
    run_id = make_run_id()
    log.info("quality checks start run_id=%s", run_id)
    cfg = GeneratorConfig()
    df = load_silver_transactions()
    bronze_counts = partition_rowcounts(BRONZE_DIR / "raw_transactions", "raw_transactions_*.parquet")
    silver_counts = partition_rowcounts(SILVER_DIR / "transactions", "silver_transactions_*.parquet")
    schema_names = set(pq.read_schema(next((SILVER_DIR / "transactions").glob("silver_transactions_*.parquet"))).names)

    checks = build_row_flag_checks(df)
    checks += build_integrity_checks(df, bronze_counts, silver_counts, schema_names, cfg)
    manifest_check, manifest_per_type = reconcile_manifest(df)
    checks.append(manifest_check)

    verdict = run_checks(checks)
    report = build_report(df, checks, verdict, manifest_per_type, run_id, sum(bronze_counts.values()))
    json_path = write_json_report(report)
    md_path = write_markdown_report(report)
    for c in checks:
        lvl = log.info if c.passed else (log.error if c.severity == SEVERITY_ERROR else log.warning)
        lvl("check %-34s %s observed=%s threshold=%s", c.name, "PASS" if c.passed else c.severity + " FAIL", c.observed, c.threshold)
    log.info("quality checks done status=%s json=%s md=%s", verdict["overall_status"], json_path, md_path)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Data quality checks trên Silver.")
    parser.add_argument("--strict", action="store_true", help="coi WARNING fail cũng là fail (debug)")
    args = parser.parse_args()
    report = run_quality_checks()
    print(json.dumps({"overall_status": report["overall_status"], "summary": report["summary"], "totals": report["totals"]}, indent=2, default=str))
    failed = (not report["overall_passed"]) or (args.strict and report["summary"]["failed_warnings"] > 0)
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
