"""Day 7 — Data Quality checks + report (chạy trên Silver).

Day 6 đã gắn cờ `_dq_*` per-row. Day 7 KHÔNG tính lại rule — mà tổng hợp cờ thành các CHECK cấp bảng
(tên + ngưỡng + severity + pass/fail), cộng vài check toàn vẹn độc lập, rồi xuất report. Chỉ ĐỌC +
BÁO CÁO; split -> quarantine là Day 8.

Severity:
- WARNING = bad-data-row (nguồn bẩn, đã biết, Day 8 quarantine xử) -> KHÔNG fail pipeline (khớp Day 13).
- ERROR   = lỗi cấu trúc/transform (mất dòng, sai schema, survivor sai, measure rỗng) -> fail pipeline.

Entrypoint: run_quality_checks() (Airflow gọi). CLI: python -m src.quality.quality_checks (-> exit 0/1).
"""

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

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
QUALITY_DIR = PROJECT_ROOT / "data" / "quality"
DOCS_DIR = PROJECT_ROOT / "docs"
BAD_DATA_DIR = PROJECT_ROOT / "data" / "bad_data_samples"
LOG_DIR = PROJECT_ROOT / "logs"
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

# Nhóm A (row-flag aggregation): cờ Silver -> (tên check, ngưỡng rate tối đa). Tất cả WARNING, so "<=".
# Ngưỡng ~ 2-3x expected từ config.error_rates (buffer dao động seed/tháng).
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

# Cột cần có trong Silver (check schema_conformance). Thiếu/sai -> ERROR.
REQUIRED_COLUMNS = [
    "transaction_id", "amount_original", "amount_vnd", "exchange_rate", "event_time", "event_date",
    "source_row_number", "_dq_errors", "_dq_bucket", "_is_valid", "_is_duplicate_survivor", "_dq_run_id",
    *RULE_THRESHOLDS.keys(),
]

# error_type trong manifest -> cờ Silver tương ứng (cho reconciliation).
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


def _evaluate(observed: float, comparator: str, threshold: float) -> bool:
    if comparator == "<=":
        return observed <= threshold
    if comparator == ">=":
        return observed >= threshold
    if comparator == "==":
        return observed == threshold
    raise ValueError(f"comparator lạ: {comparator}")


def make_run_id() -> str:
    return "quality_" + datetime.now().strftime("%Y%m%d_%H%M%S")


# ----------------------------------------------------------------------------
# Load
# ----------------------------------------------------------------------------
def load_silver_transactions() -> pd.DataFrame:
    """Đọc 7 partition Silver (chỉ cột cần) + gắn _partition_month từ tên file (đáng tin hơn event_date)."""
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
    """Đếm số dòng mỗi partition qua metadata parquet (KHÔNG load data)."""
    out = {}
    for p in sorted(directory.glob(pattern)):
        out[p.stem] = pq.read_metadata(p).num_rows
    return out


# ----------------------------------------------------------------------------
# Nhóm A — row-flag aggregation (WARNING)
# ----------------------------------------------------------------------------
def build_row_flag_checks(df: pd.DataFrame) -> list[Check]:
    n = len(df)
    checks: list[Check] = []
    for flag, (name, threshold) in RULE_THRESHOLDS.items():
        failing = int(df[flag].sum())
        rate = failing / n
        by_month = (df.groupby("_partition_month")[flag].mean()).round(6).to_dict()
        checks.append(
            Check(
                name=f"rate_{name}",
                group="row_flag",
                severity=SEVERITY_WARNING,
                description=f"Tỷ lệ dòng dính {name} (cờ {flag})",
                observed=round(rate, 6),
                threshold=threshold,
                comparator="<=",
                passed=_evaluate(rate, "<=", threshold),
                detail={"failing": failing, "total": n, "by_month": by_month},
            )
        )
    valid_rate = float(df["_is_valid"].mean())
    checks.append(
        Check(
            name="valid_rate",
            group="row_flag",
            severity=SEVERITY_WARNING,
            description="Tỷ lệ dòng hợp lệ (không lỗi cứng)",
            observed=round(valid_rate, 6),
            threshold=0.95,
            comparator=">=",
            passed=_evaluate(valid_rate, ">=", 0.95),
            detail={"valid_rows": int(df["_is_valid"].sum()), "total": n},
        )
    )
    return checks


# ----------------------------------------------------------------------------
# Nhóm B — table-level integrity (ERROR)
# ----------------------------------------------------------------------------
def build_integrity_checks(df: pd.DataFrame, bronze_counts: dict, silver_counts: dict, schema_names: set[str], cfg: GeneratorConfig) -> list[Check]:
    n = len(df)
    checks: list[Check] = []

    # 1. rowcount Silver == Bronze (cast-and-flag giữ mọi dòng)
    bronze_total = sum(bronze_counts.values())
    silver_total = sum(silver_counts.values())
    checks.append(Check(
        name="rowcount_silver_eq_bronze", group="integrity", severity=SEVERITY_ERROR,
        description="Silver giữ đúng số dòng Bronze (không mất/nhân dòng)",
        observed=abs(silver_total - bronze_total), threshold=0, comparator="==",
        passed=(silver_total == bronze_total),
        detail={"bronze_total": bronze_total, "silver_total": silver_total},
    ))

    # 2. schema conformance: đủ cột bắt buộc
    missing = [c for c in REQUIRED_COLUMNS if c not in schema_names]
    checks.append(Check(
        name="schema_conformance", group="integrity", severity=SEVERITY_ERROR,
        description="Silver có đủ cột nghiệp vụ + cờ DQ",
        observed=len(missing), threshold=0, comparator="==", passed=(len(missing) == 0),
        detail={"missing_columns": missing},
    ))

    # 3. amount_vnd not-null khi _is_valid
    bad_vnd = int((df["_is_valid"] & df["amount_vnd"].isna()).sum())
    checks.append(Check(
        name="amount_vnd_notnull_when_valid", group="integrity", severity=SEVERITY_ERROR,
        description="Mọi dòng hợp lệ phải có amount_vnd (measure chính)",
        observed=bad_vnd, threshold=0, comparator="==", passed=(bad_vnd == 0),
        detail={"violations": bad_vnd},
    ))

    # 4. exchange_rate not-null khi _is_valid
    bad_rate = int((df["_is_valid"] & df["exchange_rate"].isna()).sum())
    checks.append(Check(
        name="exchange_rate_notnull_when_valid", group="integrity", severity=SEVERITY_ERROR,
        description="Mọi dòng hợp lệ phải khớp tỷ giá",
        observed=bad_rate, threshold=0, comparator="==", passed=(bad_rate == 0),
        detail={"violations": bad_rate},
    ))

    # 5. exactly one survivor per duplicate group
    dup_members = df[df["_dq_duplicate"] | df["_is_duplicate_survivor"]]
    surv_per_group = dup_members.groupby("transaction_id")["_is_duplicate_survivor"].sum()
    bad_groups = int((surv_per_group != 1).sum())
    checks.append(Check(
        name="exactly_one_survivor_per_group", group="integrity", severity=SEVERITY_ERROR,
        description="Mỗi nhóm transaction_id trùng có đúng 1 survivor",
        observed=bad_groups, threshold=0, comparator="==", passed=(bad_groups == 0),
        detail={"dup_groups": int(len(surv_per_group)), "violating_groups": bad_groups},
    ))

    # 6. no duplicate transaction_id trong tập valid
    valid_ids = df.loc[df["_is_valid"] & (df["transaction_id"] != ""), "transaction_id"]
    dup_in_valid = int((valid_ids.value_counts() > 1).sum())
    checks.append(Check(
        name="no_dup_among_valid", group="integrity", severity=SEVERITY_ERROR,
        description="transaction_id trong tập hợp lệ phải duy nhất",
        observed=dup_in_valid, threshold=0, comparator="==", passed=(dup_in_valid == 0),
        detail={"duplicated_ids": dup_in_valid},
    ))

    # 7. bucket consistency: _is_valid <=> _dq_bucket == ""
    empty_bucket = df["_dq_bucket"].fillna("") == ""
    mismatch = int(((df["_is_valid"] & ~empty_bucket) | (~df["_is_valid"] & empty_bucket)).sum())
    checks.append(Check(
        name="bucket_consistency", group="integrity", severity=SEVERITY_ERROR,
        description="_is_valid khớp với _dq_bucket rỗng/không rỗng",
        observed=mismatch, threshold=0, comparator="==", passed=(mismatch == 0),
        detail={"mismatches": mismatch},
    ))

    # 8. event_date trong window (chỉ dòng valid)
    start = pd.Timestamp(cfg.date_start)
    end = pd.Timestamp(cfg.date_end)
    ed = pd.to_datetime(df["event_date"], errors="coerce")
    out_window = int((df["_is_valid"] & (ed.isna() | (ed < start) | (ed > end))).sum())
    checks.append(Check(
        name="event_date_in_window", group="integrity", severity=SEVERITY_ERROR,
        description=f"event_date của dòng hợp lệ trong [{cfg.date_start}, {cfg.date_end}]",
        observed=out_window, threshold=0, comparator="==", passed=(out_window == 0),
        detail={"out_of_window": out_window},
    ))
    return checks


# ----------------------------------------------------------------------------
# Nhóm F — manifest reconciliation (WARNING)
# ----------------------------------------------------------------------------
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
        passed=_evaluate(worst_coverage, ">=", 0.99),
        detail={"per_type": per_type},
    )
    return check, per_type


# ----------------------------------------------------------------------------
# Run + report
# ----------------------------------------------------------------------------
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
    bucket_counts = {k: int(v) for k, v in df.loc[df["_dq_bucket"].fillna("") != "", "_dq_bucket"].value_counts().items()}
    soft_orphans = {c: int(df[c].sum()) for c in ("_dq_fk_customer", "_dq_fk_account", "_dq_fk_merchant")}
    silver_run_id = str(df["_dq_run_id"].iloc[0]) if n else None
    return {
        "run_id": run_id,
        "silver_run_id": silver_run_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "overall_status": verdict["overall_status"],
        "overall_passed": verdict["overall_passed"],
        "totals": {
            "bronze_rows": bronze_total,
            "silver_rows": n,
            "valid_rows": valid,
            "invalid_rows": n - valid,
            "valid_rate": round(valid / n, 6) if n else None,
            "soft_orphan_rows": int(sum(soft_orphans.values())),
        },
        "summary": {k: verdict[k] for k in ("total_checks", "passed", "failed_errors", "failed_warnings")},
        "bucket_counts": dict(sorted(bucket_counts.items())),
        "bucket_by_month": _bucket_by_month(df),
        "top_error_codes": _top_error_codes(df),
        "soft_orphans": soft_orphans,
        "manifest_reconciliation": manifest_per_type,
        "checks": [asdict(c) for c in checks],
    }


def write_json_report(report: dict) -> Path:
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    path = QUALITY_DIR / "quality_report.json"
    with path.open("w", encoding="utf-8") as h:
        json.dump(report, h, ensure_ascii=False, indent=2, default=str)
    return path


def _md_table(rows: list[list], headers: list[str]) -> str:
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
    for r in rows:
        out.append("| " + " | ".join(str(x) for x in r) + " |")
    return "\n".join(out)


def write_markdown_report(report: dict) -> Path:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    t = report["totals"]
    lines = [
        "# Data Quality Report — Silver Transactions (Day 7)",
        "",
        f"- run_id: `{report['run_id']}` · silver_run_id: `{report['silver_run_id']}` · generated_at: {report['generated_at']}",
        f"- **Overall: {report['overall_status']}** — {report['summary']['passed']}/{report['summary']['total_checks']} check PASS · "
        f"{report['summary']['failed_errors']} ERROR · {report['summary']['failed_warnings']} WARNING",
        "",
        "## 1. Summary",
        f"- Bronze rows: {t['bronze_rows']:,} → Silver rows: {t['silver_rows']:,} "
        f"({'no row loss ✓' if t['bronze_rows'] == t['silver_rows'] else 'ROW LOSS ✗'})",
        f"- Valid: {t['valid_rows']:,} ({t['valid_rate']*100:.2f}%) · Invalid (quarantine-bound): {t['invalid_rows']:,}",
        f"- Soft orphan FK: {t['soft_orphan_rows']:,} → Unknown member (Day 9), không quarantine",
        "",
        "## 2. Integrity checks (ERROR)",
        _md_table(
            [[c["name"], c["observed"], c["comparator"] + " " + str(c["threshold"]), "PASS" if c["passed"] else "**FAIL**"]
             for c in report["checks"] if c["group"] == "integrity"],
            ["check", "observed", "threshold", "status"],
        ),
        "",
        "## 3. Row-flag rates (WARNING)",
        _md_table(
            [[c["name"], f"{c['observed']*100:.3f}%", f"<= {c['threshold']*100:.2f}%" if c["comparator"] == "<=" else f">= {c['threshold']*100:.2f}%",
              "PASS" if c["passed"] else "**WARN**"]
             for c in report["checks"] if c["group"] == "row_flag"],
            ["check", "observed", "threshold", "status"],
        ),
        "",
        "## 4. Quarantine buckets",
        _md_table([[k, f"{v:,}"] for k, v in report["bucket_counts"].items()], ["bucket", "count"]),
        "",
        "## 5. Top error codes (`_dq_errors`)",
        _md_table([[r["code"], f"{r['count']:,}"] for r in report["top_error_codes"]], ["error_code", "count"]),
        "",
        "## 6. Manifest reconciliation",
        _md_table(
            [[r["error_type"], f"{r['manifest']:,}", f"{r['silver_flagged']:,}", f"{r['coverage']*100:.1f}%"]
             for r in report["manifest_reconciliation"]],
            ["error_type", "manifest", "silver_flagged", "coverage"],
        ),
        "",
        f"## 7. Kết luận",
        f"Đủ điều kiện sang Day 8 (quarantine split): **{'CÓ' if report['overall_passed'] else 'CHƯA — sửa ERROR trước'}**.",
        "",
    ]
    path = DOCS_DIR / "data_quality_report.md"
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def run_quality_checks() -> dict:
    """Entrypoint (Airflow gọi): load Silver -> build checks -> run -> ghi report -> trả report dict."""
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
