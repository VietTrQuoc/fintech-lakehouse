"""Day 8 — Quarantine split (tách dòng từ Silver).

Day 6 gắn cờ, Day 7 báo cáo. Day 8 THỰC SỰ tách dòng: Silver (có `_dq_bucket`/`_is_valid`) ->
  (1) clean (valid, fact-ready)  -> data/silver/clean_transactions/
  (2) quarantine theo bucket + lý do -> data/quarantine/<bucket>/

Routing CHỈ theo `_dq_bucket` / `_is_valid` đã có (single source of truth = Day 6), KHÔNG re-compute rule.
Orphan FK là SOFT (`_is_valid=True`) -> đi vào clean (mang theo cờ `_dq_fk_*` cho Day 9 map Unknown sk=-1).

Entrypoint: run_quarantine_split() (Airflow). CLI: python -m src.quality.quarantine (-> exit 0/1).
"""

import json
import logging
import shutil
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

# Đường dẫn tuyệt đối theo vị trí file (parents[2] = src/quality/x.py -> src/quality -> src -> root).
PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
CLEAN_DIR = SILVER_DIR / "clean_transactions"          # đích clean (vẫn thuộc Silver, đã tách dòng hỏng)
QUARANTINE_DIR = PROJECT_ROOT / "data" / "quarantine"  # đích quarantine (bad rows theo bucket)
QUALITY_REPORT = PROJECT_ROOT / "data" / "quality" / "quality_report.json"  # để đối chiếu số Day 7
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Logging ra console + file (giống các module trước) -> theo dõi lúc chạy + lưu audit.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "quarantine.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("quarantine")

# 4 bucket = đúng giá trị domain của cột `_dq_bucket` (Day 6 đặt) + khớp tên trong architecture PDF.
BUCKETS = [
    "quarantine_invalid_amount",
    "quarantine_invalid_timestamp",
    "quarantine_duplicate_transactions",
    "quarantine_bad_records",
]

# 28 cột giữ ở clean = 24 nghiệp vụ/derived + 3 cờ SOFT FK + lineage. CỐ Ý BỎ các cờ hard/_dq_bucket/
# _dq_errors/_is_valid/_is_duplicate_survivor: ở tập valid chúng toàn False/rỗng -> chỉ là nhiễu.
# GIỮ 3 cờ `_dq_fk_*` vì orphan vẫn nằm trong clean (soft) và Day 9 cần biết để trỏ Unknown member sk=-1.
CLEAN_KEEP_COLS = [
    "transaction_id", "customer_id", "account_id", "merchant_id", "device_id", "card_id",
    "transaction_type", "channel", "status", "currency",
    "amount_original", "exchange_rate", "amount_vnd",
    "country", "city", "location_id",
    "event_time", "event_date", "ingestion_time", "ingestion_lag_seconds",
    "source_system", "batch_id", "file_name", "source_row_number",
    "_dq_fk_customer", "_dq_fk_account", "_dq_fk_merchant", "_dq_run_id",
]


def make_run_id() -> str:
    """1 id cho mỗi lần split (vd 'quarantine_20260616_...') -> đóng dấu vào quarantine_run_id."""
    return "quarantine_" + datetime.now().strftime("%Y%m%d_%H%M%S")


def reset_output_dirs() -> None:
    """rmtree + mkdir cả clean lẫn quarantine TRƯỚC khi ghi -> re-run xác định, không sót file cũ.

    Nếu chỉ ghi đè theo tên, file tháng/bucket của lần trước có thể còn sót -> output không khớp input.
    """
    for d in (CLEAN_DIR, QUARANTINE_DIR):
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True, exist_ok=True)


def split_partition(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    """1 partition Silver -> (clean_df, {bucket: rows_df}). Thuần DataFrame->DataFrame (test + Day 15 port).

    Tiêu chí DUY NHẤT: `_is_valid` (clean) và `_dq_bucket` (bucket).
    ⚠️ KHÔNG dựa "bất kỳ cờ _dq_* nào bật" -> vì orphan FK soft cũng bật cờ nhưng `_is_valid=True`,
    nó PHẢI vào clean (không quarantine). Chỉ `_is_valid`/`_dq_bucket` mới quyết định đi đâu.
    """
    clean_df = df.loc[df["_is_valid"], CLEAN_KEEP_COLS].copy()              # valid -> clean (chỉ giữ 28 cột)
    bucket_frames = {b: df.loc[df["_dq_bucket"] == b].copy() for b in BUCKETS}  # mỗi bucket lấy dòng của nó
    return clean_df, bucket_frames


def build_quarantine_frame(df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    """Thêm cột lý do/lineage rõ ràng cho người điều tra (đặt lên ĐẦU bảng), giữ full cột gốc để audit."""
    out = df.copy()
    out["quarantine_reason"] = out["_dq_errors"]      # bản sao _dq_errors -> đáp PDF "lưu lý do lỗi"
    out["quarantine_bucket"] = out["_dq_bucket"]      # bucket dòng này thuộc về (khi gộp file vẫn biết)
    out["quarantined_at"] = datetime.now(timezone.utc).isoformat()  # lúc split
    out["quarantine_run_id"] = run_id                 # lineage của chính bước quarantine
    lead = ["quarantine_bucket", "quarantine_reason", "quarantined_at", "quarantine_run_id"]
    return out[lead + [c for c in out.columns if c not in lead]]  # đưa 4 cột mới lên đầu


def run_quarantine_split() -> dict:
    """Điều phối: reset -> xử từng partition tháng -> ghi clean ngay + gom quarantine -> verify -> summary."""
    run_id = make_run_id()
    log.info("quarantine split start run_id=%s", run_id)
    reset_output_dirs()

    parts = sorted((SILVER_DIR / "transactions").glob("silver_transactions_*.parquet"))
    clean_counts: dict[str, int] = {}
    # Quarantine gom across-month (1 file/bucket), nên trong vòng lặp chỉ TÍCH LŨY list theo bucket,
    # sau vòng lặp mới concat + ghi 1 lần. clean thì ghi ngay theo từng tháng (giữ partition).
    bucket_acc: dict[str, list[pd.DataFrame]] = {b: [] for b in BUCKETS}
    silver_cols: list[str] = []

    for p in parts:
        month = p.stem.replace("silver_transactions_", "")
        df = pd.read_parquet(p)              # đọc FULL cột (quarantine cần đủ ngữ cảnh để audit)
        silver_cols = list(df.columns)       # nhớ schema để tạo file rỗng-có-schema nếu bucket trống
        clean_df, bucket_frames = split_partition(df)
        clean_df.to_parquet(CLEAN_DIR / f"clean_transactions_{month}.parquet", index=False)
        clean_counts[month] = len(clean_df)
        quarantined = 0
        for b, sub in bucket_frames.items():
            if len(sub):
                bucket_acc[b].append(build_quarantine_frame(sub, run_id))
                quarantined += len(sub)
        log.info("clean_transactions[%s] rows=%7d clean=%7d quarantined=%6d", month, len(df), len(clean_df), quarantined)

    # Ghi 1 file/bucket. Bucket rỗng -> vẫn ghi file rỗng-CÓ-SCHEMA để Day 9/15 glob luôn tìm thấy dataset.
    quarantine_counts: dict[str, int] = {}
    empty_cols = ["quarantine_bucket", "quarantine_reason", "quarantined_at", "quarantine_run_id"] + silver_cols
    for b in BUCKETS:
        out_dir = QUARANTINE_DIR / b
        out_dir.mkdir(parents=True, exist_ok=True)
        frame = pd.concat(bucket_acc[b], ignore_index=True) if bucket_acc[b] else pd.DataFrame(columns=empty_cols)
        frame.to_parquet(out_dir / f"{b}.parquet", index=False)
        quarantine_counts[b] = len(frame)
        log.info("quarantine[%-34s] rows=%7d", b, len(frame))

    summary = _build_summary(run_id, clean_counts, quarantine_counts)
    checks = verify(summary)
    summary["verification"] = checks
    summary["overall_passed"] = all(c["passed"] for c in checks)

    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    with (QUARANTINE_DIR / "quarantine_summary.json").open("w", encoding="utf-8") as h:
        json.dump(summary, h, ensure_ascii=False, indent=2, default=str)
    log.info("quarantine split done passed=%s", summary["overall_passed"])
    return summary


def _build_summary(run_id: str, clean_counts: dict, quarantine_counts: dict) -> dict:
    """Gom số liệu tổng. silver_total đếm qua metadata parquet (không load lại data)."""
    silver_total = sum(pq.read_metadata(p).num_rows for p in (SILVER_DIR / "transactions").glob("silver_transactions_*.parquet"))
    clean_total = sum(clean_counts.values())
    q_total = sum(quarantine_counts.values())
    return {
        "run_id": run_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "totals": {
            "silver_rows": silver_total,
            "clean_rows": clean_total,
            "quarantine_rows": q_total,
            "conserved": clean_total + q_total == silver_total,  # tổng phải bảo toàn
        },
        "clean_by_month": clean_counts,
        "bucket_counts": quarantine_counts,
    }


def verify(summary: dict) -> list[dict]:
    """Các kiểm tra SAU split (đọc lại output đã ghi). Trả về list {name, passed, detail}.

    Bất kỳ check nào fail -> overall_passed=False -> main() exit 1 (Airflow Day 13 sẽ fail task).
    """
    checks: list[dict] = []

    def add(name: str, passed: bool, detail: Any) -> None:
        checks.append({"name": name, "passed": bool(passed), "detail": detail})

    # 1. Bảo toàn: clean + quarantine == silver (không mất/nhân dòng khi split).
    t = summary["totals"]
    add("conservation_sum_eq_silver", t["clean_rows"] + t["quarantine_rows"] == t["silver_rows"],
        {"clean": t["clean_rows"], "quarantine": t["quarantine_rows"], "silver": t["silver_rows"]})

    # 2-3. Đối chiếu Day 7 (nếu có quality_report.json): clean == valid, bucket khớp report.
    if QUALITY_REPORT.exists():
        rep = json.loads(QUALITY_REPORT.read_text(encoding="utf-8"))
        add("clean_eq_valid_from_report", t["clean_rows"] == rep["totals"]["valid_rows"],
            {"clean": t["clean_rows"], "report_valid": rep["totals"]["valid_rows"]})
        add("buckets_match_report", summary["bucket_counts"] == {k: v for k, v in rep["bucket_counts"].items()},
            {"split": summary["bucket_counts"], "report": rep["bucket_counts"]})

    # 4-6. Đọc lại từng file quarantine: không lẫn dòng valid, reason không rỗng, mỗi file đúng 1 bucket.
    valid_in_q = 0
    empty_reason = 0
    impure = []
    for b in BUCKETS:
        path = QUARANTINE_DIR / b / f"{b}.parquet"
        qdf = pd.read_parquet(path, columns=["_is_valid", "quarantine_reason", "quarantine_bucket"])
        if len(qdf) == 0:
            continue
        valid_in_q += int(qdf["_is_valid"].sum())                                   # phải = 0
        empty_reason += int((qdf["quarantine_reason"].fillna("") == "").sum())      # phải = 0
        uniq = qdf["quarantine_bucket"].unique().tolist()
        if uniq != [b]:                                                             # purity: chỉ 1 bucket/file
            impure.append({b: uniq})
    add("no_valid_in_quarantine", valid_in_q == 0, {"valid_rows_found": valid_in_q})
    add("reason_non_empty", empty_reason == 0, {"empty_reason_rows": empty_reason})
    add("bucket_purity", len(impure) == 0, {"impure": impure})

    # 7. transaction_id duy nhất trong clean (degenerate dim phải unique cho fact sạch — grain §5).
    ids = pd.concat(
        [pd.read_parquet(p, columns=["transaction_id"]) for p in CLEAN_DIR.glob("clean_transactions_*.parquet")],
        ignore_index=True,
    )["transaction_id"]
    dup_ids = int((ids.value_counts() > 1).sum())
    add("transaction_id_unique_in_clean", dup_ids == 0, {"duplicated_ids": dup_ids})

    return checks


def main() -> None:
    summary = run_quarantine_split()
    # In gọn ra stdout (giống validate.py) + exit code cho Airflow/CI.
    print(json.dumps({
        "overall_passed": summary["overall_passed"],
        "totals": summary["totals"],
        "bucket_counts": summary["bucket_counts"],
        "verification": [{"name": c["name"], "passed": c["passed"]} for c in summary["verification"]],
    }, indent=2, default=str))
    raise SystemExit(0 if summary["overall_passed"] else 1)


if __name__ == "__main__":
    main()
