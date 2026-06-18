"""Day 6 — Transform Bronze -> Silver (ép kiểu + chuẩn hóa + gắn cờ DQ).

Mô hình **cast-and-flag**: ép kiểu lần đầu (Bronze toàn STRING) + chuẩn hóa giá trị, rồi GẮN CỜ chất
lượng cho từng dòng. GIỮ MỌI dòng — KHÔNG split/drop (split -> quarantine là Day 8), KHÔNG surrogate
key / SCD2 / dim_date (Day 9). Nhờ giữ mọi dòng + cờ, tầng sau biết chính xác dòng nào hỏng vì lý do gì.

Engine = pandas (cố ý: Day 15 convert chính file này Pandas -> PySpark). Hàm transform viết thuần
DataFrame -> DataFrame để dễ test + dễ port.

Luồng: Bronze(string) --[file này: cast + flag]--> Silver(typed + cột _dq_*) --Day7 report--> Day8 split.

Chạy: python -m src.transform.bronze_to_silver
"""

import logging
import shutil
from collections import Counter
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

# Import whitelist domain TỪ generator config (1 nguồn sự thật) -> tránh hard-code, lệch định nghĩa.
from src.generate.config import CHANNELS, CURRENCIES, STATUSES, TRANSACTION_TYPES

# Đường dẫn theo vị trí file (không phụ thuộc cwd). parents[2]: src/transform/x.py -> src/transform -> src -> root.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"   # nguồn
SILVER_DIR = PROJECT_ROOT / "data" / "silver"   # đích
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Logging ra console + file (giống load_bronze.py) -> theo dõi lúc chạy + lưu để audit.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "bronze_to_silver.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("silver")

# Mốc chặn timestamp "tương lai xa": dữ liệu thật max ~2026, nên year >= 2099 chắc chắn là rác inject.
FUTURE_YEAR_CUTOFF = 2099


def make_run_id() -> str:
    """1 id cho mỗi lần transform (vd 'silver_20260615_142912') -> đóng dấu lineage vào cột _dq_run_id."""
    return "silver_" + datetime.now().strftime("%Y%m%d_%H%M%S")


def clean_silver_dir(table: str) -> Path:
    """Xóa + tạo lại thư mục Silver của 1 table -> chạy lại xác định (idempotent), không sót file cũ.

    Nếu chỉ ghi đè theo tên, một file tháng cũ (vd dữ liệu lần trước nhiều tháng hơn) có thể còn sót.
    rmtree đảm bảo Silver luôn khớp đúng input lần chạy này.
    """
    out = SILVER_DIR / table
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True, exist_ok=True)
    return out


def read_id_set(table: str, column: str) -> set[str]:
    """Đọc 1 cột id của bảng Bronze thành set Python -> tra cứu O(1) khi check FK orphan."""
    df = pd.read_parquet(BRONZE_DIR / table / f"{table}.parquet", columns=[column])
    return set(df[column].astype(str).str.strip())


def build_rate_lookup() -> dict[str, float]:
    """Lookup (date|currency) -> tỷ giá VND, từ raw_exchange_rates. VND = 1.0.

    Trả về dict key "YYYY-MM-DD|CCY" để bước sau dùng .map() (vector, nhanh) thay vì merge cho 2M dòng.
    """
    rates = pd.read_parquet(BRONZE_DIR / "raw_exchange_rates" / "raw_exchange_rates.parquet")
    lookup: dict[str, float] = {}
    for _, r in rates.iterrows():
        d = str(r["date"])
        lookup[f"{d}|VND"] = float(r["rate_vnd"])
        lookup[f"{d}|USD"] = float(r["rate_usd"])
        lookup[f"{d}|EUR"] = float(r["rate_eur"])
    return lookup


def build_duplicate_index() -> tuple[set[str], dict[str, int]]:
    """1 pass TOÀN CỤC trên transaction_id của cả 7 partition -> (set id trùng, id -> source_row_number nhỏ nhất).

    Vì sao toàn cục: 2 bản trùng id có thể nằm ở 2 file tháng khác nhau -> xử riêng từng partition sẽ
    bỏ sót. Nên gom transaction_id của cả 7 file lại rồi mới tìm id xuất hiện >= 2 lần.
    survivor = dòng có source_row_number nhỏ nhất trong nhóm (chọn deterministic, giữ 1 bản, bỏ phần dư).
    Chỉ đọc 2 cột để nhẹ (không nạp toàn bộ 2M dòng đầy đủ).
    """
    parts = sorted((BRONZE_DIR / "raw_transactions").glob("raw_transactions_*.parquet"))
    frames = [pd.read_parquet(p, columns=["transaction_id", "source_row_number"]) for p in parts]
    ids = pd.concat(frames, ignore_index=True)
    ids["transaction_id"] = ids["transaction_id"].astype(str).str.strip()
    ids["source_row_number"] = pd.to_numeric(ids["source_row_number"], errors="coerce")
    ids = ids[ids["transaction_id"] != ""]  # dòng null id KHÔNG xét trùng (đã có lỗi riêng null_transaction_id)
    grp = ids.groupby("transaction_id")["source_row_number"]
    sizes = grp.size()
    dup_ids = set(sizes[sizes >= 2].index)              # các id xuất hiện >= 2 lần
    survivor_srn = grp.min().loc[list(dup_ids)].to_dict()  # mỗi id trùng -> srn nhỏ nhất = bản được giữ
    return dup_ids, survivor_srn


def transform_transactions_partition(
    df: pd.DataFrame,
    *,
    rate_lookup: dict[str, float],
    dup_ids: set[str],
    survivor_srn: dict[str, int],
    cust_ids: set[str],
    acc_ids: set[str],
    mer_ids: set[str],
    loc_ids: set[str],
    run_id: str,
) -> pd.DataFrame:
    """Cast + chuẩn hóa + gắn cờ DQ cho 1 partition transactions. Thuần DataFrame -> DataFrame (dễ test/port)."""
    idx = df.index

    # ---- 1) Cast kiểu + chuẩn hóa giá trị ------------------------------------
    # id/text: chỉ strip khoảng trắng (giữ string).
    txn_id = df["transaction_id"].astype(str).str.strip()
    customer_id = df["customer_id"].astype(str).str.strip()
    account_id = df["account_id"].astype(str).str.strip()
    merchant_id = df["merchant_id"].astype(str).str.strip()
    device_id = df["device_id"].astype(str).str.strip()
    card_id = df["card_id"].astype(str).str.strip()

    # amount: ép số; "NaN"/"amount_unknown" -> NaN (errors="coerce" không raise, giữ được dòng).
    amount = pd.to_numeric(df["amount"], errors="coerce")
    # categorical: chuẩn hóa hoa/thường TRƯỚC khi so khớp whitelist (vd "VND"/"vnd" đồng nhất).
    currency = df["currency"].astype(str).str.strip().str.upper()
    txn_type = df["transaction_type"].astype(str).str.strip().str.lower()
    channel = df["channel"].astype(str).str.strip().str.lower()
    status = df["status"].astype(str).str.strip().str.lower()
    country = df["country"].astype(str).str.strip().str.upper()
    city = df["city"].astype(str).str.strip()
    location_id = df["location_id"].astype(str).str.strip()

    # timestamp: ép datetime; "not_a_timestamp" -> NaT.
    event_ts = pd.to_datetime(df["transaction_time"], errors="coerce")
    ingest_ts = pd.to_datetime(df["ingestion_time"], errors="coerce")
    source_row_number = pd.to_numeric(df["source_row_number"], errors="coerce").astype("Int64")

    # ---- 2) Tính các cờ hợp lệ (rule DQ) -------------------------------------
    f_null_id = (txn_id == "") | txn_id.isna()
    # BẪY: 'NaN' cast ra float NaN (không phải NULL) và NaN <= 0 = False -> phải dùng isfinite để bắt
    # NaN/inf, KHÔNG chỉ so sánh < 0 (cảnh báo từ Day 4). amount hợp lệ = hữu hạn VÀ > 0.
    f_amount = pd.Series(~np.isfinite(amount), index=idx) | (amount <= 0)
    # BẪY: "2099-12-31" parse được thành datetime hợp lệ -> coerce KHÔNG bắt được. Cần rule year riêng.
    f_ts = event_ts.isna() | (event_ts.dt.year.fillna(0) >= FUTURE_YEAR_CUTOFF)
    f_currency = ~currency.isin(CURRENCIES)      # bắt "XXX"
    f_channel = ~channel.isin(CHANNELS)          # bắt "foo"
    f_type = ~txn_type.isin(TRANSACTION_TYPES)
    f_status = ~status.isin(STATUSES)
    f_location = ~location_id.isin(loc_ids)      # bắt "loc_bad_record"

    # Duplicate (đã tính toàn cục ở build_duplicate_index): cả nhóm cùng id; chỉ survivor được coi hợp lệ.
    is_dup = txn_id.isin(dup_ids) & ~f_null_id
    min_srn = txn_id.map(survivor_srn)                       # srn nhỏ nhất của nhóm (NaN nếu không trùng)
    is_survivor = is_dup & (source_row_number == min_srn.astype("Int64"))
    f_dup_hard = is_dup & ~is_survivor                      # bản dư -> sẽ quarantine

    # FK orphan = cờ SOFT: KHÔNG làm dòng invalid (Day 9 trỏ Unknown member sk=-1, không quarantine —
    # theo grain_design §7). merchant_id RỖNG là hợp lệ (transfer/withdrawal/topup không có merchant).
    f_fk_customer = ~customer_id.isin(cust_ids)
    f_fk_account = ~account_id.isin(acc_ids)
    f_fk_merchant = (merchant_id != "") & ~merchant_id.isin(mer_ids)

    # ---- 3) Cột phái sinh ----------------------------------------------------
    event_date = event_ts.dt.date                          # ngày giao dịch (Day 9 -> date_key)
    event_date_str = event_ts.dt.strftime("%Y-%m-%d")      # NaT -> NaN
    # Join tỷ giá an toàn: key "ngày|currency". ts lỗi -> key NaN; currency lạ -> không có trong lookup;
    # cả 2 -> rate NaN -> amount_vnd NaN (đã có cờ tương ứng, không raise).
    rate_key = event_date_str + "|" + currency
    exchange_rate = rate_key.map(rate_lookup)
    amount_vnd = amount * exchange_rate                     # measure additive chính cho Gold
    lag = (ingest_ts - event_ts).dt.total_seconds().round().astype("Int64")  # độ trễ nạp; NaT -> NA

    # ---- 4) Tổng hợp cờ: bucket (1 thùng theo ưu tiên) + is_valid + chuỗi errors ----
    # Ưu tiên: duplicate > timestamp > amount > bad_records (chọn 1 thùng chính cho dòng nhiều lỗi).
    bucket = np.select(
        [
            f_dup_hard,
            f_ts,
            f_amount,
            f_null_id | f_currency | f_channel | f_type | f_status | f_location,
        ],
        [
            "quarantine_duplicate_transactions",
            "quarantine_invalid_timestamp",
            "quarantine_invalid_amount",
            "quarantine_bad_records",
        ],
        default="",   # rỗng = không lỗi cứng
    )
    bucket = pd.Series(bucket, index=idx)
    is_valid = bucket == ""   # orphan FK (soft) không nằm trong bucket -> không ảnh hưởng is_valid

    # _dq_errors = gom TẤT CẢ mã lỗi của dòng thành chuỗi CSV (kể cả fk soft) để xem đầy đủ lý do.
    # Xây bằng np.where (vector) thay vì apply (chậm cho 2M dòng): nối "code," khi cờ True, rồi bỏ "," cuối.
    errors = pd.Series("", index=idx)
    for flag, code in [
        (f_null_id, "null_transaction_id"),
        (f_dup_hard, "duplicate_transaction_id"),
        (f_amount, "invalid_amount"),
        (f_ts, "invalid_timestamp"),
        (f_currency, "invalid_currency"),
        (f_channel, "invalid_channel"),
        (f_type, "invalid_type"),
        (f_status, "invalid_status"),
        (f_location, "invalid_location"),
        (f_fk_customer, "fk_customer_orphan"),
        (f_fk_account, "fk_account_orphan"),
        (f_fk_merchant, "fk_merchant_orphan"),
    ]:
        errors = errors + np.where(flag.fillna(False), code + ",", "")
    errors = errors.str.rstrip(",")

    # ---- 5) Lắp DataFrame Silver (cột nghiệp vụ đã cast + derived + cờ DQ) ----
    return pd.DataFrame(
        {
            "transaction_id": txn_id,
            "customer_id": customer_id,
            "account_id": account_id,
            "merchant_id": merchant_id,
            "device_id": device_id,
            "card_id": card_id,
            "transaction_type": txn_type,
            "channel": channel,
            "status": status,
            "currency": currency,
            "amount_original": amount,        # số tiền theo currency gốc
            "exchange_rate": exchange_rate,
            "amount_vnd": amount_vnd,         # quy đổi VND (cộng được mọi chiều)
            "country": country,
            "city": city,
            "location_id": location_id,
            "event_time": event_ts,
            "event_date": event_date,
            "ingestion_time": ingest_ts,
            "ingestion_lag_seconds": lag,
            "source_system": df["source_system"].astype(str).str.strip(),
            "batch_id": df["batch_id"].astype(str).str.strip(),
            "file_name": df["file_name"].astype(str).str.strip(),
            "source_row_number": source_row_number,
            # --- cờ DQ per-rule (bool) ---
            "_dq_null_transaction_id": f_null_id.fillna(False),
            "_dq_duplicate": f_dup_hard.fillna(False),
            "_dq_invalid_amount": f_amount.fillna(False),
            "_dq_invalid_timestamp": f_ts.fillna(False),
            "_dq_invalid_currency": f_currency.fillna(False),
            "_dq_invalid_channel": f_channel.fillna(False),
            "_dq_invalid_type": f_type.fillna(False),
            "_dq_invalid_status": f_status.fillna(False),
            "_dq_invalid_location": f_location.fillna(False),
            "_dq_fk_customer": f_fk_customer.fillna(False),   # soft
            "_dq_fk_account": f_fk_account.fillna(False),     # soft
            "_dq_fk_merchant": f_fk_merchant.fillna(False),   # soft
            # --- tổng hợp ---
            "_dq_errors": errors,                       # chuỗi mọi lý do
            "_dq_bucket": bucket,                        # thùng quarantine (Day 8 dùng để split)
            "_is_valid": is_valid,                       # True = sạch (không lỗi cứng)
            "_is_duplicate_survivor": is_survivor.fillna(False),  # bản được giữ trong nhóm trùng
            "_dq_run_id": run_id,                        # lineage mẻ transform
        },
        index=idx,
    )


def transform_transactions(run_id: str) -> dict:
    """Orchestrate transform 7 partition transactions: chuẩn bị lookup chung -> xử từng tháng -> ghi Silver."""
    out = clean_silver_dir("transactions")
    # Chuẩn bị 1 lần các tra cứu dùng chung cho mọi partition:
    rate_lookup = build_rate_lookup()
    dup_ids, survivor_srn = build_duplicate_index()
    log.info("duplicate transaction_id: %d id trung", len(dup_ids))
    cust_ids = read_id_set("raw_customers", "customer_id")
    acc_ids = read_id_set("raw_accounts", "account_id")
    mer_ids = read_id_set("raw_merchants", "merchant_id")
    loc_ids = read_id_set("raw_locations", "location_id")

    parts = sorted((BRONZE_DIR / "raw_transactions").glob("raw_transactions_*.parquet"))
    total = 0
    valid_total = 0
    bucket_counts: Counter = Counter()
    for p in parts:
        month = p.stem.replace("raw_transactions_", "")
        raw = pd.read_parquet(p)
        silver = transform_transactions_partition(
            raw,
            rate_lookup=rate_lookup,
            dup_ids=dup_ids,
            survivor_srn=survivor_srn,
            cust_ids=cust_ids,
            acc_ids=acc_ids,
            mer_ids=mer_ids,
            loc_ids=loc_ids,
            run_id=run_id,
        )
        silver.to_parquet(out / f"silver_transactions_{month}.parquet", index=False)  # giữ partition tháng
        valid = int(silver["_is_valid"].sum())
        bucket_counts.update(silver.loc[silver["_dq_bucket"] != "", "_dq_bucket"])  # đếm theo thùng
        total += len(silver)
        valid_total += valid
        log.info("silver_transactions[%s] rows=%7d valid=%7d", month, len(silver), valid)
    return {
        "transaction_rows": total,
        "valid_rows": valid_total,
        "invalid_rows": total - valid_total,
        "bucket_counts": dict(sorted(bucket_counts.items())),
    }


def transform_simple_dim(table_in: str, table_out: str, *, date_cols=(), float_cols=(), bool_cols=()) -> int:
    """Cast-only cho 1 dim: chỉ ép kiểu các cột chỉ định. KHÔNG SCD2/sk/dedup (để Day 9).

    date_cols -> date; float_cols -> số; bool_cols -> bool ('true'/'false' string -> True/False).
    """
    df = pd.read_parquet(BRONZE_DIR / table_in / f"{table_in}.parquet")
    for c in date_cols:
        df[c] = pd.to_datetime(df[c], errors="coerce").dt.date
    for c in float_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in bool_cols:
        df[c] = df[c].astype(str).str.strip().str.lower() == "true"
    out = clean_silver_dir(table_out)
    df.to_parquet(out / f"{table_out}.parquet", index=False)
    log.info("loaded %-22s rows=%7d", table_out, len(df))
    return len(df)


def transform_dims() -> dict:
    """Cast-only 5 dim. SCD2 (dim_customer), join scd_events, cards, login_events đều để Day 9."""
    counts = {}
    counts["customers"] = transform_simple_dim("raw_customers", "customers", date_cols=("dob", "signup_date"))
    counts["accounts"] = transform_simple_dim("raw_accounts", "accounts", date_cols=("opened_date",), float_cols=("current_balance",))
    counts["merchants"] = transform_simple_dim("raw_merchants", "merchants", date_cols=("onboarded_date",), float_cols=("risk_score", "base_failure_rate"))
    counts["devices"] = transform_simple_dim("raw_devices", "devices", date_cols=("first_seen",), bool_cols=("is_trusted",))
    counts["locations"] = transform_simple_dim("raw_locations", "locations", float_cols=("lat", "lon"))
    return counts


def main() -> None:
    run_id = make_run_id()
    log.info("bronze->silver start run_id=%s", run_id)
    summary = transform_transactions(run_id)
    summary["dims"] = transform_dims()
    log.info("bronze->silver done run_id=%s summary=%s", run_id, summary)


if __name__ == "__main__":
    main()
