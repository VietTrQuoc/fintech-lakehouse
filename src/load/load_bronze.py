"""Bronze loader.

Landing tầng Bronze cho Financial Transaction Data Lakehouse.

Nguyên tắc Bronze (landing zone):
- Đọc raw từ ``data/raw/`` và ghi xuống ``data/bronze/`` dạng Parquet, GẦN NHƯ nguyên trạng.
- KHÔNG làm sạch, KHÔNG ép kiểu, KHÔNG loại bad record. Mọi dòng lỗi (amount="NaN",
  timestamp sai, FK mồ côi, channel lạ...) phải đi tiếp xuống Silver để tầng Data Quality /
  Quarantine (Day 7-8) phát hiện và tách. Nếu ép kiểu ngay tại Bronze, pandas sẽ nuốt giá trị lỗi
  thành NaN (hoặc làm hỏng load) -> quarantine không còn gì để bắt -> mất tiêu chí.
- Mỗi bảng được gắn metadata lineage (_source_file, _source_system, _batch_id, _ingested_at) để
  truy vết: dòng này từ file nào, hệ thống nguồn nào, thuộc lần load nào, vào lúc nào.

Chạy: ``python -m src.load.load_bronze``
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# Định vị thư mục theo vị trí file này (không phụ thuộc cwd) -> chạy ở đâu cũng đúng đường dẫn.
# parents[2] = lùi 2 cấp: src/load/load_bronze.py -> src/load -> src -> <project root>.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / 'data' / 'raw'        # nguồn: output của generator
BRONZE_DIR = PROJECT_ROOT / 'data' / 'bronze'  # đích: tầng Bronze (Parquet)
LOG_DIR = PROJECT_ROOT / 'logs'
LOG_DIR.mkdir(parents=True, exist_ok=True)      # tạo trước, vì FileHandler bên dưới cần file tồn tại

# Logging ra CẢ console (theo dõi lúc chạy) LẪN file logs/load_bronze.log (lưu lại để audit/debug).
# Đây là yêu cầu "có logging khi đọc/load" trong checklist Day 3.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / 'load_bronze.log', encoding='utf-8')
    ]
)
log = logging.getLogger("bronze")

# Khai báo các nguồn CSV "1 file -> 1 bảng" theo dạng (tên file raw, tên bảng Bronze, nhãn hệ nguồn).
# Gom thành list để load bằng 1 vòng lặp, thêm nguồn mới chỉ cần thêm 1 dòng (không lặp code).
CSV_SOURCES = [
    ("customers.csv", "raw_customers", "profile_csv"),
    ("accounts.csv", "raw_accounts", "core_banking_csv"),
    ("merchants.csv", "raw_merchants", "merchant_csv"),
    ("devices.csv", "raw_devices", "device_registry_csv"),
    ("cards.csv", "raw_cards", "card_csv"),
    ("locations.csv", "raw_locations", "reference_csv"),
    ("customer_scd_events.csv", "raw_customer_scd_events", "customer_profile_service"),
]

def make_batch_id() -> str:
    """Sinh 1 id duy nhất cho mỗi lần chạy load (vd 'bronze_20260611_143005').

    Mọi bảng load trong cùng 1 lần chạy sẽ mang chung batch_id này -> dễ truy vết "mẻ" nào đã ghi
    gì, và là nền cho re-run/backfill sau này (xác định và thay đúng mẻ cần nạp lại).
    """
    return "bronze_" + datetime.now().strftime("%Y%m%d_%H%M%S")

def add_metadata(df: pd.DataFrame, source_file: str, source_system: str, batch: str) -> pd.DataFrame:
    """Gắn 4 cột metadata lineage vào DataFrame trước khi ghi Bronze.

    - _ingested_at : thời điểm load (UTC, ISO) -> phân biệt với event_time của dữ liệu nghiệp vụ.
    - _source_file : file gốc dòng này đến từ đâu.
    - _source_system: hệ thống nguồn (mobile_wallet, core_banking, mock_fx_api...).
    - _batch_id    : mẻ load nào ghi dòng này.
    """
    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    df["_source_file"] = source_file
    df["_source_system"] = source_system
    df["_batch_id"] = batch
    return df

def write_parquet(df: pd.DataFrame, table: str, file_stem: str) -> None:
    """Ghi DataFrame ra data/bronze/<table>/<file_stem>.parquet.

    Dùng Parquet (không phải CSV) vì: lưu theo cột + nén tốt (nhẹ hơn nhiều), giữ schema, đọc/scan
    nhanh, và là định dạng nền để Day 16 convert sang Delta Lake -> đúng tinh thần "Lakehouse".
    """
    out = BRONZE_DIR / table
    out.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out / f"{file_stem}.parquet", index=False)

def load_csv_source(file_name: str, table: str, source_system: str, batch: str) -> int:
    """Load 1 nguồn CSV phẳng -> 1 bảng Bronze. Trả về số dòng đã ghi."""
    # dtype=str + keep_default_na=False + na_filter=False => đọc MỌI cột thành chuỗi, GIỮ raw Y
    # NGUYÊN. Quan trọng: nếu để mặc định, pandas sẽ tự biến "NaN"/"" thành giá trị thiếu và có thể
    # suy kiểu số -> các bad record inject (amount lỗi, id rỗng...) bị bóp méo ngay tại Bronze.
    df = pd.read_csv(RAW_DIR / file_name, dtype=str, keep_default_na=False, na_filter=False)
    df = add_metadata(df, file_name, source_system, batch)
    write_parquet(df, table, table)
    log.info("loaded %-26s rows=%7d", table, len(df))
    return len(df)

def load_transactions(batch: str) -> int:
    """Load bảng giao dịch (2M dòng) -> Bronze, giữ nguyên cách chia partition theo tháng của raw."""
    # Raw đã chia sẵn 7 file txn_YYYY-MM.csv. Load từng file -> từng parquet để KHÔNG gộp 2M dòng vào
    # 1 file khổng lồ, và để bước sau scan/đọc theo tháng được nhanh.
    files = sorted((RAW_DIR / "transactions").glob("txn_*.csv"))
    total = 0
    for f in files:
        month = f.stem.replace("txn_", "")  # 'txn_2026-01' -> '2026-01'
        df = pd.read_csv(f, dtype=str, keep_default_na=False, na_filter=False)  # giữ raw như trên
        # Khác các bảng dim: transactions ĐÃ có sẵn lineage cấp-sự-kiện do generator sinh
        # (ingestion_time / source_system / batch_id / file_name). Nên ở đây chỉ đóng dấu thêm thông
        # tin của LẦN LOAD bronze này, tránh ghi đè lineage gốc.
        df["_source_file"] = f.name
        df["_batch_id"] = batch
        df["_bronze_ingested_at"] = datetime.now(timezone.utc).isoformat()
        # Đặt tên parquet theo tháng lấy từ TÊN FILE (đáng tin) — KHÔNG parse transaction_time để
        # chia partition, vì cột đó có dòng lỗi ("not_a_timestamp", "2099-...") sẽ làm hỏng việc chia.
        write_parquet(df, "raw_transactions", f"raw_transactions_{month}")
        log.info("loaded raw_transactions[%s] rows=%7d", month, len(df))
        total += len(df)
    return total

def load_exchange_rates(batch: str) -> int:
    """Load nguồn 'API' tỷ giá (JSON) -> raw_exchange_rates."""
    payload = json.loads((RAW_DIR / "exchange_rates.json").read_text(encoding="utf-8"))
    # JSON lồng: mỗi record có dict 'rates' -> trải phẳng thành các cột rate_vnd/usd/eur cho dễ query.
    rows = [
        {
            "date": r["date"], "base_currency": r["base_currency"],
            "rate_vnd": r["rates"]["VND"], "rate_usd": r["rates"]["USD"], "rate_eur": r["rates"]["EUR"],
            "source_system": r["source_system"],
        }
        for r in payload["records"]
    ]
    # .astype(str): ép cả bảng về chuỗi cho nhất quán nguyên tắc "Bronze giữ raw, không suy kiểu sớm".
    df = add_metadata(pd.DataFrame(rows).astype(str), "exchange_rates.json", "mock_fx_api", batch)
    write_parquet(df, "raw_exchange_rates", "raw_exchange_rates")
    log.info("loaded %-26s rows=%7d", "raw_exchange_rates", len(df))
    return len(df)


def load_customer_events(batch: str) -> int:
    """Load nguồn 'API' sự kiện khách hàng (JSON) -> raw_login_events."""
    payload = json.loads((RAW_DIR / "customer_events.json").read_text(encoding="utf-8"))
    recs = payload["records"]
    for r in recs:
        # 'metadata' là dict tùy event_type (auth_result, trust_decision...). Không thể đưa thẳng vào
        # 1 ô bảng phẳng -> serialize lại thành chuỗi JSON, giữ trọn thông tin để Silver bóc sau.
        r["metadata"] = json.dumps(r.get("metadata", {}), ensure_ascii=False)
    df = add_metadata(pd.DataFrame(recs).astype(str), "customer_events.json", "customer_activity_api", batch)
    write_parquet(df, "raw_login_events", "raw_login_events")
    log.info("loaded %-26s rows=%7d", "raw_login_events", len(df))
    return len(df)


def main() -> None:
    """Điều phối toàn bộ lần load: 1 batch_id cho cả mẻ, load lần lượt mọi nguồn, log tổng kết."""
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    batch = make_batch_id()  # 1 id chung cho tất cả bảng trong lần chạy này
    log.info("bronze load start batch=%s", batch)
    # counts: gom số dòng từng bảng để in tổng kết -> kiểm tra nhanh không thiếu/sót bảng nào.
    counts = {"raw_transactions": load_transactions(batch)}
    for file_name, table, source_system in CSV_SOURCES:
        counts[table] = load_csv_source(file_name, table, source_system, batch)
    counts["raw_exchange_rates"] = load_exchange_rates(batch)
    counts["raw_login_events"] = load_customer_events(batch)
    log.info("bronze load done batch=%s totals=%s", batch, counts)


if __name__ == "__main__":
    main()
