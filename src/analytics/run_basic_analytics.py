"""Day 4 — chạy sql/basic_analytics.sql trên Bronze parquet bằng DuckDB.

DuckDB đọc parquet trực tiếp (không cần load vào DB). Runner này:
  1. Tạo view `bronze_transactions` / `bronze_merchants` từ file parquet Bronze.
  2. Đọc file SQL, tách thành từng statement (ngăn bởi dấu ';').
  3. CREATE/SET -> chạy thầm (setup). SELECT/WITH -> chạy + in kết quả kèm nhãn '-- name:'.
  4. Ngoài việc in ra console, GHI toàn bộ kết quả ra báo cáo Markdown
     `docs/basic_analytics_report.md` (đồng bộ với các report tự sinh khác trong docs/).

Chạy: python -m src.analytics.run_basic_analytics
"""

import re
import sys
from datetime import datetime
from pathlib import Path

import duckdb

# Console Windows mặc định cp1252 -> in tên merchant tiếng Việt sẽ lỗi. Ép stdout sang utf-8.
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SQL_FILE = PROJECT_ROOT / "sql" / "basic_analytics.sql"
DOCS_DIR = PROJECT_ROOT / "docs"
REPORT = DOCS_DIR / "basic_analytics_report.md"   # đích báo cáo Markdown


def first_keyword(stmt: str) -> str:
    """Lấy keyword đầu của statement, bỏ qua các dòng comment '--' phía trên."""
    for line in stmt.splitlines():
        line = line.strip()
        if not line or line.startswith("--"):
            continue
        return line.split(None, 1)[0].upper()
    return ""


def statement_label(stmt: str) -> str | None:
    """Lấy nhãn từ comment '-- name: ...' nếu có."""
    m = re.search(r"--\s*name:\s*(.+)", stmt)
    return m.group(1).strip() if m else None


def main() -> None:
    con = duckdb.connect()
    # Đăng ký parquet Bronze thành view để SQL khỏi phải nhúng đường dẫn file.
    tx_glob = (BRONZE_DIR / "raw_transactions" / "*.parquet").as_posix()
    merchants = (BRONZE_DIR / "raw_merchants" / "raw_merchants.parquet").as_posix()
    con.execute(f"CREATE VIEW bronze_transactions AS SELECT * FROM read_parquet('{tx_glob}')")
    con.execute(f"CREATE VIEW bronze_merchants AS SELECT * FROM read_parquet('{merchants}')")

    # Đầu báo cáo: tiêu đề + thời điểm sinh + nguồn, để file tự giải thích nó từ đâu ra.
    md = [
        "# Phân tích cơ bản trên Bronze (Day 4)",
        "",
        f"> Tự sinh bởi `src/analytics/run_basic_analytics.py` lúc "
        f"{datetime.now():%Y-%m-%d %H:%M:%S}.",
        "> Nguồn: `sql/basic_analytics.sql` chạy trực tiếp trên Bronze parquet bằng DuckDB.",
        "",
    ]

    statements = [s.strip() for s in SQL_FILE.read_text(encoding="utf-8").split(";") if s.strip()]
    for stmt in statements:
        kw = first_keyword(stmt)
        if kw in ("CREATE", "SET", "DROP"):
            con.execute(stmt)  # setup, không in
            continue
        if kw in ("SELECT", "WITH"):
            label = statement_label(stmt) or "query"
            df = con.execute(stmt).df()
            table = df.to_string(index=False)
            # In ra console (giữ nguyên hành vi cũ)...
            print(f"\n===== {label} =====")
            print(table)
            # ...và gom vào báo cáo: bảng đặt trong khối ``` để giữ canh cột, không phụ thuộc tabulate.
            md += [f"## {label}", "", f"*{len(df)} dòng*", "", "```", table, "```", ""]

    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    REPORT.write_text("\n".join(md), encoding="utf-8")
    print(f"\n>> Đã ghi báo cáo: {REPORT.relative_to(PROJECT_ROOT).as_posix()}")


if __name__ == "__main__":
    main()
