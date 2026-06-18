"""Streamlit UI cho Financial Transaction Data Lakehouse.

Gồm 3 phần (sidebar):
  - Tổng quan      : KPI + trạng thái verification + sơ đồ kiến trúc medallion.
  - Dashboard      : trực quan hoá 4 data mart (daily / customer_risk / merchant_risk / fraud_features).
  - Vận hành       : chạy từng bước pipeline bằng nút bấm, xem log realtime + trạng thái PASS/FAIL.

Chạy:  streamlit run app/streamlit_app.py
Dữ liệu đọc trực tiếp từ data/gold/gold.duckdb (read-only) và các file *_summary.json.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

# ---------------------------------------------------------------- paths ----
ROOT = Path(__file__).resolve().parents[1]
DUCKDB = ROOT / "data" / "gold" / "gold.duckdb"
MARTS_DIR = ROOT / "data" / "gold" / "marts"
LOG_DIR = ROOT / "logs"
SUMMARIES = {
    "generation": ROOT / "data" / "raw" / "generation_manifest.json",
    "quality": ROOT / "data" / "quality" / "quality_report.json",
    "quarantine": ROOT / "data" / "quarantine" / "quarantine_summary.json",
    "gold": ROOT / "data" / "gold" / "gold_summary.json",
    "marts": MARTS_DIR / "marts_summary.json",
}

st.set_page_config(page_title="Finance Lakehouse", page_icon="🏦", layout="wide")

PALETTE = ["#0b4f6c", "#1b7a8c", "#f4a259", "#e76f51", "#2a9d8f", "#9b5de5"]


# ------------------------------------------------------------- helpers -----
def load_json(path: Path) -> dict | None:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return None


@st.cache_data(show_spinner=False)
def q(sql: str) -> pd.DataFrame:
    """Chạy 1 truy vấn read-only trên gold.duckdb và trả về DataFrame (mở/đóng từng lần)."""
    con = duckdb.connect(str(DUCKDB), read_only=True)
    try:
        return con.execute(sql).fetchdf()
    finally:
        con.close()


@st.cache_data(show_spinner=False)
def has_gold() -> bool:
    if not DUCKDB.exists():
        return False
    try:
        con = duckdb.connect(str(DUCKDB), read_only=True)
        n = con.execute(
            "select count(*) from information_schema.tables "
            "where table_name='fact_transaction'"
        ).fetchone()[0]
        con.close()
        return n > 0
    except Exception:
        return False


def fmt_int(x) -> str:
    try:
        return f"{int(x):,}".replace(",", ".")
    except Exception:
        return "—"


def fmt_vnd(x) -> str:
    try:
        x = float(x)
    except Exception:
        return "—"
    if abs(x) >= 1e12:
        return f"{x/1e12:,.2f} nghìn tỷ ₫"
    if abs(x) >= 1e9:
        return f"{x/1e9:,.2f} tỷ ₫"
    if abs(x) >= 1e6:
        return f"{x/1e6:,.2f} triệu ₫"
    return f"{x:,.0f} ₫"


def verif_count(summary: dict | None) -> tuple[int, int]:
    """(passed, total) từ mảng 'verification' của summary JSON."""
    if not summary:
        return (0, 0)
    v = summary.get("verification", [])
    return (sum(1 for x in v if x.get("passed")), len(v))


def gold_amount(gold_summary: dict | None) -> float | None:
    if not gold_summary:
        return None
    for v in gold_summary.get("verification", []):
        if v.get("name") == "amount_vnd_sum_conserved":
            return v.get("detail", {}).get("fact")
    return None


# ------------------------------------------------------------- sidebar -----
st.sidebar.title("🏦 Finance Lakehouse")
st.sidebar.caption("Financial Transaction Data Lakehouse · UI")
page = st.sidebar.radio(
    "Điều hướng",
    ["🏠 Tổng quan", "📊 Dashboard phân tích", "⚙️ Vận hành pipeline"],
    label_visibility="collapsed",
)
st.sidebar.divider()
if st.sidebar.button("🔄 Làm mới dữ liệu", width="stretch"):
    st.cache_data.clear()
    st.rerun()
ok = has_gold()
st.sidebar.markdown(
    f"**Trạng thái Gold:** {'🟢 sẵn sàng' if ok else '🔴 chưa có dữ liệu'}"
)
gen = load_json(SUMMARIES["generation"])
if gen:
    st.sidebar.caption(f"Cửa sổ dữ liệu: {gen.get('date_start')} → {gen.get('date_end')}")


# ============================================================ TỔNG QUAN =====
def page_overview():
    st.title("Tổng quan dự án")
    st.caption(
        "Financial Transaction Data Lakehouse — kiến trúc medallion (Bronze → Silver → "
        "Gold → Marts) cho phân tích rủi ro & phát hiện gian lận."
    )

    gen = load_json(SUMMARIES["generation"])
    quality = load_json(SUMMARIES["quality"])
    gold = load_json(SUMMARIES["gold"])
    marts = load_json(SUMMARIES["marts"])

    # ---- KPI row ----
    total_txn = (gen or {}).get("transaction_rows")
    valid = (quality or {}).get("totals", {}).get("valid_rows")
    valid_rate = (quality or {}).get("totals", {}).get("valid_rate")
    fraud = (gen or {}).get("fraud_positive_rows")
    fraud_rate = (gen or {}).get("fraud_rate")
    amount = gold_amount(gold)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Tổng giao dịch (raw)", fmt_int(total_txn))
    c2.metric(
        "Giao dịch sạch → Gold",
        fmt_int(valid),
        f"{valid_rate*100:.2f}% hợp lệ" if valid_rate else None,
    )
    c3.metric(
        "Giao dịch gian lận",
        fmt_int(fraud),
        f"{fraud_rate*100:.2f}%" if fraud_rate else None,
    )
    c4.metric("Tổng giá trị (amount_vnd)", fmt_vnd(amount))

    st.divider()

    # ---- verification badges ----
    st.subheader("Trạng thái kiểm định (verification)")
    b1, b2, b3, b4 = st.columns(4)
    if quality:
        s = quality.get("summary", {})
        passed, total = s.get("passed", 0), s.get("total_checks", 0)
        b1.metric("Data Quality (Day 7)", f"{passed}/{total} PASS",
                  "✓ đạt" if quality.get("overall_passed") else "✗ lỗi")
    else:
        b1.metric("Data Quality (Day 7)", "—")
    gp, gt = verif_count(gold)
    b2.metric("Gold (Day 9)", f"{gp}/{gt} PASS" if gt else "—",
              "✓ đạt" if (gold or {}).get("overall_passed") else None)
    mp, mt = verif_count(marts)
    b3.metric("Marts (Day 10)", f"{mp}/{mt} PASS" if mt else "—",
              "✓ đạt" if (marts or {}).get("overall_passed") else None)
    qn = (load_json(SUMMARIES["quarantine"]) or {}).get("totals", {})
    b4.metric("Quarantine", fmt_int(qn.get("quarantine_rows")) + " dòng" if qn else "—",
              "bảo toàn ✓" if qn.get("conserved") else None)

    st.divider()

    # ---- architecture diagram ----
    st.subheader("Luồng dữ liệu (medallion)")
    dot = """
    digraph G {
      rankdir=LR; bgcolor="transparent";
      node [shape=box, style="rounded,filled", fontname="DejaVu Sans", color="#0b4f6c",
            fontcolor="white", margin="0.15,0.08"];
      raw   [label="RAW\\nCSV/JSON", fillcolor="#7a7a7a"];
      bronze[label="BRONZE\\nParquet thô", fillcolor="#a97142"];
      silver[label="SILVER\\ncast & flag", fillcolor="#9ca3af"];
      qa    [label="DATA QUALITY\\n22 checks", fillcolor="#1b7a8c"];
      quar  [label="QUARANTINE\\n4 bucket", fillcolor="#e76f51"];
      gold  [label="GOLD\\nstar schema", fillcolor="#c9a227"];
      marts [label="MARTS\\n4 mart", fillcolor="#2a9d8f"];
      raw->bronze->silver->qa->gold->marts;
      qa->quar [label="bad", fontcolor="#e76f51", color="#e76f51"];
    }
    """
    st.graphviz_chart(dot, width="stretch")

    if gold and gold.get("generated_at"):
        st.caption(f"Gold build gần nhất: {gold.get('generated_at')} · run_id={gold.get('run_id')}")


# ========================================================== DASHBOARD =======
def page_dashboard():
    st.title("Dashboard phân tích")
    if not ok:
        st.warning("Chưa có dữ liệu Gold. Hãy chạy pipeline ở tab **⚙️ Vận hành pipeline**.")
        return

    t1, t2, t3, t4 = st.tabs(
        ["📅 Giao dịch theo ngày", "👤 Rủi ro khách hàng", "🏪 Rủi ro merchant", "🚨 Đặc trưng gian lận"]
    )

    # ---- daily ----
    with t1:
        try:
            d = q("SELECT * FROM mart_daily_transaction ORDER BY date_key")
            d["full_date"] = pd.to_datetime(d["full_date"])
            lo, hi = d["full_date"].min().date(), d["full_date"].max().date()
            rng = st.slider("Khoảng ngày", lo, hi, (lo, hi), format="DD/MM/YY")
            m = d[(d["full_date"].dt.date >= rng[0]) & (d["full_date"].dt.date <= rng[1])]
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Số ngày", fmt_int(len(m)))
            k2.metric("Tổng giao dịch", fmt_int(m["txn_count"].sum()))
            k3.metric("Tổng giá trị", fmt_vnd(m["total_amount_vnd"].sum()))
            k4.metric("Gian lận", fmt_int(m["fraud_count"].sum()))
            fig = px.line(m, x="full_date", y="txn_count", title="Số giao dịch mỗi ngày",
                          color_discrete_sequence=PALETTE)
            fig.update_traces(fill="tozeroy")
            st.plotly_chart(fig, width="stretch")
            cA, cB = st.columns(2)
            cA.plotly_chart(
                px.line(m, x="full_date", y="total_amount_vnd", title="Giá trị giao dịch (VND)/ngày",
                        color_discrete_sequence=PALETTE), width="stretch")
            mm = m.melt(id_vars="full_date", value_vars=["success_count", "failed_count", "pending_count"],
                        var_name="status", value_name="count")
            cB.plotly_chart(
                px.bar(mm, x="full_date", y="count", color="status", title="Trạng thái giao dịch/ngày",
                       color_discrete_sequence=PALETTE), width="stretch")
            st.plotly_chart(
                px.bar(m, x="full_date", y="fraud_count", title="Số giao dịch gian lận/ngày",
                       color_discrete_sequence=["#e76f51"]), width="stretch")
        except Exception as e:
            st.error(f"Lỗi đọc mart_daily_transaction: {e}")

    # ---- customer risk ----
    with t2:
        try:
            c = q("SELECT * FROM mart_customer_risk")
            wl = int(c["watchlist_flag"].sum())
            k1, k2, k3 = st.columns(3)
            k1.metric("Khách hàng", fmt_int(len(c)))
            k2.metric("Vào watchlist", fmt_int(wl), f"{wl/len(c)*100:.2f}%")
            k3.metric("Có ≥1 giao dịch gian lận", fmt_int(int((c["fraud_count"] > 0).sum())))
            tier = st.multiselect("Lọc risk_tier", sorted(c["risk_tier"].dropna().unique()),
                                  default=sorted(c["risk_tier"].dropna().unique()))
            cf = c[c["risk_tier"].isin(tier)] if tier else c
            cc1, cc2 = st.columns(2)
            cc1.plotly_chart(
                px.histogram(cf, x="risk_tier", color="watchlist_flag", barmode="group",
                             title="Phân bố hạng rủi ro & watchlist",
                             color_discrete_sequence=PALETTE), width="stretch")
            samp = cf.sample(min(len(cf), 6000), random_state=1)
            cc2.plotly_chart(
                px.scatter(samp, x="txn_count", y="total_amount_vnd", color="watchlist_flag",
                           hover_data=["customer_id", "fraud_count", "max_amount_zscore"],
                           title="KH: số giao dịch vs tổng giá trị", opacity=0.5,
                           color_discrete_sequence=PALETTE), width="stretch")
            st.markdown("**Top 30 khách hàng rủi ro cao** (theo z-score chi tiêu & gian lận)")
            top = cf.sort_values(["watchlist_flag", "fraud_count", "max_amount_zscore"],
                                 ascending=False).head(30)
            st.dataframe(
                top[["customer_id", "risk_tier", "home_city", "txn_count", "total_amount_vnd",
                     "fraud_count", "fraud_rate", "failed_rate", "max_amount_zscore",
                     "distinct_devices", "distinct_locations", "watchlist_flag"]],
                width="stretch", hide_index=True)
        except Exception as e:
            st.error(f"Lỗi đọc mart_customer_risk: {e}")

    # ---- merchant risk ----
    with t3:
        try:
            mr = q("SELECT * FROM mart_merchant_risk")
            k1, k2, k3 = st.columns(3)
            k1.metric("Merchant", fmt_int(len(mr)))
            k2.metric("Tổng giá trị", fmt_vnd(mr["total_amount_vnd"].sum()))
            k3.metric("Tỷ lệ fraud TB", f"{mr['fraud_rate'].mean()*100:.3f}%")
            metric = st.selectbox("Xếp hạng theo", ["risk_score", "fraud_rate", "failed_rate",
                                                     "total_amount_vnd", "txn_count"])
            top = mr.sort_values(metric, ascending=False).head(20)
            st.plotly_chart(
                px.bar(top, x=metric, y="merchant_name", orientation="h",
                       color="category", title=f"Top 20 merchant theo {metric}",
                       color_discrete_sequence=PALETTE).update_yaxes(autorange="reversed"),
                width="stretch")
            cc1, cc2 = st.columns(2)
            cc1.plotly_chart(
                px.scatter(mr, x="txn_count", y="fraud_rate", size="total_amount_vnd",
                           color="category", hover_data=["merchant_name", "risk_score"],
                           title="Merchant: lưu lượng vs tỷ lệ gian lận", opacity=0.6),
                width="stretch")
            byc = mr.groupby("category", as_index=False).agg(
                merchants=("merchant_id", "count"), avg_fraud=("fraud_rate", "mean"),
                amount=("total_amount_vnd", "sum"))
            cc2.plotly_chart(
                px.bar(byc.sort_values("amount", ascending=False), x="category", y="amount",
                       title="Giá trị giao dịch theo ngành hàng",
                       color_discrete_sequence=PALETTE), width="stretch")
            st.dataframe(top, width="stretch", hide_index=True)
        except Exception as e:
            st.error(f"Lỗi đọc mart_merchant_risk: {e}")

    # ---- fraud features ----
    with t4:
        try:
            pat = q("""
                SELECT fraud_pattern,
                       count(*) AS n,
                       avg(amount_zscore) AS avg_zscore,
                       avg(txn_count_24h) AS avg_txn24h,
                       avg(velocity_risk_score) AS avg_velocity,
                       avg(failed_txn_count_24h) AS avg_failed24h,
                       avg(CAST(is_new_device AS DOUBLE)) AS rate_new_device,
                       avg(CAST(cross_country_flag AS DOUBLE)) AS rate_cross_country
                FROM mart_fraud_features GROUP BY 1 ORDER BY n DESC
            """)
            fr = q("SELECT fraud_pattern, count(*) n, sum(amount_vnd) amt "
                   "FROM fact_transaction WHERE is_fraud=1 GROUP BY 1 ORDER BY n DESC")
            k1, k2 = st.columns(2)
            k1.metric("Giao dịch gian lận", fmt_int(int(fr["n"].sum())))
            k2.metric("Giá trị gian lận", fmt_vnd(fr["amt"].sum()))
            cc1, cc2 = st.columns(2)
            cc1.plotly_chart(
                px.bar(fr, x="fraud_pattern", y="n", color="fraud_pattern",
                       title="Phân bố giao dịch gian lận theo pattern",
                       color_discrete_sequence=PALETTE), width="stretch")
            nolegit = pat[pat["fraud_pattern"] != "legit"]
            cc2.plotly_chart(
                px.bar(nolegit, x="fraud_pattern", y="avg_velocity", color="fraud_pattern",
                       title="velocity_risk_score trung bình theo pattern",
                       color_discrete_sequence=PALETTE), width="stretch")
            st.markdown("**Tách biệt đặc trưng (feature separation) theo pattern** — "
                        "minh chứng các feature phân biệt được fraud vs hợp lệ:")
            disp = pat.copy()
            for col in ["avg_zscore", "avg_txn24h", "avg_velocity", "avg_failed24h",
                        "rate_new_device", "rate_cross_country"]:
                disp[col] = disp[col].round(3)
            st.dataframe(disp, width="stretch", hide_index=True)
        except Exception as e:
            st.error(f"Lỗi đọc mart_fraud_features: {e}")


# =========================================================== VẬN HÀNH =======
STEPS = [
    ("bronze", "2 · Nạp Bronze", "src.load.load_bronze", "load_bronze.log", False),
    ("silver", "3 · Bronze → Silver", "src.transform.bronze_to_silver", "bronze_to_silver.log", False),
    ("quality", "4 · Data Quality (Day 7)", "src.quality.quality_checks", "quality_checks.log", False),
    ("quarantine", "5 · Quarantine (Day 8)", "src.quality.quarantine", "quarantine.log", False),
    ("gold", "6 · Build Gold (Day 9)", "src.transform.build_gold", "build_gold.log", False),
    ("marts", "7 · Build Marts (Day 10)", "src.marts.build_marts", "build_marts.log", False),
]
GENERATE = ("generate", "1 · Sinh dữ liệu (Generate)", "src.generate.generate_all", None, True)


def run_module(module: str, placeholder) -> int:
    """Chạy `python -m <module>` ở project root, stream stdout vào placeholder. Trả về returncode."""
    env = dict(os.environ)
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    proc = subprocess.Popen(
        [sys.executable, "-m", module], cwd=str(ROOT), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1, encoding="utf-8", errors="replace",
    )
    lines: list[str] = []
    for line in proc.stdout:
        lines.append(line.rstrip("\n"))
        placeholder.code("\n".join(lines[-500:]), language="log")
    proc.wait()
    return proc.returncode


def page_ops():
    st.title("Vận hành pipeline")
    st.caption("Chạy từng bước hoặc toàn bộ pipeline. Lệnh tương đương: "
               "`python -m src.<module>` tại thư mục dự án.")

    # ---- run all ----
    st.subheader("Chạy toàn bộ (Bronze → Marts)")
    st.caption("Bỏ qua bước Generate (dữ liệu raw đã có). Dừng ngay nếu một bước thất bại.")
    if st.button("▶️ Chạy toàn bộ pipeline", type="primary"):
        prog = st.progress(0.0, "Bắt đầu…")
        for i, (key, label, module, logf, _) in enumerate(STEPS):
            st.markdown(f"**{label}**")
            ph = st.empty()
            prog.progress(i / len(STEPS), f"Đang chạy: {label}")
            rc = run_module(module, ph)
            if rc == 0:
                st.success(f"✓ {label} — thành công")
            else:
                st.error(f"✗ {label} — thất bại (exit {rc}). Dừng pipeline.")
                break
        else:
            prog.progress(1.0, "Hoàn tất")
            st.balloons()
        st.cache_data.clear()

    st.divider()

    # ---- individual steps ----
    st.subheader("Chạy từng bước")
    for key, label, module, logf, danger in [GENERATE, *STEPS]:
        with st.expander(label, expanded=False):
            if danger:
                st.warning("⚠️ Bước này sinh lại ~2 triệu dòng và GHI ĐÈ `data/raw/` — "
                           "mất vài phút. Chỉ chạy khi muốn tạo lại dữ liệu từ đầu.")
                confirm = st.checkbox("Tôi hiểu và muốn sinh lại toàn bộ dữ liệu", key=f"cfm_{key}")
            else:
                confirm = True
            st.code(f"python -m {module}", language="bash")
            run = st.button(f"▶ Chạy: {label}", key=f"run_{key}", disabled=not confirm)
            if run:
                ph = st.empty()
                rc = run_module(module, ph)
                (st.success if rc == 0 else st.error)(
                    f"{'✓ thành công' if rc == 0 else f'✗ thất bại (exit {rc})'}")
                st.cache_data.clear()
            if logf and (LOG_DIR / logf).exists():
                with st.popover("📄 Xem log gần nhất"):
                    txt = (LOG_DIR / logf).read_text(encoding="utf-8", errors="replace")
                    st.code("\n".join(txt.splitlines()[-200:]), language="log")


# --------------------------------------------------------------- router ----
if page.startswith("🏠"):
    page_overview()
elif page.startswith("📊"):
    page_dashboard()
else:
    page_ops()
