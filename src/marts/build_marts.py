"""Build 4 core data marts from Gold star schema via DuckDB SQL."""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from src.paths import GOLD_DB, LOG_DIR, MARTS_OUT, MARTS_SQL

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_DIR / "build_marts.log", encoding="utf-8")],
)
log = logging.getLogger("marts")

MART_NAMES = ["mart_daily_transaction", "mart_customer_risk", "mart_merchant_risk", "mart_fraud_features"]

FACT_ROWS = 1_976_026
FRAUD_TOTAL = 14_000
MERCHANT_TXN = 680_673
FRAUD_FEATURES_ROWS = 1_973_027


def run_sql_file(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    """Execute all statements in a .sql file, stripping -- comments before splitting on ;."""
    lines = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if "--" in line:
            line = line[: line.index("--")]
        lines.append(line)
    for stmt in (s.strip() for s in "\n".join(lines).split(";")):
        if stmt:
            con.execute(stmt)


def export_parquet(con: duckdb.DuckDBPyConnection, mart: str) -> Path:
    out = MARTS_OUT / f"{mart}.parquet"
    con.execute(f"COPY {mart} TO '{out.as_posix()}' (FORMAT PARQUET)")
    return out


def verify(con: duckdb.DuckDBPyConnection) -> list[dict]:
    checks: list[dict] = []

    def add(name, passed, detail):
        checks.append({"name": name, "passed": bool(passed), "detail": detail})

    # daily
    d = con.execute("SELECT SUM(txn_count), COUNT(*), SUM(fraud_count) FROM mart_daily_transaction").fetchone()
    add("daily_sum_txn", d[0] == FACT_ROWS, {"sum": d[0], "expected": FACT_ROWS})
    add("daily_rowcount_180", d[1] == 180, {"rows": d[1]})
    add("daily_fraud", d[2] == FRAUD_TOTAL, {"sum": d[2]})

    # customer
    c = con.execute("SELECT SUM(txn_count), COUNT(*), SUM(fraud_count) FROM mart_customer_risk").fetchone()
    add("customer_sum_txn", c[0] == FACT_ROWS, {"sum": c[0], "expected": FACT_ROWS})
    add("customer_rowcount", c[1] == 19988, {"rows": c[1]})
    add("customer_fraud", c[2] == FRAUD_TOTAL, {"sum": c[2]})
    bad_rate = con.execute("SELECT COUNT(*) FROM mart_customer_risk WHERE fraud_rate < 0 OR fraud_rate > 1 OR failed_rate < 0 OR failed_rate > 1").fetchone()[0]
    add("customer_rate_bounds", bad_rate == 0, {"out_of_bounds": bad_rate})

    # merchant
    m = con.execute("SELECT COUNT(*), SUM(txn_count) FROM mart_merchant_risk").fetchone()
    add("merchant_rowcount_2000", m[0] == 2000, {"rows": m[0]})
    add("merchant_sum_txn", m[1] == MERCHANT_TXN, {"sum": m[1], "expected": MERCHANT_TXN})

    # fraud_features
    ff = con.execute("SELECT COUNT(*), SUM(is_fraud), COUNT(*) FILTER (WHERE amount_zscore IS NULL), COUNT(*) FILTER (WHERE txn_count_1h > txn_count_24h) FROM mart_fraud_features").fetchone()
    add("fraud_features_rowcount", ff[0] == FRAUD_FEATURES_ROWS, {"rows": ff[0], "expected": FRAUD_FEATURES_ROWS})
    add("fraud_features_is_fraud", ff[1] == FRAUD_TOTAL, {"sum": ff[1]})
    add("fraud_features_zscore_not_null", ff[2] == 0, {"null_zscore": ff[2]})
    add("fraud_features_count_monotonic", ff[3] == 0, {"violations_1h_gt_24h": ff[3]})
    uniq = con.execute("SELECT COUNT(*) FROM (SELECT transaction_id FROM mart_fraud_features GROUP BY transaction_id HAVING COUNT(*) > 1)").fetchone()[0]
    add("fraud_features_txn_unique", uniq == 0, {"dups": uniq})

    return checks


def feature_separation(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Validate feature 'sáng' theo fraud_pattern (để log/đọc, không gating)."""
    rows = con.execute("""
        SELECT COALESCE(fraud_pattern, 'legit') AS fp, COUNT(*) AS n,
               ROUND(AVG(amount_zscore), 2) AS z, ROUND(AVG(txn_count_24h), 1) AS c24,
               ROUND(AVG(is_new_device), 3) AS ndev, ROUND(AVG(cross_country_flag), 3) AS cc,
               ROUND(AVG(failed_txn_count_24h), 2) AS fail, ROUND(AVG(velocity_risk_score), 3) AS vrisk
        FROM mart_fraud_features GROUP BY 1 ORDER BY vrisk DESC
    """).fetchall()
    cols = ["fraud_pattern", "n", "avg_zscore", "avg_txn24h", "avg_is_new_device", "avg_cross_country", "avg_failed24h", "avg_velocity_risk"]
    return [dict(zip(cols, r)) for r in rows]


def main() -> None:
    run_id = "marts_" + datetime.now().strftime("%Y%m%d_%H%M%S")
    log.info("build marts start run_id=%s", run_id)
    MARTS_OUT.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(GOLD_DB))
    con.execute("PRAGMA threads=4")
    con.execute("SET preserve_insertion_order=false")

    rowcounts = {}
    for mart in MART_NAMES:
        run_sql_file(con, MARTS_SQL / f"{mart}.sql")
        n = con.execute(f"SELECT COUNT(*) FROM {mart}").fetchone()[0]
        export_parquet(con, mart)
        rowcounts[mart] = n
        log.info("built %-26s rows=%9d -> parquet", mart, n)

    checks = verify(con)
    sep = feature_separation(con)
    con.close()

    summary = {
        "run_id": run_id, "generated_at": datetime.now(timezone.utc).isoformat(),
        "rowcounts": rowcounts, "verification": checks, "feature_separation": sep,
        "overall_passed": all(c["passed"] for c in checks),
    }
    with (MARTS_OUT / "marts_summary.json").open("w", encoding="utf-8") as h:
        json.dump(summary, h, ensure_ascii=False, indent=2, default=str)

    for c in checks:
        (log.info if c["passed"] else log.error)("check %-32s %s %s", c["name"], "PASS" if c["passed"] else "FAIL", c["detail"])
    log.info("build marts done passed=%s", summary["overall_passed"])
    print(json.dumps({"overall_passed": summary["overall_passed"], "rowcounts": rowcounts,
                      "checks": [{c["name"]: c["passed"]} for c in checks]}, indent=2, default=str))
    raise SystemExit(0 if summary["overall_passed"] else 1)


if __name__ == "__main__":
    main()
