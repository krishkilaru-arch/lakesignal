"""Database helper — connects to Unity Catalog via databricks-sql-connector.

Auth uses a service principal (client_id + client_secret) via the Databricks SDK.
Credentials are read from Streamlit secrets (.streamlit/secrets.toml).
"""
from __future__ import annotations

import json
import streamlit as st
from databricks import sql as dbsql
from databricks.sdk.core import Config


CATALOG = "lakesignal"
SCHEMA = "core"

T_TICKERS = f"{CATALOG}.{SCHEMA}.tickers"
T_NEWS = f"{CATALOG}.{SCHEMA}.news_events"
T_IMPACT = f"{CATALOG}.{SCHEMA}.impact_analysis"
T_BACKTEST = f"{CATALOG}.{SCHEMA}.backtest_results"


def _cfg() -> Config:
    """Build a Databricks SDK Config from Streamlit secrets."""
    s = st.secrets["databricks"]
    return Config(
        host=s["host"],
        client_id=s["client_id"],
        client_secret=s["client_secret"],
    )


@st.cache_resource(ttl=300)
def _connection():
    """Cached SQL connection (refreshes every 5 min)."""
    s = st.secrets["databricks"]
    cfg = _cfg()
    return dbsql.connect(
        server_hostname=s["host"].replace("https://", ""),
        http_path=s["http_path"],
        credentials_provider=lambda: cfg.authenticate,
    )


def query(sql: str, params: dict | None = None) -> list[dict]:
    """Run a SQL query and return rows as list of dicts."""
    conn = _connection()
    with conn.cursor() as cur:
        cur.execute(sql, params or {})
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return [_safe_row(dict(zip(cols, r))) for r in rows]


def _safe_row(row: dict) -> dict:
    """Convert non-JSON-safe types (ARRAY, etc.) to Python lists."""
    for k, v in row.items():
        if v is not None and not isinstance(v, (str, int, float, bool, list, dict)):
            try:
                row[k] = list(v)
            except (TypeError, ValueError):
                row[k] = str(v)
    return row


# ── Query helpers ─────────────────────────────────────────────────────────

def get_impacts(
    ticker: str | None = None,
    direction: str | None = None,
    min_magnitude: int | None = None,
    since: str | None = None,
    search: str | None = None,
    limit: int = 200,
) -> list[dict]:
    wheres = []
    if ticker:
        wheres.append(f"ia.ticker_symbol = '{ticker}'")
    if direction:
        wheres.append(f"ia.direction = '{direction}'")
    if min_magnitude:
        wheres.append(f"ia.magnitude >= {int(min_magnitude)}")
    if since:
        wheres.append(f"ia.analyzed_at >= '{since}'")
    if search:
        safe = search.replace("'", "''")
        wheres.append(f"LOWER(ne.headline) LIKE '%{safe.lower()}%'")
    where = "WHERE " + " AND ".join(wheres) if wheres else ""
    return query(f"""
        SELECT ia.impact_id, ia.ticker_symbol, ia.direction, ia.sentiment_score,
               ia.magnitude, ia.predicted_move_pct_1d, ia.predicted_move_pct_5d,
               ia.confidence, ia.risk_tags, ia.rationale, ia.analyzed_at,
               ne.headline, ne.source, ne.url
        FROM {T_IMPACT} ia
        JOIN {T_NEWS} ne ON ne.event_id = ia.event_id
        {where}
        ORDER BY ia.analyzed_at DESC
        LIMIT {limit}
    """)


def get_tickers(q: str | None = None, limit: int = 500) -> list[dict]:
    where = ""
    if q:
        safe = q.replace("'", "''").lower()
        where = f"WHERE LOWER(symbol) LIKE '%{safe}%' OR LOWER(company_name) LIKE '%{safe}%'"
    return query(f"SELECT * FROM {T_TICKERS} {where} ORDER BY symbol LIMIT {limit}")


def get_ticker_symbols() -> list[str]:
    rows = query(f"SELECT DISTINCT symbol FROM {T_TICKERS} ORDER BY symbol")
    return [r["symbol"] for r in rows]


def get_backtest_dates() -> list[str]:
    rows = query(f"SELECT DISTINCT event_date FROM {T_BACKTEST} ORDER BY event_date DESC")
    return [str(r["event_date"]) for r in rows]


def get_backtest_results(event_date: str | None = None, limit: int = 500) -> list[dict]:
    where = f"WHERE event_date = '{event_date}'" if event_date else ""
    return query(f"SELECT * FROM {T_BACKTEST} {where} ORDER BY ticker, event_date DESC LIMIT {limit}")


def get_backtest_summary() -> dict:
    rows = query(f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN direction_correct_1d = true THEN 1 ELSE 0 END) AS correct,
            SUM(CASE WHEN direction_correct_1d = false THEN 1 ELSE 0 END) AS wrong,
            SUM(CASE WHEN direction_correct_1d IS NULL THEN 1 ELSE 0 END) AS pending,
            AVG(magnitude_error_1d) AS avg_mag_error
        FROM {T_BACKTEST}
    """)
    return rows[0] if rows else {}


def add_ticker(symbol: str, company_name: str = "", sector: str = "",
               industry: str = "", exchange: str = "") -> None:
    conn = _connection()
    with conn.cursor() as cur:
        cur.execute(f"""
            MERGE INTO {T_TICKERS} t
            USING (SELECT '{symbol}' AS symbol) s ON t.symbol = s.symbol
            WHEN NOT MATCHED THEN INSERT (symbol, company_name, sector, industry, exchange, aliases)
            VALUES ('{symbol}', '{company_name}', '{sector}', '{industry}', '{exchange}', '')
        """)


def get_impact_stats() -> dict:
    rows = query(f"""
        SELECT
            COUNT(*) AS total,
            AVG(magnitude) AS avg_magnitude,
            SUM(CASE WHEN direction = 'positive' THEN 1 ELSE 0 END) AS positive,
            SUM(CASE WHEN direction = 'negative' THEN 1 ELSE 0 END) AS negative,
            SUM(CASE WHEN direction = 'neutral' THEN 1 ELSE 0 END) AS neutral
        FROM {T_IMPACT}
    """)
    return rows[0] if rows else {}
