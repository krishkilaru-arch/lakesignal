"""Databricks App — FastAPI REST surface backed by Delta + serving endpoints."""
from __future__ import annotations

import hashlib
import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from app import config as cfg
from app.analyzer import analyze
from app.delta_store import execute, query
from app.resolver import reload_cache, resolve
from app.webhooks import (
    create_subscription,
    delete_subscription,
    dispatch,
    list_subscriptions,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(name)s :: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("newsimpact.app")

app = FastAPI(title="NewsImpact (Databricks)", version="0.1.0")


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

class AnalyzeRequest(BaseModel):
    headline: str
    body: str = ""
    tickers: Optional[List[str]] = None
    persist: bool = False


class WebhookCreate(BaseModel):
    url: str
    filters: Optional[dict] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    return {
        "status": "ok",
        "time": datetime.now(timezone.utc).isoformat(),
        "catalog": cfg.CATALOG,
        "schema": cfg.SCHEMA,
        "model": cfg.MODEL_ENDPOINT,
    }


# ---------------------------------------------------------------------------
# Tickers
# ---------------------------------------------------------------------------

@app.get("/tickers")
def list_tickers(q: Optional[str] = None, limit: int = Query(200, le=1000)):
    if q:
        like = f"%{q.lower()}%"
        rows = query(
            f"""
            SELECT symbol, company_name, sector, industry, exchange
            FROM {cfg.T_TICKERS}
            WHERE lower(symbol) LIKE %(like)s
               OR lower(company_name) LIKE %(like)s
               OR lower(aliases) LIKE %(like)s
            ORDER BY symbol
            LIMIT {int(limit)}
            """,
            {"like": like},
        )
    else:
        rows = query(
            f"SELECT symbol, company_name, sector, industry, exchange FROM {cfg.T_TICKERS} ORDER BY symbol LIMIT {int(limit)}"
        )
    return rows


# ---------------------------------------------------------------------------
# News
# ---------------------------------------------------------------------------

@app.get("/news")
def list_news(
    limit: int = Query(50, le=500),
    since: Optional[str] = None,
    source: Optional[str] = None,
):
    where = []
    params: dict = {}
    if since:
        where.append("ingested_at >= %(since)s")
        params["since"] = since
    if source:
        where.append("source = %(source)s")
        params["source"] = source
    sql_ = f"SELECT event_id, source, url, headline, body, published_at, ingested_at FROM {cfg.T_NEWS}"
    if where:
        sql_ += " WHERE " + " AND ".join(where)
    sql_ += f" ORDER BY ingested_at DESC LIMIT {int(limit)}"
    return query(sql_, params)


# ---------------------------------------------------------------------------
# Impacts
# ---------------------------------------------------------------------------

@app.get("/impacts")
def list_impacts(
    ticker: Optional[str] = None,
    direction: Optional[str] = None,
    min_magnitude: Optional[int] = None,
    since: Optional[str] = None,
    limit: int = Query(100, le=1000),
):
    where = []
    params: dict = {}
    if ticker:
        where.append("ia.ticker_symbol = %(ticker)s")
        params["ticker"] = ticker.upper()
    if direction:
        where.append("ia.direction = %(direction)s")
        params["direction"] = direction
    if min_magnitude is not None:
        where.append("ia.magnitude >= %(min_mag)s")
        params["min_mag"] = int(min_magnitude)
    if since:
        where.append("ia.analyzed_at >= %(since)s")
        params["since"] = since

    sql_ = f"""
        SELECT ia.impact_id, ia.event_id, ia.ticker_symbol AS ticker, ia.direction,
               ia.sentiment_score, ia.magnitude, ia.predicted_move_pct_1d,
               ia.predicted_move_pct_5d, ia.confidence, ia.risk_tags, ia.rationale,
               ia.analyzed_at, ia.model_version,
               ne.headline, ne.url, ne.source, ne.published_at
        FROM {cfg.T_IMPACT} ia
        JOIN {cfg.T_NEWS} ne ON ne.event_id = ia.event_id
    """
    if where:
        sql_ += " WHERE " + " AND ".join(where)
    sql_ += f" ORDER BY ia.analyzed_at DESC LIMIT {int(limit)}"
    return query(sql_, params)


@app.get("/impacts/{impact_id}")
def get_impact(impact_id: str):
    rows = query(
        f"""
        SELECT ia.*, ne.headline, ne.url, ne.source, ne.published_at
        FROM {cfg.T_IMPACT} ia
        JOIN {cfg.T_NEWS} ne ON ne.event_id = ia.event_id
        WHERE ia.impact_id = %(id)s
        """,
        {"id": impact_id},
    )
    if not rows:
        raise HTTPException(status_code=404, detail="impact not found")
    return rows[0]


# ---------------------------------------------------------------------------
# Ad-hoc analyze
# ---------------------------------------------------------------------------

@app.post("/analyze")
def analyze_adhoc(req: AnalyzeRequest):
    text = f"{req.headline} {req.body}"
    tickers = [t.upper() for t in (req.tickers or [])] or resolve(text)
    if not tickers:
        return {"tickers": [], "impacts": {}, "note": "no tickers resolved"}

    scores = analyze(req.headline, req.body, tickers)
    response: dict = {"tickers": tickers, "impacts": scores}

    if not req.persist or not scores:
        return response

    # Persist a synthetic news event + impact rows.
    now = datetime.now(timezone.utc).isoformat()
    content_hash = hashlib.sha1(f"adhoc|{req.headline}|{now}".encode()).hexdigest()
    event_id = f"adhoc:{content_hash[:16]}"

    # MERGE into news_events to avoid duplicates when a client retries.
    execute(
        f"""
        MERGE INTO {cfg.T_NEWS} t
        USING (
            SELECT
                %(event_id)s        AS event_id,
                'adhoc'             AS source,
                ''                  AS url,
                %(headline)s        AS headline,
                %(body)s            AS body,
                CAST(%(now)s AS TIMESTAMP) AS published_at,
                CAST(%(now)s AS TIMESTAMP) AS ingested_at,
                %(content_hash)s    AS content_hash
        ) s
        ON t.event_id = s.event_id
        WHEN NOT MATCHED THEN INSERT *
        """,
        {
            "event_id": event_id,
            "headline": req.headline,
            "body": req.body,
            "now": now,
            "content_hash": content_hash,
        },
    )

    persisted = []
    for sym, s in scores.items():
        impact_id = str(uuid.uuid4())
        execute(
            f"""
            MERGE INTO {cfg.T_IMPACT} t
            USING (
                SELECT
                    %(impact_id)s             AS impact_id,
                    %(event_id)s              AS event_id,
                    %(ticker)s                AS ticker_symbol,
                    %(direction)s             AS direction,
                    %(sentiment_score)s       AS sentiment_score,
                    %(magnitude)s             AS magnitude,
                    %(move_1d)s               AS predicted_move_pct_1d,
                    %(move_5d)s               AS predicted_move_pct_5d,
                    %(confidence)s            AS confidence,
                    from_json(%(risk_tags)s, 'ARRAY<STRING>') AS risk_tags,
                    %(rationale)s             AS rationale,
                    CAST(%(analyzed_at)s AS TIMESTAMP) AS analyzed_at,
                    %(model_version)s         AS model_version
            ) s
            ON t.event_id = s.event_id AND t.ticker_symbol = s.ticker_symbol
            WHEN NOT MATCHED THEN INSERT *
            """,
            {
                "impact_id": impact_id,
                "event_id": event_id,
                "ticker": sym,
                "direction": s["direction"],
                "sentiment_score": s["sentiment_score"],
                "magnitude": s["magnitude"],
                "move_1d": s["predicted_move_pct_1d"],
                "move_5d": s["predicted_move_pct_5d"],
                "confidence": s["confidence"],
                "risk_tags": json.dumps(s["risk_tags"]),
                "rationale": s["rationale"],
                "analyzed_at": now,
                "model_version": cfg.MODEL_VERSION,
            },
        )
        persisted.append(
            {
                "impact_id": impact_id,
                "event_id": event_id,
                "ticker": sym,
                "direction": s["direction"],
                "sentiment_score": s["sentiment_score"],
                "magnitude": s["magnitude"],
                "predicted_move_pct_1d": s["predicted_move_pct_1d"],
                "predicted_move_pct_5d": s["predicted_move_pct_5d"],
                "confidence": s["confidence"],
                "risk_tags": s["risk_tags"],
                "rationale": s["rationale"],
                "headline": req.headline,
                "url": "",
                "published_at": now,
                "analyzed_at": now,
            }
        )

    try:
        dispatch(persisted)
    except Exception as e:  # noqa: BLE001
        log.warning("Webhook dispatch failed: %s", e)

    response["event_id"] = event_id
    response["persisted_impacts"] = persisted
    return response


# ---------------------------------------------------------------------------
# Admin
# ---------------------------------------------------------------------------

@app.post("/admin/reload_tickers")
def admin_reload():
    reload_cache()
    return {"status": "reloaded"}


# ---------------------------------------------------------------------------
# Webhooks
# ---------------------------------------------------------------------------

@app.post("/webhooks")
def webhook_create(body: WebhookCreate):
    return create_subscription(body.url, body.filters)


@app.get("/webhooks")
def webhook_list():
    return list_subscriptions(include_secret=False)


@app.delete("/webhooks/{sub_id}")
def webhook_delete(sub_id: str):
    if not delete_subscription(sub_id):
        raise HTTPException(status_code=404, detail="subscription not found")
    return {"status": "deleted", "id": sub_id}


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

WEB_DIR = Path(__file__).resolve().parent / "web"
if WEB_DIR.exists():
    app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")

    @app.get("/", include_in_schema=False)
    def dashboard():
        return FileResponse(WEB_DIR / "index.html")
