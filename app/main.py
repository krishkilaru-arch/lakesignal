"""Databricks App — FastAPI REST surface backed by Delta + serving endpoints."""
from __future__ import annotations

import hashlib
import json
import logging
import traceback as tb_mod
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from starlette.requests import Request

import config as cfg
from analyzer import analyze
from delta_store import execute, query
from resolver import reload_cache, resolve, _get_maps as resolver_maps
from webhooks import (
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
log = logging.getLogger("lakesignal.app")

app = FastAPI(title="LakeSignal (Databricks)", version="0.1.0")


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    log.error("Unhandled error on %s: %s", request.url.path, exc, exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": str(exc),
            "type": type(exc).__name__,
            "traceback": tb_mod.format_exc(),
            "path": str(request.url),
        },
    )


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


class AddTickerRequest(BaseModel):
    symbol: str
    company_name: str = ""
    sector: str = ""
    industry: str = ""
    exchange: str = ""
    aliases: str = ""


class AnalyzeUrlRequest(BaseModel):
    url: str
    persist: bool = True


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
# Debug — diagnose DB connectivity
# ---------------------------------------------------------------------------

@app.get("/debug")
def debug():
    import traceback
    info = {
        "host": cfg.DATABRICKS_HOST,
        "http_path": cfg.DATABRICKS_WAREHOUSE_HTTP_PATH,
        "catalog": cfg.CATALOG,
        "schema": cfg.SCHEMA,
    }
    try:
        rows = query(f"SELECT count(*) as cnt FROM {cfg.T_IMPACT}")
        info["query_result"] = rows
        info["status"] = "ok"
    except Exception as e:
        info["status"] = "error"
        info["error"] = str(e)
        info["traceback"] = traceback.format_exc()
    return info


# ---------------------------------------------------------------------------
# Debug — test resolver
# ---------------------------------------------------------------------------

@app.get("/debug/resolve")
def debug_resolve(text: str = Query("Palantir PLTR stock falls")):
    """Test the resolver with arbitrary text."""
    import traceback
    try:
        by_symbol, pairs = resolver_maps()
        tickers_found = resolve(text)
        return {
            "input_text": text,
            "resolved_tickers": tickers_found,
            "cache_size": len(by_symbol),
            "alias_pairs_count": len(pairs),
            "sample_symbols": list(by_symbol.keys())[:20],
            "status": "ok",
        }
    except Exception as e:
        return {
            "input_text": text,
            "error": str(e),
            "traceback": traceback.format_exc(),
            "status": "error",
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

    if not scores:
        response["note"] = f"Found tickers {tickers} but AI scoring returned empty. Check model endpoint."
        return response
    if not req.persist:
        return response

    now = datetime.now(timezone.utc).isoformat()
    content_hash = hashlib.sha1(f"adhoc|{req.headline}|{now}".encode()).hexdigest()
    event_id = f"adhoc:{content_hash[:16]}"

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
# Add Custom Ticker
# ---------------------------------------------------------------------------

@app.post("/tickers")
def add_ticker(req: AddTickerRequest):
    """Add a custom ticker to the universe (e.g. non-S&P stocks)."""
    sym = req.symbol.strip().upper()
    if not sym:
        raise HTTPException(status_code=400, detail="symbol is required")

    execute(
        f"""
        MERGE INTO {cfg.T_TICKERS} t
        USING (
            SELECT
                %(symbol)s       AS symbol,
                %(company_name)s AS company_name,
                %(sector)s       AS sector,
                %(industry)s     AS industry,
                %(exchange)s     AS exchange,
                %(aliases)s      AS aliases
        ) s
        ON t.symbol = s.symbol
        WHEN MATCHED THEN UPDATE SET
            company_name = s.company_name,
            sector = s.sector,
            industry = s.industry,
            exchange = s.exchange,
            aliases = s.aliases
        WHEN NOT MATCHED THEN INSERT *
        """,
        {
            "symbol": sym,
            "company_name": req.company_name.strip() or sym,
            "sector": req.sector.strip(),
            "industry": req.industry.strip(),
            "exchange": req.exchange.strip(),
            "aliases": req.aliases.strip(),
        },
    )

    # Reload ticker cache so resolver picks up the new ticker
    try:
        reload_cache()
    except Exception:
        pass

    return {"status": "ok", "symbol": sym, "message": f"{sym} added to ticker universe"}


# ---------------------------------------------------------------------------
# Analyze URL — paste any news link
# ---------------------------------------------------------------------------

@app.post("/analyze/url")
def analyze_url(req: AnalyzeUrlRequest):
    """Fetch a news article by URL, resolve tickers, score impacts, and persist."""
    import re
    import requests as http_req
    from bs4 import BeautifulSoup

    url = req.url.strip()
    if not url:
        raise HTTPException(status_code=400, detail="url is required")

    # 1. Fetch the article
    try:
        resp = http_req.get(url, headers={"User-Agent": "Mozilla/5.0 (LakeSignal)"}, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to fetch URL: {e}")

    # 2. Extract headline and body text
    soup = BeautifulSoup(resp.text, "html.parser")
    headline = ""
    # Priority order: og:title > article-specific h1 > title > generic h1
    for sel in [
        "meta[property='og:title']",
        "meta[name='title']",
        ".article-title", ".post-title", ".entry-title", ".headline",
        "h1.title", "h1.headline",
        "title",
        "h1",
    ]:
        tag = soup.select_one(sel)
        if tag:
            val = tag.get("content", "") if tag.name == "meta" else tag.get_text(strip=True)
            # Skip if it's just the site name (< 20 chars and no spaces)
            if val and (len(val) > 20 or " " in val):
                headline = val
                break
    # Final fallback
    if not headline:
        tag = soup.select_one("h1") or soup.select_one("title")
        if tag:
            headline = tag.get("content", "") if tag.name == "meta" else tag.get_text(strip=True)
    if not headline:
        headline = url.split("/")[-1].replace("-", " ")[:200]
    # Clean up: remove trailing " - SiteName" pattern
    headline = headline.split(" | ")[0].strip()
    if " - " in headline:
        parts = headline.rsplit(" - ", 1)
        if len(parts[1]) < 30:
            headline = parts[0].strip()
    if not headline:
        headline = url.split("/")[-1].replace("-", " ")[:200]

    body = ""
    for sel in ["article", "[role='main']", ".article-body", ".story-body", "main", ".post-content"]:
        tag = soup.select_one(sel)
        if tag:
            body = tag.get_text(separator=" ", strip=True)[:3000]
            break
    if not body:
        paragraphs = soup.find_all("p")
        body = " ".join(p.get_text(strip=True) for p in paragraphs[:30])[:3000]

    text = f"{headline} {body}"
    log.info("URL fetch OK: headline=%r body_len=%d", headline[:60], len(body))

    # 3. Resolve tickers — fully inline, no resolver module dependency
    tickers = []

    # 3a. Extract uppercase symbol candidates from text
    candidates = list(set(re.findall(r'(?<![A-Za-z])([A-Z]{2,5})(?![A-Za-z])', text)))
    log.info("Regex candidates: %s", candidates[:20])

    # 3b. Match candidates against tickers table via direct SQL
    if candidates:
        safe = [c for c in candidates if c.isalpha() and 2 <= len(c) <= 5]
        if safe:
            in_list = ", ".join(f"'{s}'" for s in safe[:50])
            sql = f"SELECT symbol FROM {cfg.T_TICKERS} WHERE symbol IN ({in_list})"
            log.info("Running SQL: %s", sql[:200])
            matched = query(sql)
            tickers = [r["symbol"] for r in matched]
            log.info("SQL matched tickers: %s", tickers)

    # 3c. Also match company names from headline words
    if not tickers:
        for word in headline.split():
            if len(word) >= 4 and word[0].isupper() and word.isalpha():
                sql = f"SELECT symbol FROM {cfg.T_TICKERS} WHERE LOWER(company_name) LIKE '%%{word.lower()}%%' LIMIT 3"
                try:
                    hits = query(sql)
                    tickers += [r["symbol"] for r in hits]
                except Exception as ex:
                    log.warning("Name match query failed for %r: %s", word, ex)
        tickers = list(dict.fromkeys(tickers))

    log.info("Final resolved tickers: %s", tickers)

    if not tickers:
        return {
            "url": url,
            "headline": headline,
            "tickers": [],
            "impacts": {},
            "note": f"No tickers found. [extracted {len(candidates)} candidates, headline='{headline[:50]}', body={len(body)} chars]",
        }

    # 4. Score impacts via LLM
    try:
        scores = analyze(headline, body, tickers)
    except Exception as score_err:
        log.error("analyze() raised: %s", score_err, exc_info=True)
        return {
            "url": url,
            "headline": headline,
            "tickers": tickers,
            "impacts": {},
            "note": f"Found tickers {tickers} but scoring failed: {type(score_err).__name__}: {score_err}",
        }
    response = {
        "url": url,
        "headline": headline,
        "body_length": len(body),
        "tickers": tickers,
        "impacts": scores,
    }

    if not scores:
        response["note"] = f"Found tickers {tickers} but AI scoring returned empty. Check model endpoint."
        return response
    if not req.persist:
        return response

    # 5. Persist to news_events
    now = datetime.now(timezone.utc).isoformat()
    content_hash = hashlib.sha1(f"url|{url}|{now}".encode()).hexdigest()
    event_id = f"url:{content_hash[:16]}"

    execute(
        f"""
        MERGE INTO {cfg.T_NEWS} t
        USING (
            SELECT
                %(event_id)s        AS event_id,
                'user_url'      AS source,
                %(url)s             AS url,
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
            "headline": headline[:1000],
            "body": body[:5000],
            "url": url,
            "now": now,
            "content_hash": content_hash,
        },
    )

    # 6. Persist impacts
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
        persisted.append({
            "impact_id": impact_id,
            "ticker": sym,
            "direction": s["direction"],
            "magnitude": s["magnitude"],
            "confidence": s["confidence"],
        })

    response["event_id"] = event_id
    response["persisted"] = persisted
    response["source"] = "user_url"
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
# Backtest API
# ---------------------------------------------------------------------------

T_BACKTEST = f"{cfg.CATALOG}.{cfg.SCHEMA}.backtest_results"


@app.get("/backtest/results")
def backtest_results(
    event_date: Optional[str] = None,
    ticker: Optional[str] = None,
    limit: int = Query(500, le=2000),
):
    """Return backtest results — predictions vs actual prices."""
    where = []
    params: dict = {}
    if event_date:
        where.append("event_date = %(event_date)s")
        params["event_date"] = event_date
    if ticker:
        where.append("ticker = %(ticker)s")
        params["ticker"] = ticker.upper()

    sql_ = f"""
        SELECT backtest_id, impact_id, ticker, headline,
               direction_predicted, magnitude_predicted,
               predicted_move_1d, predicted_move_5d, confidence,
               event_date, price_date_t0, price_date_t1, price_date_t5,
               actual_close_t0, actual_close_t1, actual_close_t5,
               actual_move_1d_pct, actual_move_5d_pct,
               direction_correct_1d, direction_correct_5d,
               magnitude_error_1d, magnitude_error_5d,
               scored_at
        FROM {T_BACKTEST}
    """
    if where:
        sql_ += " WHERE " + " AND ".join(where)
    sql_ += f" ORDER BY scored_at DESC LIMIT {int(limit)}"
    return query(sql_, params)


@app.get("/backtest/summary")
def backtest_summary():
    """Return per-date accuracy summary for the last 30 days."""
    sql_ = f"""
        SELECT event_date,
               COUNT(*) as total_predictions,
               SUM(CASE WHEN direction_correct_1d IS NOT NULL THEN 1 ELSE 0 END) as scored,
               SUM(CASE WHEN direction_correct_1d = true THEN 1 ELSE 0 END) as correct_1d,
               SUM(CASE WHEN direction_correct_1d = false THEN 1 ELSE 0 END) as wrong_1d,
               ROUND(AVG(CASE WHEN direction_correct_1d IS NOT NULL
                   THEN CAST(direction_correct_1d AS INT) END) * 100, 1) as accuracy_pct,
               ROUND(AVG(magnitude_error_1d), 2) as avg_mag_error,
               COUNT(DISTINCT ticker) as tickers
        FROM {T_BACKTEST}
        GROUP BY event_date
        ORDER BY event_date DESC
        LIMIT 30
    """
    return query(sql_)


@app.get("/backtest/dates")
def backtest_dates():
    """Return list of dates that have backtest data."""
    sql_ = f"SELECT DISTINCT event_date FROM {T_BACKTEST} ORDER BY event_date DESC LIMIT 60"
    return query(sql_)


# ---------------------------------------------------------------------------
# Pages & Static Files
# ---------------------------------------------------------------------------

WEB_DIR = Path(__file__).resolve().parent / "web"
if WEB_DIR.exists():
    app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")

    @app.get("/", include_in_schema=False)
    def dashboard():
        return FileResponse(WEB_DIR / "index.html")

    @app.get("/about", include_in_schema=False)
    def about_page():
        return FileResponse(WEB_DIR / "about.html")

    @app.get("/backtest", include_in_schema=False)
    def backtest_page():
        return FileResponse(WEB_DIR / "backtest.html")
