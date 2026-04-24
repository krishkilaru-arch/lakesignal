"""URL analysis & LLM scoring for the Streamlit app.

Fetches a URL, extracts headline + body, resolves tickers via SQL,
and scores via Databricks Foundation Model API.
"""
from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import datetime, timezone

import requests
import streamlit as st
from bs4 import BeautifulSoup
from databricks.sdk import WorkspaceClient

import db


SYSTEM_PROMPT = """You are a sell-side equity analyst. Given one news story and a
list of potentially-impacted tickers, score the likely short-term stock impact for
each ticker. Be calibrated, not dramatic: most stories are low-magnitude.

Return ONLY valid JSON matching this schema (no prose, no markdown):

{
  "impacts": [
    {
      "ticker": "AAPL",
      "direction": "positive" | "negative" | "neutral",
      "sentiment_score": -1.0 to 1.0,
      "magnitude": integer 1-10,
      "predicted_move_pct_1d": number (typical range -6 to 6),
      "predicted_move_pct_5d": number (typical range -10 to 10),
      "confidence": 0.0 to 1.0,
      "risk_tags": [ "earnings" | "m&a" | "regulatory" | "litigation" | "supply_chain" | "cyber" | "management" | "macro" | "product" | "labor" | "geopolitical" ],
      "rationale": "one sentence, <=160 chars"
    }
  ]
}
"""


def _get_openai_client():
    """Create an OpenAI-compatible client via the Databricks SDK."""
    s = st.secrets["databricks"]
    wc = WorkspaceClient(
        host=s["host"],
        client_id=s["client_id"],
        client_secret=s["client_secret"],
    )
    return wc.serving_endpoints.get_open_ai_client()


def fetch_article(url: str) -> tuple[str, str]:
    """Fetch a URL and extract headline + body text."""
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (LakeSignal)"}, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    # Extract headline: prioritize og:title over title over h1
    headline = ""
    for sel in ["meta[property='og:title']", "meta[name='title']",
                ".article-title", ".post-title", ".entry-title", ".headline",
                "title", "h1"]:
        tag = soup.select_one(sel)
        if tag:
            val = tag.get("content", "") if tag.name == "meta" else tag.get_text(strip=True)
            if val and (len(val) > 20 or " " in val):
                headline = val
                break
    if not headline:
        headline = url.split("/")[-1].replace("-", " ")[:200]
    # Strip trailing site name
    if " - " in headline:
        parts = headline.rsplit(" - ", 1)
        if len(parts[1]) < 30:
            headline = parts[0].strip()

    # Extract body
    body = ""
    for sel in ["article", "[role='main']", ".article-body", ".story-body", "main", ".post-content"]:
        tag = soup.select_one(sel)
        if tag:
            body = tag.get_text(separator=" ", strip=True)[:3000]
            break
    if not body:
        paragraphs = soup.find_all("p")
        body = " ".join(p.get_text(strip=True) for p in paragraphs[:30])[:3000]

    return headline, body


def resolve_tickers(text: str) -> list[str]:
    """Extract ticker candidates from text and match against the DB."""
    candidates = list(set(re.findall(r'(?<![A-Za-z])([A-Z]{2,5})(?![A-Za-z])', text)))
    if not candidates:
        return []
    safe = [c for c in candidates if c.isalpha() and 2 <= len(c) <= 5]
    if not safe:
        return []
    in_list = ", ".join(f"'{s}'" for s in safe[:50])
    rows = db.query(f"SELECT symbol FROM {db.T_TICKERS} WHERE symbol IN ({in_list})")
    return [r["symbol"] for r in rows]


def score_impacts(headline: str, body: str, tickers: list[str]) -> dict:
    """Call the LLM to score impacts for each ticker."""
    if not tickers:
        return {}

    # Get ticker details for context
    in_list = ", ".join(f"'{t}'" for t in tickers)
    details = db.query(f"SELECT symbol, company_name, sector FROM {db.T_TICKERS} WHERE symbol IN ({in_list})")
    detail_map = {r["symbol"]: r for r in details}

    ticker_lines = []
    for t in tickers:
        d = detail_map.get(t, {})
        ticker_lines.append(f"- {t}: {d.get('company_name', '')} ({d.get('sector', '')})")

    user = (
        f"Headline: {headline}\n\n"
        f"Body: {(body or '')[:2000]}\n\n"
        f"Tickers to score:\n" + "\n".join(ticker_lines) + "\n\n"
        "Output the JSON now."
    )

    model = st.secrets["databricks"].get("model_endpoint", "databricks-claude-sonnet-4")
    client = _get_openai_client()
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user},
        ],
        max_tokens=1024,
        temperature=0.2,
    )

    text = resp.choices[0].message.content or "{}"
    text = re.sub(r"^```(?:json)?\s*", "", text.strip())
    text = re.sub(r"\s*```$", "", text.strip())
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", text, re.DOTALL)
        parsed = json.loads(m.group(0)) if m else {"impacts": []}

    out = {}
    for entry in parsed.get("impacts", []):
        tk = str(entry.get("ticker", "")).upper()
        if tk not in tickers:
            continue
        try:
            out[tk] = {
                "direction": entry["direction"],
                "sentiment_score": float(entry["sentiment_score"]),
                "magnitude": int(entry["magnitude"]),
                "predicted_move_pct_1d": float(entry.get("predicted_move_pct_1d", 0.0)),
                "predicted_move_pct_5d": float(entry.get("predicted_move_pct_5d", 0.0)),
                "confidence": float(entry["confidence"]),
                "risk_tags": list(entry.get("risk_tags") or []),
                "rationale": str(entry.get("rationale", ""))[:160],
            }
        except (KeyError, ValueError, TypeError):
            pass
    return out


def analyze_url(url: str, persist: bool = True) -> dict:
    """Full pipeline: fetch URL → resolve tickers → score → optionally persist."""
    headline, body = fetch_article(url)
    text = f"{headline} {body}"
    tickers = resolve_tickers(text)

    if not tickers:
        return {"url": url, "headline": headline, "tickers": [], "impacts": {},
                "note": "No tickers resolved from article text."}

    scores = score_impacts(headline, body, tickers)

    result = {"url": url, "headline": headline, "tickers": tickers, "impacts": scores}

    if not persist or not scores:
        return result

    # Persist to news_events + impact_analysis
    now = datetime.now(timezone.utc).isoformat()
    content_hash = hashlib.sha1(f"url|{url}|{now}".encode()).hexdigest()
    event_id = f"url:{content_hash[:16]}"

    conn = db._connection()
    with conn.cursor() as cur:
        cur.execute(f"""
            MERGE INTO {db.T_NEWS} t
            USING (SELECT '{event_id}' AS event_id) s ON t.event_id = s.event_id
            WHEN NOT MATCHED THEN INSERT
                (event_id, source, url, headline, body, published_at, ingested_at, content_hash)
            VALUES ('{event_id}', 'user_url', '{url}', '{headline[:1000].replace("'", "''")}',
                    '{body[:5000].replace("'", "''")}', '{now}', '{now}', '{content_hash}')
        """)

        model = st.secrets["databricks"].get("model_endpoint", "databricks-claude-sonnet-4")
        model_version = f"lakesignal-0.1-{model}"
        for sym, s in scores.items():
            impact_id = str(uuid.uuid4())
            risk_json = json.dumps(s["risk_tags"])
            cur.execute(f"""
                MERGE INTO {db.T_IMPACT} t
                USING (SELECT '{event_id}' AS event_id, '{sym}' AS ticker_symbol) s
                ON t.event_id = s.event_id AND t.ticker_symbol = s.ticker_symbol
                WHEN NOT MATCHED THEN INSERT
                    (impact_id, event_id, ticker_symbol, direction, sentiment_score,
                     magnitude, predicted_move_pct_1d, predicted_move_pct_5d, confidence,
                     risk_tags, rationale, analyzed_at, model_version)
                VALUES ('{impact_id}', '{event_id}', '{sym}', '{s["direction"]}',
                        {s["sentiment_score"]}, {s["magnitude"]},
                        {s["predicted_move_pct_1d"]}, {s["predicted_move_pct_5d"]},
                        {s["confidence"]}, from_json('{risk_json}', 'ARRAY<STRING>'),
                        '{s["rationale"][:160].replace("'", "''")}', '{now}', '{model_version}')
            """)

    result["event_id"] = event_id
    result["source"] = "user_url"
    return result
