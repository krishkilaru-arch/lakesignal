# Databricks notebook source
# MAGIC %md
# MAGIC # NewsImpact — Ingest + Score
# MAGIC
# MAGIC Runs end-to-end each time it's triggered:
# MAGIC
# MAGIC 1. Pull RSS feeds → `news_events` (bronze-like, deduped by `content_hash`).
# MAGIC 2. Resolve each story to one or more tickers using `$SYM` / `(SYM)` mentions
# MAGIC    and alias matching against `tickers`.
# MAGIC 3. Score every (event, ticker) pair via `databricks-claude-sonnet-4` with a
# MAGIC    strict JSON schema. MERGE results into `impact_analysis`.
# MAGIC
# MAGIC Safe to run on a schedule — all writes are idempotent via MERGE on
# MAGIC `event_id` and `impact_id`.

# COMMAND ----------

# MAGIC %pip install --quiet feedparser==6.0.11 openai==1.51.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "newsimpact", "Catalog")
dbutils.widgets.text("schema", "core", "Schema")
dbutils.widgets.text("model_endpoint", "databricks-claude-sonnet-4", "Serving endpoint")
dbutils.widgets.text("max_events_per_run", "50", "Max events to score per run")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
MODEL_ENDPOINT = dbutils.widgets.get("model_endpoint")
MAX_EVENTS = int(dbutils.widgets.get("max_events_per_run"))
MODEL_VERSION = f"newsimpact-0.1-{MODEL_ENDPOINT}"

T_TICKERS = f"{CATALOG}.{SCHEMA}.tickers"
T_NEWS = f"{CATALOG}.{SCHEMA}.news_events"
T_IMPACT = f"{CATALOG}.{SCHEMA}.impact_analysis"

print(f"Catalog/schema: {CATALOG}.{SCHEMA}")
print(f"Model endpoint: {MODEL_ENDPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Fetch RSS feeds

# COMMAND ----------

import hashlib
import re
from datetime import datetime, timezone

import feedparser

FEEDS = [
    ("yahoo_finance", "https://finance.yahoo.com/news/rssindex"),
    ("marketwatch_top", "https://feeds.marketwatch.com/marketwatch/topstories/"),
    ("marketwatch_markets", "https://feeds.marketwatch.com/marketwatch/marketpulse/"),
    ("cnbc_topnews", "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
    ("cnbc_business", "https://www.cnbc.com/id/10001147/device/rss/rss.html"),
    ("reuters_business", "https://www.reutersagency.com/feed/?best-topics=business-finance&post_type=best"),
    ("sec_8k", "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&type=8-K&dateb=&owner=include&count=40&output=atom"),
    ("seeking_alpha_market", "https://seekingalpha.com/market_currents.xml"),
]

TAG_RE = re.compile(r"<[^>]+>")


def _strip(txt):
    return TAG_RE.sub("", txt or "").strip()


def _hash(headline, url):
    return hashlib.sha1(f"{headline.strip().lower()}|{url.strip()}".encode()).hexdigest()


def _published(entry):
    for k in ("published_parsed", "updated_parsed"):
        t = entry.get(k)
        if t:
            try:
                return datetime(*t[:6], tzinfo=timezone.utc)
            except Exception:
                pass
    return datetime.now(timezone.utc)


def fetch_all():
    now = datetime.now(timezone.utc)
    rows = []
    for source, url in FEEDS:
        try:
            parsed = feedparser.parse(url, request_headers={"User-Agent": "NewsImpact/0.1"})
        except Exception as e:
            print(f"  [{source}] parse failed: {e}")
            continue
        for entry in parsed.entries:
            headline = _strip(entry.get("title"))
            if not headline:
                continue
            link = entry.get("link") or ""
            body = _strip(entry.get("summary") or entry.get("description") or "")
            h = _hash(headline, link)
            rows.append(
                {
                    "event_id": f"{source}:{h[:16]}",
                    "source": source,
                    "url": link,
                    "headline": headline,
                    "body": body,
                    "published_at": _published(entry),
                    "ingested_at": now,
                    "content_hash": h,
                }
            )
    print(f"Fetched {len(rows)} entries from {len(FEEDS)} feeds.")
    return rows


raw_rows = fetch_all()

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
)

news_schema = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("source", StringType(), False),
        StructField("url", StringType(), True),
        StructField("headline", StringType(), False),
        StructField("body", StringType(), True),
        StructField("published_at", TimestampType(), True),
        StructField("ingested_at", TimestampType(), False),
        StructField("content_hash", StringType(), False),
    ]
)

news_df = spark.createDataFrame(raw_rows, schema=news_schema) if raw_rows else spark.createDataFrame([], news_schema)
news_df = news_df.dropDuplicates(["event_id"])
print(f"Unique events this batch: {news_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. MERGE into `news_events` (dedupe by event_id)

# COMMAND ----------

from delta.tables import DeltaTable

if news_df.count() > 0:
    tgt = DeltaTable.forName(spark, T_NEWS)
    (
        tgt.alias("t")
        .merge(news_df.alias("s"), "t.event_id = s.event_id")
        .whenNotMatchedInsertAll()
        .execute()
    )
print("news_events merged.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Find unanalyzed events

# COMMAND ----------

unanalyzed = spark.sql(f"""
    SELECT ne.event_id, ne.source, ne.url, ne.headline, ne.body, ne.published_at
    FROM {T_NEWS} ne
    LEFT ANTI JOIN {T_IMPACT} ia
      ON ia.event_id = ne.event_id
    ORDER BY ne.ingested_at DESC
    LIMIT {MAX_EVENTS}
""")

unanalyzed_rows = [r.asDict() for r in unanalyzed.collect()]
print(f"Unanalyzed events to score this run: {len(unanalyzed_rows)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Resolve tickers

# COMMAND ----------

tickers_pdf = spark.table(T_TICKERS).toPandas()

SYMBOL_DOLLAR_RE = re.compile(r"\$([A-Z]{1,5}(?:\.[A-Z])?)\b")
SYMBOL_PAREN_RE = re.compile(r"\(([A-Z]{1,5}(?:\.[A-Z])?)\)")
SYMBOL_COLON_RE = re.compile(r"\b(?:NYSE|NASDAQ|AMEX)\s*:\s*([A-Z]{1,5}(?:\.[A-Z])?)\b")
AMBIGUOUS_BARE = {"A", "T", "V", "C", "F", "GE", "GM", "MS", "UA", "USA", "US", "NY", "AI", "IT"}


def _build_alias_pairs(pdf):
    by_symbol = {}
    pairs = []
    for _, r in pdf.iterrows():
        sym = (r.get("symbol") or "").upper()
        if not sym:
            continue
        by_symbol[sym] = r.to_dict()
        name = (r.get("company_name") or "").lower()
        if name:
            pairs.append((name, sym))
            short = re.sub(
                r",?\s+(inc\.?|corp\.?|corporation|company|co\.?|ltd\.?|plc|s\.a\.|n\.v\.|group|holdings?|technologies|limited)$",
                "",
                name,
            ).strip()
            if short and short != name and len(short) >= 4:
                pairs.append((short, sym))
        for a in (r.get("aliases") or "").split("|"):
            a = a.strip().lower()
            if a and len(a) >= 3:
                pairs.append((a, sym))
    pairs.sort(key=lambda p: len(p[0]), reverse=True)
    return by_symbol, pairs


TICKERS_BY_SYMBOL, ALIAS_PAIRS = _build_alias_pairs(tickers_pdf)


def resolve(text):
    if not text:
        return []
    hits = []
    seen = set()

    def add(sym):
        if sym in TICKERS_BY_SYMBOL and sym not in seen:
            seen.add(sym)
            hits.append(sym)

    for m in SYMBOL_DOLLAR_RE.findall(text):
        add(m.upper())
    for m in SYMBOL_PAREN_RE.findall(text):
        add(m.upper())
    for m in SYMBOL_COLON_RE.findall(text):
        add(m.upper())

    low = text.lower()
    for needle, sym in ALIAS_PAIRS:
        if sym in seen:
            continue
        idx = low.find(needle)
        if idx < 0:
            continue
        before_ok = idx == 0 or not low[idx - 1].isalnum()
        end = idx + len(needle)
        after_ok = end == len(low) or not low[end].isalnum()
        if before_ok and after_ok:
            add(sym)

    for m in re.findall(r"\b([A-Z]{2,5}(?:\.[A-Z])?)\b", text):
        u = m.upper()
        if u in AMBIGUOUS_BARE:
            continue
        if u in TICKERS_BY_SYMBOL:
            add(u)
    return hits


# Build (event, tickers) pairs
work = []
for ev in unanalyzed_rows:
    tickers = resolve(f"{ev['headline']} {ev.get('body') or ''}")
    if tickers:
        work.append((ev, tickers))

print(f"Events with at least one resolved ticker: {len(work)}")
total_pairs = sum(len(ts) for _, ts in work)
print(f"Total (event, ticker) pairs to score: {total_pairs}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Score via `databricks-claude-sonnet-4`
# MAGIC
# MAGIC Uses the OpenAI-compatible `/serving-endpoints` API. Auth comes from the notebook's
# MAGIC default credentials (`DefaultCredentialProvider`), so this works in Jobs and
# MAGIC interactive runs without managing tokens.

# COMMAND ----------

import json
from openai import OpenAI

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
WORKSPACE_URL = f"https://{ctx.browserHostName().get()}" if ctx.browserHostName().isDefined() else ctx.apiUrl().get()
TOKEN = ctx.apiToken().get()

client = OpenAI(api_key=TOKEN, base_url=f"{WORKSPACE_URL}/serving-endpoints")

SYSTEM = """You are a sell-side equity analyst. Given one news story and a list of
potentially-impacted tickers, score the likely short-term stock impact for each ticker.
Be calibrated, not dramatic: most stories are low-magnitude.

Return ONLY valid JSON matching this schema:

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


def score(headline, body, tickers):
    ticker_lines = []
    for t in tickers:
        d = TICKERS_BY_SYMBOL.get(t, {})
        ticker_lines.append(f"- {t}: {d.get('company_name', '')} ({d.get('sector', '')})")
    user = (
        f"Headline: {headline}\n\n"
        f"Body: {(body or '')[:2000]}\n\n"
        f"Tickers to score:\n" + "\n".join(ticker_lines) + "\n\n"
        "Output the JSON now."
    )
    resp = client.chat.completions.create(
        model=MODEL_ENDPOINT,
        messages=[{"role": "system", "content": SYSTEM}, {"role": "user", "content": user}],
        response_format={"type": "json_object"},
        max_tokens=1024,
        temperature=0.2,
    )
    text = resp.choices[0].message.content or "{}"
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
        except (KeyError, ValueError, TypeError) as e:
            print(f"  malformed entry {entry}: {e}")
    return out


import uuid

analyzed_at = datetime.now(timezone.utc)
impact_rows = []
for ev, tickers in work:
    try:
        scores = score(ev["headline"], ev.get("body") or "", tickers)
    except Exception as e:
        print(f"  score failed for {ev['event_id']}: {e}")
        continue
    for sym, s in scores.items():
        impact_rows.append(
            {
                "impact_id": str(uuid.uuid4()),
                "event_id": ev["event_id"],
                "ticker_symbol": sym,
                "direction": s["direction"],
                "sentiment_score": s["sentiment_score"],
                "magnitude": s["magnitude"],
                "predicted_move_pct_1d": s["predicted_move_pct_1d"],
                "predicted_move_pct_5d": s["predicted_move_pct_5d"],
                "confidence": s["confidence"],
                "risk_tags": s["risk_tags"],
                "rationale": s["rationale"],
                "analyzed_at": analyzed_at,
                "model_version": MODEL_VERSION,
            }
        )

print(f"Produced {len(impact_rows)} impact rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. MERGE into `impact_analysis`

# COMMAND ----------

from pyspark.sql.types import ArrayType, DoubleType, IntegerType

impact_schema = StructType(
    [
        StructField("impact_id", StringType(), False),
        StructField("event_id", StringType(), False),
        StructField("ticker_symbol", StringType(), False),
        StructField("direction", StringType(), False),
        StructField("sentiment_score", DoubleType(), False),
        StructField("magnitude", IntegerType(), False),
        StructField("predicted_move_pct_1d", DoubleType(), True),
        StructField("predicted_move_pct_5d", DoubleType(), True),
        StructField("confidence", DoubleType(), False),
        StructField("risk_tags", ArrayType(StringType()), True),
        StructField("rationale", StringType(), True),
        StructField("analyzed_at", TimestampType(), False),
        StructField("model_version", StringType(), False),
    ]
)

if impact_rows:
    impacts_df = spark.createDataFrame(impact_rows, schema=impact_schema)
    tgt = DeltaTable.forName(spark, T_IMPACT)
    (
        tgt.alias("t")
        .merge(
            impacts_df.alias("s"),
            "t.event_id = s.event_id AND t.ticker_symbol = s.ticker_symbol",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

print("impact_analysis merged.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Done — show the latest impacts

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT ia.analyzed_at, ia.ticker_symbol, ia.direction, ia.magnitude,
               ia.predicted_move_pct_1d, ia.predicted_move_pct_5d,
               ia.confidence, ia.risk_tags, ia.rationale,
               ne.headline, ne.source, ne.url
        FROM {T_IMPACT} ia
        JOIN {T_NEWS} ne ON ne.event_id = ia.event_id
        ORDER BY ia.analyzed_at DESC
        LIMIT 25
    """)
)
