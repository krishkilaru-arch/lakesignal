# LakeSignal on Databricks

Databricks-native news-to-ticker impact scoring system. Everything lives in Unity Catalog
`lakesignal.core`, scoring runs through the `databricks-claude-sonnet-4` Foundation Model API,
backtesting validates predictions against real stock prices, and the full-stack FastAPI + HTML
front end is packaged as a Databricks App.

## Architecture

```
RSS Feeds ──► 01_ingest_and_score ──► news_events ──► impact_analysis
                                          │                  │
User URL ──► App /analyze/url ────────────┘                  │
                                                             ▼
                                        03_backtest + yfinance ──► backtest_results
                                                                        │
                                                                        ▼
                                        App (Dashboard · Track Record · About)
```

## Components

| Piece | Path | What it does |
| --- | --- | --- |
| Bootstrap notebook | `notebooks/00_bootstrap.py` | Creates catalog, schema, Delta tables, seeds ticker universe (523 S&P 500 tickers) |
| Ingest + score notebook | `notebooks/01_ingest_and_score.py` | Polls RSS feeds, resolves tickers, scores via `databricks-claude-sonnet-4`, MERGEs into Delta |
| Query examples | `notebooks/02_query_examples.py` | Reference SQL and Python for downstream consumers |
| Backtest notebook | `notebooks/03_backtest.py` | Fetches actual stock prices via yfinance, compares predictions vs reality, writes to `backtest_results` |
| Ingest job | `job/newsimpact_job.json` | Scheduled ingest+score every 5 minutes |
| Backtest job | `job/backtest_job.json` | Scheduled Mon–Fri at 5 PM ET (after market close) |
| App | `app/` | FastAPI backend + HTML/CSS/JS front end deployed as a Databricks App |
| Ticker seed | `data/tickers_seed.csv` | S&P 500 tickers used by the bootstrap notebook |
| Streamlit app | `streamlit_app/` | Public-facing Streamlit dashboard (no auth required) |

## Prerequisites

- A Databricks workspace with Unity Catalog enabled and the **Foundation Model APIs**
  (pay-per-token) endpoint `databricks-claude-sonnet-4` available in your region.
- A SQL Warehouse (Serverless recommended) — the App queries Delta through it.
- `CREATE CATALOG` privilege on your metastore (or ask an admin to run the bootstrap cell
  once and grant you `USE CATALOG` / `USE SCHEMA` / `ALL PRIVILEGES ON SCHEMA lakesignal.core`).
- Databricks CLI >= 0.221 if you plan to deploy the Job and App from the command line.

## 1. Run the bootstrap notebook

Upload `notebooks/00_bootstrap.py` and run it on any cluster. It will:

1. Create catalog `lakesignal` and schema `lakesignal.core`.
2. Create five Delta tables: `tickers`, `news_events`, `impact_analysis`, `backtest_results`, `webhook_subscriptions`.
3. Seed the ticker universe — 523 S&P 500 tickers from `data/tickers_seed.csv`.

## 2. Run the ingest + score notebook

Run `notebooks/01_ingest_and_score.py` interactively to confirm it works. It will:

- Fetch public RSS feeds (Yahoo Finance, Reuters, MarketWatch, CNBC, SEC EDGAR, Seeking Alpha).
- Resolve each story to one or more tickers using regex, alias matching, and company name lookup.
- Call `databricks-claude-sonnet-4` for structured JSON impact scoring per (event, ticker).
- MERGE new rows into `news_events` and `impact_analysis`. Idempotent — rerunning is safe.

## 3. Schedule jobs

Two scheduled jobs power LakeSignal:

```bash
# Ingest + score: every 5 minutes
databricks jobs create --json @job/newsimpact_job.json

# Backtest: Mon-Fri at 9 PM UTC (5 PM ET, after market close)
databricks jobs create --json @job/backtest_job.json
```

| Job | Schedule | What it does |
| --- | --- | --- |
| `lakesignal_ingest_and_score` | Every 5 min | Polls RSS, resolves tickers, scores impacts |
| `lakesignal_daily_backtest` | Mon–Fri 5 PM ET | Fetches actual prices, scores predictions as Correct/Wrong |

## 4. Deploy the FastAPI App

```bash
cd app
databricks apps deploy --source-code-path . lakesignal
```

The app's `app.yaml` runs `uvicorn main:app`. Environment variables:

| Variable | Value | Notes |
| --- | --- | --- |
| `LAKESIGNAL_CATALOG` | `lakesignal` | Set in app.yaml |
| `LAKESIGNAL_SCHEMA` | `core` | Set in app.yaml |
| `LAKESIGNAL_MODEL` | `databricks-claude-sonnet-4` | Set in app.yaml |
| `DATABRICKS_WAREHOUSE_ID` | Your SQL Warehouse ID | Set in app.yaml |
| `DATABRICKS_HOST` | Auto-populated | Injected by Apps runtime |

Auth uses the App's service principal via `databricks-sdk` `WorkspaceClient`.

### App pages

| Page | URL | Description |
| --- | --- | --- |
| **Dashboard** | `/` | Live impacts table with filters, sort, search, CSV export. Analyze URL bar, Add Ticker modal. |
| **Track Record** | `/backtest` | Prediction accuracy by date. Direction accuracy %, per-ticker scorecards, Correct/Wrong/Pending verdicts. |
| **About** | `/about` | Architecture overview, field glossary, how it works. |

## REST API Reference

Base URL: `https://<your-app>.databricksapps.com`

All endpoints return JSON. The app is protected by **Databricks OAuth** — browser
requests are authenticated automatically via SSO. Programmatic (machine-to-machine)
access requires an OAuth token; simple PATs will **not** work.

### Authentication for programmatic access

Databricks Apps use OAuth 2.0, not Personal Access Tokens. There are two ways to call
the API from code:

**Option A — Databricks CLI (interactive, good for testing)**

```bash
# One-time: configure a profile
databricks configure --profile my-env

# Get an OAuth token (opens browser for login, caches token)
TOKEN=$(databricks auth token --profile my-env | jq -r .access_token)

# Use it
curl -H "Authorization: Bearer $TOKEN" \
  "https://<your-app>.databricksapps.com/impacts?limit=5"
```

**Option B — Service principal M2M (automated, no browser)**

1. Create a service principal and generate an OAuth secret in the Databricks admin console.
2. Use the Databricks SDK to generate a Bearer token:

```python
from databricks.sdk import WorkspaceClient
import requests

wc = WorkspaceClient(
    host="https://<your-workspace>.cloud.databricks.com",
    client_id="<service-principal-client-id>",
    client_secret="<service-principal-oauth-secret>"
)
headers = wc.config.authenticate()

# Now call any endpoint
r = requests.get(
    "https://<your-app>.databricksapps.com/impacts?ticker=AAPL&limit=5",
    headers=headers
)
print(r.json())
```

3. Or generate the token directly via curl:

```bash
export CLIENT_ID=<service-principal-client-id>
export CLIENT_SECRET=<service-principal-oauth-secret>

TOKEN=$(curl -s --request POST \
  --url "https://accounts.cloud.databricks.com/oidc/accounts/<account-id>/v1/token" \
  --user "$CLIENT_ID:$CLIENT_SECRET" \
  --data "grant_type=client_credentials&scope=all-apis" | jq -r .access_token)

curl -H "Authorization: Bearer $TOKEN" \
  "https://<your-app>.databricksapps.com/health"
```

**Option C — Direct SQL (skip the app entirely)**

All data lives in Unity Catalog. You can query it directly from any notebook,
SQL warehouse, or BI tool without going through the REST API:

```sql
-- Latest impacts
SELECT * FROM lakesignal.core.impact_analysis
ORDER BY analyzed_at DESC LIMIT 20;

-- Backtest accuracy
SELECT ticker, direction_predicted, direction_correct_1d, actual_move_1d_pct
FROM lakesignal.core.backtest_results
WHERE event_date = '2026-04-24';
```

---

### Health

```
GET /health
```

Returns `{"status": "ok"}`. Use as a liveness probe.

---

### Tickers

#### List / search tickers

```
GET /tickers?q=<search>&limit=200
```

| Param | Type | Default | Description |
| --- | --- | --- | --- |
| `q` | string | — | Optional. Filter by symbol or company name (case-insensitive substring match). |
| `limit` | int | 200 | Max rows returned (max 1000). |

**Example:**

```bash
curl -H "Authorization: Bearer $TOKEN" "$BASE/tickers?q=apple&limit=5"
```

```json
[
  {
    "symbol": "AAPL",
    "company_name": "Apple Inc.",
    "sector": "Information Technology",
    "industry": "Consumer Electronics",
    "exchange": "NASDAQ",
    "aliases": ""
  }
]
```

#### Add a custom ticker

```
POST /tickers
Content-Type: application/json
```

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `symbol` | string | Yes | Ticker symbol, e.g. `"SLV"` |
| `company_name` | string | No | Full name, e.g. `"iShares Silver Trust"` |
| `sector` | string | No | Sector classification |
| `industry` | string | No | Industry classification |
| `exchange` | string | No | Exchange name |
| `aliases` | string | No | Comma-separated aliases |

Uses `MERGE` — safe to call repeatedly for the same symbol.

**Example:**

```bash
curl -X POST "$BASE/tickers" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"symbol": "SLV", "company_name": "iShares Silver Trust", "sector": "Commodities"}'
```

```json
{
  "status": "ok",
  "symbol": "SLV",
  "message": "Ticker SLV merged into lakesignal.core.tickers"
}
```

---

### News Events

```
GET /news?limit=50
```

| Param | Type | Default | Description |
| --- | --- | --- | --- |
| `limit` | int | 50 | Max rows returned (max 500). |

Returns an array of news events ordered by `ingested_at DESC`.

**Response fields:** `event_id`, `source`, `url`, `headline`, `body`, `published_at`, `ingested_at`, `content_hash`.

---

### Impacts

#### List impacts (filtered)

```
GET /impacts?ticker=AAPL&direction=positive&min_magnitude=5&since=2026-04-24T00:00:00&limit=100
```

| Param | Type | Default | Description |
| --- | --- | --- | --- |
| `ticker` | string | — | Filter by ticker symbol (exact match). |
| `direction` | string | — | Filter: `positive`, `negative`, or `neutral`. |
| `min_magnitude` | int | — | Minimum magnitude (1–10). |
| `since` | string | — | ISO 8601 timestamp. Only impacts analyzed after this time. |
| `limit` | int | 100 | Max rows returned (max 1000). |

**Example:**

```bash
curl -H "Authorization: Bearer $TOKEN" \
  "$BASE/impacts?ticker=PLTR&min_magnitude=3&limit=5"
```

```json
[
  {
    "impact_id": "a1b2c3d4-...",
    "event_id": "rss:abc123",
    "ticker_symbol": "PLTR",
    "direction": "positive",
    "sentiment_score": 0.72,
    "magnitude": 4,
    "predicted_move_pct_1d": 1.8,
    "predicted_move_pct_5d": 3.2,
    "confidence": 0.65,
    "risk_tags": ["product", "macro"],
    "rationale": "USDA contract signals expanding government footprint for Palantir's AIP platform.",
    "analyzed_at": "2026-04-24T05:30:00Z",
    "model_version": "lakesignal-0.1-databricks-claude-sonnet-4",
    "headline": "Palantir Stock Pops on USDA Deal",
    "source": "yahoo_finance",
    "url": "https://..."
  }
]
```

#### Get single impact

```
GET /impacts/{impact_id}
```

Returns a single impact object by its UUID.

---

### Analyze

#### Ad-hoc text analysis

```
POST /analyze
Content-Type: application/json
```

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `headline` | string | Yes | News headline to score. |
| `body` | string | No | Article body text (first ~2000 chars used). |
| `tickers` | string[] | No | Tickers to score. If omitted, auto-resolved from text. |
| `persist` | bool | No | Default `false`. If `true`, writes results to Delta tables. |

**Example:**

```bash
curl -X POST "$BASE/analyze" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "headline": "Tesla recalls 500K vehicles over safety concern",
    "tickers": ["TSLA"],
    "persist": false
  }'
```

```json
{
  "headline": "Tesla recalls 500K vehicles over safety concern",
  "tickers": ["TSLA"],
  "impacts": {
    "TSLA": {
      "direction": "negative",
      "sentiment_score": -0.65,
      "magnitude": 5,
      "predicted_move_pct_1d": -2.1,
      "predicted_move_pct_5d": -1.5,
      "confidence": 0.7,
      "risk_tags": ["product", "litigation"],
      "rationale": "Large-scale recall signals quality issues and potential regulatory costs."
    }
  }
}
```

#### Analyze a URL

```
POST /analyze/url
Content-Type: application/json
```

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `url` | string | Yes | Full URL of the news article. |
| `persist` | bool | No | Default `true`. Writes event + impacts to Delta tables. |

The endpoint fetches the page, extracts the headline and body with BeautifulSoup,
auto-resolves tickers via regex + SQL lookup, and scores via the LLM.

**Example:**

```bash
curl -X POST "$BASE/analyze/url" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"url": "https://www.benzinga.com/news/...", "persist": true}'
```

```json
{
  "url": "https://www.benzinga.com/news/...",
  "headline": "China's Silver Scene Is Buzzing Thanks To Retail And Solar...",
  "body_length": 2847,
  "tickers": ["SLV"],
  "impacts": {
    "SLV": {
      "direction": "positive",
      "sentiment_score": 0.55,
      "magnitude": 6,
      "predicted_move_pct_1d": 1.2,
      "predicted_move_pct_5d": 2.8,
      "confidence": 0.6,
      "risk_tags": ["macro", "geopolitical"],
      "rationale": "Rising retail and solar demand in China supports silver prices near term."
    }
  },
  "event_id": "url:a1b2c3d4e5f6",
  "persisted": [
    {"impact_id": "...", "ticker": "SLV", "direction": "positive", "magnitude": 6, "confidence": 0.6}
  ],
  "source": "user_url"
}
```

---

### Backtest

#### Results by date

```
GET /backtest/results?event_date=2026-04-24&ticker=PLTR&limit=500
```

| Param | Type | Default | Description |
| --- | --- | --- | --- |
| `event_date` | string | — | Filter by event date (`YYYY-MM-DD`). |
| `ticker` | string | — | Filter by ticker symbol. |
| `limit` | int | 500 | Max rows returned (max 2000). |

**Response fields:** `backtest_id`, `impact_id`, `event_id`, `ticker`, `headline`,
`direction_predicted`, `magnitude_predicted`, `predicted_move_1d`, `predicted_move_5d`,
`confidence`, `event_date`, `price_date_t0`, `price_date_t1`, `price_date_t5`,
`actual_close_t0`, `actual_close_t1`, `actual_close_t5`, `actual_move_1d_pct`,
`actual_move_5d_pct`, `direction_correct_1d`, `direction_correct_5d`,
`magnitude_error_1d`, `magnitude_error_5d`, `scored_at`.

Rows where `actual_close_t1` is `null` are **Pending** (market hasn't closed yet).

#### Accuracy summary

```
GET /backtest/summary
```

Returns aggregate accuracy statistics across all backtested predictions.

#### Available dates

```
GET /backtest/dates
```

Returns a list of `event_date` values that have backtest data, for populating date pickers.

---

### Webhooks

#### Create subscription

```
POST /webhooks
Content-Type: application/json
```

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `url` | string | Yes | Callback URL to receive POST notifications. |
| `filters` | object | No | Filter criteria (e.g. `{"ticker": "AAPL", "min_magnitude": 5}`). |

Webhook payloads are signed with HMAC-SHA256. The secret is returned on creation.

#### List subscriptions

```
GET /webhooks
```

#### Delete subscription

```
DELETE /webhooks/{sub_id}
```

---

## Data model

```sql
lakesignal.core.tickers (
  symbol STRING, company_name STRING, sector STRING, industry STRING,
  aliases STRING, exchange STRING
)

lakesignal.core.news_events (
  event_id STRING, source STRING, url STRING, headline STRING, body STRING,
  published_at TIMESTAMP, ingested_at TIMESTAMP, content_hash STRING
)

lakesignal.core.impact_analysis (
  impact_id STRING, event_id STRING, ticker_symbol STRING,
  direction STRING, sentiment_score DOUBLE, magnitude INT,
  predicted_move_pct_1d DOUBLE, predicted_move_pct_5d DOUBLE, confidence DOUBLE,
  risk_tags ARRAY<STRING>, rationale STRING,
  analyzed_at TIMESTAMP, model_version STRING
)

lakesignal.core.backtest_results (
  backtest_id STRING, impact_id STRING, event_id STRING, ticker STRING,
  headline STRING, direction_predicted STRING, magnitude_predicted INT,
  predicted_move_1d DOUBLE, predicted_move_5d DOUBLE, confidence DOUBLE,
  event_date DATE, price_date_t0 DATE, price_date_t1 DATE, price_date_t5 DATE,
  actual_close_t0 DOUBLE, actual_close_t1 DOUBLE, actual_close_t5 DOUBLE,
  actual_move_1d_pct DOUBLE, actual_move_5d_pct DOUBLE,
  direction_correct_1d BOOLEAN, direction_correct_5d BOOLEAN,
  magnitude_error_1d DOUBLE, magnitude_error_5d DOUBLE,
  scored_at TIMESTAMP
)

lakesignal.core.webhook_subscriptions (
  id STRING, url STRING, secret STRING, filters STRING,
  active BOOLEAN, created_at TIMESTAMP
)
```

## Key features

- **Full S&P 500 coverage** — 523 tickers, expandable via Add Ticker in the UI.
- **URL analysis** — paste any news URL into the dashboard to get instant AI impact scores.
- **Backtest system** — daily automated comparison of predictions vs actual stock prices.
- **Track Record page** — date-by-date accuracy with Correct/Wrong/Pending verdicts.
- **Expandable rationale** — click any impact row to see the AI's reasoning.
- **CSV export** — download filtered impacts for offline analysis.


## 5. Deploy the Streamlit App (public, no auth)

For public-facing access without Databricks OAuth, deploy the Streamlit version:

```bash
cd streamlit_app
pip install -r requirements.txt
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
# Fill in your Databricks SP credentials in secrets.toml
streamlit run app.py
```

To deploy on **Streamlit Community Cloud** (free, anyone can access):

1. Push `streamlit_app/` to GitHub.
2. Go to [share.streamlit.io](https://share.streamlit.io) → **New app** → point at `streamlit_app/app.py`.
3. Paste your secrets in **Advanced settings → Secrets**.
4. Click **Deploy**. Live at `https://your-app.streamlit.app`.

See `streamlit_app/README.md` for full setup instructions.

### Two apps, one data source

| | Databricks App (FastAPI) | Streamlit Community Cloud |
| --- | --- | --- |
| **Auth** | Databricks OAuth (SSO) | None (public) |
| **REST API** | Yes (full surface) | No (UI only) |
| **Audience** | Internal / workspace users | Public / anyone |
| **Data source** | `lakesignal.core.*` (Delta) | Same tables |

## Swapping the model

The scoring call only depends on the endpoint name. In the notebook set
`MODEL_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"` (or any Foundation Model API
endpoint) and rerun. For the app, change `LAKESIGNAL_MODEL` in `app.yaml`.
