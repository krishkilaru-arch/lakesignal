# NewsImpact on Databricks

Databricks-native port of the NewsImpact prototype. Everything lives in Unity Catalog
`newsimpact.core`, scoring runs through the `databricks-claude-sonnet-4` serving
endpoint, and the FastAPI REST layer is packaged as a Databricks App.

## Components

| Piece | Path | What it is |
| --- | --- | --- |
| Bootstrap notebook | `notebooks/00_bootstrap.py` | Creates catalog, schema, Delta tables, seeds the ticker universe |
| Ingest + score notebook | `notebooks/01_ingest_and_score.py` | Polls RSS feeds, resolves tickers, scores via `databricks-claude-sonnet-4`, MERGEs into Delta |
| Query examples | `notebooks/02_query_examples.py` | Reference SQL and Python for downstream consumers |
| Job | `job/newsimpact_job.json` | Scheduled run of the ingest+score notebook (every 5 min) |
| App | `app/` | FastAPI app deployed as a Databricks App; same REST surface as the SQLite prototype, backed by Delta |
| Ticker seed | `data/tickers_seed.csv` | 102 tickers used by the bootstrap notebook |

## Prerequisites

- A Databricks workspace with Unity Catalog enabled and the **Foundation Model APIs**
  (pay-per-token) endpoint `databricks-claude-sonnet-4` available in your region.
- A SQL Warehouse (Serverless recommended) — the App queries Delta through it.
- `CREATE CATALOG` privilege on your metastore (or ask an admin to run the bootstrap cell
  once and grant you `USE CATALOG` / `USE SCHEMA` / `ALL PRIVILEGES ON SCHEMA newsimpact.core`).
- Databricks CLI ≥ 0.221 if you plan to deploy the Job and App from the command line.

## 1. Run the bootstrap notebook

Upload `notebooks/00_bootstrap.py` (Databricks → Workspace → Import) and run it on any
cluster (serverless SQL or a small all-purpose cluster works). It will:

1. Create catalog `newsimpact` and schema `newsimpact.core` if they don't exist.
2. Create four Delta tables: `tickers`, `news_events`, `impact_analysis`, `webhook_subscriptions`.
3. Seed the ticker universe from `data/tickers_seed.csv` (upload the file to a workspace
   volume or `dbfs:/FileStore/newsimpact/` first — the notebook shows both options).

## 2. Run the ingest + score notebook

Upload `notebooks/01_ingest_and_score.py` and run it interactively to confirm it works.
It will:

- Fetch public RSS feeds (Yahoo Finance, Reuters, MarketWatch, CNBC, SEC EDGAR).
- Resolve each story to one or more tickers using explicit `$SYM` / `(SYM)` mentions
  and alias matching against the `tickers` Delta table.
- Call `databricks-claude-sonnet-4` for structured JSON impact scoring per (event, ticker)
  using the OpenAI-compatible `/serving-endpoints` API.
- MERGE new rows into `news_events` and `impact_analysis`. Idempotent — rerunning is safe.

## 3. Schedule it as a Job

From the Databricks UI: **Workflows → Create Job → Python** and point it at the notebook,
or import `job/newsimpact_job.json`:

```bash
databricks jobs create --json @job/newsimpact_job.json
```

The example schedules the notebook every 5 minutes on a tiny job cluster. Tighten the
trigger or swap to continuous if you want lower latency.

## 4. Deploy the FastAPI App

```bash
cd app
databricks apps deploy --source-code-path . newsimpact
```

The app's `app.yaml` runs `uvicorn main:app`. On first deploy, set these environment
variables in the Databricks Apps UI (Settings → Environment):

- `NEWSIMPACT_CATALOG` = `newsimpact`
- `NEWSIMPACT_SCHEMA` = `core`
- `NEWSIMPACT_MODEL` = `databricks-claude-sonnet-4`
- `DATABRICKS_WAREHOUSE_ID` = the ID of the SQL Warehouse the app should query through
- `DATABRICKS_HOST` — auto-populated by the Apps runtime (no action needed)

Auth to Delta and the serving endpoint uses the App's service principal automatically
via `DefaultCredentialProvider` from the `databricks-sdk`.

### REST surface (unchanged from the SQLite prototype)

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/health` | Liveness probe |
| GET | `/tickers?q=apple` | Search tickers |
| GET | `/news?limit=50` | Recent news events |
| GET | `/impacts?ticker=AAPL&since=...&min_magnitude=5` | Query impacts |
| GET | `/impacts/{impact_id}` | Single impact |
| POST | `/analyze` | Ad-hoc scoring |
| POST | `/webhooks` | Subscribe |
| GET | `/webhooks` / DELETE `/webhooks/{id}` | Manage subscriptions |

## Data model

```sql
newsimpact.core.tickers (
  symbol STRING, company_name STRING, sector STRING, industry STRING,
  aliases STRING, exchange STRING
)

newsimpact.core.news_events (
  event_id STRING, source STRING, url STRING, headline STRING, body STRING,
  published_at TIMESTAMP, ingested_at TIMESTAMP, content_hash STRING
)

newsimpact.core.impact_analysis (
  impact_id STRING, event_id STRING, ticker_symbol STRING,
  direction STRING, sentiment_score DOUBLE, magnitude INT,
  predicted_move_pct_1d DOUBLE, predicted_move_pct_5d DOUBLE, confidence DOUBLE,
  risk_tags ARRAY<STRING>, rationale STRING,
  analyzed_at TIMESTAMP, model_version STRING
)

newsimpact.core.webhook_subscriptions (
  id STRING, url STRING, secret STRING, filters STRING,
  active BOOLEAN, created_at TIMESTAMP
)
```

## Swapping the model

The scoring call only depends on the endpoint name. In the notebook set
`MODEL_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"` (or any other serving
endpoint that honors `response_format={"type":"json_object"}`) and rerun. For the app,
change `NEWSIMPACT_MODEL`.

## What's intentionally simple

- No streaming / DLT — a scheduled job is plenty for news velocity.
- No per-ticker feature store joins — add them in a downstream notebook if your risk
  engine wants enriched features.
- Webhook signing is HMAC-SHA256 (unchanged); secrets live in the Delta table. For
  production, move them to Databricks Secret Scopes.
