# LakeSignal — Streamlit Community Cloud Edition

Public-facing dashboard for LakeSignal. Connects to the same Databricks
Unity Catalog tables (`lakesignal.core.*`) as the internal FastAPI app,
but runs on Streamlit Community Cloud with no login required.

## Quick Start (local)

```bash
cd streamlit_app
pip install -r requirements.txt

# Copy secrets template and fill in your values
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
# Edit .streamlit/secrets.toml with your Databricks credentials

streamlit run app.py
```

## Authentication — Two Options

### Option 1: Personal Access Token (simplest)

Just your Databricks PAT + serverless warehouse. No service principal needed.

```toml
[databricks]
host = "https://dbc-XXXXX.cloud.databricks.com"
http_path = "/sql/1.0/warehouses/XXXXX"
token = "dapi..."
model_endpoint = "databricks-claude-sonnet-4"
```

Generate a PAT: Databricks workspace → User Settings → Developer → Access Tokens → Generate.

### Option 2: Service Principal OAuth (production)

Use a service principal with M2M credentials. Better for shared/production deployments
since the token auto-refreshes and isn't tied to a personal account.

```toml
[databricks]
host = "https://dbc-XXXXX.cloud.databricks.com"
http_path = "/sql/1.0/warehouses/XXXXX"
client_id = "your-sp-client-id"
client_secret = "your-sp-oauth-secret"
model_endpoint = "databricks-claude-sonnet-4"
```

Generate an OAuth secret: Admin Settings → Service Principals → Secrets → Generate.

## Deploy to Streamlit Community Cloud (free, public)

1. Push this `streamlit_app/` folder to a GitHub repo.
2. Go to [share.streamlit.io](https://share.streamlit.io) and sign in.
3. Click **New app** and point it at your repo:
   - Repository: `your-username/lakesignal`
   - Branch: `main`
   - Main file path: `streamlit_app/app.py`
4. In **Advanced settings > Secrets**, paste your secrets (Option 1 or 2 above).
5. Click **Deploy**. Your app will be live at
   `https://your-app.streamlit.app` — no login required.

## Prerequisites

- A Databricks workspace with Unity Catalog and Foundation Model APIs
- A **SQL Warehouse** (Serverless recommended)
- Either a **PAT** (Option 1) or a **Service Principal** (Option 2):
  - SP needs: `USE CATALOG` on `lakesignal`, `SELECT` on `lakesignal.core`, `CAN QUERY` on warehouse

## Files

| File | Purpose |
| --- | --- |
| `app.py` | Main Streamlit app (Dashboard, Track Record, About) |
| `db.py` | Database helper — supports PAT or SP OAuth auth |
| `analyzer.py` | URL analysis + LLM scoring via Foundation Model API |
| `requirements.txt` | Python dependencies |
| `.streamlit/config.toml` | Dark theme + Streamlit settings |
| `.streamlit/secrets.toml.example` | Secrets template (never commit real secrets) |
| `.gitignore` | Excludes secrets.toml from git |

## Architecture

```
Public user ─▶ Streamlit Community Cloud
                   │
                   ├─▶ databricks-sql-connector ─▶ lakesignal.core.* (Delta)
                   │     (PAT or SP OAuth)
                   │
                   └─▶ Foundation Model API ─▶ databricks-claude-sonnet-4
                          (for Analyze URL feature)
```

The Streamlit app is **read-heavy** — it mostly queries existing impacts
and backtest results. The "Analyze URL" feature writes to Delta tables
and calls the LLM, same as the internal FastAPI app.

## Both versions coexist

| | Databricks App (FastAPI) | Streamlit Community Cloud |
| --- | --- | --- |
| **Auth** | Databricks OAuth (SSO) | None (public) |
| **REST API** | Yes (full API surface) | No (UI only) |
| **URL** | `*.databricksapps.com` | `*.streamlit.app` |
| **Data source** | Same Delta tables | Same Delta tables |
| **LLM access** | Via SP auto-auth | Via PAT or SP secret |
