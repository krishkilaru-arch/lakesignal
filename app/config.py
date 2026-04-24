"""Config pulled from the App's environment."""
from __future__ import annotations

import os

CATALOG = os.getenv("LAKESIGNAL_CATALOG", "lakesignal")
SCHEMA = os.getenv("LAKESIGNAL_SCHEMA", "core")

T_TICKERS = f"{CATALOG}.{SCHEMA}.tickers"
T_NEWS = f"{CATALOG}.{SCHEMA}.news_events"
T_IMPACT = f"{CATALOG}.{SCHEMA}.impact_analysis"
T_WEBHOOKS = f"{CATALOG}.{SCHEMA}.webhook_subscriptions"

MODEL_ENDPOINT = os.getenv("LAKESIGNAL_MODEL", "databricks-claude-sonnet-4")
MODEL_VERSION = f"lakesignal-0.1-{MODEL_ENDPOINT}"

# Populated by the Databricks Apps runtime.
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "").rstrip("/")

# Either of these identifies the SQL warehouse.
DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
DATABRICKS_WAREHOUSE_HTTP_PATH = os.getenv("DATABRICKS_WAREHOUSE_HTTP_PATH", "")

if DATABRICKS_WAREHOUSE_ID and not DATABRICKS_WAREHOUSE_HTTP_PATH:
    DATABRICKS_WAREHOUSE_HTTP_PATH = f"/sql/1.0/warehouses/{DATABRICKS_WAREHOUSE_ID}"
