# Databricks notebook source
# MAGIC %md
# MAGIC # NewsImpact — Query Examples
# MAGIC
# MAGIC Reference queries your trading apps / risk engine can use to consume the
# MAGIC `impact_analysis` table.
# MAGIC
# MAGIC These work from:
# MAGIC - This notebook (Spark SQL)
# MAGIC - Databricks SQL warehouses (plain SQL editor)
# MAGIC - External tools via `databricks-sql-connector` pointed at a SQL warehouse
# MAGIC - The Databricks AI/BI Genie or Lakeview dashboards

# COMMAND ----------

dbutils.widgets.text("catalog", "newsimpact", "Catalog")
dbutils.widgets.text("schema", "core", "Schema")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1 — Top negative impacts in the last 24 hours

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ia.ticker_symbol, ia.magnitude, ia.predicted_move_pct_1d, ia.confidence,
# MAGIC        ia.risk_tags, ia.rationale, ne.headline, ne.source, ne.published_at
# MAGIC FROM impact_analysis ia
# MAGIC JOIN news_events ne USING (event_id)
# MAGIC WHERE ia.direction = 'negative'
# MAGIC   AND ia.analyzed_at >= current_timestamp() - INTERVAL 24 HOURS
# MAGIC ORDER BY ia.magnitude DESC, ia.confidence DESC
# MAGIC LIMIT 50

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 2 — Per-ticker impact roll-up (last 24h)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ticker_symbol,
# MAGIC   COUNT(*) AS mentions,
# MAGIC   ROUND(AVG(sentiment_score), 3) AS avg_sentiment,
# MAGIC   MAX(magnitude) AS max_magnitude,
# MAGIC   ROUND(AVG(predicted_move_pct_1d), 2) AS avg_pred_move_1d,
# MAGIC   ROUND(AVG(confidence), 2) AS avg_confidence
# MAGIC FROM impact_analysis
# MAGIC WHERE analyzed_at >= current_timestamp() - INTERVAL 24 HOURS
# MAGIC GROUP BY ticker_symbol
# MAGIC ORDER BY mentions DESC, avg_sentiment

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 3 — Risk-tag breakdown for a single ticker

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tag, COUNT(*) AS mentions, AVG(sentiment_score) AS avg_sentiment
# MAGIC FROM impact_analysis
# MAGIC LATERAL VIEW explode(risk_tags) AS tag
# MAGIC WHERE ticker_symbol = 'AAPL'
# MAGIC   AND analyzed_at >= current_timestamp() - INTERVAL 30 DAYS
# MAGIC GROUP BY tag
# MAGIC ORDER BY mentions DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 4 — Incremental pull via a cursor
# MAGIC
# MAGIC External consumers should track the latest `analyzed_at` they've seen and pull
# MAGIC anything newer.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- replace :cursor with the client's last-seen timestamp
# MAGIC SELECT impact_id, event_id, ticker_symbol, direction, sentiment_score, magnitude,
# MAGIC        predicted_move_pct_1d, predicted_move_pct_5d, confidence, risk_tags,
# MAGIC        rationale, analyzed_at, model_version
# MAGIC FROM impact_analysis
# MAGIC WHERE analyzed_at > :cursor
# MAGIC ORDER BY analyzed_at ASC
# MAGIC LIMIT 1000

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 5 — Python read from a non-Databricks client
# MAGIC
# MAGIC Paste this into any Python environment (your trading app, Jupyter, etc.).

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC # pip install databricks-sql-connector
# MAGIC import os
# MAGIC from databricks import sql
# MAGIC
# MAGIC with sql.connect(
# MAGIC     server_hostname=os.environ["DATABRICKS_HOST"].removeprefix("https://"),
# MAGIC     http_path=os.environ["DATABRICKS_WAREHOUSE_HTTP_PATH"],
# MAGIC     access_token=os.environ["DATABRICKS_TOKEN"],
# MAGIC ) as conn:
# MAGIC     with conn.cursor() as cur:
# MAGIC         cur.execute("""
# MAGIC             SELECT * FROM newsimpact.core.impact_analysis
# MAGIC             WHERE ticker_symbol = ? AND analyzed_at > ?
# MAGIC             ORDER BY analyzed_at
# MAGIC         """, ("AAPL", "2026-04-23T00:00:00Z"))
# MAGIC         for row in cur.fetchall():
# MAGIC             print(row)
# MAGIC ```
