# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
# MAGIC %md
# MAGIC # LakeSignal — Bootstrap
# MAGIC
# MAGIC Creates the Unity Catalog objects and seeds the ticker universe.
# MAGIC Run this once per workspace. Safe to re-run — everything is idempotent.
# MAGIC
# MAGIC **Creates**
# MAGIC - Catalog `lakesignal`
# MAGIC - Schema  `lakesignal.core`
# MAGIC - Tables  `tickers`, `news_events`, `impact_analysis`, `webhook_subscriptions`
# MAGIC - Loads `data/tickers_seed.csv` into `tickers`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "lakesignal", "Catalog")
dbutils.widgets.text("schema", "core", "Schema")
dbutils.widgets.text(
    "tickers_csv_path",
    "/Volumes/lakesignal/core/seed/tickers_seed.csv",
    "Path to tickers_seed.csv (Volume or dbfs:/FileStore/…)",
)

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
TICKERS_CSV = dbutils.widgets.get("tickers_csv_path")

print(f"Using {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Catalog and schema

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA  IF NOT EXISTS {CATALOG}.{SCHEMA}")
# Optional: create a Volume to stage the seed CSV.
spark.sql(f"CREATE VOLUME  IF NOT EXISTS {CATALOG}.{SCHEMA}.seed")
print("Catalog, schema, and seed Volume ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Delta tables

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.tickers (
    symbol       STRING NOT NULL,
    company_name STRING NOT NULL,
    sector       STRING,
    industry     STRING,
    aliases      STRING,
    exchange     STRING,
    CONSTRAINT pk_tickers PRIMARY KEY (symbol) RELY
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.news_events (
    event_id     STRING NOT NULL,
    source       STRING NOT NULL,
    url          STRING,
    headline     STRING NOT NULL,
    body         STRING,
    published_at TIMESTAMP,
    ingested_at  TIMESTAMP NOT NULL,
    content_hash STRING NOT NULL,
    CONSTRAINT pk_news_events PRIMARY KEY (event_id) RELY
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.impact_analysis (
    impact_id             STRING NOT NULL,
    event_id              STRING NOT NULL,
    ticker_symbol         STRING NOT NULL,
    direction             STRING NOT NULL,
    sentiment_score       DOUBLE NOT NULL,
    magnitude             INT    NOT NULL,
    predicted_move_pct_1d DOUBLE,
    predicted_move_pct_5d DOUBLE,
    confidence            DOUBLE NOT NULL,
    risk_tags             ARRAY<STRING>,
    rationale             STRING,
    analyzed_at           TIMESTAMP NOT NULL,
    model_version         STRING NOT NULL,
    CONSTRAINT pk_impact PRIMARY KEY (impact_id) RELY
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.webhook_subscriptions (
    id         STRING NOT NULL,
    url        STRING NOT NULL,
    secret     STRING NOT NULL,
    filters    STRING,
    active     BOOLEAN NOT NULL,
    created_at TIMESTAMP NOT NULL,
    CONSTRAINT pk_webhooks PRIMARY KEY (id) RELY
) USING DELTA
""")

print("Delta tables ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Seed the tickers table
# MAGIC
# MAGIC Two ways to stage `tickers_seed.csv`:
# MAGIC
# MAGIC 1. **Volume (preferred)**: upload the file to the `seed` Volume created above
# MAGIC    (Catalog Explorer → lakesignal → core → seed → Upload).
# MAGIC 2. **DBFS FileStore**: upload to `dbfs:/FileStore/lakesignal/tickers_seed.csv`
# MAGIC    and set the widget accordingly.
# MAGIC
# MAGIC The cell below reads the widget path.

# COMMAND ----------

from pyspark.sql.functions import col, lower, trim, upper

seed_df = (
    spark.read.option("header", True)
    .option("inferSchema", False)
    .csv(TICKERS_CSV)
    .select(
        upper(trim(col("symbol"))).alias("symbol"),
        trim(col("company_name")).alias("company_name"),
        trim(col("sector")).alias("sector"),
        trim(col("industry")).alias("industry"),
        lower(trim(col("aliases"))).alias("aliases"),
        trim(col("exchange")).alias("exchange"),
    )
)
print(f"Read {seed_df.count()} ticker rows from {TICKERS_CSV}")

# COMMAND ----------

from delta.tables import DeltaTable

tgt = DeltaTable.forName(spark, f"{CATALOG}.{SCHEMA}.tickers")
(
    tgt.alias("t")
    .merge(seed_df.alias("s"), "t.symbol = s.symbol")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
print("Merged tickers.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Quick sanity checks

# COMMAND ----------

display(spark.sql(f"SELECT COUNT(*) AS ticker_count FROM {CATALOG}.{SCHEMA}.tickers"))

# COMMAND ----------

display(
    spark.sql(
        f"SELECT symbol, company_name, sector FROM {CATALOG}.{SCHEMA}.tickers ORDER BY symbol LIMIT 10"
    )
)
