# Databricks notebook source
# MAGIC %md
# MAGIC # NorthPeak Retail Demo - 00 Setup
# MAGIC
# MAGIC Creates the Unity Catalog schema used by the demo.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "northpeak_retail_demo")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")
spark.sql("CREATE VOLUME IF NOT EXISTS ml_tmp")

print(f"Using schema: {catalog}.{schema}")
print(f"Using MLflow temp volume: /Volumes/{catalog}/{schema}/ml_tmp")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog() AS catalog, current_schema() AS schema;
