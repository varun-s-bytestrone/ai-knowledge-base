# Databricks notebook source
# MAGIC %md
# MAGIC # NorthPeak Retail Demo - 05 Replenishment Orders
# MAGIC
# MAGIC Simulates the SAP write-back by creating a replenishment order table.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "northpeak_retail_demo")
dbutils.widgets.text("safety_stock_days", "2")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
safety_stock_days = int(dbutils.widgets.get("safety_stock_days"))

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

forecast = spark.table("demand_forecast")

weekly_demand = (
    forecast
    .groupBy(
        "store_id",
        "store_name",
        "region",
        "product_id",
        "product_name",
        "category",
        "current_stock",
    )
    .agg(F.sum("predicted_units").alias("predicted_weekly_units"))
    .withColumn("avg_daily_forecast", F.col("predicted_weekly_units") / F.lit(7.0))
    .withColumn("target_stock", F.ceil(F.col("predicted_weekly_units") + (F.col("avg_daily_forecast") * F.lit(safety_stock_days))))
    .withColumn("suggested_order_qty", F.greatest(F.col("target_stock") - F.col("current_stock"), F.lit(0)).cast("int"))
    .withColumn(
        "stock_risk",
        F.when(F.col("current_stock") <= F.col("avg_daily_forecast"), F.lit("HIGH"))
        .when(F.col("current_stock") <= F.col("avg_daily_forecast") * 3, F.lit("MEDIUM"))
        .otherwise(F.lit("LOW")),
    )
    .withColumn("created_date", F.current_date())
    .withColumn("integration_status", F.when(F.col("suggested_order_qty") > 0, F.lit("READY_FOR_SAP")).otherwise(F.lit("NO_ACTION")))
)

orders = weekly_demand.filter(F.col("suggested_order_qty") > 0)

orders.write.format("delta").mode("overwrite").saveAsTable("sap_replenishment_orders")

display(orders.orderBy(F.desc("suggested_order_qty")).limit(50))

