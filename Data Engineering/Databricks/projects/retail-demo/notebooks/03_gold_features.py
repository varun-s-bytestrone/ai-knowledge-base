# Databricks notebook source
# MAGIC %md
# MAGIC # NorthPeak Retail Demo - 03 Gold Features
# MAGIC
# MAGIC Builds a business-ready demand table for dashboards and model training.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "northpeak_retail_demo")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

sales = spark.table("sales_clean")
inventory = spark.table("inventory_clean")
weather = spark.table("weather_clean")
products = spark.table("product_master")
stores = spark.table("store_master")

gold = (
    sales
    .join(inventory, ["store_id", "product_id", "date"], "left")
    .join(weather, ["store_id", "date"], "left")
    .join(products.select("product_id", "product_name", "category"), "product_id", "left")
    .join(stores.select("store_id", "store_name", "region"), "store_id", "left")
    .withColumn("day_of_week", F.dayofweek("date"))
    .withColumn("week_of_year", F.weekofyear("date"))
    .withColumn("was_promo_day", ((F.pmod(F.col("store_id") + F.col("product_id") + F.dayofyear("date"), F.lit(29))) == 0))
    .withColumn("current_stock", F.coalesce(F.col("current_stock"), F.lit(0)))
    .withColumn("temperature_f", F.coalesce(F.col("temperature_f"), F.lit(70.0)))
    .withColumn("precipitation_inches", F.coalesce(F.col("precipitation_inches"), F.lit(0.0)))
    .withColumn("foot_traffic_index", F.coalesce(F.col("foot_traffic_index"), F.lit(100.0)))
    .select(
        "store_id",
        "store_name",
        "region",
        "product_id",
        "product_name",
        "category",
        "date",
        "units_sold",
        "current_stock",
        "temperature_f",
        "precipitation_inches",
        "foot_traffic_index",
        "was_promo_day",
        "day_of_week",
        "week_of_year",
    )
)

gold.write.format("delta").mode("overwrite").saveAsTable("daily_demand_by_store_product")

display(gold.orderBy(F.desc("date")).limit(20))

