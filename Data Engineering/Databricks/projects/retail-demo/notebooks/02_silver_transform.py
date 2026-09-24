# Databricks notebook source
# MAGIC %md
# MAGIC # NorthPeak Retail Demo - 02 Silver Transform
# MAGIC
# MAGIC Cleans raw SAP-like and weather extracts into reliable, queryable Silver tables.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "northpeak_retail_demo")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

sales = spark.table("raw_sap_sales")
products = spark.table("product_master")
stores = spark.table("store_master")

sales_clean = (
    sales
    .filter(F.col("test_record_flag") == "N")
    .join(products, "sap_product_code", "inner")
    .join(stores.filter(F.col("active_flag") == "Y").select("store_id"), "store_id", "inner")
    .withColumn(
        "date",
        F.when(F.col("transaction_date").rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date("transaction_date", "yyyy-MM-dd"))
        .when(F.col("transaction_date").rlike(r"^\d{2}/\d{2}/\d{4}$"), F.to_date("transaction_date", "MM/dd/yyyy"))
        .when(F.col("transaction_date").rlike(r"^\d{4}/\d{2}/\d{2}$"), F.to_date("transaction_date", "yyyy/MM/dd")),
    )
    .filter(F.col("date").isNotNull())
    .dropDuplicates(["transaction_id"])
    .groupBy("store_id", "product_id", "date")
    .agg(F.sum("units_sold").alias("units_sold"))
)

sales_clean.write.format("delta").mode("overwrite").saveAsTable("sales_clean")

display(sales_clean.limit(20))

# COMMAND ----------

inventory = spark.table("raw_sap_inventory")

inventory_clean = (
    inventory
    .join(products, "sap_product_code", "inner")
    .join(stores.filter(F.col("active_flag") == "Y").select("store_id"), "store_id", "inner")
    .withColumn("date", F.to_date("snapshot_date"))
    .filter(F.col("date").isNotNull())
    .groupBy("store_id", "product_id", "date")
    .agg(F.max("current_stock").alias("current_stock"))
)

inventory_clean.write.format("delta").mode("overwrite").saveAsTable("inventory_clean")

display(inventory_clean.limit(20))

# COMMAND ----------

weather = spark.table("raw_weather")

weather_clean = (
    weather
    .join(stores.filter(F.col("active_flag") == "Y").select("store_id"), "store_id", "inner")
    .withColumn("date", F.to_date("weather_date"))
    .filter(F.col("date").isNotNull())
    .dropDuplicates(["store_id", "date"])
    .select(
        "store_id",
        "date",
        F.col("temperature_f").cast("double").alias("temperature_f"),
        F.col("precipitation_inches").cast("double").alias("precipitation_inches"),
        F.col("foot_traffic_index").cast("double").alias("foot_traffic_index"),
    )
)

weather_clean.write.format("delta").mode("overwrite").saveAsTable("weather_clean")

display(weather_clean.limit(20))
