# Databricks notebook source
# MAGIC %md
# MAGIC # NorthPeak Retail Demo - 01 Bronze Ingest
# MAGIC
# MAGIC Simulates raw SAP S/4HANA sales and inventory extracts plus third-party weather data.

# COMMAND ----------

from datetime import date, timedelta
import math
import random

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "northpeak_retail_demo")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

random.seed(42)

# COMMAND ----------

store_rows = []
regions = ["North", "South", "East", "West"]

for store_id in range(1, 21):
    store_rows.append(
        (
            store_id,
            f"Store {store_id:03d}",
            regions[(store_id - 1) % len(regions)],
            "Y" if store_id <= 18 else "N",
        )
    )

store_master = spark.createDataFrame(
    store_rows,
    ["store_id", "store_name", "region", "active_flag"],
)

product_rows = []
categories = ["Snacks", "Drinks", "Household", "Personal Care", "Seasonal"]

for product_id in range(1, 81):
    product_rows.append(
        (
            product_id,
            f"SAP-P{product_id:04d}",
            f"Product {product_id:04d}",
            categories[(product_id - 1) % len(categories)],
        )
    )

product_master = spark.createDataFrame(
    product_rows,
    ["product_id", "sap_product_code", "product_name", "category"],
)

store_master.write.format("delta").mode("overwrite").saveAsTable("store_master")
product_master.write.format("delta").mode("overwrite").saveAsTable("product_master")

display(store_master)
display(product_master)

# COMMAND ----------

start_date = date.today() - timedelta(days=180)
sales_rows = []

for day_offset in range(180):
    business_date = start_date + timedelta(days=day_offset)
    dow = business_date.weekday()
    weekend_boost = 1.25 if dow in [5, 6] else 1.0

    for store_id in range(1, 21):
        store_factor = 1 + (store_id % 5) * 0.05

        for product_id in range(1, 81):
            category_factor = 1 + (product_id % 7) * 0.08
            seasonal_factor = 1 + 0.25 * math.sin(day_offset / 18)
            promo_boost = 1.6 if (day_offset + product_id + store_id) % 29 == 0 else 1.0

            expected = 5.5 * store_factor * category_factor * seasonal_factor * weekend_boost * promo_boost
            units_sold = max(0, int(random.gauss(expected, 2.0)))

            if random.random() < 0.015:
                transaction_date = business_date.strftime("%m/%d/%Y")
            elif random.random() < 0.015:
                transaction_date = business_date.strftime("%Y/%m/%d")
            else:
                transaction_date = business_date.isoformat()

            sales_rows.append(
                (
                    f"TXN-{day_offset:03d}-{store_id:03d}-{product_id:04d}",
                    store_id,
                    f"SAP-P{product_id:04d}",
                    transaction_date,
                    units_sold,
                    "N",
                )
            )

            if random.random() < 0.003:
                sales_rows.append(sales_rows[-1])

raw_sap_sales = spark.createDataFrame(
    sales_rows,
    ["transaction_id", "store_id", "sap_product_code", "transaction_date", "units_sold", "test_record_flag"],
)

raw_sap_sales.write.format("delta").mode("overwrite").saveAsTable("raw_sap_sales")

display(raw_sap_sales.limit(20))

# COMMAND ----------

inventory_rows = []

for day_offset in range(180):
    business_date = start_date + timedelta(days=day_offset)

    for store_id in range(1, 21):
        for product_id in range(1, 81):
            base_stock = 60 + (product_id % 10) * 8 + (store_id % 4) * 5
            stock_noise = random.randint(-18, 25)
            current_stock = max(0, base_stock + stock_noise - int(day_offset * 0.03))

            inventory_rows.append(
                (
                    store_id,
                    f"SAP-P{product_id:04d}",
                    business_date.isoformat(),
                    current_stock,
                    "EA",
                )
            )

raw_sap_inventory = spark.createDataFrame(
    inventory_rows,
    ["store_id", "sap_product_code", "snapshot_date", "current_stock", "unit_of_measure"],
)

raw_sap_inventory.write.format("delta").mode("overwrite").saveAsTable("raw_sap_inventory")

display(raw_sap_inventory.limit(20))

# COMMAND ----------

weather_rows = []

for day_offset in range(180):
    business_date = start_date + timedelta(days=day_offset)

    for store_id in range(1, 21):
        region_offset = (store_id % 4) * 3
        temperature = 62 + region_offset + 12 * math.sin(day_offset / 24) + random.gauss(0, 4)
        precipitation = max(0.0, random.gauss(0.12, 0.25))
        foot_traffic_index = max(40, min(160, 100 + random.gauss(0, 18) + (8 if business_date.weekday() in [5, 6] else 0)))

        weather_rows.append(
            (
                store_id,
                business_date.isoformat(),
                round(float(temperature), 1),
                round(float(precipitation), 2),
                round(float(foot_traffic_index), 1),
            )
        )

raw_weather = spark.createDataFrame(
    weather_rows,
    ["store_id", "weather_date", "temperature_f", "precipitation_inches", "foot_traffic_index"],
)

raw_weather.write.format("delta").mode("overwrite").saveAsTable("raw_weather")

display(raw_weather.limit(20))

