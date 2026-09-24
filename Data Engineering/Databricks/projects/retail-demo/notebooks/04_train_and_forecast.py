# Databricks notebook source
# MAGIC %md
# MAGIC # NorthPeak Retail Demo - 04 Train And Forecast
# MAGIC
# MAGIC Trains a lightweight Spark ML demand model, tracks it in MLflow, and writes next-week forecasts.

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import functions as F
import mlflow
import mlflow.spark
import os

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "northpeak_retail_demo")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")
spark.sql("CREATE VOLUME IF NOT EXISTS ml_tmp")

mlflow_tmp_path = f"/Volumes/{catalog}/{schema}/ml_tmp"
os.environ["MLFLOW_DFS_TMP"] = mlflow_tmp_path

# COMMAND ----------

training_data = (
    spark.table("daily_demand_by_store_product")
    .select(
        "store_id",
        "region",
        "product_id",
        "category",
        "date",
        F.col("units_sold").cast("double").alias("label"),
        F.col("current_stock").cast("double").alias("current_stock"),
        F.col("temperature_f").cast("double").alias("temperature_f"),
        F.col("precipitation_inches").cast("double").alias("precipitation_inches"),
        F.col("foot_traffic_index").cast("double").alias("foot_traffic_index"),
        F.col("was_promo_day").cast("int").alias("was_promo_day"),
        F.col("day_of_week").cast("double").alias("day_of_week"),
        F.col("week_of_year").cast("double").alias("week_of_year"),
    )
)

train_df, test_df = training_data.randomSplit([0.8, 0.2], seed=42)

region_indexer = StringIndexer(inputCol="region", outputCol="region_index", handleInvalid="keep")
category_indexer = StringIndexer(inputCol="category", outputCol="category_index", handleInvalid="keep")

assembler = VectorAssembler(
    inputCols=[
        "store_id",
        "product_id",
        "current_stock",
        "temperature_f",
        "precipitation_inches",
        "foot_traffic_index",
        "was_promo_day",
        "day_of_week",
        "week_of_year",
        "region_index",
        "category_index",
    ],
    outputCol="features",
)

regressor = GBTRegressor(
    labelCol="label",
    featuresCol="features",
    maxIter=25,
    maxDepth=5,
    seed=42,
)

pipeline = Pipeline(stages=[region_indexer, category_indexer, assembler, regressor])

# COMMAND ----------

mlflow.set_experiment(f"/Users/{spark.sql('SELECT current_user()').first()[0]}/northpeak-demand-forecast")

with mlflow.start_run(run_name="northpeak_gradient_boosted_forecast"):
    model = pipeline.fit(train_df)
    predictions = model.transform(test_df)

    evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="mae")
    mae = evaluator.evaluate(predictions)

    mlflow.log_param("model_type", "Spark ML GBTRegressor")
    mlflow.log_param("max_iter", 25)
    mlflow.log_param("max_depth", 5)
    mlflow.log_metric("mae", mae)
    mlflow.spark.log_model(model, "model", dfs_tmpdir=mlflow_tmp_path)

    print(f"MAE: {mae:.3f}")

# COMMAND ----------

latest_date = spark.table("daily_demand_by_store_product").agg(F.max("date").alias("latest_date")).first()["latest_date"]

latest_features = (
    spark.table("daily_demand_by_store_product")
    .filter(F.col("date") == F.lit(latest_date))
    .select(
        "store_id",
        "store_name",
        "region",
        "product_id",
        "product_name",
        "category",
        "current_stock",
        "temperature_f",
        "precipitation_inches",
        "foot_traffic_index",
    )
)

future_dates = (
    spark.range(1, 8)
    .withColumn("forecast_date", F.date_add(F.lit(latest_date), F.col("id").cast("int")))
    .drop("id")
)

future_features = (
    latest_features.crossJoin(future_dates)
    .withColumn("date", F.col("forecast_date"))
    .withColumn("day_of_week", F.dayofweek("forecast_date").cast("double"))
    .withColumn("week_of_year", F.weekofyear("forecast_date").cast("double"))
    .withColumn("was_promo_day", ((F.pmod(F.col("store_id") + F.col("product_id") + F.dayofyear("forecast_date"), F.lit(29))) == 0).cast("int"))
)

forecast = (
    model.transform(future_features)
    .withColumn("predicted_units", F.greatest(F.round("prediction", 0), F.lit(0)).cast("int"))
    .select(
        "store_id",
        "store_name",
        "region",
        "product_id",
        "product_name",
        "category",
        "forecast_date",
        "current_stock",
        "temperature_f",
        "precipitation_inches",
        "foot_traffic_index",
        "was_promo_day",
        "predicted_units",
    )
)

forecast.write.format("delta").mode("overwrite").saveAsTable("demand_forecast")

display(forecast.orderBy("forecast_date", "store_id", "product_id").limit(50))
