# NorthPeak Retail Databricks Free Edition Demo

This demo implements the NorthPeak Retail demand forecasting scenario on Databricks Free Edition.

The production story says SAP S/4HANA, SLT, Azure Data Lake Storage, third-party APIs, Databricks Jobs, Delta Lake, Unity Catalog, MLflow, dashboards, and SAP write-back. In Free Edition, the enterprise integrations are simulated, while the lakehouse flow is real:

- SAP sales and inventory extracts are generated as raw Delta tables.
- Weather API data is generated as a raw Delta table.
- Bronze, Silver, and Gold Delta tables are created in Unity Catalog.
- A Spark ML forecasting model is trained and logged with MLflow.
- Forecasts and replenishment orders are written back as Gold tables.
- SQL queries are provided for a Databricks SQL dashboard.

## Free Edition Mapping

| Production Component | Demo Equivalent |
|---|---|
| SAP S/4HANA sales and inventory | Generated SAP-like Delta tables |
| SAP SLT replication | `01_bronze_ingest.py` notebook |
| Azure Data Lake Storage | Managed Databricks / Unity Catalog tables |
| Third-party weather API | Generated weather extract |
| Bronze raw layer | `raw_sap_sales`, `raw_sap_inventory`, `raw_weather` |
| Silver clean layer | `sales_clean`, `inventory_clean`, `weather_clean` |
| Gold feature layer | `daily_demand_by_store_product` |
| ML training | Spark ML Gradient-Boosted Trees plus MLflow |
| SAP write-back | `sap_replenishment_orders` output table |

## Files

```text
databricks-northpeak-demo/
  README.md
  notebooks/
    00_setup.py
    01_bronze_ingest.py
    02_silver_transform.py
    03_gold_features.py
    04_train_and_forecast.py
    05_replenishment_orders.py
  sql/
    dashboard_queries.sql
  jobs/
    northpeak_job_outline.yml
```

## Step 1: Create Databricks Free Edition Workspace

Sign up for Databricks Free Edition from Databricks' Free Edition page. Use the Free Edition workspace, not the retired Community Edition.

Free Edition is serverless-only and quota-limited, which is fine for this demo.

## Step 2: Import The Notebooks

In Databricks:

1. Go to **Workspace**.
2. Create a folder named `NorthPeak Retail Demo`.
3. Import each file from `notebooks/` as a notebook.
4. Keep the numeric prefixes so the execution order is obvious.

These files use Databricks source notebook format, so importing `.py` files works normally.

## Step 3: Run The Notebooks Manually Once

Run them in this order:

```text
00_setup
01_bronze_ingest
02_silver_transform
03_gold_features
04_train_and_forecast
05_replenishment_orders
```

Default catalog/schema:

```text
catalog = workspace
schema  = northpeak_retail_demo
```

If your Free Edition workspace uses a different writable catalog, change the widget value in `00_setup.py` and reuse the same values in the other notebooks.

## Step 4: Check Tables

After the run, check Catalog Explorer or run:

```sql
SHOW TABLES IN workspace.northpeak_retail_demo;
```

Expected tables:

```text
store_master
product_master
raw_sap_sales
raw_sap_inventory
raw_weather
sales_clean
inventory_clean
weather_clean
daily_demand_by_store_product
demand_forecast
sap_replenishment_orders
```

## Step 5: Create A Job

Create a Databricks Job with these notebook tasks:

```text
setup
bronze_ingest
silver_transform
gold_features
train_and_forecast
replenishment_orders
```

Set dependencies:

```text
setup -> bronze_ingest -> silver_transform -> gold_features -> train_and_forecast -> replenishment_orders
```

Use serverless compute. Schedule it daily if you want the “nightly pipeline” story.

## Step 6: Build The Dashboard

Open `sql/dashboard_queries.sql` and run the queries in Databricks SQL.

Suggested dashboard visuals:

- Bar chart: largest replenishment gaps by product/store.
- Table: stores at stockout risk.
- KPI: total suggested replenishment units.
- KPI: count of store-product pairs requiring replenishment.
- Line chart: seven-day forecast by date.

## Demo Talk Track

Use this explanation:

> In production, NorthPeak would use SAP SLT to replicate sales and inventory into cloud storage. In this Free Edition demo, I simulate those SAP extracts as raw Bronze Delta tables. From there, the rest of the architecture is real Databricks: Delta tables, medallion layers, Unity Catalog, Jobs, MLflow tracking, forecasting, SQL analytics, and a simulated SAP replenishment outbound table.

## Reset The Demo

To rerun from scratch:

```sql
DROP SCHEMA IF EXISTS workspace.northpeak_retail_demo CASCADE;
```

Then run `00_setup.py` again.

