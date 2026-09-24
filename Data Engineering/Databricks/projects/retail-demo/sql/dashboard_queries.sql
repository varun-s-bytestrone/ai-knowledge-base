-- NorthPeak Retail Databricks SQL Dashboard Queries
-- Replace workspace.northpeak_retail_demo if you used a different catalog/schema.

-- 1. KPI: total replenishment units recommended.
SELECT
  SUM(suggested_order_qty) AS total_suggested_order_units
FROM workspace.northpeak_retail_demo.sap_replenishment_orders;

-- 2. KPI: number of store-product pairs requiring replenishment.
SELECT
  COUNT(*) AS replenishment_line_count
FROM workspace.northpeak_retail_demo.sap_replenishment_orders;

-- 3. Table: highest priority replenishment actions.
SELECT
  store_id,
  store_name,
  region,
  product_id,
  product_name,
  category,
  current_stock,
  predicted_weekly_units,
  target_stock,
  suggested_order_qty,
  stock_risk,
  integration_status
FROM workspace.northpeak_retail_demo.sap_replenishment_orders
ORDER BY
  CASE stock_risk
    WHEN 'HIGH' THEN 1
    WHEN 'MEDIUM' THEN 2
    ELSE 3
  END,
  suggested_order_qty DESC
LIMIT 100;

-- 4. Bar chart: replenishment need by category.
SELECT
  category,
  SUM(suggested_order_qty) AS suggested_order_qty
FROM workspace.northpeak_retail_demo.sap_replenishment_orders
GROUP BY category
ORDER BY suggested_order_qty DESC;

-- 5. Line chart: seven-day forecast.
SELECT
  forecast_date,
  SUM(predicted_units) AS predicted_units
FROM workspace.northpeak_retail_demo.demand_forecast
GROUP BY forecast_date
ORDER BY forecast_date;

-- 6. Store manager view: forecast versus stock.
SELECT
  store_id,
  store_name,
  product_id,
  product_name,
  category,
  current_stock,
  SUM(predicted_units) AS predicted_weekly_units,
  SUM(predicted_units) - current_stock AS forecast_stock_gap
FROM workspace.northpeak_retail_demo.demand_forecast
GROUP BY
  store_id,
  store_name,
  product_id,
  product_name,
  category,
  current_stock
ORDER BY forecast_stock_gap DESC;

