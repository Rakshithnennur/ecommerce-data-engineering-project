-- Databricks SQL: build gold marts from silver curated tables

CREATE SCHEMA IF NOT EXISTS ${catalog_name}.${gold_schema};

CREATE OR REPLACE TABLE ${catalog_name}.${gold_schema}.fact_orders
USING DELTA
PARTITIONED BY (order_date)
AS
SELECT
  o.order_id,
  o.customer_id,
  oi.product_id,
  o.order_status,
  o.order_date,
  o.order_ts,
  oi.quantity,
  oi.unit_price,
  oi.line_amount,
  o.order_total,
  current_timestamp() AS gold_loaded_ts
FROM ${catalog_name}.${silver_schema}.orders o
JOIN ${catalog_name}.${silver_schema}.order_items oi
  ON o.order_id = oi.order_id;

CREATE OR REPLACE TABLE ${catalog_name}.${gold_schema}.dim_customer_current
USING DELTA
AS
SELECT
  customer_id,
  first_name,
  last_name,
  full_name,
  email,
  city,
  country,
  effective_from,
  effective_to,
  is_current,
  updated_ts
FROM ${catalog_name}.${silver_schema}.customers
WHERE is_current = true;

CREATE OR REPLACE TABLE ${catalog_name}.${gold_schema}.dim_product
USING DELTA
AS
SELECT
  product_id,
  product_name,
  category,
  brand,
  updated_ts
FROM ${catalog_name}.${silver_schema}.products;

CREATE OR REPLACE TABLE ${catalog_name}.${gold_schema}.kpi_daily_sales
USING DELTA
AS
SELECT
  order_date,
  COUNT(DISTINCT order_id) AS total_orders,
  COUNT(DISTINCT customer_id) AS active_customers,
  SUM(line_amount) AS gross_sales,
  AVG(line_amount) AS avg_line_amount,
  SUM(CASE WHEN order_status = 'CANCELLED' THEN line_amount ELSE 0 END) AS cancelled_sales,
  SUM(CASE WHEN order_status = 'COMPLETED' THEN line_amount ELSE 0 END) AS completed_sales,
  current_timestamp() AS gold_loaded_ts
FROM ${catalog_name}.${gold_schema}.fact_orders
GROUP BY order_date;
