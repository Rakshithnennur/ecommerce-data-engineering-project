from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

widgets = dbutils.widgets.getAll()

def param(name: str, default: str) -> str:
    return widgets.get(name, default)

catalog_name = param("catalog_name", "main")
bronze_schema = param("bronze_schema", "bronze")
silver_schema = param("silver_schema", "silver")
pipeline_run_id = param("pipeline_run_id", "manual_run")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{silver_schema}")

def with_audit_columns(df):
    return (
        df.withColumn("created_ts", F.current_timestamp())
          .withColumn("updated_ts", F.current_timestamp())
          .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
    )


def merge_upsert(source_df, target_table, keys):
    if not spark.catalog.tableExists(target_table):
        source_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        return

    delta_target = DeltaTable.forName(spark, target_table)
    merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in keys])

    (
        delta_target.alias("t")
        .merge(source_df.alias("s"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# Orders standardization + dedupe

orders_bronze = spark.table(f"{catalog_name}.{bronze_schema}.orders")
orders_clean = (
    orders_bronze
    .withColumn("order_status", F.upper(F.trim(F.col("order_status"))))
    .withColumn("order_total", F.coalesce(F.col("order_total"), F.lit(0.00)))
    .withColumn("order_date", F.to_date("order_ts"))
    .filter(F.col("order_id").isNotNull() & F.col("customer_id").isNotNull())
)

order_window = Window.partitionBy("order_id").orderBy(F.col("order_ts").desc(), F.col("_ingest_ts").desc())
orders_deduped = (
    orders_clean
    .withColumn("_rn", F.row_number().over(order_window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

orders_silver = with_audit_columns(orders_deduped)
merge_upsert(orders_silver, f"{catalog_name}.{silver_schema}.orders", ["order_id"])

# Clean, standardize, validate, and deduplicate order item records before loading into the Silver layer

items_bronze = spark.table(f"{catalog_name}.{bronze_schema}.order_items")
items_clean = (
    items_bronze
    .withColumn("quantity", F.coalesce(F.col("quantity"), F.lit(1)))
    .withColumn("unit_price", F.coalesce(F.col("unit_price"), F.lit(0.00)))
    .withColumn("line_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
    .withColumn("order_date", F.to_date("order_ts"))
    .filter(F.col("order_item_id").isNotNull() & F.col("order_id").isNotNull() & F.col("product_id").isNotNull())
)

item_window = Window.partitionBy("order_item_id").orderBy(F.col("order_ts").desc(), F.col("_ingest_ts").desc())
items_deduped = (
    items_clean
    .withColumn("_rn", F.row_number().over(item_window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

items_silver = with_audit_columns(items_deduped)
merge_upsert(items_silver, f"{catalog_name}.{silver_schema}.order_items", ["order_item_id"])

# Customers with SCD Type 2 dimensions

customers_bronze = spark.table(f"{catalog_name}.{bronze_schema}.customers")
customers_clean = (
    customers_bronze
    .withColumn("email", F.lower(F.trim(F.col("email"))))
    .withColumn("full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
    .filter(F.col("customer_id").isNotNull())
)

cust_window = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc_nulls_last(), F.col("_ingest_ts").desc())
customers_current = (
    customers_clean
    .withColumn("_rn", F.row_number().over(cust_window))
    .withColumn("is_current", F.when(F.col("_rn") == 1, F.lit(True)).otherwise(F.lit(False)))
    .withColumn("effective_from", F.col("updated_at"))
    .withColumn("effective_to", F.lead("updated_at").over(Window.partitionBy("customer_id").orderBy("updated_at")))
    .drop("_rn")
)

customers_silver = with_audit_columns(customers_current)
merge_upsert(customers_silver, f"{catalog_name}.{silver_schema}.customers", ["customer_id", "effective_from"])

# Products conformance

products_bronze = spark.table(f"{catalog_name}.{bronze_schema}.products")
products_clean = (
    products_bronze
    .withColumn("product_name", F.initcap(F.trim(F.col("product_name"))))
    .withColumn("category", F.upper(F.trim(F.col("category"))))
    .withColumn("brand", F.initcap(F.trim(F.col("brand"))))
    .filter(F.col("product_id").isNotNull())
)

prod_window = Window.partitionBy("product_id").orderBy(F.col("updated_at").desc_nulls_last(), F.col("_ingest_ts").desc())
products_deduped = (
    products_clean
    .withColumn("_rn", F.row_number().over(prod_window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

products_silver = with_audit_columns(products_deduped)
merge_upsert(products_silver, f"{catalog_name}.{silver_schema}.products", ["product_id"])

# Quarantine invalid data (Stored in orders_quarantine delta table)

invalid_orders = orders_bronze.filter(F.col("order_total") < 0)
if invalid_orders.limit(1).count() > 0:
    invalid_orders.withColumn("dq_rule", F.lit("order_total_must_be_non_negative")) \
        .write.format("delta").mode("append").saveAsTable(f"{catalog_name}.{silver_schema}.orders_quarantine")
