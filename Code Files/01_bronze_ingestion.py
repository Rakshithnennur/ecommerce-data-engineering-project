from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DecimalType, IntegerType

# Parameters
widgets = dbutils.widgets.getAll()

def param(name: str, default: str) -> str:
    return widgets.get(name, default)

catalog_name = param("catalog_name", "main")
bronze_schema = param("bronze_schema", "bronze")
source_base_path = param("source_base_path", "dbfs:/mnt/raw/ecommerce")
checkpoint_base_path = param("checkpoint_base_path", "dbfs:/mnt/checkpoints/ecommerce")
schema_base_path = param("schema_base_path", "dbfs:/mnt/schemas/ecommerce")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{bronze_schema}")

# Schemas Definition
orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("order_ts", TimestampType(), False),
    StructField("order_status", StringType(), True),
    StructField("order_total", DecimalType(18, 2), True)
])

order_items_schema = StructType([
    StructField("order_item_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DecimalType(18, 2), True),
    StructField("order_ts", TimestampType(), False)
])

customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("updated_at", TimestampType(), True)
])

products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("updated_at", TimestampType(), True)
])

entity_schemas = {
    "orders": orders_schema,
    "order_items": order_items_schema,
    "customers": customers_schema,
    "products": products_schema,
}

def ingest_entity(entity_name: str, schema: StructType) -> None:
    source_path = f"{source_base_path}/{entity_name}"
    checkpoint_path = f"{checkpoint_base_path}/bronze/{entity_name}"
    schema_path = f"{schema_base_path}/{entity_name}"
    target_table = f"{catalog_name}.{bronze_schema}.{entity_name}"

    (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_path)
        .schema(schema)
        .load(source_path)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_batch_id", F.lit(None).cast("string"))
        .writeStream
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target_table)
    )

for name, schema in entity_schemas.items():
    ingest_entity(name, schema)
