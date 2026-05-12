# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Load Operational Source to Bronze
# MAGIC
# MAGIC **Propósito:** copiar las tablas operacionales desde `dbo` hacia `bronze` como tablas Delta.
# MAGIC
# MAGIC
# MAGIC Crea o reemplaza:
# MAGIC - `bronze.customers`
# MAGIC - `bronze.products`
# MAGIC - `bronze.stores`
# MAGIC - `bronze.promotions`
# MAGIC - `bronze.orders`
# MAGIC - `bronze.order_items`
# MAGIC
# MAGIC Cada tabla Bronze agrega columnas técnicas de ingestión.

# COMMAND ----------

try:
    dbutils.widgets.text("catalog_name", "fi_group", "Catalog name")
    dbutils.widgets.text("source_schema", "dbo", "Source schema")
    dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema")
    dbutils.widgets.text("audit_schema", "audit", "Audit schema")
    dbutils.widgets.text("load_mode", "full_refresh", "Load mode")
    dbutils.widgets.text("source_batch", "initial_load", "Source batch")
except NameError:
    pass

catalog_name = dbutils.widgets.get("catalog_name")
source_schema = dbutils.widgets.get("source_schema")
bronze_schema = dbutils.widgets.get("bronze_schema")
audit_schema = dbutils.widgets.get("audit_schema")
load_mode = dbutils.widgets.get("load_mode")
source_batch = dbutils.widgets.get("source_batch")

print(f"Catalog: {catalog_name}")
print(f"Source: {catalog_name}.{source_schema}")
print(f"Bronze: {catalog_name}.{bronze_schema}")
print(f"Load mode: {load_mode}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Función reusable para copiar tabla fuente a Bronze
# MAGIC
# MAGIC Para esta prueba usamos `CREATE OR REPLACE TABLE AS SELECT`, porque el dataset inicial es controlado. En un escenario productivo se usaría Auto Loader, COPY INTO o streaming/micro-batches.

# COMMAND ----------


import uuid
run_id = str(uuid.uuid4())
source_tables = ["customers", "products", "stores", "promotions", "orders", "order_items"]

def load_to_bronze(table_name: str) -> int:
    source_table = f"{catalog_name}.{source_schema}.{table_name}"
    target_table = f"{catalog_name}.{bronze_schema}.{table_name}"

    spark.sql(f"""
    CREATE OR REPLACE TABLE {target_table}
    USING DELTA
    AS
    SELECT
        *,
        current_timestamp() AS bronze_ingestion_timestamp,
        '{source_batch}' AS bronze_source_batch,
        '{run_id}' AS bronze_run_id
    FROM {source_table}
    """)

    return spark.table(target_table).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cargar `customers` a Bronze

# COMMAND ----------

customers_count = load_to_bronze("customers")
print(f"bronze.customers rows: {customers_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Cargar `products` a Bronze

# COMMAND ----------

products_count = load_to_bronze("products")
print(f"bronze.products rows: {products_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cargar `stores` a Bronze

# COMMAND ----------

stores_count = load_to_bronze("stores")
print(f"bronze.stores rows: {stores_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cargar `promotions` a Bronze

# COMMAND ----------


promotions_count = load_to_bronze("promotions")
print(f"bronze.promotions rows: {promotions_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cargar `orders` a Bronze

# COMMAND ----------

orders_count = load_to_bronze("orders")
print(f"bronze.orders rows: {orders_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cargar `order_items` a Bronze

# COMMAND ----------


order_items_count = load_to_bronze("order_items")
print(f"bronze.order_items rows: {order_items_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validar conteos Bronze

# COMMAND ----------

validation_sql = f"""
SELECT 'customers' AS table_name, COUNT(*) AS total_rows FROM {catalog_name}.{bronze_schema}.customers
UNION ALL
SELECT 'products', COUNT(*) FROM {catalog_name}.{bronze_schema}.products
UNION ALL
SELECT 'stores', COUNT(*) FROM {catalog_name}.{bronze_schema}.stores
UNION ALL
SELECT 'promotions', COUNT(*) FROM {catalog_name}.{bronze_schema}.promotions
UNION ALL
SELECT 'orders', COUNT(*) FROM {catalog_name}.{bronze_schema}.orders
UNION ALL
SELECT 'order_items', COUNT(*) FROM {catalog_name}.{bronze_schema}.order_items
ORDER BY table_name
"""

display(spark.sql(validation_sql))

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Registrar auditoría de carga Bronze

# COMMAND ----------


counts = {
    "customers": customers_count,
    "products": products_count,
    "stores": stores_count,
    "promotions": promotions_count,
    "orders": orders_count,
    "order_items": order_items_count
}

for table_name, row_count in counts.items():
    spark.sql(f"""
    INSERT INTO {catalog_name}.{audit_schema}.etl_run_log
    SELECT
        '{run_id}' AS run_id,
        '01_load_source_to_bronze' AS process_name,
        'bronze' AS layer_name,
        '{catalog_name}.{bronze_schema}.{table_name}' AS target_table,
        current_timestamp() AS start_time,
        current_timestamp() AS end_time,
        'SUCCESS' AS status,
        {row_count} AS rows_inserted,
        0 AS rows_updated,
        0 AS rows_deleted,
        NULL AS error_message,
        current_timestamp() AS created_at
    """)

print(f"Bronze load completed. run_id={run_id}")