# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Create Catalog, Schemas and Audit Base
# MAGIC
# MAGIC **Propósito:** preparar el entorno Lakehouse.
# MAGIC
# MAGIC Crea:
# MAGIC - Catálogo `fi_group`
# MAGIC - Esquemas `dbo`, `bronze`, `silver`, `gold`, `audit`
# MAGIC - Tablas de auditoría base
# MAGIC - Validación de tablas fuente en `dbo`

# COMMAND ----------


try:
    dbutils.widgets.text("catalog_name", "fi_group", "Catalog name")
    dbutils.widgets.text("source_schema", "dbo", "Source schema")
    dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema")
    dbutils.widgets.text("silver_schema", "silver", "Silver schema")
    dbutils.widgets.text("gold_schema", "gold", "Gold schema")
    dbutils.widgets.text("audit_schema", "audit", "Audit schema")
except NameError:
    pass

catalog_name = dbutils.widgets.get("catalog_name")
source_schema = dbutils.widgets.get("source_schema")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")
audit_schema = dbutils.widgets.get("audit_schema")

print(f"Catalog: {catalog_name}")
print(f"Schemas: {source_schema}, {bronze_schema}, {silver_schema}, {gold_schema}, {audit_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC Crear catálogo y esquemas Lakehouse

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"USE CATALOG {catalog_name}")

for schema_name in [source_schema, bronze_schema, silver_schema, gold_schema, audit_schema]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

print("Catalog and schemas are ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC Crear tablas de auditoría base

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{audit_schema}.etl_run_log (
    run_id STRING NOT NULL,
    process_name STRING NOT NULL,
    layer_name STRING,
    target_table STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status STRING,
    rows_inserted BIGINT,
    rows_updated BIGINT,
    rows_deleted BIGINT,
    error_message STRING,
    created_at TIMESTAMP
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{audit_schema}.data_quality_results (
    run_id STRING NOT NULL,
    rule_name STRING NOT NULL,
    table_name STRING NOT NULL,
    check_status STRING NOT NULL,
    failed_records BIGINT,
    check_timestamp TIMESTAMP,
    comments STRING
)
USING DELTA
""")

print("Audit tables are ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Validar existencia de tablas fuente en `dbo`
# MAGIC
# MAGIC Este notebook asume que el script inicial de objetos ya fue ejecutado y que las tablas existen en `fi_group.dbo`.

# COMMAND ----------

required_source_tables = ["customers", "products", "stores", "promotions", "orders", "order_items"]

missing_tables = []
for table_name in required_source_tables:
    try:
        spark.table(f"{catalog_name}.{source_schema}.{table_name}").limit(1).count()
    except Exception:
        missing_tables.append(f"{catalog_name}.{source_schema}.{table_name}")

if missing_tables:
    raise Exception("Missing source tables: " + ", ".join(missing_tables))

print("All required source tables exist.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validar esquemas creados

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {catalog_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC Registrar ejecución exitosa

# COMMAND ----------


import uuid
run_id = str(uuid.uuid4())

spark.sql(f"""
INSERT INTO {catalog_name}.{audit_schema}.etl_run_log
SELECT
    '{run_id}' AS run_id,
    '00_create_catalog_and_schemas' AS process_name,
    'setup' AS layer_name,
    '{catalog_name}' AS target_table,
    current_timestamp() AS start_time,
    current_timestamp() AS end_time,
    'SUCCESS' AS status,
    0 AS rows_inserted,
    0 AS rows_updated,
    0 AS rows_deleted,
    NULL AS error_message,
    current_timestamp() AS created_at
""")

print(f"Setup completed. run_id={run_id}")