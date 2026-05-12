# Databricks notebook source
# MAGIC %md
# MAGIC # 04_Incremental — Simulación de cambios incrementales
# MAGIC
# MAGIC **Objetivo:** generar un delta reproducible sobre las tablas fuente `dbo` para demostrar nuevos registros, cambios de dimensiones, cancelaciones y correcciones de líneas.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "fi_group")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.text("silver_schema", "silver")
dbutils.widgets.text("gold_schema", "gold")
dbutils.widgets.text("audit_schema", "audit")
dbutils.widgets.text("run_id", "manual_run")

catalog_name = dbutils.widgets.get("catalog_name")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")
audit_schema = dbutils.widgets.get("audit_schema")
run_id = dbutils.widgets.get("run_id")

spark.sql(f"USE CATALOG {catalog_name}")

source_schema = "dbo"


# COMMAND ----------

# MAGIC %md
# MAGIC ## Agregar columna técnica `source_updated_at` al origen si no existe

# COMMAND ----------


for table_name in ["customers", "products", "stores", "promotions", "orders", "order_items"]:
    try:
        spark.sql(f"ALTER TABLE {catalog_name}.dbo.{table_name} ADD COLUMNS (source_updated_at TIMESTAMP)")
        print(f"Column added to {table_name}")
    except Exception as e:
        print(f"Column already exists or could not be added in {table_name}: {e}")

for table_name in ["customers", "products", "stores", "promotions", "orders", "order_items"]:
    spark.sql(f"UPDATE {catalog_name}.dbo.{table_name} SET source_updated_at = COALESCE(source_updated_at, TIMESTAMP('2026-01-01 00:00:00'))")


# COMMAND ----------

# MAGIC %md
# MAGIC ##  Simular cambios en dimensiones y catálogos

# COMMAND ----------

spark.sql(f"""
UPDATE {catalog_name}.dbo.customers
SET country = 'France', customer_status = 'active', source_updated_at = current_timestamp()
WHERE customer_id = 10
""")

spark.sql(f"""
UPDATE {catalog_name}.dbo.products
SET price = CAST(price * 1.10 AS DECIMAL(12,2)), source_updated_at = current_timestamp()
WHERE product_id = 5
""")

spark.sql(f"""
DELETE FROM {catalog_name}.dbo.promotions WHERE promotion_id = 900001
""")

spark.sql(f"""
INSERT INTO {catalog_name}.dbo.promotions
SELECT
    900001 AS promotion_id,
    'PROMO900001' AS promotion_code,
    5 AS product_id,
    DATE('2026-01-01') AS start_date,
    DATE('2026-12-31') AS end_date,
    CAST(25.00 AS DECIMAL(5,2)) AS discount_pct,
    'all' AS channel,
    current_timestamp() AS source_updated_at
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Simular nuevo pedido con líneas

# COMMAND ----------

spark.sql(f"DELETE FROM {catalog_name}.dbo.order_items WHERE order_id = 900001")
spark.sql(f"DELETE FROM {catalog_name}.dbo.orders WHERE order_id = 900001")

spark.sql(f"""
INSERT INTO {catalog_name}.dbo.orders
SELECT
    900001 AS order_id,
    current_timestamp() AS order_date,
    10 AS customer_id,
    'online' AS channel,
    CAST(NULL AS INT) AS store_id,
    'completed' AS order_status,
    'card' AS payment_method,
    CAST(0 AS DECIMAL(14,2)) AS total_amount,
    current_timestamp() AS source_updated_at
""")

spark.sql(f"""
INSERT INTO {catalog_name}.dbo.order_items
SELECT
    900001 AS order_item_id,
    900001 AS order_id,
    5 AS product_id,
    2 AS quantity,
    CAST((SELECT price FROM {catalog_name}.dbo.products WHERE product_id = 5) AS DECIMAL(12,2)) AS unit_price,
    CAST(0 AS DECIMAL(12,2)) AS discount,
    CAST(2 * (SELECT price FROM {catalog_name}.dbo.products WHERE product_id = 5) AS DECIMAL(14,2)) AS line_amount,
    current_timestamp() AS source_updated_at
UNION ALL
SELECT
    900002 AS order_item_id,
    900001 AS order_id,
    1 AS product_id,
    1 AS quantity,
    CAST((SELECT price FROM {catalog_name}.dbo.products WHERE product_id = 1) AS DECIMAL(12,2)) AS unit_price,
    CAST(0 AS DECIMAL(12,2)) AS discount,
    CAST((SELECT price FROM {catalog_name}.dbo.products WHERE product_id = 1) AS DECIMAL(14,2)) AS line_amount,
    current_timestamp() AS source_updated_at
""")

spark.sql(f"""
UPDATE {catalog_name}.dbo.orders o
SET total_amount = (
    SELECT CAST(SUM(line_amount) AS DECIMAL(14,2))
    FROM {catalog_name}.dbo.order_items oi
    WHERE oi.order_id = o.order_id
), source_updated_at = current_timestamp()
WHERE order_id = 900001
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ##  Simular cancelación y corrección de línea existente

# COMMAND ----------


spark.sql(f"""
UPDATE {catalog_name}.dbo.orders
SET order_status = 'cancelled', source_updated_at = current_timestamp()
WHERE order_id = 100
""")

spark.sql(f"""
UPDATE {catalog_name}.dbo.order_items
SET quantity = quantity + 1,
    line_amount = CAST((quantity + 1) * unit_price AS DECIMAL(14,2)),
    source_updated_at = current_timestamp()
WHERE order_item_id = 1000
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Validar delta simulado

# COMMAND ----------


display(spark.sql(f"""
SELECT 'customers changed' AS scenario, COUNT(*) AS records FROM {catalog_name}.dbo.customers WHERE customer_id = 10
UNION ALL SELECT 'products changed', COUNT(*) FROM {catalog_name}.dbo.products WHERE product_id = 5
UNION ALL SELECT 'new promotion', COUNT(*) FROM {catalog_name}.dbo.promotions WHERE promotion_id = 900001
UNION ALL SELECT 'new order', COUNT(*) FROM {catalog_name}.dbo.orders WHERE order_id = 900001
UNION ALL SELECT 'new order items', COUNT(*) FROM {catalog_name}.dbo.order_items WHERE order_id = 900001
UNION ALL SELECT 'cancelled order', COUNT(*) FROM {catalog_name}.dbo.orders WHERE order_id = 100 AND order_status = 'cancelled'
UNION ALL SELECT 'corrected order item', COUNT(*) FROM {catalog_name}.dbo.order_items WHERE order_item_id = 1000
"""))
