# Databricks notebook source
# MAGIC %md
# MAGIC # 02_Silver — Limpieza, enriquecimiento y resolución de promociones
# MAGIC
# MAGIC **Objetivo:** construir la capa Silver desde Bronze, aplicando limpieza básica, normalización de catálogos, enriquecimiento de líneas de venta y resolución explícita de promociones a nivel de línea de pedido.
# MAGIC

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


# COMMAND ----------

# MAGIC %md
# MAGIC ##  Crear tablas Silver limpias desde Bronze

# COMMAND ----------



spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{silver_schema}.customers_clean
USING DELTA AS
SELECT
    CAST(customer_id AS INT) AS customer_id,
    TRIM(customer_code) AS customer_code,
    INITCAP(TRIM(customer_name)) AS customer_name,
    LOWER(TRIM(email)) AS email,
    INITCAP(TRIM(country)) AS country,
    CAST(signup_date AS DATE) AS signup_date,
    LOWER(TRIM(customer_status)) AS customer_status,
    SHA2(CONCAT_WS('||', customer_code, customer_name, email, country, CAST(signup_date AS STRING), customer_status), 256) AS record_hash,
    current_timestamp() AS silver_processed_at,
    '{run_id}' AS silver_run_id
FROM {catalog_name}.{bronze_schema}.customers
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{silver_schema}.products_clean
USING DELTA AS
SELECT
    CAST(product_id AS INT) AS product_id,
    TRIM(product_code) AS product_code,
    INITCAP(TRIM(product_name)) AS product_name,
    INITCAP(TRIM(category)) AS category,
    INITCAP(TRIM(subcategory)) AS subcategory,
    INITCAP(TRIM(brand)) AS brand,
    CAST(price AS DECIMAL(12,2)) AS price,
    CAST(is_active AS BOOLEAN) AS is_active,
    SHA2(CONCAT_WS('||', product_code, product_name, category, subcategory, brand, CAST(price AS STRING), CAST(is_active AS STRING)), 256) AS record_hash,
    current_timestamp() AS silver_processed_at,
    '{run_id}' AS silver_run_id
FROM {catalog_name}.{bronze_schema}.products
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{silver_schema}.stores_clean
USING DELTA AS
SELECT
    CAST(store_id AS INT) AS store_id,
    TRIM(store_code) AS store_code,
    INITCAP(TRIM(store_name)) AS store_name,
    INITCAP(TRIM(region)) AS region,
    INITCAP(TRIM(country)) AS country,
    CAST(open_date AS DATE) AS open_date,
    SHA2(CONCAT_WS('||', store_code, store_name, region, country, CAST(open_date AS STRING)), 256) AS record_hash,
    current_timestamp() AS silver_processed_at,
    '{run_id}' AS silver_run_id
FROM {catalog_name}.{bronze_schema}.stores
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{silver_schema}.promotions_clean
USING DELTA AS
SELECT
    CAST(promotion_id AS INT) AS promotion_id,
    TRIM(promotion_code) AS promotion_code,
    CAST(product_id AS INT) AS product_id,
    CAST(start_date AS DATE) AS start_date,
    CAST(end_date AS DATE) AS end_date,
    CAST(discount_pct AS DECIMAL(5,2)) AS discount_pct,
    LOWER(TRIM(channel)) AS channel,
    current_timestamp() AS silver_processed_at,
    '{run_id}' AS silver_run_id
FROM {catalog_name}.{bronze_schema}.promotions
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear órdenes y líneas limpias

# COMMAND ----------


spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{silver_schema}.orders_clean
USING DELTA AS
SELECT
    CAST(order_id AS INT) AS order_id,
    CAST(order_date AS TIMESTAMP) AS order_date,
    CAST(TO_DATE(order_date) AS DATE) AS order_date_only,
    CAST(DATE_FORMAT(TO_DATE(order_date), 'yyyyMMdd') AS INT) AS order_date_key,
    CAST(customer_id AS INT) AS customer_id,
    LOWER(TRIM(channel)) AS channel,
    CAST(store_id AS INT) AS store_id,
    LOWER(TRIM(order_status)) AS order_status,
    LOWER(TRIM(payment_method)) AS payment_method,
    CAST(total_amount AS DECIMAL(14,2)) AS total_amount,
    CASE WHEN LOWER(TRIM(order_status)) = 'cancelled' THEN true ELSE false END AS is_cancelled,
    CASE WHEN LOWER(TRIM(order_status)) = 'returned' THEN true ELSE false END AS is_returned,
    SHA2(CONCAT_WS('||', order_id, CAST(order_date AS STRING), customer_id, channel, store_id, order_status, payment_method, CAST(total_amount AS STRING)), 256) AS record_hash,
    current_timestamp() AS silver_processed_at,
    '{run_id}' AS silver_run_id
FROM {catalog_name}.{bronze_schema}.orders
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{silver_schema}.order_items_clean
USING DELTA AS
SELECT
    CAST(order_item_id AS INT) AS order_item_id,
    CAST(order_id AS INT) AS order_id,
    CAST(product_id AS INT) AS product_id,
    CAST(quantity AS INT) AS quantity,
    CAST(unit_price AS DECIMAL(12,2)) AS unit_price,
    CAST(discount AS DECIMAL(12,2)) AS source_discount_amount,
    CAST(line_amount AS DECIMAL(14,2)) AS source_line_amount,
    CAST(quantity * unit_price AS DECIMAL(18,2)) AS gross_amount,
    SHA2(CONCAT_WS('||', order_item_id, order_id, product_id, quantity, CAST(unit_price AS STRING), CAST(discount AS STRING), CAST(line_amount AS STRING)), 256) AS record_hash,
    current_timestamp() AS silver_processed_at,
    '{run_id}' AS silver_run_id
FROM {catalog_name}.{bronze_schema}.order_items
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ##  Resolver promociones candidatas
# MAGIC
# MAGIC Regla implementada:
# MAGIC 1. Buscar promoción por `product_id`.
# MAGIC 2. La fecha del pedido debe estar entre `start_date` y `end_date`.
# MAGIC 3. El canal debe coincidir con el canal de la orden o ser `all`.
# MAGIC 4. Si hay más de una promoción, tomar la de mayor descuento.
# MAGIC 5. Si hay empate, tomar la de fecha de inicio más reciente.

# COMMAND ----------


spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{silver_schema}.promotion_resolution
USING DELTA AS
WITH candidates AS (
    SELECT
        oi.order_item_id,
        o.order_id,
        oi.product_id,
        o.channel AS order_channel,
        o.order_date_only,
        p.promotion_id,
        p.promotion_code,
        p.discount_pct,
        p.start_date,
        p.end_date,
        p.channel AS promotion_channel,
        ROW_NUMBER() OVER (
            PARTITION BY oi.order_item_id
            ORDER BY p.discount_pct DESC, p.start_date DESC, p.promotion_id DESC
        ) AS rn
    FROM {catalog_name}.{silver_schema}.order_items_clean oi
    INNER JOIN {catalog_name}.{silver_schema}.orders_clean o
        ON oi.order_id = o.order_id
    INNER JOIN {catalog_name}.{silver_schema}.promotions_clean p
        ON oi.product_id = p.product_id
       AND o.order_date_only BETWEEN p.start_date AND p.end_date
       AND (p.channel = o.channel OR p.channel = 'all')
)
SELECT
    order_item_id,
    order_id,
    product_id,
    order_channel,
    order_date_only,
    promotion_id,
    promotion_code,
    discount_pct,
    start_date,
    end_date,
    promotion_channel,
    current_timestamp() AS resolved_at,
    '{run_id}' AS silver_run_id
FROM candidates
WHERE rn = 1
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear tabla Silver enriquecida de ventas

# COMMAND ----------



spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{silver_schema}.sales_enriched
USING DELTA AS
SELECT
    o.order_id,
    oi.order_item_id,
    o.order_date,
    o.order_date_only,
    o.order_date_key,
    o.customer_id,
    o.store_id,
    o.channel,
    o.payment_method,
    o.order_status,
    o.is_cancelled,
    o.is_returned,

    oi.product_id,
    oi.quantity,
    oi.unit_price,
    oi.gross_amount,

    COALESCE(pr.promotion_id, -1) AS resolved_promotion_id,
    COALESCE(pr.promotion_code, 'NO_PROMOTION') AS resolved_promotion_code,
    COALESCE(CAST(pr.discount_pct AS DECIMAL(5,2)), CAST(0 AS DECIMAL(5,2))) AS discount_pct,

    CAST(oi.gross_amount * COALESCE(pr.discount_pct, 0) / 100 AS DECIMAL(18,2)) AS calculated_discount_amount,
    CAST(oi.gross_amount - (oi.gross_amount * COALESCE(pr.discount_pct, 0) / 100) AS DECIMAL(18,2)) AS calculated_net_amount,

    oi.source_discount_amount,
    oi.source_line_amount,

    c.customer_code,
    c.customer_name,
    c.country AS customer_country,
    c.customer_status,

    p.product_code,
    p.product_name,
    p.category,
    p.subcategory,
    p.brand,
    p.price AS product_list_price,
    p.is_active AS product_is_active,

    s.store_code,
    s.store_name,
    s.region AS store_region,
    s.country AS store_country,

    SHA2(CONCAT_WS('||',
        o.order_id, oi.order_item_id, o.order_date, o.customer_id, o.store_id,
        o.channel, o.payment_method, o.order_status, oi.product_id, oi.quantity,
        CAST(oi.unit_price AS STRING), COALESCE(pr.promotion_id, -1)
    ), 256) AS fact_record_hash,

    current_timestamp() AS silver_processed_at,
    '{run_id}' AS silver_run_id
FROM {catalog_name}.{silver_schema}.orders_clean o
INNER JOIN {catalog_name}.{silver_schema}.order_items_clean oi
    ON o.order_id = oi.order_id
LEFT JOIN {catalog_name}.{silver_schema}.promotion_resolution pr
    ON oi.order_item_id = pr.order_item_id
LEFT JOIN {catalog_name}.{silver_schema}.customers_clean c
    ON o.customer_id = c.customer_id
LEFT JOIN {catalog_name}.{silver_schema}.products_clean p
    ON oi.product_id = p.product_id
LEFT JOIN {catalog_name}.{silver_schema}.stores_clean s
    ON o.store_id = s.store_id
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Validaciones Silver

# COMMAND ----------


display(spark.sql(f"""
SELECT 'sales_enriched' AS table_name, COUNT(*) AS total_rows FROM {catalog_name}.{silver_schema}.sales_enriched
UNION ALL SELECT 'promotion_resolution', COUNT(*) FROM {catalog_name}.{silver_schema}.promotion_resolution
UNION ALL SELECT 'customers_clean', COUNT(*) FROM {catalog_name}.{silver_schema}.customers_clean
UNION ALL SELECT 'products_clean', COUNT(*) FROM {catalog_name}.{silver_schema}.products_clean
UNION ALL SELECT 'orders_clean', COUNT(*) FROM {catalog_name}.{silver_schema}.orders_clean
UNION ALL SELECT 'order_items_clean', COUNT(*) FROM {catalog_name}.{silver_schema}.order_items_clean
"""))

display(spark.sql(f"""
SELECT
    channel,
    COUNT(*) AS lines,
    SUM(gross_amount) AS gross_amount,
    SUM(calculated_discount_amount) AS discount_amount,
    SUM(calculated_net_amount) AS net_amount
FROM {catalog_name}.{silver_schema}.sales_enriched
GROUP BY channel
ORDER BY channel
"""))
