# Databricks notebook source
# MAGIC %md
# MAGIC # 03_Gold — Carga inicial del modelo dimensional
# MAGIC
# MAGIC **Objetivo:** poblar dimensiones y hechos Gold desde Silver.
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
# MAGIC ## Reiniciar tablas Gold manteniendo diseño

# COMMAND ----------


for table_name in [
    "fact_sales",
    "dim_customer", "dim_product", "dim_store", "dim_promotion", "dim_date",
    "dim_channel", "dim_payment_method", "dim_order_status"
]:
    spark.sql(f"DELETE FROM {catalog_name}.{gold_schema}.{table_name}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Insertar registros Unknown / Default

# COMMAND ----------

spark.sql(f"""
INSERT INTO {catalog_name}.{gold_schema}.dim_customer (
    customer_key,
    customer_id,
    customer_code,
    customer_name,
    email,
    country,
    signup_date,
    customer_status,
    effective_from,
    effective_to,
    is_current,
    record_hash,
    created_at,
    updated_at
)
SELECT
    -1 AS customer_key,
    -1 AS customer_id,
    'UNKNOWN' AS customer_code,
    'Unknown Customer' AS customer_name,
    'unknown@email.com' AS email,
    'Unknown' AS country,
    DATE('1900-01-01') AS signup_date,
    'unknown' AS customer_status,
    TIMESTAMP('1900-01-01 00:00:00') AS effective_from,
    NULL AS effective_to,
    true AS is_current,
    NULL AS record_hash,
    current_timestamp() AS created_at,
    current_timestamp() AS updated_at
WHERE NOT EXISTS (
    SELECT 1
    FROM {catalog_name}.{gold_schema}.dim_customer
    WHERE customer_key = -1
)
""")

spark.sql(f"""
INSERT INTO {catalog_name}.{gold_schema}.dim_product (
    product_key,
    product_id,
    product_code,
    product_name,
    category,
    subcategory,
    brand,
    price,
    is_active,
    effective_from,
    effective_to,
    is_current,
    record_hash,
    created_at,
    updated_at
)
SELECT
    -1 AS product_key,
    -1 AS product_id,
    'UNKNOWN' AS product_code,
    'Unknown Product' AS product_name,
    'Unknown' AS category,
    'Unknown' AS subcategory,
    'Unknown' AS brand,
    CAST(0 AS DECIMAL(12,2)) AS price,
    false AS is_active,
    TIMESTAMP('1900-01-01 00:00:00') AS effective_from,
    NULL AS effective_to,
    true AS is_current,
    NULL AS record_hash,
    current_timestamp() AS created_at,
    current_timestamp() AS updated_at
WHERE NOT EXISTS (
    SELECT 1
    FROM {catalog_name}.{gold_schema}.dim_product
    WHERE product_key = -1
)
""")

spark.sql(f"""
INSERT INTO {catalog_name}.{gold_schema}.dim_store (
    store_key,
    store_id,
    store_code,
    store_name,
    region,
    country,
    open_date,
    effective_from,
    effective_to,
    is_current,
    record_hash,
    created_at,
    updated_at
)
SELECT
    -1 AS store_key,
    -1 AS store_id,
    'UNKNOWN' AS store_code,
    'Unknown Store' AS store_name,
    'Unknown' AS region,
    'Unknown' AS country,
    DATE('1900-01-01') AS open_date,
    TIMESTAMP('1900-01-01 00:00:00') AS effective_from,
    NULL AS effective_to,
    true AS is_current,
    NULL AS record_hash,
    current_timestamp() AS created_at,
    current_timestamp() AS updated_at
WHERE NOT EXISTS (
    SELECT 1
    FROM {catalog_name}.{gold_schema}.dim_store
    WHERE store_key = -1
)
""")

spark.sql(f"""
INSERT INTO {catalog_name}.{gold_schema}.dim_promotion (
    promotion_key,
    promotion_id,
    promotion_code,
    product_id,
    channel,
    discount_pct,
    start_date,
    end_date,
    created_at,
    updated_at
)
SELECT
    -1 AS promotion_key,
    -1 AS promotion_id,
    'NO_PROMOTION' AS promotion_code,
    -1 AS product_id,
    'all' AS channel,
    CAST(0 AS DECIMAL(5,2)) AS discount_pct,
    DATE('1900-01-01') AS start_date,
    DATE('9999-12-31') AS end_date,
    current_timestamp() AS created_at,
    current_timestamp() AS updated_at
WHERE NOT EXISTS (
    SELECT 1
    FROM {catalog_name}.{gold_schema}.dim_promotion
    WHERE promotion_key = -1
)
""")

spark.sql(f"""
INSERT INTO {catalog_name}.{gold_schema}.dim_channel (
    channel_key,
    channel_name,
    created_at
)
SELECT
    -1 AS channel_key,
    'unknown' AS channel_name,
    current_timestamp() AS created_at
WHERE NOT EXISTS (
    SELECT 1
    FROM {catalog_name}.{gold_schema}.dim_channel
    WHERE channel_key = -1
)
""")

spark.sql(f"""
INSERT INTO {catalog_name}.{gold_schema}.dim_payment_method (
    payment_method_key,
    payment_method_name,
    created_at
)
SELECT
    -1 AS payment_method_key,
    'unknown' AS payment_method_name,
    current_timestamp() AS created_at
WHERE NOT EXISTS (
    SELECT 1
    FROM {catalog_name}.{gold_schema}.dim_payment_method
    WHERE payment_method_key = -1
)
""")

spark.sql(f"""
INSERT INTO {catalog_name}.{gold_schema}.dim_order_status (
    order_status_key,
    order_status_name,
    is_cancelled,
    is_returned,
    created_at
)
SELECT
    -1 AS order_status_key,
    'unknown' AS order_status_name,
    false AS is_cancelled,
    false AS is_returned,
    current_timestamp() AS created_at
WHERE NOT EXISTS (
    SELECT 1
    FROM {catalog_name}.{gold_schema}.dim_order_status
    WHERE order_status_key = -1
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cargar dimensiones SCD base

# COMMAND ----------

spark.sql(f"""
INSERT INTO {catalog_name}.{gold_schema}.dim_customer (
    customer_key,
    customer_id,
    customer_code,
    customer_name,
    email,
    country,
    signup_date,
    customer_status,
    effective_from,
    effective_to,
    is_current,
    record_hash,
    created_at,
    updated_at
)
SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_key,
    customer_id,
    customer_code,
    customer_name,
    email,
    country,
    signup_date,
    customer_status,
    current_timestamp() AS effective_from,
    NULL AS effective_to,
    true AS is_current,
    record_hash,
    current_timestamp() AS created_at,
    current_timestamp() AS updated_at
FROM {catalog_name}.{silver_schema}.customers_clean
WHERE customer_id IS NOT NULL
""")

spark.sql(f"""
INSERT INTO {catalog_name}.{gold_schema}.dim_product (
    product_key,
    product_id,
    product_code,
    product_name,
    category,
    subcategory,
    brand,
    price,
    is_active,
    effective_from,
    effective_to,
    is_current,
    record_hash,
    created_at,
    updated_at
)
SELECT
    ROW_NUMBER() OVER (ORDER BY product_id) AS product_key,
    product_id,
    product_code,
    product_name,
    category,
    subcategory,
    brand,
    price,
    is_active,
    current_timestamp() AS effective_from,
    NULL AS effective_to,
    true AS is_current,
    record_hash,
    current_timestamp() AS created_at,
    current_timestamp() AS updated_at
FROM {catalog_name}.{silver_schema}.products_clean
WHERE product_id IS NOT NULL
""")

spark.sql(f"""
INSERT INTO {catalog_name}.{gold_schema}.dim_store (
    store_key,
    store_id,
    store_code,
    store_name,
    region,
    country,
    open_date,
    effective_from,
    effective_to,
    is_current,
    record_hash,
    created_at,
    updated_at
)
SELECT
    ROW_NUMBER() OVER (ORDER BY store_id) AS store_key,
    store_id,
    store_code,
    store_name,
    region,
    country,
    open_date,
    current_timestamp() AS effective_from,
    NULL AS effective_to,
    true AS is_current,
    record_hash,
    current_timestamp() AS created_at,
    current_timestamp() AS updated_at
FROM {catalog_name}.{silver_schema}.stores_clean
WHERE store_id IS NOT NULL
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cargar promociones y dimensiones pequeñas

# COMMAND ----------

spark.sql(f"""
INSERT INTO {catalog_name}.{gold_schema}.dim_promotion (
    promotion_key,
    promotion_id,
    promotion_code,
    product_id,
    channel,
    discount_pct,
    start_date,
    end_date,
    created_at,
    updated_at
)
SELECT
    ROW_NUMBER() OVER (ORDER BY promotion_id) AS promotion_key,
    promotion_id,
    promotion_code,
    product_id,
    channel,
    discount_pct,
    start_date,
    end_date,
    current_timestamp() AS created_at,
    current_timestamp() AS updated_at
FROM {catalog_name}.{silver_schema}.promotions_clean
WHERE promotion_id IS NOT NULL
""")

spark.sql(f"""
INSERT INTO {catalog_name}.{gold_schema}.dim_channel (
    channel_key,
    channel_name,
    created_at
)
SELECT
    ROW_NUMBER() OVER (ORDER BY channel) AS channel_key,
    channel AS channel_name,
    current_timestamp() AS created_at
FROM (
    SELECT DISTINCT channel
    FROM {catalog_name}.{silver_schema}.orders_clean
    WHERE channel IS NOT NULL
      AND lower(channel) <> 'unknown'
)
""")

spark.sql(f"""
INSERT INTO {catalog_name}.{gold_schema}.dim_payment_method (
    payment_method_key,
    payment_method_name,
    created_at
)
SELECT
    ROW_NUMBER() OVER (ORDER BY payment_method) AS payment_method_key,
    payment_method AS payment_method_name,
    current_timestamp() AS created_at
FROM (
    SELECT DISTINCT payment_method
    FROM {catalog_name}.{silver_schema}.orders_clean
    WHERE payment_method IS NOT NULL
      AND lower(payment_method) <> 'unknown'
)
""")

spark.sql(f"""
INSERT INTO {catalog_name}.{gold_schema}.dim_order_status (
    order_status_key,
    order_status_name,
    is_cancelled,
    is_returned,
    created_at
)
SELECT
    ROW_NUMBER() OVER (ORDER BY order_status) AS order_status_key,
    order_status AS order_status_name,
    CASE WHEN lower(order_status) = 'cancelled' THEN true ELSE false END AS is_cancelled,
    CASE WHEN lower(order_status) = 'returned' THEN true ELSE false END AS is_returned,
    current_timestamp() AS created_at
FROM (
    SELECT DISTINCT order_status
    FROM {catalog_name}.{silver_schema}.orders_clean
    WHERE order_status IS NOT NULL
      AND lower(order_status) <> 'unknown'
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cargar dimensión fecha

# COMMAND ----------


spark.sql(f"""
INSERT INTO {catalog_name}.{gold_schema}.dim_date
WITH date_bounds AS (
    SELECT MIN(order_date_only) AS min_date, MAX(order_date_only) AS max_date
    FROM {catalog_name}.{silver_schema}.orders_clean
), calendar AS (
    SELECT EXPLODE(SEQUENCE(min_date, max_date, INTERVAL 1 DAY)) AS full_date
    FROM date_bounds
)
SELECT
    CAST(DATE_FORMAT(full_date, 'yyyyMMdd') AS INT) AS date_key,
    full_date,
    DAY(full_date) AS day_number,
    DATE_FORMAT(full_date, 'EEEE') AS day_name,
    WEEKOFYEAR(full_date) AS week_number,
    MONTH(full_date) AS month_number,
    DATE_FORMAT(full_date, 'MMMM') AS month_name,
    QUARTER(full_date) AS quarter_number,
    YEAR(full_date) AS year_number,
    CASE WHEN DAYOFWEEK(full_date) IN (1, 7) THEN true ELSE false END AS is_weekend
FROM calendar
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Cargar tabla de hechos `fact_sales`

# COMMAND ----------

spark.sql(f"""
INSERT INTO {catalog_name}.{gold_schema}.fact_sales (
    sales_key,
    order_id,
    order_item_id,
    order_date_key,
    customer_key,
    product_key,
    store_key,
    promotion_key,
    channel_key,
    payment_method_key,
    order_status_key,
    quantity,
    unit_price,
    gross_amount,
    discount_pct,
    discount_amount,
    net_amount,
    source_discount_amount,
    source_line_amount,
    is_cancelled,
    is_returned,
    source_record_hash,
    created_at,
    updated_at
)
SELECT
    ROW_NUMBER() OVER (ORDER BY se.order_item_id) AS sales_key,
    se.order_id,
    se.order_item_id,
    COALESCE(dd.date_key, -1) AS order_date_key,
    COALESCE(dc.customer_key, -1) AS customer_key,
    COALESCE(dp.product_key, -1) AS product_key,
    COALESCE(ds.store_key, -1) AS store_key,
    COALESCE(dpr.promotion_key, -1) AS promotion_key,
    COALESCE(dch.channel_key, -1) AS channel_key,
    COALESCE(dpm.payment_method_key, -1) AS payment_method_key,
    COALESCE(dos.order_status_key, -1) AS order_status_key,
    se.quantity,
    se.unit_price,
    CAST(se.gross_amount AS DECIMAL(14,2)) AS gross_amount,
    se.discount_pct,
    CAST(se.calculated_discount_amount AS DECIMAL(14,2)) AS discount_amount,
    CAST(se.calculated_net_amount AS DECIMAL(14,2)) AS net_amount,
    CAST(se.source_discount_amount AS DECIMAL(14,2)) AS source_discount_amount,
    CAST(se.source_line_amount AS DECIMAL(14,2)) AS source_line_amount,
    se.is_cancelled,
    se.is_returned,
    se.fact_record_hash AS source_record_hash,
    current_timestamp() AS created_at,
    current_timestamp() AS updated_at
FROM {catalog_name}.{silver_schema}.sales_enriched se
LEFT JOIN {catalog_name}.{gold_schema}.dim_date dd
    ON se.order_date_key = dd.date_key
LEFT JOIN {catalog_name}.{gold_schema}.dim_customer dc
    ON se.customer_id = dc.customer_id
   AND dc.is_current = true
LEFT JOIN {catalog_name}.{gold_schema}.dim_product dp
    ON se.product_id = dp.product_id
   AND dp.is_current = true
LEFT JOIN {catalog_name}.{gold_schema}.dim_store ds
    ON COALESCE(se.store_id, -1) = ds.store_id
   AND ds.is_current = true
LEFT JOIN {catalog_name}.{gold_schema}.dim_promotion dpr
    ON COALESCE(se.resolved_promotion_id, -1) = dpr.promotion_id
LEFT JOIN {catalog_name}.{gold_schema}.dim_channel dch
    ON se.channel = dch.channel_name
LEFT JOIN {catalog_name}.{gold_schema}.dim_payment_method dpm
    ON se.payment_method = dpm.payment_method_name
LEFT JOIN {catalog_name}.{gold_schema}.dim_order_status dos
    ON se.order_status = dos.order_status_name
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimizar tablas Gold

# COMMAND ----------

for statement in [
    f"OPTIMIZE {catalog_name}.{gold_schema}.fact_sales ZORDER BY (order_date_key, customer_key, product_key)",
    f"OPTIMIZE {catalog_name}.{gold_schema}.dim_customer ZORDER BY (customer_id)",
    f"OPTIMIZE {catalog_name}.{gold_schema}.dim_product ZORDER BY (product_id)",
    f"OPTIMIZE {catalog_name}.{gold_schema}.dim_promotion ZORDER BY (product_id, start_date, end_date)"
]:
    try:
        spark.sql(statement)
    except Exception as e:
        print(f"Optimization skipped or failed: {statement} -> {e}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Validaciones de carga inicial

# COMMAND ----------

display(spark.sql(f"""
SELECT 'dim_customer' AS table_name, COUNT(*) AS rows_count FROM {catalog_name}.{gold_schema}.dim_customer
UNION ALL SELECT 'dim_product', COUNT(*) FROM {catalog_name}.{gold_schema}.dim_product
UNION ALL SELECT 'dim_store', COUNT(*) FROM {catalog_name}.{gold_schema}.dim_store
UNION ALL SELECT 'dim_promotion', COUNT(*) FROM {catalog_name}.{gold_schema}.dim_promotion
UNION ALL SELECT 'dim_date', COUNT(*) FROM {catalog_name}.{gold_schema}.dim_date
UNION ALL SELECT 'fact_sales', COUNT(*) FROM {catalog_name}.{gold_schema}.fact_sales
"""))

display(spark.sql(f"""
SELECT
    dch.channel_name,
    COUNT(DISTINCT fs.order_id) AS orders,
    COUNT(*) AS order_lines,
    SUM(fs.gross_amount) AS gross_sales,
    SUM(fs.discount_amount) AS discount_amount,
    SUM(fs.net_amount) AS net_sales
FROM {catalog_name}.{gold_schema}.fact_sales fs
LEFT JOIN {catalog_name}.{gold_schema}.dim_channel dch
    ON fs.channel_key = dch.channel_key
GROUP BY dch.channel_name
ORDER BY dch.channel_name
"""))
