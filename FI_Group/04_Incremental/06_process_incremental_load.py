# Databricks notebook source
# MAGIC %md
# MAGIC # 04_Incremental — Procesamiento incremental con MERGE
# MAGIC
# MAGIC **Objetivo:** sincronizar cambios desde `dbo` hacia Bronze, refrescar Silver y actualizar Gold con `MERGE` y lógica SCD2 básica.
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
# MAGIC ## Preparar columnas técnicas en Bronze

# COMMAND ----------



for table_name in ["customers", "products", "stores", "promotions", "orders", "order_items"]:
    for col_def in ["source_updated_at TIMESTAMP", "bronze_ingestion_timestamp TIMESTAMP", "bronze_source_batch STRING", "bronze_run_id STRING"]:
        try:
            spark.sql(f"ALTER TABLE {catalog_name}.{bronze_schema}.{table_name} ADD COLUMNS ({col_def})")
        except Exception as e:
            pass


# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE incremental desde dbo hacia Bronze

# COMMAND ----------


merge_specs = {
    "customers": ["customer_id"],
    "products": ["product_id"],
    "stores": ["store_id"],
    "promotions": ["promotion_id"],
    "orders": ["order_id"],
    "order_items": ["order_item_id"],
}

technical_cols = [
    "bronze_ingestion_timestamp",
    "bronze_source_batch",
    "bronze_run_id",
    "source_updated_at"
]

for table_name, keys in merge_specs.items():

    source_table = f"{catalog_name}.dbo.{table_name}"
    target_table = f"{catalog_name}.{bronze_schema}.{table_name}"

    src_cols = spark.table(source_table).columns
    tgt_cols = spark.table(target_table).columns

    common_cols = [
        c for c in src_cols
        if c in tgt_cols and c not in technical_cols
    ]

    update_assignments = ",\n        ".join([
        f"t.{c} = s.{c}"
        for c in common_cols
        if c not in keys
    ])

    if update_assignments:
        update_assignments += f""",
        t.bronze_ingestion_timestamp = current_timestamp(),
        t.bronze_source_batch = 'incremental',
        t.bronze_run_id = '{run_id}',
        t.source_updated_at = current_timestamp()"""
    else:
        update_assignments = f"""
        t.bronze_ingestion_timestamp = current_timestamp(),
        t.bronze_source_batch = 'incremental',
        t.bronze_run_id = '{run_id}',
        t.source_updated_at = current_timestamp()"""

    insert_cols = common_cols + [
        "bronze_ingestion_timestamp",
        "bronze_source_batch",
        "bronze_run_id",
        "source_updated_at"
    ]

    insert_values = (
        [f"s.{c}" for c in common_cols]
        + ["current_timestamp()", "'incremental'", f"'{run_id}'", "current_timestamp()"]
    )

    on_clause = " AND ".join([f"t.{k} = s.{k}" for k in keys])

    merge_sql = f"""
    MERGE INTO {target_table} AS t
    USING {source_table} AS s
    ON {on_clause}
    WHEN MATCHED THEN UPDATE SET
        {update_assignments}
    WHEN NOT MATCHED THEN INSERT (
        {', '.join(insert_cols)}
    )
    VALUES (
        {', '.join(insert_values)}
    )
    """

    spark.sql(merge_sql)
    print(f"Merged table: {table_name}")


# COMMAND ----------

# MAGIC %md
# MAGIC ##  Refrescar Silver
# MAGIC
# MAGIC el refresh de Silver se hace recreando tablas derivadas desde Bronze.

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD2 incremental para dim_customer

# COMMAND ----------


# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW src_customer_changes AS
SELECT s.*
FROM {catalog_name}.{silver_schema}.customers_clean s
LEFT JOIN {catalog_name}.{gold_schema}.dim_customer d
    ON s.customer_id = d.customer_id AND d.is_current = true
WHERE d.customer_id IS NULL OR COALESCE(s.record_hash, '') <> COALESCE(d.record_hash, '')
""")

spark.sql(f"""
UPDATE {catalog_name}.{gold_schema}.dim_customer d
SET effective_to = current_timestamp(), is_current = false, updated_at = current_timestamp()
WHERE d.is_current = true
  AND EXISTS (
      SELECT 1 FROM src_customer_changes s
      WHERE s.customer_id = d.customer_id
        AND COALESCE(s.record_hash, '') <> COALESCE(d.record_hash, '')
  )
""")

spark.sql(f"""
INSERT INTO {catalog_name}.{gold_schema}.dim_customer
SELECT
    (SELECT COALESCE(MAX(customer_key), 0) FROM {catalog_name}.{gold_schema}.dim_customer) + ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_key,
    customer_id, customer_code, customer_name, email, country, signup_date, customer_status,
    current_timestamp() AS effective_from,
    NULL AS effective_to,
    true AS is_current,
    record_hash,
    current_timestamp() AS created_at,
    current_timestamp() AS updated_at
FROM src_customer_changes
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD2 incremental para dim_product

# COMMAND ----------


spark.sql(f"""
CREATE OR REPLACE TEMP VIEW src_product_changes AS
SELECT s.*
FROM {catalog_name}.{silver_schema}.products_clean s
LEFT JOIN {catalog_name}.{gold_schema}.dim_product d
    ON s.product_id = d.product_id AND d.is_current = true
WHERE d.product_id IS NULL OR COALESCE(s.record_hash, '') <> COALESCE(d.record_hash, '')
""")

spark.sql(f"""
UPDATE {catalog_name}.{gold_schema}.dim_product d
SET effective_to = current_timestamp(), is_current = false, updated_at = current_timestamp()
WHERE d.is_current = true
  AND EXISTS (
      SELECT 1 FROM src_product_changes s
      WHERE s.product_id = d.product_id
        AND COALESCE(s.record_hash, '') <> COALESCE(d.record_hash, '')
  )
""")

spark.sql(f"""
INSERT INTO {catalog_name}.{gold_schema}.dim_product
SELECT
    (SELECT COALESCE(MAX(product_key), 0) FROM {catalog_name}.{gold_schema}.dim_product) + ROW_NUMBER() OVER (ORDER BY product_id) AS product_key,
    product_id, product_code, product_name, category, subcategory, brand, price, is_active,
    current_timestamp() AS effective_from,
    NULL AS effective_to,
    true AS is_current,
    record_hash,
    current_timestamp() AS created_at,
    current_timestamp() AS updated_at
FROM src_product_changes
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE para promociones y dimensiones pequeñas

# COMMAND ----------


spark.sql(f"""
MERGE INTO {catalog_name}.{gold_schema}.dim_promotion AS t
USING (
    SELECT
        COALESCE(existing.promotion_key,
            COALESCE((SELECT MAX(promotion_key) FROM {catalog_name}.{gold_schema}.dim_promotion), 0)
            + ROW_NUMBER() OVER (ORDER BY s.promotion_id)
        ) AS promotion_key,
        s.promotion_id,
        s.promotion_code,
        s.product_id,
        s.channel,
        s.discount_pct,
        s.start_date,
        s.end_date
    FROM {catalog_name}.{silver_schema}.promotions_clean s
    LEFT JOIN {catalog_name}.{gold_schema}.dim_promotion existing
        ON s.promotion_id = existing.promotion_id
    WHERE s.promotion_id IS NOT NULL
) AS s
ON t.promotion_id = s.promotion_id
WHEN MATCHED AND (
       COALESCE(t.promotion_code, '') <> COALESCE(s.promotion_code, '')
    OR COALESCE(t.product_id, -1) <> COALESCE(s.product_id, -1)
    OR COALESCE(lower(t.channel), '') <> COALESCE(lower(s.channel), '')
    OR COALESCE(t.discount_pct, CAST(0 AS DECIMAL(5,2))) <> COALESCE(s.discount_pct, CAST(0 AS DECIMAL(5,2)))
    OR COALESCE(t.start_date, DATE('1900-01-01')) <> COALESCE(s.start_date, DATE('1900-01-01'))
    OR COALESCE(t.end_date, DATE('9999-12-31')) <> COALESCE(s.end_date, DATE('9999-12-31'))
)
THEN UPDATE SET
    t.promotion_code = s.promotion_code,
    t.product_id = s.product_id,
    t.channel = s.channel,
    t.discount_pct = s.discount_pct,
    t.start_date = s.start_date,
    t.end_date = s.end_date,
    t.updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
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
VALUES (
    s.promotion_key,
    s.promotion_id,
    s.promotion_code,
    s.product_id,
    s.channel,
    s.discount_pct,
    s.start_date,
    s.end_date,
    current_timestamp(),
    current_timestamp()
)
""")

for tbl in ["dim_channel", "dim_payment_method", "dim_order_status"]:
    spark.sql(f"DELETE FROM {catalog_name}.{gold_schema}.{tbl} WHERE 1=1")

spark.sql(f"""
INSERT INTO {catalog_name}.{gold_schema}.dim_channel (
    channel_key,
    channel_name,
    created_at
)
SELECT -1, 'unknown', current_timestamp()
UNION ALL
SELECT
    ROW_NUMBER() OVER (ORDER BY channel) AS channel_key,
    channel AS channel_name,
    current_timestamp() AS created_at
FROM (
    SELECT DISTINCT lower(channel) AS channel
    FROM {catalog_name}.{silver_schema}.orders_clean
    WHERE channel IS NOT NULL
)
""")

spark.sql(f"""
INSERT INTO {catalog_name}.{gold_schema}.dim_payment_method (
    payment_method_key,
    payment_method_name,
    created_at
)
SELECT -1, 'unknown', current_timestamp()
UNION ALL
SELECT
    ROW_NUMBER() OVER (ORDER BY payment_method) AS payment_method_key,
    payment_method AS payment_method_name,
    current_timestamp() AS created_at
FROM (
    SELECT DISTINCT lower(payment_method) AS payment_method
    FROM {catalog_name}.{silver_schema}.orders_clean
    WHERE payment_method IS NOT NULL
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
SELECT -1, 'unknown', false, false, current_timestamp()
UNION ALL
SELECT
    ROW_NUMBER() OVER (ORDER BY order_status) AS order_status_key,
    order_status AS order_status_name,
    CASE WHEN order_status = 'cancelled' THEN true ELSE false END AS is_cancelled,
    CASE WHEN order_status = 'returned' THEN true ELSE false END AS is_returned,
    current_timestamp() AS created_at
FROM (
    SELECT DISTINCT lower(order_status) AS order_status
    FROM {catalog_name}.{silver_schema}.orders_clean
    WHERE order_status IS NOT NULL
)
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Actualizar dim_date para nuevas fechas

# COMMAND ----------


spark.sql(f"""
MERGE INTO {catalog_name}.{gold_schema}.dim_date t
USING (
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
) s
ON t.date_key = s.date_key
WHEN NOT MATCHED THEN INSERT *
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ##  MERGE incremental de fact_sales

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW src_fact_sales_incremental AS
WITH source_prepared AS (
    SELECT
        se.*,
        ROW_NUMBER() OVER (ORDER BY se.order_item_id) AS new_row_number
    FROM {catalog_name}.{silver_schema}.sales_enriched se
), source_mapped AS (
    SELECT
        COALESCE(
            existing.sales_key,
            (SELECT COALESCE(MAX(sales_key), 0) FROM {catalog_name}.{gold_schema}.fact_sales) + sp.new_row_number
        ) AS sales_key,

        sp.order_id,
        sp.order_item_id,
        COALESCE(dd.date_key, sp.order_date_key, -1) AS order_date_key,

        COALESCE(dc.customer_key, -1) AS customer_key,
        COALESCE(dp.product_key, -1) AS product_key,
        COALESCE(ds.store_key, -1) AS store_key,
        COALESCE(dpr.promotion_key, -1) AS promotion_key,
        COALESCE(dch.channel_key, -1) AS channel_key,
        COALESCE(dpm.payment_method_key, -1) AS payment_method_key,
        COALESCE(dos.order_status_key, -1) AS order_status_key,

        sp.quantity,
        sp.unit_price,
        CAST(COALESCE(sp.gross_amount, 0) AS DECIMAL(18,2)) AS gross_amount,
        CAST(COALESCE(sp.discount_pct, 0) AS DECIMAL(9,2)) AS discount_pct,
        CAST(COALESCE(sp.calculated_discount_amount, 0) AS DECIMAL(18,2)) AS discount_amount,
        CAST(COALESCE(sp.calculated_net_amount, COALESCE(sp.gross_amount, 0) - COALESCE(sp.calculated_discount_amount, 0)) AS DECIMAL(18,2)) AS net_amount,
        CAST(COALESCE(sp.source_discount_amount, 0) AS DECIMAL(18,2)) AS source_discount_amount,
        CAST(COALESCE(sp.source_line_amount, sp.gross_amount, 0) AS DECIMAL(18,2)) AS source_line_amount,

        COALESCE(sp.is_cancelled, false) AS is_cancelled,
        COALESCE(sp.is_returned, false) AS is_returned,
        sp.fact_record_hash AS source_record_hash

    FROM source_prepared sp

    LEFT JOIN {catalog_name}.{gold_schema}.fact_sales existing
        ON sp.order_item_id = existing.order_item_id

    LEFT JOIN {catalog_name}.{gold_schema}.dim_date dd
        ON sp.order_date_key = dd.date_key

    LEFT JOIN {catalog_name}.{gold_schema}.dim_customer dc
        ON sp.customer_id = dc.customer_id
        AND dc.is_current = true

    LEFT JOIN {catalog_name}.{gold_schema}.dim_product dp
        ON sp.product_id = dp.product_id
        AND dp.is_current = true

    LEFT JOIN {catalog_name}.{gold_schema}.dim_store ds
        ON COALESCE(sp.store_id, -1) = ds.store_id
        AND ds.is_current = true

    LEFT JOIN {catalog_name}.{gold_schema}.dim_promotion dpr
        ON COALESCE(sp.resolved_promotion_id, -1) = dpr.promotion_id

    LEFT JOIN {catalog_name}.{gold_schema}.dim_channel dch
        ON lower(COALESCE(sp.channel, 'unknown')) = lower(dch.channel_name)

    LEFT JOIN {catalog_name}.{gold_schema}.dim_payment_method dpm
        ON lower(COALESCE(sp.payment_method, 'unknown')) = lower(dpm.payment_method_name)

    LEFT JOIN {catalog_name}.{gold_schema}.dim_order_status dos
        ON lower(COALESCE(sp.order_status, 'unknown')) = lower(dos.order_status_name)
)
SELECT
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
    source_record_hash
FROM source_mapped
""")

spark.sql(f"""
MERGE INTO {catalog_name}.{gold_schema}.fact_sales AS t
USING src_fact_sales_incremental AS s
ON t.order_item_id = s.order_item_id
WHEN MATCHED
AND COALESCE(t.source_record_hash, '') <> COALESCE(s.source_record_hash, '')
THEN UPDATE SET
    t.order_id = s.order_id,
    t.order_date_key = s.order_date_key,
    t.customer_key = s.customer_key,
    t.product_key = s.product_key,
    t.store_key = s.store_key,
    t.promotion_key = s.promotion_key,
    t.channel_key = s.channel_key,
    t.payment_method_key = s.payment_method_key,
    t.order_status_key = s.order_status_key,
    t.quantity = s.quantity,
    t.unit_price = s.unit_price,
    t.gross_amount = s.gross_amount,
    t.discount_pct = s.discount_pct,
    t.discount_amount = s.discount_amount,
    t.net_amount = s.net_amount,
    t.source_discount_amount = s.source_discount_amount,
    t.source_line_amount = s.source_line_amount,
    t.is_cancelled = s.is_cancelled,
    t.is_returned = s.is_returned,
    t.source_record_hash = s.source_record_hash,
    t.updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
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
VALUES (
    s.sales_key,
    s.order_id,
    s.order_item_id,
    s.order_date_key,
    s.customer_key,
    s.product_key,
    s.store_key,
    s.promotion_key,
    s.channel_key,
    s.payment_method_key,
    s.order_status_key,
    s.quantity,
    s.unit_price,
    s.gross_amount,
    s.discount_pct,
    s.discount_amount,
    s.net_amount,
    s.source_discount_amount,
    s.source_line_amount,
    s.is_cancelled,
    s.is_returned,
    s.source_record_hash,
    current_timestamp(),
    current_timestamp()
)
""")

print("Incremental MERGE completed for gold.fact_sales")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Validar resultado incremental

# COMMAND ----------


display(spark.sql(f"""
SELECT 'current customers' AS metric, COUNT(*) AS value FROM {catalog_name}.{gold_schema}.dim_customer WHERE is_current = true
UNION ALL SELECT 'historical customers', COUNT(*) FROM {catalog_name}.{gold_schema}.dim_customer WHERE is_current = false
UNION ALL SELECT 'current products', COUNT(*) FROM {catalog_name}.{gold_schema}.dim_product WHERE is_current = true
UNION ALL SELECT 'historical products', COUNT(*) FROM {catalog_name}.{gold_schema}.dim_product WHERE is_current = false
UNION ALL SELECT 'fact rows', COUNT(*) FROM {catalog_name}.{gold_schema}.fact_sales
UNION ALL SELECT 'new incremental order rows', COUNT(*) FROM {catalog_name}.{gold_schema}.fact_sales WHERE order_id = 900001
"""))
