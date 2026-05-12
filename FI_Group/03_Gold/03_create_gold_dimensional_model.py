# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Create Gold Dimensional Model
# MAGIC
# MAGIC **Propósito:** crear el modelo dimensional Gold vacío para el datamart de ventas.
# MAGIC
# MAGIC Crea:
# MAGIC - Dimensiones Gold
# MAGIC - Tabla de hechos `fact_sales`
# MAGIC - Registros Unknown / Default
# MAGIC - Vistas de documentación del modelo
# MAGIC - Constraints informativas opcionales para Unity Catalog
# MAGIC
# MAGIC **Grano de la fact:** una fila por línea de pedido.

# COMMAND ----------

try:
    dbutils.widgets.text("catalog_name", "fi_group", "Catalog name")
    dbutils.widgets.text("gold_schema", "gold", "Gold schema")
    dbutils.widgets.text("audit_schema", "audit", "Audit schema")
    dbutils.widgets.dropdown("add_constraints", "false", ["true", "false"], "Add informational constraints")
except NameError:
    pass

catalog_name = dbutils.widgets.get("catalog_name")
gold_schema = dbutils.widgets.get("gold_schema")
audit_schema = dbutils.widgets.get("audit_schema")
add_constraints = dbutils.widgets.get("add_constraints").lower() == "true"

print(f"Catalog: {catalog_name}")
print(f"Gold schema: {gold_schema}")
print(f"Add constraints: {add_constraints}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear dimensiones principales Gold

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{gold_schema}.dim_customer (
    customer_key BIGINT NOT NULL,
    customer_id INT NOT NULL,
    customer_code STRING,
    customer_name STRING,
    email STRING,
    country STRING,
    signup_date DATE,
    customer_status STRING,
    effective_from TIMESTAMP NOT NULL,
    effective_to TIMESTAMP,
    is_current BOOLEAN NOT NULL,
    record_hash STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{gold_schema}.dim_product (
    product_key BIGINT NOT NULL,
    product_id INT NOT NULL,
    product_code STRING,
    product_name STRING,
    category STRING,
    subcategory STRING,
    brand STRING,
    price DECIMAL(12,2),
    is_active BOOLEAN,
    effective_from TIMESTAMP NOT NULL,
    effective_to TIMESTAMP,
    is_current BOOLEAN NOT NULL,
    record_hash STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{gold_schema}.dim_store (
    store_key BIGINT NOT NULL,
    store_id INT NOT NULL,
    store_code STRING,
    store_name STRING,
    region STRING,
    country STRING,
    open_date DATE,
    effective_from TIMESTAMP NOT NULL,
    effective_to TIMESTAMP,
    is_current BOOLEAN NOT NULL,
    record_hash STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{gold_schema}.dim_promotion (
    promotion_key BIGINT NOT NULL,
    promotion_id INT NOT NULL,
    promotion_code STRING,
    product_id INT,
    channel STRING,
    discount_pct DECIMAL(5,2),
    start_date DATE,
    end_date DATE,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
""")

print("Core dimensions created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Crear dimensiones auxiliares

# COMMAND ----------


spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{gold_schema}.dim_date (
    date_key INT NOT NULL,
    full_date DATE NOT NULL,
    day_number INT,
    day_name STRING,
    week_number INT,
    month_number INT,
    month_name STRING,
    quarter_number INT,
    year_number INT,
    is_weekend BOOLEAN
)
USING DELTA
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{gold_schema}.dim_channel (
    channel_key BIGINT NOT NULL,
    channel_name STRING NOT NULL,
    created_at TIMESTAMP
)
USING DELTA
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{gold_schema}.dim_payment_method (
    payment_method_key BIGINT NOT NULL,
    payment_method_name STRING NOT NULL,
    created_at TIMESTAMP
)
USING DELTA
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{gold_schema}.dim_order_status (
    order_status_key BIGINT NOT NULL,
    order_status_name STRING NOT NULL,
    is_cancelled BOOLEAN,
    is_returned BOOLEAN,
    created_at TIMESTAMP
)
USING DELTA
""")

print("Auxiliary dimensions created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear tabla de hechos `fact_sales`

# COMMAND ----------


spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{gold_schema}.fact_sales (
    sales_key BIGINT NOT NULL,
    order_id INT NOT NULL,
    order_item_id INT NOT NULL,
    order_date_key INT NOT NULL,
    customer_key BIGINT NOT NULL,
    product_key BIGINT NOT NULL,
    store_key BIGINT NOT NULL,
    promotion_key BIGINT NOT NULL,
    channel_key BIGINT NOT NULL,
    payment_method_key BIGINT NOT NULL,
    order_status_key BIGINT NOT NULL,
    quantity INT,
    unit_price DECIMAL(12,2),
    gross_amount DECIMAL(14,2),
    discount_pct DECIMAL(5,2),
    discount_amount DECIMAL(14,2),
    net_amount DECIMAL(14,2),
    source_discount_amount DECIMAL(14,2),
    source_line_amount DECIMAL(14,2),
    is_cancelled BOOLEAN,
    is_returned BOOLEAN,
    source_record_hash STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
""")

print("fact_sales created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insertar registros Unknown / Default

# COMMAND ----------


spark.sql(f"""
INSERT INTO {catalog_name}.{gold_schema}.dim_customer
SELECT
    -1, -1, 'UNKNOWN', 'Unknown Customer', 'unknown@email.com', 'Unknown', DATE('1900-01-01'), 'Unknown',
    TIMESTAMP('1900-01-01 00:00:00'), NULL, true, NULL, current_timestamp(), current_timestamp()
WHERE NOT EXISTS (SELECT 1 FROM {catalog_name}.{gold_schema}.dim_customer WHERE customer_key = -1)
""")

spark.sql(f"""
INSERT INTO {catalog_name}.{gold_schema}.dim_product
SELECT
    -1, -1, 'UNKNOWN', 'Unknown Product', 'Unknown', 'Unknown', 'Unknown', CAST(0 AS DECIMAL(12,2)), false,
    TIMESTAMP('1900-01-01 00:00:00'), NULL, true, NULL, current_timestamp(), current_timestamp()
WHERE NOT EXISTS (SELECT 1 FROM {catalog_name}.{gold_schema}.dim_product WHERE product_key = -1)
""")

spark.sql(f"""
INSERT INTO {catalog_name}.{gold_schema}.dim_store
SELECT
    -1, -1, 'UNKNOWN', 'Unknown Store', 'Unknown', 'Unknown', DATE('1900-01-01'),
    TIMESTAMP('1900-01-01 00:00:00'), NULL, true, NULL, current_timestamp(), current_timestamp()
WHERE NOT EXISTS (SELECT 1 FROM {catalog_name}.{gold_schema}.dim_store WHERE store_key = -1)
""")

spark.sql(f"""
INSERT INTO {catalog_name}.{gold_schema}.dim_promotion
SELECT
    -1, -1, 'NO_PROMOTION', -1, 'ALL', CAST(0 AS DECIMAL(5,2)), DATE('1900-01-01'), DATE('9999-12-31'), current_timestamp(), current_timestamp()
WHERE NOT EXISTS (SELECT 1 FROM {catalog_name}.{gold_schema}.dim_promotion WHERE promotion_key = -1)
""")

spark.sql(f"""
INSERT INTO {catalog_name}.{gold_schema}.dim_channel
SELECT -1, 'Unknown', current_timestamp()
WHERE NOT EXISTS (SELECT 1 FROM {catalog_name}.{gold_schema}.dim_channel WHERE channel_key = -1)
""")

spark.sql(f"""
INSERT INTO {catalog_name}.{gold_schema}.dim_payment_method
SELECT -1, 'Unknown', current_timestamp()
WHERE NOT EXISTS (SELECT 1 FROM {catalog_name}.{gold_schema}.dim_payment_method WHERE payment_method_key = -1)
""")

spark.sql(f"""
INSERT INTO {catalog_name}.{gold_schema}.dim_order_status
SELECT -1, 'Unknown', false, false, current_timestamp()
WHERE NOT EXISTS (SELECT 1 FROM {catalog_name}.{gold_schema}.dim_order_status WHERE order_status_key = -1)
""")

print("Unknown records inserted.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear vistas de documentación del modelo

# COMMAND ----------


spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{gold_schema}.vw_model_relationships AS
SELECT 'fact_sales' AS fact_table, 'customer_key' AS fact_column, 'dim_customer' AS dimension_table, 'customer_key' AS dimension_column
UNION ALL SELECT 'fact_sales', 'product_key', 'dim_product', 'product_key'
UNION ALL SELECT 'fact_sales', 'store_key', 'dim_store', 'store_key'
UNION ALL SELECT 'fact_sales', 'promotion_key', 'dim_promotion', 'promotion_key'
UNION ALL SELECT 'fact_sales', 'order_date_key', 'dim_date', 'date_key'
UNION ALL SELECT 'fact_sales', 'channel_key', 'dim_channel', 'channel_key'
UNION ALL SELECT 'fact_sales', 'payment_method_key', 'dim_payment_method', 'payment_method_key'
UNION ALL SELECT 'fact_sales', 'order_status_key', 'dim_order_status', 'order_status_key'
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{gold_schema}.vw_fact_sales_grain AS
SELECT
    'fact_sales' AS table_name,
    'One row per order line / Una fila por línea de pedido' AS grain_description,
    'order_item_id' AS natural_key,
    'sales_key' AS surrogate_key,
    'Sales, discounts, promotions, cancellations and returns analysis' AS business_purpose
""")

print("Documentation views created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Constraints informativas opcionales

# COMMAND ----------


constraint_statements = [
    f"ALTER TABLE {catalog_name}.{gold_schema}.dim_customer ADD CONSTRAINT dim_customer_pk PRIMARY KEY (customer_key) NOT ENFORCED",
    f"ALTER TABLE {catalog_name}.{gold_schema}.dim_product ADD CONSTRAINT dim_product_pk PRIMARY KEY (product_key) NOT ENFORCED",
    f"ALTER TABLE {catalog_name}.{gold_schema}.dim_store ADD CONSTRAINT dim_store_pk PRIMARY KEY (store_key) NOT ENFORCED",
    f"ALTER TABLE {catalog_name}.{gold_schema}.dim_promotion ADD CONSTRAINT dim_promotion_pk PRIMARY KEY (promotion_key) NOT ENFORCED",
    f"ALTER TABLE {catalog_name}.{gold_schema}.dim_date ADD CONSTRAINT dim_date_pk PRIMARY KEY (date_key) NOT ENFORCED",
    f"ALTER TABLE {catalog_name}.{gold_schema}.dim_channel ADD CONSTRAINT dim_channel_pk PRIMARY KEY (channel_key) NOT ENFORCED",
    f"ALTER TABLE {catalog_name}.{gold_schema}.dim_payment_method ADD CONSTRAINT dim_payment_method_pk PRIMARY KEY (payment_method_key) NOT ENFORCED",
    f"ALTER TABLE {catalog_name}.{gold_schema}.dim_order_status ADD CONSTRAINT dim_order_status_pk PRIMARY KEY (order_status_key) NOT ENFORCED",
    f"ALTER TABLE {catalog_name}.{gold_schema}.fact_sales ADD CONSTRAINT fact_sales_pk PRIMARY KEY (sales_key) NOT ENFORCED",
    f"ALTER TABLE {catalog_name}.{gold_schema}.fact_sales ADD CONSTRAINT fact_sales_dim_customer_fk FOREIGN KEY (customer_key) REFERENCES {catalog_name}.{gold_schema}.dim_customer(customer_key) NOT ENFORCED",
    f"ALTER TABLE {catalog_name}.{gold_schema}.fact_sales ADD CONSTRAINT fact_sales_dim_product_fk FOREIGN KEY (product_key) REFERENCES {catalog_name}.{gold_schema}.dim_product(product_key) NOT ENFORCED",
    f"ALTER TABLE {catalog_name}.{gold_schema}.fact_sales ADD CONSTRAINT fact_sales_dim_store_fk FOREIGN KEY (store_key) REFERENCES {catalog_name}.{gold_schema}.dim_store(store_key) NOT ENFORCED",
    f"ALTER TABLE {catalog_name}.{gold_schema}.fact_sales ADD CONSTRAINT fact_sales_dim_promotion_fk FOREIGN KEY (promotion_key) REFERENCES {catalog_name}.{gold_schema}.dim_promotion(promotion_key) NOT ENFORCED",
    f"ALTER TABLE {catalog_name}.{gold_schema}.fact_sales ADD CONSTRAINT fact_sales_dim_date_fk FOREIGN KEY (order_date_key) REFERENCES {catalog_name}.{gold_schema}.dim_date(date_key) NOT ENFORCED",
    f"ALTER TABLE {catalog_name}.{gold_schema}.fact_sales ADD CONSTRAINT fact_sales_dim_channel_fk FOREIGN KEY (channel_key) REFERENCES {catalog_name}.{gold_schema}.dim_channel(channel_key) NOT ENFORCED",
    f"ALTER TABLE {catalog_name}.{gold_schema}.fact_sales ADD CONSTRAINT fact_sales_dim_payment_method_fk FOREIGN KEY (payment_method_key) REFERENCES {catalog_name}.{gold_schema}.dim_payment_method(payment_method_key) NOT ENFORCED",
    f"ALTER TABLE {catalog_name}.{gold_schema}.fact_sales ADD CONSTRAINT fact_sales_dim_order_status_fk FOREIGN KEY (order_status_key) REFERENCES {catalog_name}.{gold_schema}.dim_order_status(order_status_key) NOT ENFORCED"
]

if add_constraints:
    for stmt in constraint_statements:
        try:
            spark.sql(stmt)
            print(f"OK: {stmt}")
        except Exception as e:
            print(f"WARNING - constraint skipped: {stmt}\nReason: {str(e)[:300]}")
else:
    print("Constraints were skipped because add_constraints=false.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Validar objetos Gold

# COMMAND ----------


display(spark.sql(f"SHOW TABLES IN {catalog_name}.{gold_schema}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validar estructura de `fact_sales`

# COMMAND ----------


display(spark.sql(f"DESCRIBE TABLE {catalog_name}.{gold_schema}.fact_sales"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validar registros Unknown

# COMMAND ----------


unknown_validation_sql = f"""
SELECT 'dim_customer' AS table_name, COUNT(*) AS unknown_records FROM {catalog_name}.{gold_schema}.dim_customer WHERE customer_key = -1
UNION ALL SELECT 'dim_product', COUNT(*) FROM {catalog_name}.{gold_schema}.dim_product WHERE product_key = -1
UNION ALL SELECT 'dim_store', COUNT(*) FROM {catalog_name}.{gold_schema}.dim_store WHERE store_key = -1
UNION ALL SELECT 'dim_promotion', COUNT(*) FROM {catalog_name}.{gold_schema}.dim_promotion WHERE promotion_key = -1
UNION ALL SELECT 'dim_channel', COUNT(*) FROM {catalog_name}.{gold_schema}.dim_channel WHERE channel_key = -1
UNION ALL SELECT 'dim_payment_method', COUNT(*) FROM {catalog_name}.{gold_schema}.dim_payment_method WHERE payment_method_key = -1
UNION ALL SELECT 'dim_order_status', COUNT(*) FROM {catalog_name}.{gold_schema}.dim_order_status WHERE order_status_key = -1
"""
display(spark.sql(unknown_validation_sql))

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Registrar auditoría de creación Gold

# COMMAND ----------


import uuid
run_id = str(uuid.uuid4())

spark.sql(f"""
INSERT INTO {catalog_name}.{audit_schema}.etl_run_log
SELECT
    '{run_id}' AS run_id,
    '03_create_gold_dimensional_model' AS process_name,
    'gold' AS layer_name,
    '{catalog_name}.{gold_schema}' AS target_table,
    current_timestamp() AS start_time,
    current_timestamp() AS end_time,
    'SUCCESS' AS status,
    0 AS rows_inserted,
    0 AS rows_updated,
    0 AS rows_deleted,
    NULL AS error_message,
    current_timestamp() AS created_at
""")

print(f"Gold dimensional model created. run_id={run_id}")