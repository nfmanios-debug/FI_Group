# Databricks notebook source
# MAGIC %md
# MAGIC # 05_Quality_Audit — Validaciones de calidad y auditoría
# MAGIC
# MAGIC **Objetivo:** ejecutar controles técnicos y funcionales sobre el datamart Gold.
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
# MAGIC ## Tabla data_quality_rules

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS fi_group.audit.data_quality_rules (
# MAGIC     rule_id STRING,
# MAGIC     rule_name STRING,
# MAGIC     layer_name STRING,
# MAGIC     target_table STRING,
# MAGIC     rule_description STRING,
# MAGIC     severity STRING,
# MAGIC     is_active BOOLEAN,
# MAGIC     created_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejecutar reglas de calidad

# COMMAND ----------


spark.sql(f"DELETE FROM {catalog_name}.{audit_schema}.data_quality_results WHERE run_id = '{run_id}'")

checks = [
    ("DQ_FACT_DUPLICATED_ORDER_ITEM", f"SELECT COUNT(*) AS failed_records FROM (SELECT order_item_id, COUNT(*) c FROM {catalog_name}.{gold_schema}.fact_sales GROUP BY order_item_id HAVING c > 1)"),
    ("DQ_FACT_UNKNOWN_CUSTOMER", f"SELECT COUNT(*) AS failed_records FROM {catalog_name}.{gold_schema}.fact_sales WHERE customer_key = -1"),
    ("DQ_FACT_UNKNOWN_PRODUCT", f"SELECT COUNT(*) AS failed_records FROM {catalog_name}.{gold_schema}.fact_sales WHERE product_key = -1"),
    ("DQ_NEGATIVE_QUANTITY", f"SELECT COUNT(*) AS failed_records FROM {catalog_name}.{gold_schema}.fact_sales WHERE quantity <= 0"),
    ("DQ_NEGATIVE_NET_AMOUNT", f"SELECT COUNT(*) AS failed_records FROM {catalog_name}.{gold_schema}.fact_sales WHERE net_amount < 0"),
    ("DQ_DISCOUNT_GREATER_THAN_GROSS", f"SELECT COUNT(*) AS failed_records FROM {catalog_name}.{gold_schema}.fact_sales WHERE discount_amount > gross_amount"),
    ("DQ_CURRENT_CUSTOMER_DUPLICATES", f"SELECT COUNT(*) AS failed_records FROM (SELECT customer_id, COUNT(*) c FROM {catalog_name}.{gold_schema}.dim_customer WHERE is_current = true GROUP BY customer_id HAVING c > 1)"),
    ("DQ_CURRENT_PRODUCT_DUPLICATES", f"SELECT COUNT(*) AS failed_records FROM (SELECT product_id, COUNT(*) c FROM {catalog_name}.{gold_schema}.dim_product WHERE is_current = true GROUP BY product_id HAVING c > 1)")
]

for rule_name, query in checks:
    failed = spark.sql(query).collect()[0]["failed_records"]
    status = "PASS" if failed == 0 else "FAIL"
    spark.sql(f"""
    INSERT INTO {catalog_name}.{audit_schema}.data_quality_results
    SELECT
        '{run_id}' AS run_id,
        '{rule_name}' AS rule_name,
        '{catalog_name}.{gold_schema}' AS table_name,
        '{status}' AS check_status,
        CAST({failed} AS BIGINT) AS failed_records,
        current_timestamp() AS check_timestamp,
        'Automated technical validation' AS comments
    """)


# COMMAND ----------

# MAGIC %md
# MAGIC ##  Resumen de calidad

# COMMAND ----------


display(spark.sql(f"""
SELECT *
FROM {catalog_name}.{audit_schema}.data_quality_results
WHERE run_id = '{run_id}'
ORDER BY check_status DESC, rule_name
"""))

failed_checks = spark.sql(f"""
SELECT COUNT(*) AS failed_checks
FROM {catalog_name}.{audit_schema}.data_quality_results
WHERE run_id = '{run_id}' AND check_status = 'FAIL'
""").collect()[0]["failed_checks"]

if failed_checks > 0:
    raise Exception(f"Data quality failed. Failed checks: {failed_checks}")
else:
    print("All data quality checks passed")


# COMMAND ----------

# MAGIC %md
# MAGIC ## KPIs de negocio para validación

# COMMAND ----------

display(spark.sql(f"""
SELECT
    dd.year_number,
    dd.month_number,
    dch.channel_name,
    COUNT(DISTINCT fs.order_id) AS orders,
    COUNT(*) AS order_lines,
    SUM(fs.gross_amount) AS gross_sales,
    SUM(fs.discount_amount) AS discount_amount,
    SUM(fs.net_amount) AS net_sales,
    SUM(CASE WHEN fs.is_cancelled THEN 1 ELSE 0 END) AS cancelled_lines,
    SUM(CASE WHEN fs.is_returned THEN 1 ELSE 0 END) AS returned_lines
FROM {catalog_name}.{gold_schema}.fact_sales fs
LEFT JOIN {catalog_name}.{gold_schema}.dim_date dd ON fs.order_date_key = dd.date_key
LEFT JOIN {catalog_name}.{gold_schema}.dim_channel dch ON fs.channel_key = dch.channel_key
GROUP BY dd.year_number, dd.month_number, dch.channel_name
ORDER BY dd.year_number, dd.month_number, dch.channel_name
"""))


# COMMAND ----------

# MAGIC %md
# MAGIC ##  resultados data_quality_rules

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO fi_group.audit.data_quality_rules
# MAGIC SELECT 'DQ001', 'fact_sales_not_empty', 'gold', 'fact_sales',
# MAGIC        'Validate that fact_sales contains records after initial load',
# MAGIC        'critical', true, current_timestamp()
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1 FROM fi_group.audit.data_quality_rules WHERE rule_id = 'DQ001'
# MAGIC );
# MAGIC
# MAGIC INSERT INTO fi_group.audit.data_quality_rules
# MAGIC SELECT 'DQ002', 'fact_sales_no_null_keys', 'gold', 'fact_sales',
# MAGIC        'Validate that all dimensional keys in fact_sales are not null',
# MAGIC        'critical', true, current_timestamp()
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1 FROM fi_group.audit.data_quality_rules WHERE rule_id = 'DQ002'
# MAGIC );
# MAGIC
# MAGIC INSERT INTO fi_group.audit.data_quality_rules
# MAGIC SELECT 'DQ003', 'net_amount_calculation', 'gold', 'fact_sales',
# MAGIC        'Validate that net_amount equals gross_amount minus discount_amount',
# MAGIC        'high', true, current_timestamp()
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1 FROM fi_group.audit.data_quality_rules WHERE rule_id = 'DQ003'
# MAGIC );
# MAGIC
# MAGIC INSERT INTO fi_group.audit.data_quality_rules
# MAGIC SELECT 'DQ004', 'referential_integrity_gold_model', 'gold', 'fact_sales',
# MAGIC        'Validate that fact_sales keys can be resolved against Gold dimensions',
# MAGIC        'critical', true, current_timestamp()
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1 FROM fi_group.audit.data_quality_rules WHERE rule_id = 'DQ004'
# MAGIC );