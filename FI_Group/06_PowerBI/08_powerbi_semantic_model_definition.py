# Databricks notebook source
# MAGIC %md
# MAGIC # 06_PowerBI — Definición del modelo semántico
# MAGIC
# MAGIC **Objetivo:** documentar el modelo semántico esperado para Power BI desde Gold.
# MAGIC
# MAGIC Este notebook cubre el punto 4.8 de la guía técnica.

# COMMAND ----------


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
# MAGIC ## Celda 1 — Crear vista de tablas recomendadas para Power BI

# COMMAND ----------


# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{gold_schema}.vw_powerbi_model_tables AS
SELECT 'fact_sales' AS table_name, 'Fact' AS table_type, 'Main sales fact at order line grain' AS description
UNION ALL SELECT 'dim_date', 'Dimension', 'Calendar dimension'
UNION ALL SELECT 'dim_customer', 'Dimension', 'Customer dimension, current records for reporting'
UNION ALL SELECT 'dim_product', 'Dimension', 'Product dimension, current records for reporting'
UNION ALL SELECT 'dim_store', 'Dimension', 'Store dimension'
UNION ALL SELECT 'dim_promotion', 'Dimension', 'Promotion dimension'
UNION ALL SELECT 'dim_channel', 'Dimension', 'Sales channel dimension'
UNION ALL SELECT 'dim_payment_method', 'Dimension', 'Payment method dimension'
UNION ALL SELECT 'dim_order_status', 'Dimension', 'Order status dimension'
""")

display(spark.sql(f"SELECT * FROM {catalog_name}.{gold_schema}.vw_powerbi_model_tables"))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Celda 2 — Crear vista con medidas DAX sugeridas

# COMMAND ----------


# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{gold_schema}.vw_powerbi_dax_measures AS
SELECT 'Gross Sales' AS measure_name, 'SUM(fact_sales[gross_amount])' AS dax_expression, 'Sales' AS display_folder
UNION ALL SELECT 'Discount Amount', 'SUM(fact_sales[discount_amount])', 'Sales'
UNION ALL SELECT 'Net Sales', 'SUM(fact_sales[net_amount])', 'Sales'
UNION ALL SELECT 'Orders', 'DISTINCTCOUNT(fact_sales[order_id])', 'Orders'
UNION ALL SELECT 'Order Lines', 'COUNTROWS(fact_sales)', 'Orders'
UNION ALL SELECT 'Units Sold', 'SUM(fact_sales[quantity])', 'Sales'
UNION ALL SELECT 'Average Order Value', 'DIVIDE([Net Sales], [Orders])', 'Sales'
UNION ALL SELECT 'Discount Rate', 'DIVIDE([Discount Amount], [Gross Sales])', 'Promotions'
UNION ALL SELECT 'Cancelled Orders', 'CALCULATE(DISTINCTCOUNT(fact_sales[order_id]), fact_sales[is_cancelled] = TRUE())', 'Orders'
UNION ALL SELECT 'Returned Orders', 'CALCULATE(DISTINCTCOUNT(fact_sales[order_id]), fact_sales[is_returned] = TRUE())', 'Orders'
UNION ALL SELECT 'Promoted Sales', 'CALCULATE([Net Sales], dim_promotion[promotion_id] <> -1)', 'Promotions'
""")

display(spark.sql(f"SELECT * FROM {catalog_name}.{gold_schema}.vw_powerbi_dax_measures"))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Celda 3 — Relaciones esperadas
# MAGIC
# MAGIC Relaciones recomendadas en Power BI:
# MAGIC
# MAGIC - `fact_sales[order_date_key]` many-to-one `dim_date[date_key]`
# MAGIC - `fact_sales[customer_key]` many-to-one `dim_customer[customer_key]`
# MAGIC - `fact_sales[product_key]` many-to-one `dim_product[product_key]`
# MAGIC - `fact_sales[store_key]` many-to-one `dim_store[store_key]`
# MAGIC - `fact_sales[promotion_key]` many-to-one `dim_promotion[promotion_key]`
# MAGIC - `fact_sales[channel_key]` many-to-one `dim_channel[channel_key]`
# MAGIC - `fact_sales[payment_method_key]` many-to-one `dim_payment_method[payment_method_key]`
# MAGIC - `fact_sales[order_status_key]` many-to-one `dim_order_status[order_status_key]`
# MAGIC
# MAGIC Dirección de filtro recomendada: **single direction** desde dimensiones hacia fact.