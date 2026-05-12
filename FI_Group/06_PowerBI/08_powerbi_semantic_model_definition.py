# Databricks notebook source
# MAGIC %md
# MAGIC # 06_PowerBI — Definición del modelo semántico
# MAGIC
# MAGIC **Objetivo:** documentar el modelo semántico esperado para Power BI desde Gold.

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
# MAGIC ## Crear vista de tablas para Power BI

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{gold_schema}.vw_powerbi_model_tables AS
SELECT 
    'fact_sales' AS nombre_tabla, 
    'Hechos' AS tipo_tabla, 
    'Tabla principal de hechos de ventas con grano a nivel de línea de pedido' AS descripcion

UNION ALL SELECT 
    'dim_date', 
    'Dimensión', 
    'Dimensión calendario para análisis por día, mes, trimestre y año'

UNION ALL SELECT 
    'dim_customer', 
    'Dimensión', 
    'Dimensión de clientes con registros vigentes para análisis comercial'

UNION ALL SELECT 
    'dim_product', 
    'Dimensión', 
    'Dimensión de productos con categoría, subcategoría, marca y precio'

UNION ALL SELECT 
    'dim_store', 
    'Dimensión', 
    'Dimensión de tiendas con información de región, país y fecha de apertura'

UNION ALL SELECT 
    'dim_promotion', 
    'Dimensión', 
    'Dimensión de promociones aplicadas a nivel de línea de venta'

UNION ALL SELECT 
    'dim_channel', 
    'Dimensión', 
    'Dimensión de canal de venta, por ejemplo online o tienda física'

UNION ALL SELECT 
    'dim_payment_method', 
    'Dimensión', 
    'Dimensión de método de pago utilizado en el pedido'

UNION ALL SELECT 
    'dim_order_status', 
    'Dimensión', 
    'Dimensión de estado del pedido para identificar ventas completadas, canceladas o devueltas'
""")

display(spark.sql(f"""
SELECT * 
FROM {catalog_name}.{gold_schema}.vw_powerbi_model_tables
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Celda 2 — Crear vista con medidas DAX sugeridas

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{gold_schema}.vw_powerbi_dax_measures AS
SELECT 
    'Ventas Brutas' AS nombre_medida, 
    'SUM(fact_sales[gross_amount])' AS expresion_dax, 
    'Ventas' AS carpeta_visualizacion,
    'Suma total del importe bruto antes de descuentos' AS descripcion

UNION ALL SELECT 
    'Importe de Descuento', 
    'SUM(fact_sales[discount_amount])', 
    'Ventas',
    'Suma total de los descuentos aplicados a las ventas'

UNION ALL SELECT 
    'Ventas Netas', 
    'SUM(fact_sales[net_amount])', 
    'Ventas',
    'Suma total de ventas después de aplicar descuentos'

UNION ALL SELECT 
    'Pedidos', 
    'DISTINCTCOUNT(fact_sales[order_id])', 
    'Pedidos',
    'Cantidad de pedidos únicos'

UNION ALL SELECT 
    'Líneas de Pedido', 
    'COUNTROWS(fact_sales)', 
    'Pedidos',
    'Cantidad total de líneas de pedido registradas en la tabla de hechos'

UNION ALL SELECT 
    'Unidades Vendidas', 
    'SUM(fact_sales[quantity])', 
    'Ventas',
    'Cantidad total de unidades vendidas'

UNION ALL SELECT 
    'Valor Promedio por Pedido', 
    'DIVIDE([Ventas Netas], [Pedidos])', 
    'Ventas',
    'Promedio de venta neta por pedido'

UNION ALL SELECT 
    'Tasa de Descuento', 
    'DIVIDE([Importe de Descuento], [Ventas Brutas])', 
    'Promociones',
    'Porcentaje de descuento aplicado sobre las ventas brutas'

UNION ALL SELECT 
    'Pedidos Cancelados', 
    'CALCULATE(DISTINCTCOUNT(fact_sales[order_id]), fact_sales[is_cancelled] = TRUE())', 
    'Pedidos',
    'Cantidad de pedidos únicos marcados como cancelados'

UNION ALL SELECT 
    'Pedidos Devueltos', 
    'CALCULATE(DISTINCTCOUNT(fact_sales[order_id]), fact_sales[is_returned] = TRUE())', 
    'Pedidos',
    'Cantidad de pedidos únicos marcados como devueltos'

UNION ALL SELECT 
    'Ventas con Promoción', 
    'CALCULATE([Ventas Netas], dim_promotion[promotion_id] <> -1)', 
    'Promociones',
    'Ventas netas asociadas a líneas con promoción aplicada'

UNION ALL SELECT 
    'Ventas sin Promoción', 
    'CALCULATE([Ventas Netas], dim_promotion[promotion_id] = -1)', 
    'Promociones',
    'Ventas netas asociadas a líneas sin promoción aplicada'

UNION ALL SELECT 
    'Descuento por Promoción', 
    'CALCULATE([Importe de Descuento], dim_promotion[promotion_id] <> -1)', 
    'Promociones',
    'Importe total de descuento asociado a promociones'

UNION ALL SELECT 
    'Participación de Ventas con Promoción', 
    'DIVIDE([Ventas con Promoción], [Ventas Netas])', 
    'Promociones',
    'Porcentaje de ventas netas que provienen de líneas con promoción'
""")

display(spark.sql(f"""
SELECT * 
FROM {catalog_name}.{gold_schema}.vw_powerbi_dax_measures
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Relaciones esperadas
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
# MAGIC Dirección de filtro : **single direction** desde dimensiones hacia fact.
