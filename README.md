# FI_Group — Databricks Lakehouse Retail Datamart

Este proyecto implementa una solución de ingeniería de datos en Databricks para construir un datamart analítico de ventas retail usando arquitectura Lakehouse / Medallion.

La solución parte de tablas operacionales en fi_group.dbo, las procesa por capas Bronze, Silver y Gold, aplica reglas de negocio, resuelve promociones a nivel de línea de venta, ejecuta cargas iniciales e incrementales con MERGE INTO, 
valida calidad de datos y deja el modelo preparado para consumo desde Power BI.

---

## Arquitectura

El catálogo principal es:

fi_group

La solución usa los siguientes esquemas:

dbo       -- Tablas fuente operacionales
bronze    -- Copia raw/controlada de las fuentes
silver    -- Datos limpios, enriquecidos y con reglas de negocio
gold      -- Modelo dimensional para analítica y Power BI
audit     -- Auditoría, reglas y resultados de calidad

Flujo general:

dbo → bronze → silver → gold → Power BI

---

## Estructura del proyecto

FI_Group/
├── 00_Setup/
│   └── 00_create_catalog_and_schemas
├── 01_Bronze/
│   └── 01_load_source_to_bronze
├── 02_Silver/
│   └── 02_clean_enrich_and_resolve_promotions
├── 03_Gold/
│   ├── 03_create_gold_dimensional_model
│   └── 04_initial_load_gold_model
├── 04_Incremental/
│   ├── 05_simulate_incremental_source_changes
│   └── 06_process_incremental_load
├── 05_Quality_Audit/
│   └── 07_validate_data_quality_and_audit_checks
├── 06_PowerBI/
│   └── 08_powerbi_semantic_model_definition
└── 07_Docs/

---

## Qué hace cada notebook

### 00_Setup

Crea la estructura base del proyecto en Databricks:

- Catálogo fi_group.
- Esquemas dbo, bronze, silver, gold y audit.
- Tablas de auditoría.
- Validación de existencia de tablas fuente.

---

### 01_Bronze

Carga las tablas operacionales desde fi_group.dbo hacia fi_group.bronze.

Tablas procesadas:

customers
products
stores
promotions
orders
order_items

Agrega columnas técnicas para trazabilidad:

bronze_ingestion_timestamp
bronze_source_batch
bronze_run_id

---

### 02_Silver

Construye la capa Silver con datos limpios y enriquecidos.

Realiza:

- Limpieza básica.
- Normalización de campos.
- Enriquecimiento de órdenes y líneas de venta.
- Unión con clientes, productos, tiendas, canales, métodos de pago y estados.
- Resolución de promociones a nivel de línea de pedido.
- Cálculo de importes brutos, descuentos e importes netos.
- Generación de hashes para detección de cambios.

Objeto principal:

fi_group.silver.sales_enriched

---

### 03_Gold — Modelo Dimensional

Crea el modelo estrella en fi_group.gold.

Dimensiones:

dim_date
dim_customer
dim_product
dim_store
dim_promotion
dim_channel
dim_payment_method
dim_order_status

Tabla de hechos:

fact_sales

El grano de fact_sales es:

Una fila por línea de pedido.

También crea registros Unknown con llave -1 para manejar datos no encontrados.

---

### 04_Gold Initial Load

Carga inicialmente el modelo Gold desde Silver.

Puebla:

- Dimensiones.
- Tabla calendario.
- Tabla de hechos fact_sales.

Calcula métricas como:

gross_amount
discount_amount
net_amount
source_discount_amount
source_line_amount

---

### 05_Simulate Incremental Source Changes

Simula cambios incrementales en las tablas fuente.

Escenarios cubiertos:

- Nuevos pedidos.
- Nuevas líneas de pedido.
- Cambios en clientes.
- Cambios en productos.
- Nuevas promociones.
- Correcciones de líneas.
- Cambios de estado como cancelaciones o devoluciones.

Este notebook permite probar la carga incremental sin depender de archivos delta externos.

---

### 06_Process Incremental Load

Procesa los cambios incrementales usando MERGE INTO.

Realiza:

- Sincronización de dbo hacia bronze.
- Actualización de dimensiones Gold.
- Actualización de fact_sales.
- Inserción de nuevos registros.
- Actualización de registros existentes.
- Detección de cambios mediante hash.
- Reprocesamiento de métricas afectadas.

---

### 07_Quality Audit

Ejecuta validaciones de calidad y auditoría.

Valida:

- Existencia de datos en fact_sales.
- Claves nulas.
- Integridad lógica entre fact y dimensiones.
- Métricas de ventas.
- Registros Unknown.
- Ventas con y sin promoción.
- Pedidos cancelados y devueltos.

Tablas principales:

audit.etl_run_log
audit.data_quality_rules
audit.data_quality_results

---

### 08_PowerBI Semantic Model

Documenta el modelo semántico esperado para Power BI.

Define:

- Tablas Gold a importar.
- Relaciones entre dimensiones y hechos.
- Cardinalidades.
- Dirección de filtro.
- Medidas DAX.
- Columnas técnicas a ocultar.
- Validaciones SQL contra el modelo Gold.

Power BI debe consumir únicamente tablas de:

fi_group.gold

---

## Modelo Gold

La tabla principal es:

gold.fact_sales

Relaciones esperadas:

dim_date.date_key                     → fact_sales.order_date_key
dim_customer.customer_key             → fact_sales.customer_key
dim_product.product_key               → fact_sales.product_key
dim_store.store_key                   → fact_sales.store_key
dim_promotion.promotion_key           → fact_sales.promotion_key
dim_channel.channel_key               → fact_sales.channel_key
dim_payment_method.payment_method_key → fact_sales.payment_method_key
dim_order_status.order_status_key     → fact_sales.order_status_key

Todas las relaciones son:

1 a muchos
Dimensión → Fact
Filtro simple

---

## Workflow Databricks

El pipeline está orquestado mediante el workflow:

WF_FI_GROUP_DATAMART

Orden de ejecución:

1. create_catalog_and_schemas
2. load_source_to_bronze
3. clean_enrich_and_resolve_promotions
4. create_gold_dimensional_model
5. initial_load_gold_model
6. validate_data_quality_after_initial_load
7. simulate_incremental_source_changes
8. process_incremental_load_first_pass
9. refresh_silver_after_incremental
10. process_incremental_load_second_pass
11. validate_data_quality_final
12. powerbi_semantic_model_definition

---

## Reset para prueba limpia

Para ejecutar una prueba limpia conservando los datos fuente:

USE CATALOG fi_group;

DROP SCHEMA IF EXISTS bronze CASCADE;
DROP SCHEMA IF EXISTS silver CASCADE;
DROP SCHEMA IF EXISTS gold CASCADE;
DROP SCHEMA IF EXISTS audit CASCADE;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS audit;

Esto conserva:

fi_group.dbo

que contiene las tablas fuente iniciales.

---

## Modelo Power BI

Modo recomendado:

Import

Tablas a importar:

fact_sales
dim_date
dim_customer
dim_product
dim_store
dim_promotion
dim_channel
dim_payment_method
dim_order_status

Medidas principales:

Ventas Brutas = SUM(fact_sales[gross_amount])

Importe de Descuento = SUM(fact_sales[discount_amount])

Ventas Netas = SUM(fact_sales[net_amount])

Pedidos = DISTINCTCOUNT(fact_sales[order_id])

Líneas de Pedido = COUNTROWS(fact_sales)

Unidades Vendidas = SUM(fact_sales[quantity])

Valor Promedio por Pedido = DIVIDE([Ventas Netas], [Pedidos])

Tasa de Descuento = DIVIDE([Importe de Descuento], [Ventas Brutas])

Pedidos Cancelados =
CALCULATE(
    DISTINCTCOUNT(fact_sales[order_id]),
    fact_sales[is_cancelled] = TRUE()
)

Pedidos Devueltos =
CALCULATE(
    DISTINCTCOUNT(fact_sales[order_id]),
    fact_sales[is_returned] = TRUE()
)

Ventas con Promoción =
CALCULATE(
    [Ventas Netas],
    dim_promotion[promotion_id] <> -1
)

---

## Preguntas analíticas soportadas

La solución permite responder:

- Ventas netas por día, mes, canal, región, categoría o producto.
- Impacto de promociones sobre ventas y descuentos.
- Pedidos cancelados o devueltos.
- Nuevos pedidos procesados incrementalmente.
- Correcciones de registros existentes.
- Productos y categorías con mayor venta.
- Canales con mayor participación de ingresos.

---

## Buenas prácticas aplicadas

- Arquitectura Medallion.
- Delta Lake.
- Modelo estrella en Gold.
- MERGE INTO para cargas incrementales.
- Detección de cambios por hash.
- Registros Unknown para integridad analítica.
- Auditoría de ejecución.
- Validaciones de calidad de datos.
- Orquestación con Databricks Workflows.
- Separación entre fuente, integración y consumo.
- Modelo semántico preparado para Power BI.

---


## Autor

Nelson Fabian Manios Ascencio
Senior Data Engineer
