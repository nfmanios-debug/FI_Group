# FI Group Databricks Retail Datamart — Orden de Ejecución

Orden recomendado para las tareas del Workflow de Databricks:

1. `00_Setup/00_create_catalog_and_schemas`
2. `01_Bronze/01_load_source_to_bronze`
3. `02_Silver/02_clean_enrich_and_resolve_promotions`
4. `03_Gold/03_create_gold_dimensional_model`
5. `03_Gold/04_initial_load_gold_model`
6. `04_Incremental/05_simulate_incremental_source_changes`
7. `01_Bronze/01_load_source_to_bronze` o `04_Incremental/06_process_incremental_load` dependiendo del modo de prueba
8. `02_Silver/02_clean_enrich_and_resolve_promotions`
9. `04_Incremental/06_process_incremental_load`
10. `05_Quality_Audit/07_validate_data_quality_and_audit_checks`
11. `06_PowerBI/08_powerbi_semantic_model_definition`

Para el flujo de trabajo (workflow) final de extremo a extremo, la secuencia recomendada es:

`00_Setup -> 01_Bronze -> 02_Silver -> 03_Gold Creación -> 04 Carga Inicial Gold -> 05 Simulación Delta -> 02_Silver Actualización -> 06 Carga Incremental -> 07 Auditoría de Calidad -> 08 Documentación PowerBI`