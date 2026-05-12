# FI Group Databricks Retail Datamart — Execution Order

Recommended Databricks Workflow task order:

1. `00_Setup/00_create_catalog_and_schemas`
2. `01_Bronze/01_load_source_to_bronze`
3. `02_Silver/02_clean_enrich_and_resolve_promotions`
4. `03_Gold/03_create_gold_dimensional_model`
5. `03_Gold/04_initial_load_gold_model`
6. `04_Incremental/05_simulate_incremental_source_changes`
7. `01_Bronze/01_load_source_to_bronze` or `04_Incremental/06_process_incremental_load` depending on test mode
8. `02_Silver/02_clean_enrich_and_resolve_promotions`
9. `04_Incremental/06_process_incremental_load`
10. `05_Quality_Audit/07_validate_data_quality_and_audit_checks`
11. `06_PowerBI/08_powerbi_semantic_model_definition`

For the final end-to-end workflow, the recommended sequence is:

`00_Setup -> 01_Bronze -> 02_Silver -> 03_Gold Create -> 04 Gold Initial Load -> 05 Simulate Delta -> 02_Silver Refresh -> 06 Incremental Load -> 07 Quality Audit -> 08 PowerBI Documentation`
