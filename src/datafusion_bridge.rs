mod pg_catalog_types;
use pg_catalog_types::{
    BOOL_OPCLASS_OID, BTREE_AM_OID, DEFAULT_COLLATION_OID, HEAP_AM_OID, INFORMATION_SCHEMA_NAME,
    INFORMATION_SCHEMA_NAMESPACE_OID, INT4_OPCLASS_OID, INT8_OPCLASS_OID, PG_CAST_OID_OFFSET,
    PG_CATALOG_NAMESPACE_OID, PG_CATALOG_SCHEMA_NAME, PG_PROC_OID_OFFSET, POSTGRES_ROLE_OID,
    PRIMARY_KEY_CONSTRAINT_OID_OFFSET, TEXT_OPCLASS_OID, UNIQUE_CONSTRAINT_OID_OFFSET,
    column_attnum, index_relation_oid, information_schema_data_type, load_table_stats, opclass_oid,
    pg_type_align, pg_type_byval, pg_type_category, pg_type_len, pg_type_name, pg_type_oid,
    pg_type_storage, register_empty_table, table_row_type_oid, to_arrow_schema, type_collation_oid,
    view_relation_oid,
};
pub use pg_catalog_types::{
    register_pg_catalog_functions, register_pg_catalog_functions_with_view_defs,
};
mod scan_runtime;
use scan_runtime::{
    FastPredicate, FastTopNCandidate, FastTopNScanPlan, TopNCandidate, TopNScanProfile,
    elapsed_ns_u64, scan_profile_enabled,
};
mod table_provider_base;
use table_provider_base::KvTableProvider;
mod physical_exec;
use physical_exec::{
    KvAggregateSpec, KvPhysicalAggregateExec, KvPhysicalTopNExec, KvScanPlan, KvTopNPlan,
};
mod physical_predicates;
use physical_predicates::{
    compare_column_values, compile_filter_column_idx, compile_kv_predicate, plan_primary_key_range,
    reverse_operator, scalar_value_to_column_value,
};
mod physical_planner;
mod table_provider_load;
mod table_provider_scans;
mod table_provider_topn;
pub use physical_planner::{
    KvAggregateExtensionPlanner, KvAggregateOptimizerRule, KvQueryPlanner, KvTopNExtensionPlanner,
    KvTopNOptimizerRule,
};
mod table_provider_trait;
use table_provider_trait::ColumnBuilder;
pub use table_provider_trait::{
    build_user_catalog_provider, register_all_tables, register_catalog_tables,
    register_catalog_tables_with_options, register_catalog_tables_with_options_for_mode,
};
mod pg_type_rows;
use pg_type_rows::{
    register_pg_attribute_table, register_pg_class_table, register_pg_constraint_table,
    register_pg_index_table, register_pg_type_table,
};
mod pg_catalog_rows;
use pg_catalog_rows::{
    register_dependency_catalog_tables, register_index_support_catalog_tables,
    register_pg_cast_table, register_pg_proc_table, register_pg_settings_table,
    register_role_catalog_tables, register_sequence_view_rule_policy_catalog_tables,
};
mod stats_and_catalogs;
use stats_and_catalogs::{
    register_information_schema_columns, register_information_schema_constraints,
    register_information_schema_empty_views, register_information_schema_privileges,
    register_information_schema_schemata, register_information_schema_tables,
    register_information_schema_views, register_mysql_information_schema, register_pg_indexes_view,
    register_pg_sequences_view, register_pg_tables_view, register_pg_views_view,
    register_statistic_catalog_tables,
};
mod mysql_information_schema;
use mysql_information_schema::{
    mysql_info_character_length, mysql_info_column_key, mysql_info_column_nullable,
    mysql_info_column_type, mysql_info_data_type, mysql_info_datetime_precision,
    mysql_info_is_auto_increment, mysql_info_is_character_type, mysql_info_numeric_precision_scale,
    mysql_info_push_constraint, register_mysql_information_schema_static_tables,
};
mod arrow_response;
#[cfg(test)]
use arrow_response::arrow_type_to_pg;
pub use arrow_response::{arrow_array_value_to_string, arrow_to_pgwire_response};
#[cfg(test)]
mod tests;
