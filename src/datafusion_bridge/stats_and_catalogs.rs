mod postgres;
pub(super) use postgres::{
    register_information_schema_columns, register_information_schema_constraints,
    register_information_schema_schemata, register_information_schema_tables,
    register_pg_indexes_view, register_pg_sequences_view, register_pg_tables_view,
    register_pg_views_view, register_statistic_catalog_tables,
};
mod postgres_information_schema_aux;
pub(super) use postgres_information_schema_aux::{
    register_information_schema_empty_views, register_information_schema_privileges,
    register_information_schema_views,
};
mod mysql;
pub(super) use mysql::register_mysql_information_schema;
