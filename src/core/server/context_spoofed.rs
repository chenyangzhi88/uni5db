use pgwire::api::results::Response;
use pgwire::api::{ClientInfo, METADATA_USER};
use pgwire::error::PgWireResult;

use super::GatewayServer;
use super::shared::{
    METADATA_SEARCH_PATH, METADATA_TRANSACTION_ISOLATION, strip_pg_catalog_function_qualifiers,
};
use crate::core::response::{
    empty_query_response, multi_text_row_response, single_int4_row_response,
    single_text_row_response,
};
use crate::error::unsupported;

impl GatewayServer {
    pub(super) fn normalize_sql_whitespace(sql: &str) -> String {
        sql.split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_lowercase()
    }

    pub(super) async fn catalog_query_response<C>(
        &self,
        client: &C,
        sql: &str,
    ) -> Option<PgWireResult<Vec<Response>>>
    where
        C: ClientInfo,
    {
        let normalized = Self::normalize_sql_whitespace(sql);
        if normalized.contains("from pg_catalog.pg_database")
            && normalized.contains("pg_get_userbyid")
            && normalized.contains("pg_encoding_to_char")
        {
            let owner = client
                .metadata()
                .get(METADATA_USER)
                .cloned()
                .unwrap_or_else(|| "postgres".to_string());
            let rows = match self.catalog.list_databases().await {
                Ok(databases) => databases
                    .into_iter()
                    .map(|database| {
                        vec![
                            Some(database.database_name),
                            Some(owner.clone()),
                            Some("UTF8".to_string()),
                            Some("C.UTF-8".to_string()),
                            Some("C.UTF-8".to_string()),
                            None,
                            Some("libc".to_string()),
                            None,
                        ]
                    })
                    .collect::<Vec<_>>(),
                Err(error) => return Some(Err(error)),
            };
            return Some(multi_text_row_response(
                &[
                    "Name",
                    "Owner",
                    "Encoding",
                    "Collate",
                    "Ctype",
                    "ICU Locale",
                    "Locale Provider",
                    "Access privileges",
                ],
                &rows,
            ));
        }
        if normalized.contains("from pg_catalog.pg_class c")
            && normalized.contains("pg_catalog.pg_namespace n")
            && normalized.contains("pg_catalog.pg_get_userbyid(c.relowner)")
            && normalized.contains("case c.relkind")
        {
            let rows = match self
                .catalog
                .list_tables(&self.session_catalog(client).database_name)
                .await
            {
                Ok(tables) => tables
                    .into_iter()
                    .map(|table| {
                        vec![
                            Some(table.schema_name),
                            Some(table.table_name),
                            Some("table".to_string()),
                            Some(
                                client
                                    .metadata()
                                    .get(METADATA_USER)
                                    .cloned()
                                    .unwrap_or_else(|| "postgres".to_string()),
                            ),
                        ]
                    })
                    .collect::<Vec<_>>(),
                Err(error) => return Some(Err(error)),
            };
            return Some(multi_text_row_response(
                &["Schema", "Name", "Type", "Owner"],
                &rows,
            ));
        }
        None
    }

    pub(super) fn spoofed_response<C>(
        &self,
        client: &C,
        sql: &str,
    ) -> Option<PgWireResult<Vec<Response>>>
    where
        C: ClientInfo,
    {
        let normalized_sql = strip_pg_catalog_function_qualifiers(sql);
        let normalized = normalized_sql.trim().to_ascii_lowercase();
        let session = self.session_catalog(client);

        match normalized.as_str() {
            "select 1" | "select 1;" => {
                return Some(single_int4_row_response("?column?", 1));
            }
            "select version()" | "select version();" => {
                return Some(single_text_row_response(
                    "version",
                    "PostgreSQL 14.0 (pg_gateway)",
                ));
            }
            "select current_schema()" | "select current_schema();" => {
                return Some(single_text_row_response(
                    "current_schema",
                    &session.schema_name,
                ));
            }
            "select current_database()" | "select current_database();" => {
                return Some(single_text_row_response(
                    "current_database",
                    &session.database_name,
                ));
            }
            "select current_user"
            | "select current_user();"
            | "select session_user"
            | "select session_user;" => {
                let user = client
                    .metadata()
                    .get(METADATA_USER)
                    .cloned()
                    .unwrap_or_else(|| "postgres".to_string());
                let column = if normalized.contains("session_user") {
                    "session_user"
                } else {
                    "current_user"
                };
                return Some(single_text_row_response(column, &user));
            }
            "show search_path" | "show search_path;" => {
                let search_path = client
                    .metadata()
                    .get(METADATA_SEARCH_PATH)
                    .cloned()
                    .unwrap_or_else(|| session.schema_name.clone());
                return Some(single_text_row_response("search_path", &search_path));
            }
            "show transaction isolation level"
            | "show transaction isolation level;"
            | "show transaction_isolation"
            | "show transaction_isolation;" => {
                let isolation = client
                    .metadata()
                    .get(METADATA_TRANSACTION_ISOLATION)
                    .map(String::as_str)
                    .unwrap_or(self.default_transaction_isolation().as_pg_str());
                return Some(single_text_row_response("transaction_isolation", isolation));
            }
            "show standard_conforming_strings" | "show standard_conforming_strings;" => {
                return Some(single_text_row_response(
                    "standard_conforming_strings",
                    "on",
                ));
            }
            "show server_version" | "show server_version;" => {
                return Some(single_text_row_response("server_version", "14.0"));
            }
            "show server_version_num" | "show server_version_num;" => {
                return Some(single_text_row_response("server_version_num", "140000"));
            }
            "show client_encoding" | "show client_encoding;" => {
                return Some(single_text_row_response("client_encoding", "UTF8"));
            }
            "show timezone" | "show timezone;" | "show time zone" | "show time zone;" => {
                return Some(single_text_row_response("TimeZone", "UTC"));
            }
            _ => {}
        }

        if normalized.starts_with("set ") {
            if Self::is_supported_noop_set(&normalized) {
                return Some(Ok(vec![empty_query_response()]));
            }
            return Some(Err(unsupported(format!(
                "session parameter is not supported yet: {sql}"
            ))));
        }

        let implemented_catalog_relation = normalized.contains("pg_catalog.pg_class")
            || normalized.contains("pg_catalog.pg_attribute")
            || normalized.contains("pg_catalog.pg_index")
            || normalized.contains("pg_catalog.pg_constraint")
            || normalized.contains("pg_catalog.pg_proc")
            || normalized.contains("pg_catalog.pg_cast")
            || normalized.contains("pg_catalog.pg_settings")
            || normalized.contains("pg_catalog.pg_tables")
            || normalized.contains("pg_catalog.pg_type")
            || normalized.contains("pg_catalog.pg_authid")
            || normalized.contains("pg_catalog.pg_auth_members")
            || normalized.contains("pg_catalog.pg_roles")
            || normalized.contains("pg_catalog.pg_user")
            || normalized.contains("pg_catalog.pg_group")
            || normalized.contains("pg_catalog.pg_attrdef")
            || normalized.contains("pg_catalog.pg_depend")
            || normalized.contains("pg_catalog.pg_description")
            || normalized.contains("pg_catalog.pg_shdepend")
            || normalized.contains("pg_catalog.pg_shdescription")
            || normalized.contains("pg_catalog.pg_am")
            || normalized.contains("pg_catalog.pg_opclass")
            || normalized.contains("pg_catalog.pg_opfamily")
            || normalized.contains("pg_catalog.pg_operator")
            || normalized.contains("pg_catalog.pg_collation")
            || normalized.contains("pg_catalog.pg_sequence")
            || normalized.contains("pg_catalog.pg_sequences")
            || normalized.contains("pg_catalog.pg_rewrite")
            || normalized.contains("pg_catalog.pg_views")
            || normalized.contains("pg_catalog.pg_trigger")
            || normalized.contains("pg_catalog.pg_policy")
            || normalized.contains("pg_catalog.pg_statistic")
            || normalized.contains("pg_catalog.pg_stats")
            || normalized.contains("pg_catalog.pg_statistic_ext")
            || normalized.contains("pg_catalog.pg_statistic_ext_data")
            || normalized.contains("pg_catalog.pg_stats_ext")
            || normalized.contains("pg_catalog.pg_stats_ext_exprs")
            || normalized.contains("pg_catalog.pg_indexes")
            || normalized.contains("information_schema.tables")
            || normalized.contains("information_schema.columns")
            || normalized.contains("information_schema.schemata")
            || normalized.contains("information_schema.table_constraints")
            || normalized.contains("information_schema.key_column_usage")
            || normalized.contains("information_schema.statistics")
            || normalized.contains("information_schema.referential_constraints")
            || normalized.contains("information_schema.constraint_column_usage")
            || normalized.contains("information_schema.constraint_table_usage")
            || normalized.contains("information_schema.column_privileges")
            || normalized.contains("information_schema.table_privileges")
            || normalized.contains("information_schema.views")
            || normalized.contains("information_schema.engines")
            || normalized.contains("information_schema.character_sets")
            || normalized.contains("information_schema.collations")
            || normalized.contains("information_schema.processlist")
            || normalized.contains("information_schema.global_variables")
            || normalized.contains("information_schema.session_variables")
            || normalized.contains("information_schema.sequences")
            || normalized.contains("information_schema.routines")
            || normalized.contains("information_schema.parameters");
        if (normalized.contains("pg_class") && !implemented_catalog_relation)
            || (normalized.contains("information_schema") && !implemented_catalog_relation)
        {
            return Some(Ok(vec![empty_query_response()]));
        }

        None
    }

    pub(super) fn is_supported_noop_set(normalized: &str) -> bool {
        const SUPPORTED_PREFIXES: &[&str] = &[
            "set application_name",
            "set client_encoding",
            "set standard_conforming_strings",
            "set extra_float_digits",
            "set datestyle",
            "set timezone",
            "set time zone",
            "set statement_timeout",
            "set lock_timeout",
            "set idle_in_transaction_session_timeout",
        ];
        SUPPORTED_PREFIXES
            .iter()
            .any(|prefix| normalized.starts_with(prefix))
    }
}
