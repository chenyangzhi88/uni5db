use pgwire::error::PgWireResult;

use super::shared::{mysql_column_type_name, mysql_escape_identifier};
use super::{GatewayServer, MySqlColumnMetadata};
use crate::catalog::{DEFAULT_SCHEMA_NAME, IndexCatalog};
use crate::error::user_error;
use crate::sql::nextval_sequence_name;
use crate::types::ColumnSchema;

impl GatewayServer {
    pub(crate) async fn mysql_describe_table(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> PgWireResult<Vec<MySqlColumnMetadata>> {
        let search_path = vec![
            self.default_schema_name().to_string(),
            DEFAULT_SCHEMA_NAME.to_string(),
        ];
        let (_schema_name, schema) = self
            .resolve_table_schema(database_name, &search_path, table_name)
            .await?;
        let Some(schema) = schema else {
            return Err(user_error(
                "42P01",
                format!("table '{table_name}' does not exist"),
            ));
        };
        let indexes = self.catalog.list_indexes_for_table(schema.table_id).await?;
        let mut rows = Vec::with_capacity(schema.columns.len());
        for column in &schema.columns {
            let key = mysql_column_key(
                column,
                &schema.primary_key,
                &schema.unique_constraints,
                &indexes,
            );
            let extra = mysql_column_extra(column);
            rows.push(MySqlColumnMetadata {
                field: column.name.clone(),
                column_type: mysql_column_type_name(&column.data_type),
                nullable: if column.nullable { "YES" } else { "NO" }.to_string(),
                key,
                default_value: column.default.clone(),
                extra,
            });
        }
        Ok(rows)
    }

    pub(crate) async fn mysql_describe_table_fast(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> PgWireResult<Vec<MySqlColumnMetadata>> {
        let search_path = vec![
            self.default_schema_name().to_string(),
            DEFAULT_SCHEMA_NAME.to_string(),
        ];
        let (_schema_name, schema) = self
            .resolve_table_schema(database_name, &search_path, table_name)
            .await?;
        let Some(schema) = schema else {
            return Err(user_error(
                "42P01",
                format!("table '{table_name}' does not exist"),
            ));
        };
        let mut rows = Vec::with_capacity(schema.columns.len());
        for column in &schema.columns {
            let key = if column.primary_key || column.name == schema.primary_key {
                "PRI"
            } else if schema.unique_constraints.iter().any(|constraint| {
                !constraint.primary_key
                    && constraint.columns.len() == 1
                    && constraint.columns[0].eq_ignore_ascii_case(&column.name)
            }) {
                "UNI"
            } else {
                ""
            };
            rows.push(MySqlColumnMetadata {
                field: column.name.clone(),
                column_type: mysql_column_type_name(&column.data_type),
                nullable: if column.nullable { "YES" } else { "NO" }.to_string(),
                key: key.to_string(),
                default_value: column.default.clone(),
                extra: mysql_column_extra(column),
            });
        }
        Ok(rows)
    }

    pub(crate) async fn mysql_show_tables(&self, database_name: &str) -> PgWireResult<Vec<String>> {
        let mut tables = self
            .catalog
            .list_tables(database_name)
            .await?
            .into_iter()
            .map(|table| table.table_name)
            .collect::<Vec<_>>();
        tables.sort();
        Ok(tables)
    }

    pub(crate) async fn mysql_show_databases(&self) -> PgWireResult<Vec<String>> {
        let mut databases = self
            .catalog
            .list_databases()
            .await?
            .into_iter()
            .map(|database| database.database_name)
            .collect::<Vec<_>>();
        databases.sort();
        Ok(databases)
    }

    pub(crate) async fn mysql_show_create_table(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> PgWireResult<String> {
        let search_path = vec![
            self.default_schema_name().to_string(),
            DEFAULT_SCHEMA_NAME.to_string(),
        ];
        let (_schema_name, schema) = self
            .resolve_table_schema(database_name, &search_path, table_name)
            .await?;
        let Some(schema) = schema else {
            return Err(user_error(
                "42P01",
                format!("table '{table_name}' does not exist"),
            ));
        };
        let indexes = self.catalog.list_indexes_for_table(schema.table_id).await?;
        let mut lines = Vec::new();
        for column in &schema.columns {
            let mut line = format!(
                "  `{}` {}",
                mysql_escape_identifier(&column.name),
                mysql_column_type_name(&column.data_type)
            );
            if !column.nullable || column.primary_key || column.name == schema.primary_key {
                line.push_str(" NOT NULL");
            }
            if column
                .default
                .as_deref()
                .and_then(nextval_sequence_name)
                .is_some()
            {
                line.push_str(" AUTO_INCREMENT");
            } else if let Some(default) = &column.default {
                line.push_str(" DEFAULT ");
                line.push_str(default);
            }
            if let Some(character_set) = &column.character_set {
                line.push_str(" CHARACTER SET ");
                line.push_str(character_set);
            }
            if let Some(collation) = &column.collation {
                line.push_str(" COLLATE ");
                line.push_str(collation);
            }
            if let Some(on_update) = &column.on_update {
                line.push_str(" ON UPDATE ");
                line.push_str(on_update);
            }
            lines.push(line);
        }
        if schema.has_user_primary_key() {
            lines.push(format!(
                "  PRIMARY KEY (`{}`)",
                mysql_escape_identifier(&schema.primary_key)
            ));
        }
        for constraint in &schema.unique_constraints {
            if constraint.primary_key {
                continue;
            }
            let columns = constraint
                .columns
                .iter()
                .map(|column| format!("`{}`", mysql_escape_identifier(column)))
                .collect::<Vec<_>>()
                .join(", ");
            lines.push(format!(
                "  UNIQUE KEY `{}` ({columns})",
                mysql_escape_identifier(&constraint.name)
            ));
        }
        for index in indexes {
            let columns = index
                .column_names
                .iter()
                .map(|column| format!("`{}`", mysql_escape_identifier(column)))
                .collect::<Vec<_>>()
                .join(", ");
            let prefix = if index.unique { "UNIQUE KEY" } else { "KEY" };
            lines.push(format!(
                "  {prefix} `{}` ({columns})",
                mysql_escape_identifier(&index.index_name)
            ));
        }
        Ok(format!(
            "CREATE TABLE `{}` (\n{}\n) ENGINE=UniDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
            mysql_escape_identifier(&schema.table_name),
            lines.join(",\n")
        ))
    }
}

fn mysql_column_key(
    column: &ColumnSchema,
    primary_key: &str,
    unique_constraints: &[crate::types::UniqueConstraintSchema],
    indexes: &[IndexCatalog],
) -> String {
    if column.primary_key || column.name == primary_key {
        "PRI".to_string()
    } else if unique_constraints.iter().any(|constraint| {
        !constraint.primary_key
            && constraint.columns.len() == 1
            && constraint.columns[0].eq_ignore_ascii_case(&column.name)
    }) || indexes.iter().any(|index| {
        index.unique
            && index.column_names.len() == 1
            && index.column_names[0].eq_ignore_ascii_case(&column.name)
    }) {
        "UNI".to_string()
    } else if indexes.iter().any(|index| {
        !index.unique
            && index
                .column_names
                .iter()
                .any(|name| name.eq_ignore_ascii_case(&column.name))
    }) {
        "MUL".to_string()
    } else {
        String::new()
    }
}

fn mysql_column_extra(column: &ColumnSchema) -> String {
    let mut extras = Vec::new();
    if column
        .default
        .as_deref()
        .and_then(nextval_sequence_name)
        .is_some()
    {
        extras.push("auto_increment".to_string());
    }
    if let Some(on_update) = &column.on_update {
        extras.push(format!("on update {on_update}"));
    }
    extras.join(" ")
}
