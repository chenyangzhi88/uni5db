use std::collections::BTreeSet;

use pgwire::error::PgWireResult;

use super::GatewayServer;
use super::shared::parse_check_expr;
use crate::catalog::resolve_table_reference;
use crate::error::user_error;
use crate::filter::row_matches_filter;
use crate::types::{ColumnValue, RowMap, TableSchema, UniqueConstraintSchema};

impl GatewayServer {
    pub(super) fn validate_row_constraints(schema: &TableSchema, row: &RowMap) -> PgWireResult<()> {
        for column in &schema.columns {
            let value = row.get(&column.name).unwrap_or(&ColumnValue::Null);
            if !column.nullable && value.is_null() {
                return Err(user_error(
                    "23502",
                    format!(
                        "null value in column '{}' violates not-null constraint",
                        column.name
                    ),
                ));
            }
        }
        for constraint in &schema.check_constraints {
            let expr = parse_check_expr(&constraint.expr)?;
            if !row_matches_filter(row, schema, &expr) {
                return Err(user_error(
                    "23514",
                    format!("check constraint '{}' is violated", constraint.name),
                ));
            }
        }
        Ok(())
    }

    pub(super) fn resolve_foreign_table_reference(
        current_schema: &str,
        table_name: &str,
    ) -> PgWireResult<(String, String)> {
        if table_name.contains('.') {
            resolve_table_reference(table_name)
        } else {
            Ok((current_schema.to_string(), table_name.to_string()))
        }
    }

    pub(super) fn foreign_key_values(row: &RowMap, columns: &[String]) -> Vec<ColumnValue> {
        columns
            .iter()
            .map(|column| row.get(column).cloned().unwrap_or(ColumnValue::Null))
            .collect()
    }

    pub(super) fn row_matches_columns(
        row: &RowMap,
        columns: &[String],
        values: &[ColumnValue],
    ) -> bool {
        columns.iter().zip(values).all(|(column, value)| {
            row.get(column)
                .map(|row_value| row_value == value)
                .unwrap_or(false)
        })
    }

    pub(super) async fn validate_foreign_keys_at(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        row: &RowMap,
    ) -> PgWireResult<()> {
        for fk in &schema.foreign_keys {
            if fk.columns.len() != fk.referred_columns.len() || fk.columns.is_empty() {
                return Err(user_error(
                    "42830",
                    format!("foreign key constraint '{}' is malformed", fk.name),
                ));
            }
            let values = Self::foreign_key_values(row, &fk.columns);
            if values.iter().any(ColumnValue::is_null) {
                continue;
            }
            let (foreign_schema_name, foreign_table_name) =
                Self::resolve_foreign_table_reference(schema_name, &fk.foreign_table)?;
            let foreign_schema = self
                .resolve_table_schema(
                    database_name,
                    &[foreign_schema_name.clone()],
                    &foreign_table_name,
                )
                .await?
                .1
                .ok_or_else(|| {
                    user_error(
                        "42P01",
                        format!("referenced table '{}' does not exist", fk.foreign_table),
                    )
                })?;
            for referred_column in &fk.referred_columns {
                if foreign_schema.find_column(referred_column).is_none() {
                    return Err(user_error(
                        "42830",
                        format!(
                            "there is no unique constraint matching given keys for referenced table '{}'",
                            foreign_table_name
                        ),
                    ));
                }
            }

            let found = if fk.referred_columns.len() == 1
                && fk.referred_columns[0] == foreign_schema.primary_key
            {
                self.read_visible_row_at(
                    session_id,
                    database_name,
                    &foreign_schema_name,
                    &foreign_schema,
                    &values[0],
                )
                .await?
                .is_some()
            } else {
                self.scan_visible_rows_at(
                    session_id,
                    database_name,
                    &foreign_schema_name,
                    &foreign_schema,
                    None,
                )
                .await?
                .into_iter()
                .any(|foreign_row| {
                    Self::row_matches_columns(&foreign_row, &fk.referred_columns, &values)
                })
            };
            if !found {
                return Err(user_error(
                    "23503",
                    format!(
                        "insert or update on table '{}' violates foreign key constraint '{}'",
                        schema.table_name, fk.name
                    ),
                ));
            }
        }
        Ok(())
    }

    pub(super) async fn validate_no_foreign_key_references_at(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        row: &RowMap,
    ) -> PgWireResult<()> {
        for table in self.catalog.list_tables(database_name).await? {
            for fk in &table.schema.foreign_keys {
                if fk.columns.len() != fk.referred_columns.len() || fk.columns.is_empty() {
                    continue;
                }
                let (foreign_schema_name, foreign_table_name) =
                    Self::resolve_foreign_table_reference(&table.schema_name, &fk.foreign_table)?;
                if foreign_schema_name != schema_name || foreign_table_name != schema.table_name {
                    continue;
                }
                let referred_values = Self::foreign_key_values(row, &fk.referred_columns);
                if referred_values.iter().any(ColumnValue::is_null) {
                    continue;
                }
                let referenced = self
                    .scan_visible_rows_at(
                        session_id,
                        database_name,
                        &table.schema_name,
                        &table.schema,
                        None,
                    )
                    .await?
                    .into_iter()
                    .any(|child_row| {
                        let child_values = Self::foreign_key_values(&child_row, &fk.columns);
                        child_values == referred_values
                    });
                if referenced {
                    return Err(user_error(
                        "23503",
                        format!(
                            "update or delete on table '{}' violates foreign key constraint '{}'",
                            schema.table_name, fk.name
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    pub(super) async fn validate_unique_constraint_at(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        constraint: &UniqueConstraintSchema,
        current_pk: Option<&ColumnValue>,
        current_row: Option<&RowMap>,
    ) -> PgWireResult<()> {
        let rows = self
            .scan_visible_row_entries_at(session_id, database_name, schema_name, schema, None)
            .await?;
        let current_key = current_row.map(|row| {
            constraint
                .columns
                .iter()
                .map(|column| row.get(column).cloned().unwrap_or(ColumnValue::Null))
                .collect::<Vec<_>>()
        });
        let mut seen = BTreeSet::new();
        for (pk, row) in rows {
            if current_pk.is_some_and(|current| current == &pk) {
                continue;
            }
            let key = constraint
                .columns
                .iter()
                .map(|column| row.get(column).cloned().unwrap_or(ColumnValue::Null))
                .collect::<Vec<_>>();
            if key.iter().any(ColumnValue::is_null) {
                continue;
            }
            if current_key
                .as_ref()
                .is_some_and(|candidate| candidate == &key)
            {
                return Err(user_error(
                    "23505",
                    format!(
                        "duplicate key value violates unique constraint '{}'",
                        constraint.name
                    ),
                ));
            }
            if !seen.insert(format!("{key:?}")) {
                return Err(user_error(
                    "23505",
                    format!(
                        "duplicate key value violates unique constraint '{}'",
                        constraint.name
                    ),
                ));
            }
        }
        Ok(())
    }

    pub(super) async fn validate_unique_constraints_at(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        pk_value: &ColumnValue,
        row: &RowMap,
    ) -> PgWireResult<()> {
        for constraint in &schema.unique_constraints {
            if constraint.primary_key {
                continue;
            }
            self.validate_unique_constraint_at(
                session_id,
                database_name,
                schema_name,
                schema,
                constraint,
                Some(pk_value),
                Some(row),
            )
            .await?;
        }
        Ok(())
    }
}
