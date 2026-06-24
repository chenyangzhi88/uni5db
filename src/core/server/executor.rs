use pgwire::api::results::{FieldFormat, Response};
use pgwire::error::PgWireResult;

use super::GatewayServer;
use super::shared::{mysql_insert_command_tag, sequence_state_key};
use crate::codec::{index_table_prefix, table_data_prefix};
use crate::core::response::{
    build_pg_response_with_format, build_pg_returning_response, command_complete,
    command_complete_rows, multi_text_row_response,
};
use crate::error::user_error;
use crate::mode::GatewayMode;
use crate::sql::{evaluate_returning_row, nextval_sequence_name};
use crate::storage_layout;
use crate::types::{ColumnValue, InsertConflictAction, QueryPlan, ReadAccess};

impl GatewayServer {
    pub(super) async fn execute_plan(
        &self,
        plan: QueryPlan,
        session_id: Option<i32>,
        result_format: FieldFormat,
    ) -> PgWireResult<Response> {
        match plan {
            QueryPlan::Noop { tag } => Ok(command_complete(&tag)),
            QueryPlan::CreateDatabase {
                database_name,
                if_not_exists,
            } => {
                self.catalog
                    .create_database(&database_name, if_not_exists)
                    .await?;
                Ok(command_complete("CREATE DATABASE"))
            }
            QueryPlan::CreateSchema {
                database_name,
                schema_name,
                if_not_exists,
            } => {
                self.catalog
                    .create_schema(&database_name, &schema_name, if_not_exists)
                    .await?;
                Ok(command_complete("CREATE SCHEMA"))
            }
            QueryPlan::CreateTable {
                database_name,
                schema_name,
                schema,
                auto_increment_start,
                indexes,
            } => {
                self.catalog
                    .store_table(&database_name, &schema_name, &schema)
                    .await?;
                self.invalidate_datafusion_catalog(&database_name).await;
                for column in &schema.columns {
                    if let Some(sequence) =
                        column.default.as_deref().and_then(nextval_sequence_name)
                    {
                        let (sequence_schema, sequence_name) =
                            self.resolve_sequence_name(&schema_name, &sequence)?;
                        self.create_sequence_at(
                            &database_name,
                            &sequence_schema,
                            &sequence_name,
                            true,
                            auto_increment_start.unwrap_or(1),
                            1,
                        )
                        .await?;
                    }
                }
                for (index_name, column_names, unique) in indexes {
                    self.create_index_at(
                        session_id,
                        &database_name,
                        &schema_name,
                        &schema.table_name,
                        &index_name,
                        &column_names,
                        unique,
                        true,
                    )
                    .await?;
                }
                Ok(command_complete("CREATE TABLE"))
            }
            QueryPlan::CreateSequence {
                database_name,
                schema_name,
                sequence_name,
                if_not_exists,
                start,
                increment,
            } => {
                self.create_sequence_at(
                    &database_name,
                    &schema_name,
                    &sequence_name,
                    if_not_exists,
                    start,
                    increment,
                )
                .await?;
                Ok(command_complete("CREATE SEQUENCE"))
            }
            QueryPlan::CreateView {
                database_name,
                schema_name,
                view_name,
                definition,
                or_replace,
                if_not_exists,
            } => {
                self.catalog
                    .store_view(
                        &database_name,
                        &schema_name,
                        &view_name,
                        &definition,
                        or_replace,
                        if_not_exists,
                    )
                    .await?;
                self.invalidate_datafusion_catalog(&database_name).await;
                Ok(command_complete("CREATE VIEW"))
            }
            QueryPlan::CreateTableAs {
                database_name,
                schema_name,
                schema,
                rows,
            } => {
                self.catalog
                    .store_table(&database_name, &schema_name, &schema)
                    .await?;
                self.invalidate_datafusion_catalog(&database_name).await;
                let mut inserted = 0usize;
                for row in rows {
                    let pk_value = if schema.has_user_primary_key() {
                        row.get(&schema.primary_key)
                            .cloned()
                            .unwrap_or(ColumnValue::Null)
                    } else {
                        self.allocate_internal_row_id(schema.table_id).await?
                    };
                    self.write_row_at(
                        session_id,
                        &database_name,
                        &schema_name,
                        &schema,
                        &pk_value,
                        &row,
                    )
                    .await?;
                    inserted += 1;
                }
                Ok(command_complete_rows("SELECT", inserted))
            }
            QueryPlan::AlterTableAddPrimaryKey {
                database_name,
                schema_name,
                table_name,
                column_name,
            } => {
                self.alter_table_add_primary_key(
                    session_id,
                    &database_name,
                    &schema_name,
                    &table_name,
                    &column_name,
                )
                .await?;
                self.invalidate_datafusion_catalog(&database_name).await;
                Ok(command_complete("ALTER TABLE"))
            }
            QueryPlan::AlterTable {
                database_name,
                schema_name,
                table_name,
                operations,
            } => {
                for operation in operations {
                    self.alter_table_at(
                        session_id,
                        &database_name,
                        &schema_name,
                        &table_name,
                        operation,
                    )
                    .await?;
                }
                self.invalidate_datafusion_catalog(&database_name).await;
                Ok(command_complete("ALTER TABLE"))
            }
            QueryPlan::CreateIndex {
                database_name,
                schema_name,
                table_name,
                index_name,
                column_names,
                unique,
                if_not_exists,
            } => {
                self.create_index_at(
                    session_id,
                    &database_name,
                    &schema_name,
                    &table_name,
                    &index_name,
                    &column_names,
                    unique,
                    if_not_exists,
                )
                .await?;
                Ok(command_complete("CREATE INDEX"))
            }
            QueryPlan::DropTables {
                database_name,
                tables,
                if_exists,
            } => {
                for (schema_name, table_name) in tables {
                    let Some(table) = self
                        .catalog
                        .load_table(&database_name, &schema_name, &table_name)
                        .await?
                    else {
                        if if_exists {
                            continue;
                        }
                        return Err(user_error(
                            "42P01",
                            format!("table '{schema_name}.{table_name}' does not exist"),
                        ));
                    };
                    self.delete_index_data_for_table(
                        session_id,
                        &database_name,
                        &schema_name,
                        &table_name,
                    )
                    .await?;
                    self.catalog
                        .drop_table(&database_name, &schema_name, &table_name)
                        .await?;
                    self.invalidate_datafusion_catalog(&database_name).await;
                    let range = storage_layout::row_range(
                        table.schema.table_id,
                        table.schema.table_epoch,
                        None,
                    );
                    for (key, _) in self
                        .txn_scan_range(session_id, &range)
                        .await
                        .map_err(|e| user_error("XX000", e))?
                    {
                        self.txn_delete(session_id, &key)
                            .await
                            .map_err(|e| user_error("XX000", e))?;
                    }
                    let version_range = storage_layout::row_versions_range(
                        table.schema.table_id,
                        table.schema.table_epoch,
                        None,
                    );
                    for (key, _) in self
                        .txn_scan_range(session_id, &version_range)
                        .await
                        .map_err(|e| user_error("XX000", e))?
                    {
                        self.txn_delete(session_id, &key)
                            .await
                            .map_err(|e| user_error("XX000", e))?;
                    }
                    for prefix in [
                        storage_layout::olap_chunk_meta_prefix(
                            table.schema.table_id,
                            table.schema.table_epoch,
                        ),
                        storage_layout::olap_chunk_column_prefix(
                            table.schema.table_id,
                            table.schema.table_epoch,
                        ),
                        storage_layout::stats_prefix(
                            table.schema.table_id,
                            table.schema.table_epoch,
                        ),
                    ] {
                        for (key, _) in self
                            .txn_scan_prefix(session_id, &prefix)
                            .await
                            .map_err(|e| user_error("XX000", e))?
                        {
                            self.txn_delete(session_id, &key)
                                .await
                                .map_err(|e| user_error("XX000", e))?;
                        }
                    }
                    let rows = self
                        .txn_scan_prefix(
                            session_id,
                            &table_data_prefix(&database_name, &schema_name, &table_name),
                        )
                        .await
                        .map_err(|e| user_error("XX000", e))?;
                    for (key, _) in rows {
                        self.txn_delete(session_id, &key)
                            .await
                            .map_err(|e| user_error("XX000", e))?;
                    }
                }
                Ok(command_complete("DROP TABLE"))
            }
            QueryPlan::DropIndexes {
                database_name,
                indexes,
                if_exists,
            } => {
                let database = self
                    .catalog
                    .get_database(&database_name)
                    .await?
                    .ok_or_else(|| {
                        user_error(
                            "3D000",
                            format!("database '{database_name}' does not exist"),
                        )
                    })?;
                for (schema_name, index_name) in indexes {
                    let Some(schema_meta) = self
                        .catalog
                        .get_schema(database.database_id, &schema_name)
                        .await?
                    else {
                        if if_exists {
                            continue;
                        }
                        return Err(user_error(
                            "3F000",
                            format!("schema '{schema_name}' does not exist"),
                        ));
                    };
                    let Some(index) = self
                        .catalog
                        .drop_index(database.database_id, schema_meta.schema_id, &index_name)
                        .await?
                    else {
                        if if_exists {
                            continue;
                        }
                        return Err(user_error(
                            "42P01",
                            format!("index '{index_name}' does not exist"),
                        ));
                    };
                    for (key, _) in self
                        .txn_scan_prefix(
                            session_id,
                            &storage_layout::index_all_prefix(index.index_id),
                        )
                        .await
                        .map_err(|e| user_error("XX000", e))?
                    {
                        self.txn_delete(session_id, &key)
                            .await
                            .map_err(|e| user_error("XX000", e))?;
                    }
                    let legacy_prefix =
                        index_table_prefix(&database_name, &schema_name, &index.table_name);
                    for (key, _) in self
                        .txn_scan_prefix(session_id, &legacy_prefix)
                        .await
                        .map_err(|e| user_error("XX000", e))?
                    {
                        if String::from_utf8_lossy(&key).contains(&format!("/{index_name}/")) {
                            self.txn_delete(session_id, &key)
                                .await
                                .map_err(|e| user_error("XX000", e))?;
                        }
                    }
                }
                Ok(command_complete("DROP INDEX"))
            }
            QueryPlan::DropSequences {
                database_name,
                sequences,
                if_exists,
            } => {
                for (schema_name, sequence_name) in sequences {
                    let key = sequence_state_key(&database_name, &schema_name, &sequence_name);
                    if self
                        .store
                        .get(&key)
                        .await
                        .map_err(|e| user_error("XX000", e))?
                        .is_none()
                        && !if_exists
                    {
                        return Err(user_error(
                            "42P01",
                            format!("sequence '{schema_name}.{sequence_name}' does not exist"),
                        ));
                    }
                    self.store
                        .delete(&key)
                        .await
                        .map_err(|e| user_error("XX000", e))?;
                }
                Ok(command_complete("DROP SEQUENCE"))
            }
            QueryPlan::DropViews {
                database_name,
                views,
                if_exists,
            } => {
                for (schema_name, view_name) in views {
                    let dropped = self
                        .catalog
                        .drop_view(&database_name, &schema_name, &view_name)
                        .await?;
                    if dropped.is_none() && !if_exists {
                        return Err(user_error(
                            "42P01",
                            format!("view '{schema_name}.{view_name}' does not exist"),
                        ));
                    }
                    if dropped.is_some() {
                        self.invalidate_datafusion_catalog(&database_name).await;
                    }
                }
                Ok(command_complete("DROP VIEW"))
            }
            QueryPlan::DropSchemas {
                database_name,
                schemas,
                if_exists,
            } => {
                for schema_name in schemas {
                    let tables = self.catalog.list_tables(&database_name).await?;
                    for table in tables
                        .into_iter()
                        .filter(|table| table.schema_name == schema_name)
                    {
                        self.delete_index_data_for_table(
                            session_id,
                            &database_name,
                            &schema_name,
                            &table.table_name,
                        )
                        .await?;
                        let _ = self
                            .catalog
                            .drop_table(&database_name, &schema_name, &table.table_name)
                            .await?;
                        self.invalidate_datafusion_catalog(&database_name).await;
                    }
                    for view in self
                        .catalog
                        .list_views(&database_name)
                        .await?
                        .into_iter()
                        .filter(|view| view.schema_name == schema_name)
                    {
                        if self
                            .catalog
                            .drop_view(&database_name, &schema_name, &view.view_name)
                            .await?
                            .is_some()
                        {
                            self.invalidate_datafusion_catalog(&database_name).await;
                        }
                    }
                    if self
                        .catalog
                        .drop_schema(&database_name, &schema_name)
                        .await?
                        .is_none()
                        && !if_exists
                    {
                        return Err(user_error(
                            "3F000",
                            format!("schema '{schema_name}' does not exist"),
                        ));
                    }
                }
                Ok(command_complete("DROP SCHEMA"))
            }
            QueryPlan::DropDatabases {
                databases,
                if_exists,
            } => {
                for database_name in databases {
                    if self.catalog.drop_database(&database_name).await?.is_none() && !if_exists {
                        return Err(user_error(
                            "3D000",
                            format!("database '{database_name}' does not exist"),
                        ));
                    }
                    self.invalidate_datafusion_catalog(&database_name).await;
                }
                Ok(command_complete("DROP DATABASE"))
            }
            QueryPlan::TruncateTables {
                database_name,
                tables,
            } => {
                for (schema_name, schema) in tables {
                    let table_name = schema.table_name.clone();
                    let table = self
                        .catalog
                        .bump_table_epoch(&database_name, &schema_name, &table_name)
                        .await?;
                    self.invalidate_datafusion_catalog(&database_name).await;
                    let old_epoch = table.schema.table_epoch.saturating_sub(1).max(1);
                    for prefix in [
                        storage_layout::olap_chunk_meta_prefix(table.schema.table_id, old_epoch),
                        storage_layout::olap_chunk_column_prefix(table.schema.table_id, old_epoch),
                        storage_layout::stats_prefix(table.schema.table_id, old_epoch),
                    ] {
                        for (key, _) in self
                            .txn_scan_prefix(session_id, &prefix)
                            .await
                            .map_err(|e| user_error("XX000", e))?
                        {
                            self.txn_delete(session_id, &key)
                                .await
                                .map_err(|e| user_error("XX000", e))?;
                        }
                    }
                    let rows = self
                        .txn_scan_prefix(
                            session_id,
                            &table_data_prefix(&database_name, &schema_name, &table_name),
                        )
                        .await
                        .map_err(|e| user_error("XX000", e))?;
                    for (key, _) in rows {
                        self.txn_delete(session_id, &key)
                            .await
                            .map_err(|e| user_error("XX000", e))?;
                    }
                    self.delete_index_data_for_table(
                        session_id,
                        &database_name,
                        &schema_name,
                        &table_name,
                    )
                    .await?;
                    if self.mode == GatewayMode::MySql {
                        self.reset_auto_increment_sequence(&database_name, &schema_name, &schema)
                            .await?;
                    }
                }
                Ok(if self.mode == GatewayMode::MySql {
                    command_complete_rows("TRUNCATE TABLE", 0)
                } else {
                    command_complete("TRUNCATE TABLE")
                })
            }
            QueryPlan::ExplainRows { rows } => {
                let columns = [
                    "id",
                    "select_type",
                    "table",
                    "partitions",
                    "type",
                    "possible_keys",
                    "key",
                    "key_len",
                    "ref",
                    "rows",
                    "filtered",
                    "Extra",
                ];
                multi_text_row_response(&columns, &rows).map(|mut responses| responses.remove(0))
            }
            QueryPlan::PostgresExplainRows { rows } => {
                multi_text_row_response(&["QUERY PLAN"], &rows)
                    .map(|mut responses| responses.remove(0))
            }
            QueryPlan::AnalyzeTables {
                database_name,
                tables,
            } => {
                for (schema_name, schema) in tables {
                    self.refresh_olap_storage_at(session_id, &database_name, &schema_name, &schema)
                        .await?;
                }
                self.invalidate_datafusion_catalog(&database_name).await;
                Ok(command_complete("ANALYZE"))
            }
            QueryPlan::TableMaintenanceRows { rows } => {
                let columns = ["Table", "Op", "Msg_type", "Msg_text"];
                multi_text_row_response(&columns, &rows).map(|mut responses| responses.remove(0))
            }
            QueryPlan::InsertRows {
                database_name,
                schema_name,
                schema,
                rows,
                on_conflict,
                returning,
            } => {
                let mut inserted = 0usize;
                let mut affected_rows = 0usize;
                let mut first_insert_id = None;
                let mut returned_rows = Vec::with_capacity(rows.len());
                for row in rows {
                    let mut pk_value = if schema.has_user_primary_key() {
                        row.get(&schema.primary_key)
                            .cloned()
                            .unwrap_or(ColumnValue::Null)
                    } else {
                        self.allocate_internal_row_id(schema.table_id).await?
                    };
                    if pk_value.is_null() {
                        return Err(user_error(
                            "23502",
                            format!("null value for primary key '{}'", schema.primary_key),
                        ));
                    }
                    let existing = if let Some(on_conflict) = &on_conflict {
                        self.find_existing_row_for_conflict(
                            session_id,
                            &database_name,
                            &schema_name,
                            &schema,
                            on_conflict,
                            &row,
                        )
                        .await?
                    } else {
                        None
                    };
                    let mut final_row = row;
                    if let Some(existing_row) = existing {
                        let existing_key = existing_row.0;
                        let existing_row = existing_row.1;
                        match &on_conflict {
                            Some(
                                InsertConflictAction::DoNothing
                                | InsertConflictAction::DoNothingAnyUnique { .. },
                            ) => continue,
                            Some(InsertConflictAction::ReplaceAnyUnique { .. }) => {
                                self.delete_row_at(
                                    session_id,
                                    &database_name,
                                    &schema_name,
                                    &schema,
                                    &existing_key,
                                )
                                .await?;
                                affected_rows += 2;
                            }
                            Some(InsertConflictAction::DoUpdate {
                                assignments,
                                selection,
                                ..
                            }) => {
                                final_row = match self.apply_insert_conflict_update(
                                    &schema,
                                    existing_row.clone(),
                                    &final_row,
                                    assignments,
                                    selection.as_ref(),
                                )? {
                                    Some(updated) => {
                                        pk_value = existing_key;
                                        affected_rows +=
                                            if updated == existing_row { 0 } else { 2 };
                                        updated
                                    }
                                    None => continue,
                                };
                            }
                            Some(InsertConflictAction::DoUpdateAnyUnique {
                                assignments, ..
                            }) => {
                                final_row = match self.apply_insert_conflict_update(
                                    &schema,
                                    existing_row.clone(),
                                    &final_row,
                                    assignments,
                                    None,
                                )? {
                                    Some(updated) => {
                                        pk_value = existing_key;
                                        affected_rows +=
                                            if updated == existing_row { 0 } else { 2 };
                                        updated
                                    }
                                    None => continue,
                                };
                            }
                            _ => {}
                        }
                    } else {
                        if on_conflict.is_none()
                            && schema.has_user_primary_key()
                            && self
                                .read_visible_row_at(
                                    session_id,
                                    &database_name,
                                    &schema_name,
                                    &schema,
                                    &pk_value,
                                )
                                .await?
                                .is_some()
                        {
                            return Err(user_error(
                                "23505",
                                format!(
                                    "duplicate key value violates unique constraint '{}_pkey'",
                                    schema.table_name
                                ),
                            ));
                        }
                        affected_rows += 1;
                    }

                    self.write_row_at(
                        session_id,
                        &database_name,
                        &schema_name,
                        &schema,
                        &pk_value,
                        &final_row,
                    )
                    .await?;
                    if let Some(insert_id) = self
                        .advance_auto_increment_from_row(
                            &database_name,
                            &schema_name,
                            &schema,
                            &final_row,
                        )
                        .await?
                    {
                        first_insert_id.get_or_insert(insert_id);
                    }
                    if returning.is_some() {
                        returned_rows.push(final_row);
                    }
                    inserted += 1;
                }
                self.catalog
                    .store_table(&database_name, &schema_name, &schema)
                    .await?;
                if let Some(projection) = returning {
                    let mut output_rows = Vec::with_capacity(returned_rows.len());
                    for row in returned_rows {
                        output_rows.push(evaluate_returning_row(&projection, &schema, &row, None)?);
                    }
                    build_pg_returning_response(output_rows, &schema, &projection, result_format)
                } else {
                    Ok(if self.mode == GatewayMode::MySql {
                        mysql_insert_command_tag(affected_rows, first_insert_id)
                    } else {
                        command_complete_rows("INSERT 0", inserted)
                    })
                }
            }
            QueryPlan::SelectRows {
                database_name,
                schema_name,
                schema,
                projection,
                access,
                limit,
                offset,
            } => {
                let mut rows_are_windowed = false;
                let rows = match access {
                    ReadAccess::PointLookup { key } => self
                        .read_visible_row_at(
                            session_id,
                            &database_name,
                            &schema_name,
                            &schema,
                            &key,
                        )
                        .await?
                        .into_iter()
                        .collect(),
                    ReadAccess::PrimaryKeyInLookup { keys } => {
                        self.read_visible_rows_by_pk_in_at(
                            session_id,
                            &database_name,
                            &schema_name,
                            &schema,
                            &keys,
                        )
                        .await?
                    }
                    ReadAccess::PrimaryKeyRangeScan {
                        lower,
                        upper,
                        filter,
                    } => {
                        let can_window_scan = offset > 0 || limit.is_some();
                        rows_are_windowed = can_window_scan;
                        self.scan_visible_row_entries_by_pk_range_at(
                            session_id,
                            &database_name,
                            &schema_name,
                            &schema,
                            lower.as_ref().map(|(value, inclusive)| (value, *inclusive)),
                            upper.as_ref().map(|(value, inclusive)| (value, *inclusive)),
                            filter.as_ref(),
                            if can_window_scan { limit } else { None },
                            if can_window_scan { offset } else { 0 },
                        )
                        .await?
                        .into_iter()
                        .map(|(_, row)| row)
                        .collect()
                    }
                    ReadAccess::SecondaryIndexLookup {
                        index_name,
                        column_name: _,
                        key,
                        filter,
                    } => {
                        self.scan_index_rows_at(
                            session_id,
                            &database_name,
                            &schema_name,
                            &schema,
                            &index_name,
                            &key,
                            filter.as_ref(),
                        )
                        .await?
                    }
                    ReadAccess::SecondaryIndexRangeScan {
                        index_name,
                        column_name: _,
                        lower,
                        upper,
                        filter,
                    } => self
                        .scan_index_row_entries_by_range_at(
                            session_id,
                            &database_name,
                            &schema_name,
                            &schema,
                            &index_name,
                            lower.as_ref().map(|(value, inclusive)| (value, *inclusive)),
                            upper.as_ref().map(|(value, inclusive)| (value, *inclusive)),
                            filter.as_ref(),
                        )
                        .await?
                        .into_iter()
                        .map(|(_, row)| row)
                        .collect(),
                    ReadAccess::PrefixScan { filter } => {
                        let can_window_scan = offset > 0 || limit.is_some();
                        if can_window_scan {
                            rows_are_windowed = true;
                            self.scan_visible_row_entries_by_pk_range_at(
                                session_id,
                                &database_name,
                                &schema_name,
                                &schema,
                                None,
                                None,
                                filter.as_ref(),
                                limit,
                                offset,
                            )
                            .await?
                            .into_iter()
                            .map(|(_, row)| row)
                            .collect()
                        } else {
                            self.scan_visible_rows_at(
                                session_id,
                                &database_name,
                                &schema_name,
                                &schema,
                                filter.as_ref(),
                            )
                            .await?
                        }
                    }
                };
                let rows = if rows_are_windowed {
                    rows
                } else if offset > 0 || limit.is_some() {
                    let iter = rows.into_iter().skip(offset);
                    match limit {
                        Some(limit) => iter.take(limit).collect(),
                        None => iter.collect(),
                    }
                } else {
                    rows
                };
                build_pg_response_with_format(rows, &schema, &projection, result_format)
            }
            QueryPlan::UpdateRows {
                database_name,
                schema_name,
                schema,
                assignments,
                access,
                limit,
                order_by_primary_key,
                returning,
            } => {
                if let Some(projection) = returning {
                    let rows = self
                        .collect_updated_rows(
                            session_id,
                            &database_name,
                            &schema_name,
                            &schema,
                            &assignments,
                            access,
                            limit,
                            order_by_primary_key,
                        )
                        .await?;
                    let mut output_rows = Vec::with_capacity(rows.len());
                    for row in rows {
                        output_rows.push(evaluate_returning_row(&projection, &schema, &row, None)?);
                    }
                    build_pg_returning_response(output_rows, &schema, &projection, result_format)
                } else {
                    let updated = self
                        .apply_update_rows(
                            session_id,
                            &database_name,
                            &schema_name,
                            &schema,
                            &assignments,
                            access,
                            limit,
                            order_by_primary_key,
                        )
                        .await?;
                    Ok(command_complete_rows("UPDATE", updated))
                }
            }
            QueryPlan::DeleteRows {
                database_name,
                schema_name,
                schema,
                access,
                limit,
                order_by_primary_key,
                returning,
            } => {
                if let Some(projection) = returning {
                    let rows = self
                        .collect_deleted_rows(
                            session_id,
                            &database_name,
                            &schema_name,
                            &schema,
                            access,
                            limit,
                            order_by_primary_key,
                        )
                        .await?;
                    let mut output_rows = Vec::with_capacity(rows.len());
                    for row in rows {
                        output_rows.push(evaluate_returning_row(&projection, &schema, &row, None)?);
                    }
                    build_pg_returning_response(output_rows, &schema, &projection, result_format)
                } else {
                    let deleted = self
                        .apply_delete_rows(
                            session_id,
                            &database_name,
                            &schema_name,
                            &schema,
                            access,
                            limit,
                            order_by_primary_key,
                        )
                        .await?;
                    Ok(command_complete_rows("DELETE", deleted))
                }
            }
        }
    }
}
