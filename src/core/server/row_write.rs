use std::collections::BTreeSet;

use pgwire::error::PgWireResult;

use super::GatewayServer;
use super::shared::{
    PRIMARY_KEY_BACKFILL_WRITE_BATCH_SIZE, mysql_auto_increment_column, parse_check_expr,
};
use crate::codec::{
    cell_key, encode_cell_value, index_entry_key, index_table_prefix, row_marker_key,
};
use crate::error::{unsupported, user_error};
use crate::filter::row_matches_filter;
use crate::sql::nextval_sequence_name;
use crate::storage_layout;
use crate::types::{ColumnValue, RowMap, TableAlterOperation, TableSchema};

impl GatewayServer {
    pub(super) async fn write_row_at(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        pk_value: &ColumnValue,
        row: &RowMap,
    ) -> PgWireResult<()> {
        self.write_row_at_with_olap_refresh(
            session_id,
            database_name,
            schema_name,
            schema,
            pk_value,
            row,
            true,
        )
        .await
    }

    pub(super) async fn write_row_at_with_olap_refresh(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        pk_value: &ColumnValue,
        row: &RowMap,
        refresh_olap: bool,
    ) -> PgWireResult<()> {
        Self::validate_row_constraints(schema, row)?;
        let old_row = self
            .read_visible_row_at(session_id, database_name, schema_name, schema, pk_value)
            .await?;
        if old_row.is_some() {
            self.ensure_row_write_not_stale(session_id, schema, pk_value)
                .await?;
        }
        self.validate_foreign_keys_at(session_id, database_name, schema_name, schema, row)
            .await?;
        self.validate_index_entries_at(
            session_id,
            database_name,
            schema_name,
            schema,
            pk_value,
            row,
        )
        .await?;
        self.validate_unique_constraints_at(
            session_id,
            database_name,
            schema_name,
            schema,
            pk_value,
            row,
        )
        .await?;
        if let Some(old_row) = old_row {
            self.delete_index_entries_at(
                session_id,
                database_name,
                schema_name,
                schema,
                pk_value,
                &old_row,
            )
            .await?;
        }

        let v2_key = storage_layout::row_key(schema.table_id, schema.table_epoch, pk_value);
        let v2_value = storage_layout::encode_row_record(schema, row);
        self.txn_put(session_id, &v2_key, &v2_value)
            .await
            .map_err(|e| user_error("XX000", e))?;

        let marker = row_marker_key(database_name, schema_name, &schema.table_name, pk_value);
        let _ = self.txn_delete(session_id, &marker).await;
        for column in &schema.columns {
            let key = cell_key(
                database_name,
                schema_name,
                &schema.table_name,
                &column.name,
                pk_value,
            );
            let _ = self.txn_delete(session_id, &key).await;
        }
        self.write_index_entries_at(
            session_id,
            database_name,
            schema_name,
            schema,
            pk_value,
            row,
        )
        .await?;
        if refresh_olap {
            self.refresh_olap_storage_at(session_id, database_name, schema_name, schema)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn delete_row_at(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        pk_value: &ColumnValue,
    ) -> PgWireResult<()> {
        if let Some(row) = self
            .read_visible_row_at(session_id, database_name, schema_name, schema, pk_value)
            .await?
        {
            self.ensure_row_write_not_stale(session_id, schema, pk_value)
                .await?;
            self.validate_no_foreign_key_references_at(
                session_id,
                database_name,
                schema_name,
                schema,
                &row,
            )
            .await?;
            self.delete_index_entries_at(
                session_id,
                database_name,
                schema_name,
                schema,
                pk_value,
                &row,
            )
            .await?;
        }
        let marker = row_marker_key(database_name, schema_name, &schema.table_name, pk_value);
        let v2_key = storage_layout::row_key(schema.table_id, schema.table_epoch, pk_value);
        self.txn_delete(session_id, &v2_key)
            .await
            .map_err(|e| user_error("XX000", e))?;
        self.txn_delete(session_id, &marker)
            .await
            .map_err(|e| user_error("XX000", e))?;
        for column in &schema.columns {
            let key = cell_key(
                database_name,
                schema_name,
                &schema.table_name,
                &column.name,
                pk_value,
            );
            self.txn_delete(session_id, &key)
                .await
                .map_err(|e| user_error("XX000", e))?;
        }
        self.refresh_olap_storage_at(session_id, database_name, schema_name, schema)
            .await?;
        Ok(())
    }

    pub(super) async fn alter_table_add_primary_key(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        table_name: &str,
        column_name: &str,
    ) -> PgWireResult<()> {
        let table = self
            .catalog
            .load_table(database_name, schema_name, table_name)
            .await?
            .ok_or_else(|| user_error("42P01", format!("table '{table_name}' does not exist")))?;
        let old_schema = table.schema;
        if old_schema.has_user_primary_key() && old_schema.primary_key != column_name {
            return Err(unsupported(
                "fast-path ALTER TABLE ADD PRIMARY KEY does not support replacing an existing primary key",
            ));
        }

        let mut new_schema = old_schema.clone();
        new_schema.schema_version = new_schema.schema_version.saturating_add(1).max(1);
        new_schema.table_epoch = new_schema.table_epoch.saturating_add(1).max(1);
        let mut found = false;
        for column in &mut new_schema.columns {
            let is_target = column.name == column_name;
            column.primary_key = is_target;
            if is_target {
                column.nullable = false;
                found = true;
            }
        }
        if !found {
            return Err(user_error(
                "42703",
                format!("column '{column_name}' not found"),
            ));
        }
        new_schema.primary_key = column_name.to_string();

        let rows = self
            .scan_visible_row_entries_at(session_id, database_name, schema_name, &old_schema, None)
            .await?;
        let indexes = self
            .catalog
            .list_indexes_for_table(old_schema.table_id)
            .await?;
        let mut seen_keys = BTreeSet::new();
        let mut rewrites = Vec::with_capacity(rows.len());
        for (old_pk, row) in rows {
            Self::validate_row_constraints(&new_schema, &row)?;
            let pk_value = row.get(column_name).cloned().unwrap_or(ColumnValue::Null);
            if pk_value.is_null() {
                return Err(user_error(
                    "23502",
                    format!("column '{column_name}' contains null values"),
                ));
            }
            let dedupe_key = storage_layout::encode_key_value(&pk_value);
            if !seen_keys.insert(dedupe_key) {
                return Err(user_error(
                    "23505",
                    format!(
                        "could not create primary key on '{}': duplicate values exist",
                        column_name
                    ),
                ));
            }
            rewrites.push((old_pk, pk_value, row));
        }

        self.delete_index_data_for_table(session_id, database_name, schema_name, table_name)
            .await?;

        for (old_pk, new_pk, _) in &rewrites {
            if old_pk != new_pk {
                let marker =
                    row_marker_key(database_name, schema_name, &old_schema.table_name, old_pk);
                self.txn_delete(session_id, &marker)
                    .await
                    .map_err(|e| user_error("XX000", e))?;
                for column in &old_schema.columns {
                    let key = cell_key(
                        database_name,
                        schema_name,
                        &old_schema.table_name,
                        &column.name,
                        old_pk,
                    );
                    self.txn_delete(session_id, &key)
                        .await
                        .map_err(|e| user_error("XX000", e))?;
                }
            }
        }

        let mut entries = Vec::with_capacity(PRIMARY_KEY_BACKFILL_WRITE_BATCH_SIZE);
        for (_, new_pk, row) in &rewrites {
            entries.push((
                storage_layout::row_key(new_schema.table_id, new_schema.table_epoch, new_pk),
                storage_layout::encode_row_record(&new_schema, row),
            ));
            entries.push((
                row_marker_key(database_name, schema_name, &new_schema.table_name, new_pk),
                vec![1],
            ));
            for column in &new_schema.columns {
                let value = row.get(&column.name).unwrap_or(&ColumnValue::Null);
                entries.push((
                    cell_key(
                        database_name,
                        schema_name,
                        &new_schema.table_name,
                        &column.name,
                        new_pk,
                    ),
                    encode_cell_value(value),
                ));
            }
            for index in &indexes {
                let index_value = Self::indexed_value(row, index);
                entries.push((
                    storage_layout::index_entry_key(index.index_id, &index_value, new_pk),
                    vec![1],
                ));
                entries.push((
                    index_entry_key(
                        database_name,
                        schema_name,
                        &new_schema.table_name,
                        &index.index_name,
                        &index_value,
                        new_pk,
                    ),
                    vec![1],
                ));
            }

            if entries.len() >= PRIMARY_KEY_BACKFILL_WRITE_BATCH_SIZE {
                self.txn_put_batch(session_id, std::mem::take(&mut entries))
                    .await
                    .map_err(|e| user_error("XX000", e))?;
            }
        }
        if !entries.is_empty() {
            self.txn_put_batch(session_id, entries)
                .await
                .map_err(|e| user_error("XX000", e))?;
        }

        self.catalog
            .store_table(database_name, schema_name, &new_schema)
            .await?;
        self.refresh_olap_storage_at(session_id, database_name, schema_name, &new_schema)
            .await?;
        Ok(())
    }

    pub(super) async fn alter_table_at(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        table_name: &str,
        operation: TableAlterOperation,
    ) -> PgWireResult<()> {
        let table = self
            .catalog
            .load_table(database_name, schema_name, table_name)
            .await?
            .ok_or_else(|| user_error("42P01", format!("table '{table_name}' does not exist")))?;
        let mut schema = table.schema;
        match operation {
            TableAlterOperation::AddColumn {
                mut column,
                if_not_exists,
            } => {
                if schema.find_column(&column.name).is_some() {
                    if if_not_exists {
                        return Ok(());
                    }
                    return Err(user_error(
                        "42701",
                        format!("column '{}' already exists", column.name),
                    ));
                }
                let rows = self
                    .scan_visible_row_entries_at(
                        session_id,
                        database_name,
                        schema_name,
                        &schema,
                        None,
                    )
                    .await?;
                if !column.nullable && column.default.is_none() && !rows.is_empty() {
                    return Err(user_error(
                        "23502",
                        format!("column '{}' contains null values", column.name),
                    ));
                }
                column.column_id = schema
                    .columns
                    .iter()
                    .map(|c| c.column_id)
                    .max()
                    .unwrap_or(0)
                    + 1;
                let mut new_schema = schema.clone();
                new_schema.columns.push(column.clone());
                new_schema.schema_version = new_schema.schema_version.saturating_add(1).max(1);
                for (pk, mut row) in rows {
                    let value = self
                        .column_default_value_at(database_name, schema_name, &column)
                        .await?;
                    row.insert(column.name.clone(), value);
                    self.write_row_at_with_olap_refresh(
                        session_id,
                        database_name,
                        schema_name,
                        &new_schema,
                        &pk,
                        &row,
                        false,
                    )
                    .await?;
                }
                self.catalog
                    .store_table(database_name, schema_name, &new_schema)
                    .await?;
                self.refresh_olap_storage_at(session_id, database_name, schema_name, &new_schema)
                    .await?;
            }
            TableAlterOperation::ModifyColumn {
                column_name,
                mut column,
            } => {
                let Some(pos) = schema
                    .columns
                    .iter()
                    .position(|existing| existing.name == column_name)
                else {
                    return Err(user_error(
                        "42703",
                        format!("column '{column_name}' not found"),
                    ));
                };
                if column.name != column_name && schema.find_column(&column.name).is_some() {
                    return Err(user_error(
                        "42701",
                        format!("column '{}' already exists", column.name),
                    ));
                }
                let previous = schema.columns[pos].clone();
                column.column_id = previous.column_id;
                column.primary_key = previous.primary_key || column.primary_key;
                if schema.primary_key == column_name {
                    schema.primary_key = column.name.clone();
                    column.primary_key = true;
                    column.nullable = false;
                }
                for constraint in &mut schema.unique_constraints {
                    for constraint_column in &mut constraint.columns {
                        if *constraint_column == column_name {
                            *constraint_column = column.name.clone();
                        }
                    }
                }
                for fk in &mut schema.foreign_keys {
                    for fk_column in &mut fk.columns {
                        if *fk_column == column_name {
                            *fk_column = column.name.clone();
                        }
                    }
                }
                schema.columns[pos] = column;
                schema.schema_version = schema.schema_version.saturating_add(1).max(1);
                self.catalog
                    .store_table(database_name, schema_name, &schema)
                    .await?;
                self.refresh_olap_storage_at(session_id, database_name, schema_name, &schema)
                    .await?;
            }
            TableAlterOperation::DropColumn {
                column_name,
                if_exists,
            } => {
                if schema.primary_key == column_name {
                    return Err(unsupported(
                        "dropping a primary key column is not supported yet",
                    ));
                }
                let rows = self
                    .scan_visible_row_entries_at(
                        session_id,
                        database_name,
                        schema_name,
                        &schema,
                        None,
                    )
                    .await?;
                let Some(dropped_column) = schema
                    .columns
                    .iter()
                    .find(|column| column.name == column_name)
                    .cloned()
                else {
                    if if_exists {
                        return Ok(());
                    }
                    return Err(user_error(
                        "42703",
                        format!("column '{column_name}' not found"),
                    ));
                };
                schema.columns.retain(|column| column.name != column_name);
                schema
                    .unique_constraints
                    .retain(|constraint| !constraint.columns.iter().any(|c| c == &column_name));
                schema
                    .check_constraints
                    .retain(|constraint| !constraint.expr.contains(&column_name));
                schema.schema_version = schema.schema_version.saturating_add(1).max(1);
                for (pk, mut row) in rows {
                    row.remove(&column_name);
                    self.write_row_at_with_olap_refresh(
                        session_id,
                        database_name,
                        schema_name,
                        &schema,
                        &pk,
                        &row,
                        false,
                    )
                    .await?;
                    let dropped_key = cell_key(
                        database_name,
                        schema_name,
                        &schema.table_name,
                        &dropped_column.name,
                        &pk,
                    );
                    let _ = self.txn_delete(session_id, &dropped_key).await;
                }
                self.catalog
                    .store_table(database_name, schema_name, &schema)
                    .await?;
                self.refresh_olap_storage_at(session_id, database_name, schema_name, &schema)
                    .await?;
            }
            TableAlterOperation::RenameColumn { old_name, new_name } => {
                if schema.find_column(&new_name).is_some() {
                    return Err(user_error(
                        "42701",
                        format!("column '{new_name}' already exists"),
                    ));
                }
                let Some(column) = schema.columns.iter_mut().find(|c| c.name == old_name) else {
                    return Err(user_error(
                        "42703",
                        format!("column '{old_name}' not found"),
                    ));
                };
                column.name = new_name.clone();
                if schema.primary_key == old_name {
                    schema.primary_key = new_name.clone();
                }
                for constraint in &mut schema.unique_constraints {
                    for column_name in &mut constraint.columns {
                        if *column_name == old_name {
                            *column_name = new_name.clone();
                        }
                    }
                }
                for fk in &mut schema.foreign_keys {
                    for column_name in &mut fk.columns {
                        if *column_name == old_name {
                            *column_name = new_name.clone();
                        }
                    }
                }
                schema.schema_version = schema.schema_version.saturating_add(1).max(1);
                self.catalog
                    .store_table(database_name, schema_name, &schema)
                    .await?;
                self.refresh_olap_storage_at(session_id, database_name, schema_name, &schema)
                    .await?;
            }
            TableAlterOperation::RenameTable { new_name } => {
                self.catalog
                    .rename_table(database_name, schema_name, table_name, &new_name)
                    .await?;
            }
            TableAlterOperation::SetDefault {
                column_name,
                default,
            } => {
                let Some(column) = schema.columns.iter_mut().find(|c| c.name == column_name) else {
                    return Err(user_error(
                        "42703",
                        format!("column '{column_name}' not found"),
                    ));
                };
                column.default = default;
                schema.schema_version = schema.schema_version.saturating_add(1).max(1);
                self.catalog
                    .store_table(database_name, schema_name, &schema)
                    .await?;
            }
            TableAlterOperation::SetNotNull {
                column_name,
                nullable,
            } => {
                let Some(pos) = schema.columns.iter().position(|c| c.name == column_name) else {
                    return Err(user_error(
                        "42703",
                        format!("column '{column_name}' not found"),
                    ));
                };
                if !nullable {
                    let rows = self
                        .scan_visible_row_entries_at(
                            session_id,
                            database_name,
                            schema_name,
                            &schema,
                            None,
                        )
                        .await?;
                    if rows.iter().any(|(_, row)| {
                        row.get(&column_name)
                            .unwrap_or(&ColumnValue::Null)
                            .is_null()
                    }) {
                        return Err(user_error(
                            "23502",
                            format!("column '{column_name}' contains null values"),
                        ));
                    }
                }
                schema.columns[pos].nullable = nullable;
                schema.schema_version = schema.schema_version.saturating_add(1).max(1);
                self.catalog
                    .store_table(database_name, schema_name, &schema)
                    .await?;
            }
            TableAlterOperation::AddCheck { constraint } => {
                let rows = self
                    .scan_visible_row_entries_at(
                        session_id,
                        database_name,
                        schema_name,
                        &schema,
                        None,
                    )
                    .await?;
                let expr = parse_check_expr(&constraint.expr)?;
                if rows
                    .iter()
                    .any(|(_, row)| !row_matches_filter(row, &schema, &expr))
                {
                    return Err(user_error(
                        "23514",
                        format!("check constraint '{}' is violated", constraint.name),
                    ));
                }
                schema.check_constraints.push(constraint);
                schema.schema_version = schema.schema_version.saturating_add(1).max(1);
                self.catalog
                    .store_table(database_name, schema_name, &schema)
                    .await?;
            }
            TableAlterOperation::AddUnique { constraint } => {
                self.validate_unique_constraint_at(
                    session_id,
                    database_name,
                    schema_name,
                    &schema,
                    &constraint,
                    None,
                    None,
                )
                .await?;
                schema.unique_constraints.push(constraint);
                schema.schema_version = schema.schema_version.saturating_add(1).max(1);
                self.catalog
                    .store_table(database_name, schema_name, &schema)
                    .await?;
            }
            TableAlterOperation::AddForeignKey { constraint } => {
                schema.foreign_keys.push(constraint);
                schema.schema_version = schema.schema_version.saturating_add(1).max(1);
                self.catalog
                    .store_table(database_name, schema_name, &schema)
                    .await?;
            }
            TableAlterOperation::DropForeignKey { name, if_exists } => {
                let before = schema.foreign_keys.len();
                schema
                    .foreign_keys
                    .retain(|constraint| !constraint.name.eq_ignore_ascii_case(&name));
                if schema.foreign_keys.len() == before {
                    if if_exists {
                        return Ok(());
                    }
                    return Err(user_error(
                        "42704",
                        format!("foreign key '{name}' does not exist"),
                    ));
                }
                schema.schema_version = schema.schema_version.saturating_add(1).max(1);
                self.catalog
                    .store_table(database_name, schema_name, &schema)
                    .await?;
            }
            TableAlterOperation::AddIndex {
                index_name,
                column_names,
                unique,
                if_not_exists,
            } => {
                self.create_index_at(
                    session_id,
                    database_name,
                    schema_name,
                    table_name,
                    &index_name,
                    &column_names,
                    unique,
                    if_not_exists,
                )
                .await?;
            }
            TableAlterOperation::DropIndex {
                index_name,
                if_exists,
            } => {
                let database =
                    self.catalog
                        .get_database(database_name)
                        .await?
                        .ok_or_else(|| {
                            user_error(
                                "3D000",
                                format!("database '{database_name}' does not exist"),
                            )
                        })?;
                let schema_meta = self
                    .catalog
                    .get_schema(database.database_id, schema_name)
                    .await?
                    .ok_or_else(|| {
                        user_error("3F000", format!("schema '{schema_name}' does not exist"))
                    })?;
                let Some(index) = self
                    .catalog
                    .drop_index(database.database_id, schema_meta.schema_id, &index_name)
                    .await?
                else {
                    if if_exists {
                        return Ok(());
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
                let legacy_prefix = index_table_prefix(database_name, schema_name, table_name);
                for (key, _) in self
                    .txn_scan_prefix(session_id, &legacy_prefix)
                    .await
                    .map_err(|e| user_error("XX000", e))?
                {
                    if String::from_utf8_lossy(&key).contains(&format!(":{index_name}:")) {
                        self.txn_delete(session_id, &key)
                            .await
                            .map_err(|e| user_error("XX000", e))?;
                    }
                }
            }
            TableAlterOperation::RenameIndex { old_name, new_name } => {
                let indexes = self.catalog.list_indexes_for_table(schema.table_id).await?;
                let index = indexes
                    .iter()
                    .find(|index| index.index_name.eq_ignore_ascii_case(&old_name))
                    .ok_or_else(|| {
                        user_error("42P01", format!("index '{old_name}' does not exist"))
                    })?
                    .clone();
                self.create_index_at(
                    session_id,
                    database_name,
                    schema_name,
                    table_name,
                    &new_name,
                    &index.column_names,
                    index.unique,
                    false,
                )
                .await?;
                let database =
                    self.catalog
                        .get_database(database_name)
                        .await?
                        .ok_or_else(|| {
                            user_error(
                                "3D000",
                                format!("database '{database_name}' does not exist"),
                            )
                        })?;
                let schema_meta = self
                    .catalog
                    .get_schema(database.database_id, schema_name)
                    .await?
                    .ok_or_else(|| {
                        user_error("3F000", format!("schema '{schema_name}' does not exist"))
                    })?;
                if let Some(index) = self
                    .catalog
                    .drop_index(database.database_id, schema_meta.schema_id, &old_name)
                    .await?
                {
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
                }
            }
            TableAlterOperation::SetAutoIncrement { value } => {
                let Some(column) = mysql_auto_increment_column(&schema) else {
                    return Err(user_error(
                        "42809",
                        "AUTO_INCREMENT requires an auto_increment column",
                    ));
                };
                if let Some(sequence) = column.default.as_deref().and_then(nextval_sequence_name) {
                    self.advance_sequence_to_at_least(database_name, schema_name, &sequence, value)
                        .await?;
                }
            }
        }
        Ok(())
    }

    pub(super) fn update_zone(zone: &mut storage_layout::ColumnZoneMap, value: &ColumnValue) {
        if value.is_null() {
            zone.null_count = zone.null_count.saturating_add(1);
            return;
        }
        if zone
            .min
            .as_ref()
            .is_none_or(|current| value.partial_cmp(current).is_some_and(|ord| ord.is_lt()))
        {
            zone.min = Some(value.clone());
        }
        if zone
            .max
            .as_ref()
            .is_none_or(|current| value.partial_cmp(current).is_some_and(|ord| ord.is_gt()))
        {
            zone.max = Some(value.clone());
        }
    }
}
