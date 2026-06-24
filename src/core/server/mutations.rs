use pgwire::error::PgWireResult;
use sqlparser::ast::Expr;

use super::GatewayServer;
use crate::error::{unsupported, user_error};
use crate::sql::{
    EvalContext, coerce_column_value, column_default_value, evaluate_row_bool,
    evaluate_row_expression,
};
use crate::types::{
    ColumnValue, InsertConflictAction, InsertConflictAssignment, RowMap, TableSchema,
    UpdateAssignment, WriteAccess,
};

impl GatewayServer {
    pub(super) async fn find_existing_row_for_conflict(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        on_conflict: &InsertConflictAction,
        candidate_row: &RowMap,
    ) -> PgWireResult<Option<(ColumnValue, RowMap)>> {
        let target_columns = match on_conflict {
            InsertConflictAction::DoNothing => vec![schema.primary_key.clone()],
            InsertConflictAction::DoNothingAnyUnique { target_column_sets }
            | InsertConflictAction::ReplaceAnyUnique { target_column_sets } => {
                for target_columns in target_column_sets {
                    if let Some(existing) = self
                        .find_existing_row_for_conflict_columns(
                            session_id,
                            database_name,
                            schema_name,
                            schema,
                            target_columns,
                            candidate_row,
                        )
                        .await?
                    {
                        return Ok(Some(existing));
                    }
                }
                return Ok(None);
            }
            InsertConflictAction::DoUpdate { target_columns, .. } => target_columns.clone(),
            InsertConflictAction::DoUpdateAnyUnique {
                target_column_sets, ..
            } => {
                for target_columns in target_column_sets {
                    if let Some(existing) = self
                        .find_existing_row_for_conflict_columns(
                            session_id,
                            database_name,
                            schema_name,
                            schema,
                            target_columns,
                            candidate_row,
                        )
                        .await?
                    {
                        return Ok(Some(existing));
                    }
                }
                return Ok(None);
            }
        };

        self.find_existing_row_for_conflict_columns(
            session_id,
            database_name,
            schema_name,
            schema,
            &target_columns,
            candidate_row,
        )
        .await
    }

    pub(super) async fn find_existing_row_for_conflict_columns(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        target_columns: &[String],
        candidate_row: &RowMap,
    ) -> PgWireResult<Option<(ColumnValue, RowMap)>> {
        let target_values: Vec<ColumnValue> = target_columns
            .iter()
            .map(|column| {
                candidate_row
                    .get(column)
                    .cloned()
                    .ok_or_else(|| user_error("23502", format!("null value for {column}")))
            })
            .collect::<PgWireResult<Vec<_>>>()?;
        if target_values.iter().any(ColumnValue::is_null) {
            return Ok(None);
        }

        if target_columns.len() == 1 && target_columns[0] == schema.primary_key {
            let pk_value = target_values[0].clone();
            return match self
                .read_visible_row_at(session_id, database_name, schema_name, schema, &pk_value)
                .await?
            {
                Some(row) => Ok(Some((pk_value, row))),
                None => Ok(None),
            };
        }

        let mut candidates = Vec::new();
        let indexes = self.catalog.list_indexes_for_table(schema.table_id).await?;
        if let Some(index_meta) = indexes
            .iter()
            .find(|index| index.column_names == target_columns && index.table_id == schema.table_id)
        {
            let target_value = if target_values.len() == 1 {
                target_values[0].clone()
            } else {
                ColumnValue::Array(target_values.to_vec())
            };
            let rows = self
                .scan_index_row_entries_at(
                    session_id,
                    database_name,
                    schema_name,
                    schema,
                    &index_meta.index_name,
                    &target_value,
                    None,
                )
                .await?;
            for (pk_value, row) in rows {
                if Self::row_matches_columns(&row, target_columns, &target_values) {
                    candidates.push((pk_value, row));
                }
            }
        }

        if candidates.is_empty() {
            let all_rows = self
                .scan_visible_row_entries_at(session_id, database_name, schema_name, schema, None)
                .await?;
            for (pk_value, row) in all_rows {
                if Self::row_matches_columns(&row, target_columns, &target_values) {
                    candidates.push((pk_value, row));
                }
            }
        }

        match candidates.len() {
            0 => Ok(None),
            1 => Ok(candidates.into_iter().next()),
            _ => Err(user_error(
                "23505",
                "duplicate key value violates unique constraint".to_string(),
            )),
        }
    }

    pub(super) fn apply_insert_conflict_update(
        &self,
        schema: &TableSchema,
        existing_row: RowMap,
        excluded_row: &RowMap,
        assignments: &[InsertConflictAssignment],
        selection: Option<&Expr>,
    ) -> PgWireResult<Option<RowMap>> {
        if let Some(selection) = selection {
            let ctx = EvalContext {
                row: &existing_row,
                excluded_row: Some(excluded_row),
            };
            if !evaluate_row_bool(selection, schema, &ctx)? {
                return Ok(None);
            }
        }

        let source_row = existing_row.clone();
        let mut row = existing_row;
        for assignment in assignments {
            let target = schema
                .find_column(&assignment.column)
                .ok_or_else(|| user_error("42703", "column does not exist"))?;
            let ctx = EvalContext {
                row: &source_row,
                excluded_row: Some(excluded_row),
            };
            let value = coerce_column_value(
                evaluate_row_expression(&assignment.value, schema, &ctx)?,
                &target.data_type,
            )?;
            if assignment.column == schema.primary_key
                && row
                    .get(&schema.primary_key)
                    .is_some_and(|current| *current != value)
            {
                return Err(unsupported(
                    "ON CONFLICT DO UPDATE cannot modify the primary key in fast path",
                ));
            }
            row.insert(assignment.column.clone(), value);
        }
        let assigned_columns = assignments
            .iter()
            .map(|assignment| assignment.column.as_str())
            .collect::<Vec<_>>();
        self.apply_on_update_columns(&mut row, schema, &assigned_columns)?;
        Ok(Some(row))
    }

    pub(super) fn apply_on_update_columns(
        &self,
        row: &mut RowMap,
        schema: &TableSchema,
        assigned_columns: &[&str],
    ) -> PgWireResult<()> {
        for column in &schema.columns {
            let Some(on_update) = column.on_update.as_deref() else {
                continue;
            };
            if assigned_columns
                .iter()
                .any(|assigned| assigned.eq_ignore_ascii_case(&column.name))
            {
                continue;
            }
            let value = column_default_value(on_update, &column.data_type)?;
            row.insert(column.name.clone(), value);
        }
        Ok(())
    }

    pub(super) fn apply_update_assignments(
        &self,
        row: &mut RowMap,
        schema: &TableSchema,
        assignments: &[UpdateAssignment],
    ) -> PgWireResult<()> {
        let base_row = row.clone();
        for assignment in assignments {
            let UpdateAssignment::Expr { column, expr } = assignment;
            let target = schema
                .find_column(column)
                .ok_or_else(|| user_error("42703", format!("column '{}' not found", column)))?;
            let ctx = EvalContext {
                row: &base_row,
                excluded_row: None,
            };
            let value = coerce_column_value(
                evaluate_row_expression(expr, schema, &ctx)?,
                &target.data_type,
            )?;
            row.insert(column.clone(), value);
        }
        let assigned_columns = assignments
            .iter()
            .map(|assignment| match assignment {
                UpdateAssignment::Expr { column, .. } => column.as_str(),
            })
            .collect::<Vec<_>>();
        self.apply_on_update_columns(row, schema, &assigned_columns)?;
        if row.len() != base_row.len() {
            for (column, value) in base_row {
                row.entry(column).or_insert(value);
            }
        }
        Ok(())
    }

    pub(super) fn apply_write_limit(
        rows: &mut Vec<(ColumnValue, RowMap)>,
        limit: Option<usize>,
        order_by_primary_key: bool,
    ) {
        if order_by_primary_key {
            rows.sort_by(|(left, _), (right, _)| {
                left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal)
            });
        }
        if let Some(limit) = limit {
            rows.truncate(limit);
        }
    }

    // ── mutations ────────────────────────────────────────────────────

    pub(super) async fn apply_update_rows(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        assignments: &[UpdateAssignment],
        access: WriteAccess,
        limit: Option<usize>,
        order_by_primary_key: bool,
    ) -> PgWireResult<usize> {
        if limit == Some(0) {
            return Ok(0);
        }
        match access {
            WriteAccess::PointLookup { key } => {
                let mut row = match self
                    .read_visible_row_at(session_id, database_name, schema_name, schema, &key)
                    .await?
                {
                    Some(row) => row,
                    None => return Err(user_error("02000", "no row found for UPDATE")),
                };
                self.apply_update_assignments(&mut row, schema, assignments)?;
                self.write_row_at_with_olap_refresh(
                    session_id,
                    database_name,
                    schema_name,
                    schema,
                    &key,
                    &row,
                    true,
                )
                .await?;
                Ok(1)
            }
            WriteAccess::PrimaryKeyRangeScan {
                lower,
                upper,
                filter,
            } => {
                let mut rows = self
                    .scan_visible_row_entries_by_pk_range_at(
                        session_id,
                        database_name,
                        schema_name,
                        schema,
                        lower.as_ref().map(|(value, inclusive)| (value, *inclusive)),
                        upper.as_ref().map(|(value, inclusive)| (value, *inclusive)),
                        filter.as_ref(),
                        None,
                        0,
                    )
                    .await?;
                Self::apply_write_limit(&mut rows, limit, order_by_primary_key);
                let mut updated = 0usize;
                for (key, mut row) in rows {
                    self.apply_update_assignments(&mut row, schema, assignments)?;
                    self.write_row_at_with_olap_refresh(
                        session_id,
                        database_name,
                        schema_name,
                        schema,
                        &key,
                        &row,
                        true,
                    )
                    .await?;
                    updated += 1;
                }
                Ok(updated)
            }
            WriteAccess::SecondaryIndexLookup {
                index_name,
                key,
                filter,
            } => {
                let mut rows = self
                    .scan_index_row_entries_at(
                        session_id,
                        database_name,
                        schema_name,
                        schema,
                        &index_name,
                        &key,
                        filter.as_ref(),
                    )
                    .await?;
                Self::apply_write_limit(&mut rows, limit, order_by_primary_key);
                let mut updated = 0usize;
                for (pk, mut row) in rows {
                    self.apply_update_assignments(&mut row, schema, assignments)?;
                    self.write_row_at_with_olap_refresh(
                        session_id,
                        database_name,
                        schema_name,
                        schema,
                        &pk,
                        &row,
                        true,
                    )
                    .await?;
                    updated += 1;
                }
                Ok(updated)
            }
            WriteAccess::SecondaryIndexRangeScan {
                index_name,
                lower,
                upper,
                filter,
            } => {
                let mut rows = self
                    .scan_index_row_entries_by_range_at(
                        session_id,
                        database_name,
                        schema_name,
                        schema,
                        &index_name,
                        lower.as_ref().map(|(value, inclusive)| (value, *inclusive)),
                        upper.as_ref().map(|(value, inclusive)| (value, *inclusive)),
                        filter.as_ref(),
                    )
                    .await?;
                Self::apply_write_limit(&mut rows, limit, order_by_primary_key);
                let mut updated = 0usize;
                for (pk, mut row) in rows {
                    self.apply_update_assignments(&mut row, schema, assignments)?;
                    self.write_row_at_with_olap_refresh(
                        session_id,
                        database_name,
                        schema_name,
                        schema,
                        &pk,
                        &row,
                        true,
                    )
                    .await?;
                    updated += 1;
                }
                Ok(updated)
            }
            WriteAccess::PrefixScan { filter } => {
                let mut rows = self
                    .scan_visible_row_entries_at(
                        session_id,
                        database_name,
                        schema_name,
                        schema,
                        filter.as_ref(),
                    )
                    .await?;
                Self::apply_write_limit(&mut rows, limit, order_by_primary_key);
                let mut updated = 0usize;
                for (key, mut row) in rows {
                    self.apply_update_assignments(&mut row, schema, assignments)?;
                    self.write_row_at_with_olap_refresh(
                        session_id,
                        database_name,
                        schema_name,
                        schema,
                        &key,
                        &row,
                        true,
                    )
                    .await?;
                    updated += 1;
                }
                Ok(updated)
            }
        }
    }

    pub(super) async fn collect_updated_rows(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        assignments: &[UpdateAssignment],
        access: WriteAccess,
        limit: Option<usize>,
        order_by_primary_key: bool,
    ) -> PgWireResult<Vec<RowMap>> {
        if limit == Some(0) {
            return Ok(Vec::new());
        }
        match access {
            WriteAccess::PointLookup { key } => {
                let mut row = match self
                    .read_visible_row_at(session_id, database_name, schema_name, schema, &key)
                    .await?
                {
                    Some(row) => row,
                    None => return Err(user_error("02000", "no row found for UPDATE")),
                };
                self.apply_update_assignments(&mut row, schema, assignments)?;
                self.write_row_at_with_olap_refresh(
                    session_id,
                    database_name,
                    schema_name,
                    schema,
                    &key,
                    &row,
                    true,
                )
                .await?;
                Ok(vec![row])
            }
            WriteAccess::PrimaryKeyRangeScan {
                lower,
                upper,
                filter,
            } => {
                let mut rows = self
                    .scan_visible_row_entries_by_pk_range_at(
                        session_id,
                        database_name,
                        schema_name,
                        schema,
                        lower.as_ref().map(|(value, inclusive)| (value, *inclusive)),
                        upper.as_ref().map(|(value, inclusive)| (value, *inclusive)),
                        filter.as_ref(),
                        None,
                        0,
                    )
                    .await?;
                Self::apply_write_limit(&mut rows, limit, order_by_primary_key);
                let mut updated_rows = Vec::with_capacity(rows.len());
                for (key, mut row) in rows {
                    self.apply_update_assignments(&mut row, schema, assignments)?;
                    self.write_row_at_with_olap_refresh(
                        session_id,
                        database_name,
                        schema_name,
                        schema,
                        &key,
                        &row,
                        true,
                    )
                    .await?;
                    updated_rows.push(row);
                }
                Ok(updated_rows)
            }
            WriteAccess::SecondaryIndexLookup {
                index_name,
                key,
                filter,
            } => {
                let mut rows = self
                    .scan_index_row_entries_at(
                        session_id,
                        database_name,
                        schema_name,
                        schema,
                        &index_name,
                        &key,
                        filter.as_ref(),
                    )
                    .await?;
                Self::apply_write_limit(&mut rows, limit, order_by_primary_key);
                let mut updated_rows = Vec::with_capacity(rows.len());
                for (pk, mut row) in rows {
                    self.apply_update_assignments(&mut row, schema, assignments)?;
                    self.write_row_at_with_olap_refresh(
                        session_id,
                        database_name,
                        schema_name,
                        schema,
                        &pk,
                        &row,
                        true,
                    )
                    .await?;
                    updated_rows.push(row);
                }
                Ok(updated_rows)
            }
            WriteAccess::SecondaryIndexRangeScan {
                index_name,
                lower,
                upper,
                filter,
            } => {
                let mut rows = self
                    .scan_index_row_entries_by_range_at(
                        session_id,
                        database_name,
                        schema_name,
                        schema,
                        &index_name,
                        lower.as_ref().map(|(value, inclusive)| (value, *inclusive)),
                        upper.as_ref().map(|(value, inclusive)| (value, *inclusive)),
                        filter.as_ref(),
                    )
                    .await?;
                Self::apply_write_limit(&mut rows, limit, order_by_primary_key);
                let mut updated_rows = Vec::with_capacity(rows.len());
                for (pk, mut row) in rows {
                    self.apply_update_assignments(&mut row, schema, assignments)?;
                    self.write_row_at_with_olap_refresh(
                        session_id,
                        database_name,
                        schema_name,
                        schema,
                        &pk,
                        &row,
                        true,
                    )
                    .await?;
                    updated_rows.push(row);
                }
                Ok(updated_rows)
            }
            WriteAccess::PrefixScan { filter } => {
                let mut rows = self
                    .scan_visible_row_entries_at(
                        session_id,
                        database_name,
                        schema_name,
                        schema,
                        filter.as_ref(),
                    )
                    .await?;
                Self::apply_write_limit(&mut rows, limit, order_by_primary_key);
                let mut updated_rows = Vec::with_capacity(rows.len());
                for (key, mut row) in rows {
                    self.apply_update_assignments(&mut row, schema, assignments)?;
                    self.write_row_at_with_olap_refresh(
                        session_id,
                        database_name,
                        schema_name,
                        schema,
                        &key,
                        &row,
                        true,
                    )
                    .await?;
                    updated_rows.push(row);
                }
                Ok(updated_rows)
            }
        }
    }

    pub(super) async fn apply_delete_rows(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        access: WriteAccess,
        limit: Option<usize>,
        order_by_primary_key: bool,
    ) -> PgWireResult<usize> {
        if limit == Some(0) {
            return Ok(0);
        }
        match access {
            WriteAccess::PointLookup { key } => {
                if self
                    .read_visible_row_at(session_id, database_name, schema_name, schema, &key)
                    .await?
                    .is_none()
                {
                    return Ok(0);
                }
                self.delete_row_at(session_id, database_name, schema_name, schema, &key)
                    .await?;
                Ok(1)
            }
            WriteAccess::PrimaryKeyRangeScan {
                lower,
                upper,
                filter,
            } => {
                let mut rows = self
                    .scan_visible_row_entries_by_pk_range_at(
                        session_id,
                        database_name,
                        schema_name,
                        schema,
                        lower.as_ref().map(|(value, inclusive)| (value, *inclusive)),
                        upper.as_ref().map(|(value, inclusive)| (value, *inclusive)),
                        filter.as_ref(),
                        None,
                        0,
                    )
                    .await?;
                Self::apply_write_limit(&mut rows, limit, order_by_primary_key);
                let mut deleted = 0usize;
                for (key, _) in rows {
                    self.delete_row_at(session_id, database_name, schema_name, schema, &key)
                        .await?;
                    deleted += 1;
                }
                Ok(deleted)
            }
            WriteAccess::SecondaryIndexLookup {
                index_name,
                key,
                filter,
            } => {
                let mut rows = self
                    .scan_index_row_entries_at(
                        session_id,
                        database_name,
                        schema_name,
                        schema,
                        &index_name,
                        &key,
                        filter.as_ref(),
                    )
                    .await?;
                Self::apply_write_limit(&mut rows, limit, order_by_primary_key);
                let mut deleted = 0usize;
                for (pk, _) in rows {
                    self.delete_row_at(session_id, database_name, schema_name, schema, &pk)
                        .await?;
                    deleted += 1;
                }
                Ok(deleted)
            }
            WriteAccess::SecondaryIndexRangeScan {
                index_name,
                lower,
                upper,
                filter,
            } => {
                let mut rows = self
                    .scan_index_row_entries_by_range_at(
                        session_id,
                        database_name,
                        schema_name,
                        schema,
                        &index_name,
                        lower.as_ref().map(|(value, inclusive)| (value, *inclusive)),
                        upper.as_ref().map(|(value, inclusive)| (value, *inclusive)),
                        filter.as_ref(),
                    )
                    .await?;
                Self::apply_write_limit(&mut rows, limit, order_by_primary_key);
                let mut deleted = 0usize;
                for (pk, _) in rows {
                    self.delete_row_at(session_id, database_name, schema_name, schema, &pk)
                        .await?;
                    deleted += 1;
                }
                Ok(deleted)
            }
            WriteAccess::PrefixScan { filter } => {
                let mut rows = self
                    .scan_visible_row_entries_at(
                        session_id,
                        database_name,
                        schema_name,
                        schema,
                        filter.as_ref(),
                    )
                    .await?;
                Self::apply_write_limit(&mut rows, limit, order_by_primary_key);
                let mut deleted = 0usize;
                for (key, _) in rows {
                    self.delete_row_at(session_id, database_name, schema_name, schema, &key)
                        .await?;
                    deleted += 1;
                }
                Ok(deleted)
            }
        }
    }

    pub(super) async fn collect_deleted_rows(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        access: WriteAccess,
        limit: Option<usize>,
        order_by_primary_key: bool,
    ) -> PgWireResult<Vec<RowMap>> {
        if limit == Some(0) {
            return Ok(Vec::new());
        }
        match access {
            WriteAccess::PointLookup { key } => {
                let row = match self
                    .read_visible_row_at(session_id, database_name, schema_name, schema, &key)
                    .await?
                {
                    Some(row) => row,
                    None => return Ok(Vec::new()),
                };
                self.delete_row_at(session_id, database_name, schema_name, schema, &key)
                    .await?;
                Ok(vec![row])
            }
            WriteAccess::PrimaryKeyRangeScan {
                lower,
                upper,
                filter,
            } => {
                let mut rows = self
                    .scan_visible_row_entries_by_pk_range_at(
                        session_id,
                        database_name,
                        schema_name,
                        schema,
                        lower.as_ref().map(|(value, inclusive)| (value, *inclusive)),
                        upper.as_ref().map(|(value, inclusive)| (value, *inclusive)),
                        filter.as_ref(),
                        None,
                        0,
                    )
                    .await?;
                Self::apply_write_limit(&mut rows, limit, order_by_primary_key);
                let mut deleted_rows = Vec::with_capacity(rows.len());
                for (key, row) in rows {
                    self.delete_row_at(session_id, database_name, schema_name, schema, &key)
                        .await?;
                    deleted_rows.push(row);
                }
                Ok(deleted_rows)
            }
            WriteAccess::SecondaryIndexLookup {
                index_name,
                key,
                filter,
            } => {
                let mut rows = self
                    .scan_index_row_entries_at(
                        session_id,
                        database_name,
                        schema_name,
                        schema,
                        &index_name,
                        &key,
                        filter.as_ref(),
                    )
                    .await?;
                Self::apply_write_limit(&mut rows, limit, order_by_primary_key);
                let mut deleted_rows = Vec::with_capacity(rows.len());
                for (pk, row) in rows {
                    self.delete_row_at(session_id, database_name, schema_name, schema, &pk)
                        .await?;
                    deleted_rows.push(row);
                }
                Ok(deleted_rows)
            }
            WriteAccess::SecondaryIndexRangeScan {
                index_name,
                lower,
                upper,
                filter,
            } => {
                let mut rows = self
                    .scan_index_row_entries_by_range_at(
                        session_id,
                        database_name,
                        schema_name,
                        schema,
                        &index_name,
                        lower.as_ref().map(|(value, inclusive)| (value, *inclusive)),
                        upper.as_ref().map(|(value, inclusive)| (value, *inclusive)),
                        filter.as_ref(),
                    )
                    .await?;
                Self::apply_write_limit(&mut rows, limit, order_by_primary_key);
                let mut deleted_rows = Vec::with_capacity(rows.len());
                for (pk, row) in rows {
                    self.delete_row_at(session_id, database_name, schema_name, schema, &pk)
                        .await?;
                    deleted_rows.push(row);
                }
                Ok(deleted_rows)
            }
            WriteAccess::PrefixScan { filter } => {
                let mut rows = self
                    .scan_visible_row_entries_at(
                        session_id,
                        database_name,
                        schema_name,
                        schema,
                        filter.as_ref(),
                    )
                    .await?;
                Self::apply_write_limit(&mut rows, limit, order_by_primary_key);
                let mut deleted_rows = Vec::with_capacity(rows.len());
                for (key, row) in rows {
                    self.delete_row_at(session_id, database_name, schema_name, schema, &key)
                        .await?;
                    deleted_rows.push(row);
                }
                Ok(deleted_rows)
            }
        }
    }
}
