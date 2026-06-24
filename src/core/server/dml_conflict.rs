use pgwire::error::PgWireResult;
use sqlparser::ast::{
    Assignment, ConflictTarget, OnConflictAction as AstOnConflictAction, OnInsert,
};

use super::GatewayServer;
use crate::catalog::object_name_to_string;
use crate::error::{unsupported, user_error};
use crate::sql::extract_assignment_column;
use crate::types::{InsertConflictAction, InsertConflictAssignment, TableSchema, UpdateAssignment};

impl GatewayServer {
    pub(super) async fn resolve_insert_on_conflict(
        &self,
        schema: &TableSchema,
        on: Option<OnInsert>,
        ignore: bool,
        replace_into: bool,
    ) -> PgWireResult<Option<InsertConflictAction>> {
        if replace_into {
            let target_column_sets = self.mysql_duplicate_key_targets(schema).await?;
            if target_column_sets.is_empty() {
                return Ok(None);
            }
            return Ok(Some(InsertConflictAction::ReplaceAnyUnique {
                target_column_sets,
            }));
        }
        if ignore {
            let target_column_sets = self.mysql_duplicate_key_targets(schema).await?;
            if target_column_sets.is_empty() {
                return Ok(None);
            }
            return Ok(Some(InsertConflictAction::DoNothingAnyUnique {
                target_column_sets,
            }));
        }
        let Some(on) = on else {
            return Ok(None);
        };

        let (target_columns, action) = match on {
            OnInsert::OnConflict(on_conflict) => {
                let target_columns = match on_conflict.conflict_target {
                    None => {
                        if !schema.has_user_primary_key() {
                            return Err(unsupported(
                                "ON CONFLICT without target requires a user-defined primary key in fast path",
                            ));
                        }
                        vec![schema.primary_key.clone()]
                    }
                    Some(ConflictTarget::Columns(columns)) => {
                        if columns.is_empty() {
                            return Err(unsupported("ON CONFLICT target cannot be empty"));
                        }
                        columns
                            .into_iter()
                            .map(|identifier| identifier.value)
                            .collect::<Vec<_>>()
                    }
                    Some(ConflictTarget::OnConstraint(constraint_name)) => {
                        let constraint_name = object_name_to_string(&constraint_name)?;
                        let constraint = schema
                            .unique_constraints
                            .iter()
                            .find(|constraint| {
                                constraint.name.eq_ignore_ascii_case(&constraint_name)
                            })
                            .ok_or_else(|| {
                                user_error(
                                    "42704",
                                    format!("constraint '{constraint_name}' does not exist"),
                                )
                            })?;
                        if constraint.columns.is_empty() {
                            return Err(unsupported(
                                "ON CONFLICT ON CONSTRAINT requires constraint with at least one column",
                            ));
                        }
                        constraint.columns.clone()
                    }
                };
                let action = match on_conflict.action {
                    AstOnConflictAction::DoNothing => InsertConflictAction::DoNothing,
                    AstOnConflictAction::DoUpdate(do_update) => {
                        let assignments = do_update
                            .assignments
                            .into_iter()
                            .map(|assignment| {
                                self.resolve_insert_conflict_assignment(
                                    schema,
                                    assignment,
                                    &target_columns,
                                )
                            })
                            .collect::<PgWireResult<Vec<_>>>()?;
                        InsertConflictAction::DoUpdate {
                            target_columns: target_columns.clone(),
                            assignments,
                            selection: do_update.selection,
                        }
                    }
                };
                (target_columns, action)
            }
            OnInsert::DuplicateKeyUpdate(assignments) => {
                let assignments = assignments
                    .into_iter()
                    .map(|assignment| {
                        self.resolve_insert_conflict_assignment(schema, assignment, &[])
                    })
                    .collect::<PgWireResult<Vec<_>>>()?;
                let target_column_sets = self.mysql_duplicate_key_targets(schema).await?;
                if target_column_sets.is_empty() {
                    return Err(unsupported(
                        "ON DUPLICATE KEY UPDATE requires a primary key, unique constraint, or unique index",
                    ));
                }
                (
                    target_column_sets[0].clone(),
                    InsertConflictAction::DoUpdateAnyUnique {
                        target_column_sets,
                        assignments,
                    },
                )
            }
            _ => {
                return Err(unsupported(
                    "only ON CONFLICT and ON DUPLICATE KEY UPDATE are supported in fast path",
                ));
            }
        };

        if !target_columns
            .iter()
            .all(|column| schema.find_column(column).is_some())
        {
            return Err(unsupported("upsert target column not found"));
        }

        let target_is_keyed = if matches!(action, InsertConflictAction::DoUpdateAnyUnique { .. }) {
            true
        } else if matches!(
            action,
            InsertConflictAction::DoNothingAnyUnique { .. }
                | InsertConflictAction::ReplaceAnyUnique { .. }
        ) {
            true
        } else if target_columns == [schema.primary_key.clone()] {
            true
        } else {
            schema
                .unique_constraints
                .iter()
                .any(|constraint| constraint.primary_key || constraint.columns == target_columns)
        };

        if !target_is_keyed {
            return Err(unsupported(
                "upsert target must match the primary key or a unique constraint",
            ));
        }

        Ok(Some(action))
    }

    pub(super) async fn mysql_duplicate_key_targets(
        &self,
        schema: &TableSchema,
    ) -> PgWireResult<Vec<Vec<String>>> {
        let mut targets = Vec::new();
        if schema.has_user_primary_key() {
            targets.push(vec![schema.primary_key.clone()]);
        }
        for constraint in &schema.unique_constraints {
            if constraint.columns.is_empty() {
                continue;
            }
            if !targets.iter().any(|target| target == &constraint.columns) {
                targets.push(constraint.columns.clone());
            }
        }
        for index in self.catalog.list_indexes_for_table(schema.table_id).await? {
            if !index.unique || index.column_names.is_empty() {
                continue;
            }
            if !targets.iter().any(|target| target == &index.column_names) {
                targets.push(index.column_names);
            }
        }
        Ok(targets)
    }

    pub(super) fn resolve_insert_conflict_assignment(
        &self,
        schema: &TableSchema,
        assignment: Assignment,
        target_columns: &[String],
    ) -> PgWireResult<InsertConflictAssignment> {
        let column = extract_assignment_column(&assignment)?;
        let _target_column = schema
            .find_column(&column)
            .ok_or_else(|| user_error("42703", format!("column '{column}' not found")))?;
        if target_columns.iter().any(|name| name == &column) {
            return Err(unsupported(
                "ON CONFLICT DO UPDATE cannot update the conflict target columns in fast path",
            ));
        }

        Ok(InsertConflictAssignment {
            column,
            value: assignment.value,
        })
    }

    pub(super) fn resolve_update_assignment(
        &self,
        schema: &TableSchema,
        assignment: Assignment,
    ) -> PgWireResult<UpdateAssignment> {
        let column = extract_assignment_column(&assignment)?;
        schema
            .find_column(&column)
            .ok_or_else(|| user_error("42703", format!("column '{column}' not found")))?;

        Ok(UpdateAssignment::Expr {
            column,
            expr: assignment.value,
        })
    }
}
