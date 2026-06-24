use std::collections::HashMap;

use sqlparser::ast::Expr;

use super::ColumnValue;

pub type RowMap = HashMap<String, ColumnValue>;

pub fn apply_assignments(row: &mut RowMap, assignments: &[(String, ColumnValue)]) {
    for (column, value) in assignments {
        row.insert(column.clone(), value.clone());
    }
}

#[derive(Clone, Debug)]
pub enum UpdateAssignment {
    Expr { column: String, expr: Expr },
}

#[derive(Clone, Debug)]
pub struct InsertConflictAssignment {
    pub column: String,
    pub value: Expr,
}

#[derive(Clone, Debug)]
pub enum InsertConflictAction {
    DoNothing,
    DoNothingAnyUnique {
        target_column_sets: Vec<Vec<String>>,
    },
    ReplaceAnyUnique {
        target_column_sets: Vec<Vec<String>>,
    },
    DoUpdate {
        target_columns: Vec<String>,
        assignments: Vec<InsertConflictAssignment>,
        selection: Option<Expr>,
    },
    DoUpdateAnyUnique {
        target_column_sets: Vec<Vec<String>>,
        assignments: Vec<InsertConflictAssignment>,
    },
}

#[derive(Clone, Debug)]
pub enum ReturningProjection {
    Wildcard,
    Column(String),
    Expr { expr: Expr, output_name: String },
}
