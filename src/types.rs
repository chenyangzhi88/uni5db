pub const INTERNAL_ROWID_COLUMN: &str = "__pg_rowid";

mod column_value;
mod data_type;
mod query_plan;
mod row;
mod schema;

pub use column_value::ColumnValue;
pub use data_type::{DataType, MySqlIntKind};
pub use query_plan::{QueryPlan, ReadAccess, TableAlterOperation, WriteAccess};
pub use row::{
    InsertConflictAction, InsertConflictAssignment, ReturningProjection, RowMap, UpdateAssignment,
    apply_assignments,
};
pub use schema::{
    CheckConstraintSchema, ColumnSchema, ForeignKeyConstraintSchema, TableSchema,
    UniqueConstraintSchema, parse_column_schema,
};

#[cfg(test)]
mod tests;
