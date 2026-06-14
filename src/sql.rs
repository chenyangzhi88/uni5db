use std::cmp::Ordering;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use pgwire::error::PgWireResult;
use serde_json::Value as JsonValue;
use sqlparser::ast::{
    Assignment, AssignmentTarget, BinaryOperator, DateTimeField, Expr, FunctionArg,
    FunctionArgExpr, FunctionArguments, Ident, Query, Select, SelectItem, SetExpr, Statement,
    TableFactor, TableWithJoins, TrimWhereField, TypedString, UnaryOperator, Value as SqlValue,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::error::{unsupported, user_error};
use crate::types::{ColumnValue, DataType, MySqlIntKind, ReturningProjection, RowMap, TableSchema};

mod values;
pub use values::*;
mod projection;
pub use projection::*;
mod eval_core;
pub use eval_core::*;
mod functions;
use functions::*;
mod operators;
use operators::*;
mod json;
use json::*;
mod fast_path;
pub use fast_path::*;
#[cfg(test)]
mod tests;
