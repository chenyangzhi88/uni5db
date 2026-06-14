use std::sync::Arc;

use pgwire::error::PgWireResult;
use serde_json::{Value, json};
use sqlparser::ast::{ObjectName, SchemaName};

use crate::codec::{SCHEMA_PREFIX, schema_key};
use crate::error::{unsupported, user_error};
use crate::mem_store::KvStore;
use crate::mode::GatewayMode;
use crate::types::{
    CheckConstraintSchema, ColumnSchema, ForeignKeyConstraintSchema, TableSchema,
    UniqueConstraintSchema, parse_column_schema,
};

mod types;
pub use types::*;
mod codec;
mod store;
pub use codec::*;
mod legacy;
use legacy::*;
#[cfg(test)]
mod tests;
