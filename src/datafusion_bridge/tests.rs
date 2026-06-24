use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use pgwire::api::Type;
use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, decode_table_schema};
use crate::codec::{cell_key, encode_cell_value, row_marker_key, schema_key};
use crate::error::user_error;
use crate::mem_store::KvStore;
use crate::types::{ColumnSchema, ColumnValue, DataType, RowMap, TableSchema};

use super::{
    ColumnBuilder, KvTableProvider, arrow_array_value_to_string, arrow_to_pgwire_response,
    arrow_type_to_pg, register_all_tables, to_arrow_schema,
};

mod support;
use support::{event_schema, olap_schema, register_schema_and_rows, test_schema};
mod advanced;
mod aggregates;
mod predicates;
mod scans;
