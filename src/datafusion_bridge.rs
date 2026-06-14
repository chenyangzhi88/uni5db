use std::any::Any;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt;
use std::hash::Hasher;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering as AtomicOrdering},
};
use std::time::Instant;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    ListArray, ListBuilder, RecordBatch, StringArray, StringBuilder,
};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatchOptions;
use async_trait::async_trait;
use datafusion::catalog::{
    CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider, TableProvider,
};
use datafusion::common::{DFSchemaRef, Result as DfResult, ScalarValue, tree_node::Transformed};
use datafusion::datasource::memory::MemTable;
use datafusion::datasource::source_as_provider;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::{
    Expr as DfExpr, Extension, LogicalPlan, Operator, ScalarFunctionImplementation,
    TableProviderFilterPushDown, TableType, UserDefinedLogicalNode, Volatility, create_udf,
};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ColumnarValue, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::SessionContext;
use futures::stream;
use pgwire::api::Type;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::catalog::{
    CatalogStore, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, IndexCatalog, TableCatalog,
    ViewCatalog,
};
use crate::codec::{
    SCHEMA_PREFIX, cell_key, decode_cell_value, decode_pk_from_row_marker_key, row_marker_prefix,
};
use crate::error::user_error;
use crate::mem_store::{
    KvAggregateOp, KvAggregateScan, KvCompareOp, KvPredicate, KvRangeVisitor, KvScanProjection,
    KvStore,
};
use crate::mode::GatewayMode;
use crate::storage_layout;
use crate::types::{ColumnValue, DataType, RowMap, TableSchema};

mod pg_catalog_types;
use pg_catalog_types::*;
pub use pg_catalog_types::{
    register_pg_catalog_functions, register_pg_catalog_functions_with_view_defs,
};
mod table_provider_base;
use table_provider_base::*;
mod physical_planner;
mod table_provider_load;
mod table_provider_scans;
mod table_provider_topn;
pub use physical_planner::*;
mod table_provider_trait;
pub use table_provider_trait::*;
mod pg_type_rows;
use pg_type_rows::*;
mod pg_catalog_rows;
use pg_catalog_rows::*;
mod stats_and_catalogs;
use stats_and_catalogs::*;
mod mysql_information_schema;
use mysql_information_schema::*;
mod arrow_response;
pub use arrow_response::*;
#[cfg(test)]
mod tests;
