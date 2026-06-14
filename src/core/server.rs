use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use ::datafusion::catalog::CatalogProvider;
use ::datafusion::execution::SessionStateBuilder;
use ::datafusion::execution::context::SessionContext;
use ::datafusion::prelude::SessionConfig;
use async_trait::async_trait;
use futures::{Sink, SinkExt};
use pgwire::api::PgWireConnectionState;
use pgwire::api::auth::md5pass::hash_md5_password;
use pgwire::api::auth::sasl::{SASLState, scram};
use pgwire::api::auth::{
    AuthSource, DefaultServerParameterProvider, LoginInfo, Password, ServerParameterProvider,
    StartupHandler,
};
use pgwire::api::cancel::CancelHandler;
use pgwire::api::copy::CopyHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    CopyResponse, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo,
    Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, METADATA_DATABASE, METADATA_USER, PgWireServerHandlers, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::cancel::CancelRequest;
use pgwire::messages::copy::{CopyData, CopyDone, CopyFail};
use pgwire::messages::response::{ReadyForQuery, TransactionStatus};
use pgwire::messages::startup::{
    Authentication, BackendKeyData, ParameterStatus, PasswordMessageFamily,
};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use sqlparser::ast::{
    AlterTableOperation, Assignment, ColumnDef, ColumnOption, ColumnOptionDef, ConflictTarget,
    CopyLegacyCsvOption, CopyLegacyOption, CopyOption, CopySource, CopyTarget, CreateTableLikeKind,
    Expr, GroupByExpr, Ident, LimitClause, ObjectName, ObjectType,
    OnConflictAction as AstOnConflictAction, OnInsert, OrderBy, OrderByKind, Query, SchemaName,
    SequenceOptions, SetExpr, Statement, TableConstraint, TableObject, TableWithJoins,
};

use crate::catalog::{
    CatalogStore, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, IndexCatalog, object_name_to_string,
    resolve_table_reference, schema_name_to_string,
};
use crate::codec::{
    cell_key, decode_cell_value, decode_pk_from_index_entry_key, decode_pk_from_row_marker_key,
    encode_cell_value, index_entry_key, index_entry_prefix, index_table_prefix, row_marker_key,
    row_marker_prefix, table_data_prefix,
};
use crate::core::response::{
    build_pg_response_with_format, build_pg_returning_response, command_complete,
    command_complete_rows, empty_query_response, field_infos_for_projection,
    field_infos_for_returning_projection, multi_text_row_response, single_int4_row_response,
    single_int8_row_response, single_text_row_response,
};
use crate::datafusion_bridge::{
    KvAggregateOptimizerRule, KvQueryPlanner, KvTopNOptimizerRule, arrow_array_value_to_string,
    arrow_to_pgwire_response, build_user_catalog_provider,
    register_catalog_tables_with_options_for_mode, register_pg_catalog_functions,
};
use crate::dialect::{self, TransactionIsolationLevel, parser};
use crate::error::{unsupported, user_error};
use crate::filter::row_matches_filter;
use crate::mem_store::{KvRangeVisitor, KvScanProjection, KvStore, KvTransaction};
use crate::mode::GatewayMode;
use crate::sql::{
    EvalContext, coerce_column_value, column_default_value, evaluate_returning_row,
    evaluate_row_bool, evaluate_row_expression, expr_identifier_name, extract_assignment_column,
    extract_column_range_filter, extract_insert_values, extract_primary_key_filter,
    extract_primary_key_range_filter, extract_single_table_name,
    extract_table_name_from_table_with_joins, is_default_expr, nextval_sequence_name,
    resolve_projection, resolve_returning_projection, sql_expr_to_column_value,
    supports_fast_path_filter, supports_fast_path_projection,
};
use crate::storage_layout;
use crate::types::MySqlIntKind;
use crate::types::{
    CheckConstraintSchema, ColumnSchema, ColumnValue, DataType, ForeignKeyConstraintSchema,
    INTERNAL_ROWID_COLUMN, InsertConflictAction, InsertConflictAssignment, QueryPlan, ReadAccess,
    RowMap, TableAlterOperation, TableSchema, UniqueConstraintSchema, UpdateAssignment,
    WriteAccess,
};

mod shared;
pub use shared::GatewayServer;
pub(crate) use shared::MySqlColumnMetadata;
use shared::*;
mod context;
mod copy;
mod datafusion;
mod ddl_planner;
mod dml_planner;
mod executor;
mod mutations;
mod mysql_locks;
mod prepared;
mod protocol_traits;
pub use protocol_traits::GatewayFactory;
mod row_write;
mod scans;
mod search_path;
mod session_commands;
#[cfg(test)]
mod tests;
mod transactions;
