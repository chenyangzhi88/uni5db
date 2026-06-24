use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    ListBuilder, StringArray, StringBuilder,
};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::SchemaProvider;
use datafusion::common::{Result as DfResult, ScalarValue};
use datafusion::datasource::memory::MemTable;
use datafusion::logical_expr::{ScalarFunctionImplementation, Volatility, create_udf};
use datafusion::physical_plan::ColumnarValue;
use datafusion::prelude::SessionContext;
use pgwire::error::PgWireResult;

use crate::catalog::{DEFAULT_DATABASE_NAME, IndexCatalog, TableCatalog, ViewCatalog};
use crate::error::user_error;
use crate::mem_store::KvStore;
use crate::storage_layout;
use crate::types::{DataType, TableSchema};

pub(super) const PG_CATALOG_SCHEMA_NAME: &str = "pg_catalog";
pub(super) const INFORMATION_SCHEMA_NAME: &str = "information_schema";
pub(super) const PG_CATALOG_NAMESPACE_OID: i32 = 10_000;
pub(super) const INFORMATION_SCHEMA_NAMESPACE_OID: i32 = 10_001;
pub(super) const POSTGRES_ROLE_OID: i32 = 10;
pub(super) const HEAP_AM_OID: i32 = 2;
pub(super) const BTREE_AM_OID: i32 = 403;
pub(super) const DEFAULT_COLLATION_OID: i32 = 100;
pub(super) const BOOL_OPCLASS_OID: i32 = 1_001;
pub(super) const INT4_OPCLASS_OID: i32 = 1_002;
pub(super) const INT8_OPCLASS_OID: i32 = 1_003;
pub(super) const TEXT_OPCLASS_OID: i32 = 1_004;
pub(super) const INDEX_RELATION_OID_OFFSET: i32 = 100_000;
pub(super) const PRIMARY_KEY_CONSTRAINT_OID_OFFSET: i32 = 200_000;
pub(super) const UNIQUE_CONSTRAINT_OID_OFFSET: i32 = 300_000;
pub(super) const TABLE_ROW_TYPE_OID_OFFSET: i32 = 400_000;
pub(super) const VIEW_RELATION_OID_OFFSET: i32 = 500_000;
pub(super) const PG_PROC_OID_OFFSET: i32 = 600_000;
pub(super) const PG_CAST_OID_OFFSET: i32 = 700_000;

// ── Arrow schema conversion ──────────────────────────────────────────

pub(super) fn to_arrow_data_type(dt: &DataType) -> ArrowDataType {
    match dt {
        DataType::Int16 => ArrowDataType::Int16,
        DataType::Int32 => ArrowDataType::Int32,
        DataType::Int64 => ArrowDataType::Int64,
        DataType::MySqlInt { kind, unsigned } => match (kind, unsigned) {
            (crate::types::MySqlIntKind::Tiny, false)
            | (crate::types::MySqlIntKind::Small, false) => ArrowDataType::Int16,
            (crate::types::MySqlIntKind::Int, true) | (crate::types::MySqlIntKind::Big, false) => {
                ArrowDataType::Int64
            }
            (crate::types::MySqlIntKind::Big, true) => ArrowDataType::Utf8,
            _ => ArrowDataType::Int32,
        },
        DataType::Float32 => ArrowDataType::Float32,
        DataType::Float64 => ArrowDataType::Float64,
        DataType::MySqlFloat { .. } => ArrowDataType::Float32,
        DataType::MySqlDouble { .. } => ArrowDataType::Float64,
        DataType::Bytea | DataType::Binary(_) | DataType::VarBinary(_) | DataType::Blob { .. } => {
            ArrowDataType::Binary
        }
        DataType::Array(_) => {
            ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Utf8, true)))
        }
        DataType::Text
        | DataType::MySqlText { .. }
        | DataType::VarChar(_)
        | DataType::Char(_)
        | DataType::Time
        | DataType::MySqlTime { .. }
        | DataType::TimeTz
        | DataType::Interval => ArrowDataType::Utf8,
        DataType::Boolean => ArrowDataType::Boolean,
        DataType::Bit(_) | DataType::Year => ArrowDataType::Int32,
        DataType::Numeric { .. }
        | DataType::Date
        | DataType::Timestamp
        | DataType::MySqlDateTime { .. }
        | DataType::MySqlTimestamp { .. }
        | DataType::TimestampTz
        | DataType::Uuid
        | DataType::Json
        | DataType::Jsonb
        | DataType::Domain(_)
        | DataType::Enum(_)
        | DataType::Set(_)
        | DataType::Geometry(_) => ArrowDataType::Utf8,
    }
}

pub(super) fn to_arrow_schema(schema: &TableSchema) -> ArrowSchema {
    let fields: Vec<Field> = schema
        .columns
        .iter()
        .map(|c| Field::new(&c.name, to_arrow_data_type(&c.data_type), c.nullable))
        .collect();
    ArrowSchema::new(fields)
}

pub(super) fn pg_type_oid(dt: &DataType) -> i32 {
    match dt {
        DataType::Boolean => 16,
        DataType::Bytea => 17,
        DataType::Binary(_) | DataType::VarBinary(_) | DataType::Blob { .. } => 17,
        DataType::Int16 => 21,
        DataType::MySqlInt {
            kind: crate::types::MySqlIntKind::Tiny | crate::types::MySqlIntKind::Small,
            unsigned: false,
        } => 21,
        DataType::Int64 => 20,
        DataType::MySqlInt {
            kind: crate::types::MySqlIntKind::Int | crate::types::MySqlIntKind::Big,
            ..
        } => 20,
        DataType::Int32 => 23,
        DataType::MySqlInt { .. } | DataType::Bit(_) | DataType::Year => 23,
        DataType::Text | DataType::MySqlText { .. } => 25,
        DataType::VarChar(_) => 1043,
        DataType::Char(_) => 1042,
        DataType::Json => 114,
        DataType::Float32 => 700,
        DataType::MySqlFloat { .. } => 700,
        DataType::Float64 => 701,
        DataType::MySqlDouble { .. } => 701,
        DataType::Date => 1082,
        DataType::Time => 1083,
        DataType::MySqlTime { .. } => 1083,
        DataType::Timestamp | DataType::MySqlDateTime { .. } | DataType::MySqlTimestamp { .. } => {
            1114
        }
        DataType::TimestampTz => 1184,
        DataType::Interval => 1186,
        DataType::TimeTz => 1266,
        DataType::Numeric { .. } => 1700,
        DataType::Uuid => 2950,
        DataType::Jsonb => 3802,
        DataType::Array(inner) => pg_array_type_oid(inner),
        DataType::Domain(_) | DataType::Enum(_) | DataType::Set(_) | DataType::Geometry(_) => 25,
    }
}

pub(super) fn pg_array_type_oid(dt: &DataType) -> i32 {
    match dt {
        DataType::Boolean => 1000,
        DataType::Bytea => 1001,
        DataType::Binary(_) | DataType::VarBinary(_) | DataType::Blob { .. } => 1001,
        DataType::Int16 => 1005,
        DataType::MySqlInt {
            kind: crate::types::MySqlIntKind::Tiny | crate::types::MySqlIntKind::Small,
            unsigned: false,
        } => 1005,
        DataType::Int32 => 1007,
        DataType::MySqlInt {
            kind: crate::types::MySqlIntKind::Big,
            ..
        } => 1016,
        DataType::MySqlInt { .. } | DataType::Bit(_) | DataType::Year => 1007,
        DataType::Text
        | DataType::MySqlText { .. }
        | DataType::VarChar(_)
        | DataType::Char(_)
        | DataType::Time
        | DataType::MySqlTime { .. }
        | DataType::TimeTz
        | DataType::Interval
        | DataType::Domain(_)
        | DataType::Enum(_)
        | DataType::Set(_)
        | DataType::Geometry(_) => 1009,
        DataType::Int64 => 1016,
        DataType::Float32 => 1021,
        DataType::MySqlFloat { .. } => 1021,
        DataType::Float64 => 1022,
        DataType::MySqlDouble { .. } => 1022,
        DataType::Json => 199,
        DataType::Timestamp | DataType::MySqlDateTime { .. } | DataType::MySqlTimestamp { .. } => {
            1115
        }
        DataType::Date => 1182,
        DataType::TimestampTz => 1185,
        DataType::Numeric { .. } => 1231,
        DataType::Uuid => 2951,
        DataType::Jsonb => 3807,
        DataType::Array(_) => 1009,
    }
}

pub(super) fn pg_type_name(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "bool".to_string(),
        DataType::Int16 => "int2".to_string(),
        DataType::Int64 => "int8".to_string(),
        DataType::Int32 => "int4".to_string(),
        DataType::MySqlInt {
            kind: crate::types::MySqlIntKind::Tiny | crate::types::MySqlIntKind::Small,
            unsigned: false,
        } => "int2".to_string(),
        DataType::MySqlInt {
            kind: crate::types::MySqlIntKind::Int | crate::types::MySqlIntKind::Big,
            ..
        } => "int8".to_string(),
        DataType::MySqlInt { .. } | DataType::Bit(_) | DataType::Year => "int4".to_string(),
        DataType::Float32 => "float4".to_string(),
        DataType::Float64 => "float8".to_string(),
        DataType::MySqlFloat { .. } => "float4".to_string(),
        DataType::MySqlDouble { .. } => "float8".to_string(),
        DataType::Numeric { .. } => "numeric".to_string(),
        DataType::Text | DataType::MySqlText { .. } => "text".to_string(),
        DataType::VarChar(_) => "varchar".to_string(),
        DataType::Char(_) => "bpchar".to_string(),
        DataType::Date => "date".to_string(),
        DataType::Time => "time".to_string(),
        DataType::MySqlTime { .. } => "time".to_string(),
        DataType::TimeTz => "timetz".to_string(),
        DataType::Interval => "interval".to_string(),
        DataType::Timestamp => "timestamp".to_string(),
        DataType::MySqlDateTime { .. } | DataType::MySqlTimestamp { .. } => "timestamp".to_string(),
        DataType::TimestampTz => "timestamptz".to_string(),
        DataType::Uuid => "uuid".to_string(),
        DataType::Bytea => "bytea".to_string(),
        DataType::Binary(_) | DataType::VarBinary(_) | DataType::Blob { .. } => "bytea".to_string(),
        DataType::Json => "json".to_string(),
        DataType::Jsonb => "jsonb".to_string(),
        DataType::Array(inner) => format!("_{}", pg_type_name(inner)),
        DataType::Domain(name) => name.clone(),
        DataType::Enum(_) | DataType::Set(_) | DataType::Geometry(_) => "text".to_string(),
    }
}

pub(super) fn pg_type_len(dt: &DataType) -> i32 {
    match dt {
        DataType::Boolean => 1,
        DataType::Int16 => 2,
        DataType::Int32 => 4,
        DataType::Int64 => 8,
        DataType::MySqlInt {
            kind: crate::types::MySqlIntKind::Tiny | crate::types::MySqlIntKind::Small,
            unsigned: false,
        } => 2,
        DataType::MySqlInt {
            kind: crate::types::MySqlIntKind::Int | crate::types::MySqlIntKind::Big,
            ..
        } => 8,
        DataType::MySqlInt { .. } | DataType::Bit(_) | DataType::Year => 4,
        DataType::Float32 => 4,
        DataType::Float64 => 8,
        DataType::MySqlFloat { .. } => 4,
        DataType::MySqlDouble { .. } => 8,
        DataType::Date => 4,
        DataType::Time
        | DataType::MySqlTime { .. }
        | DataType::TimeTz
        | DataType::Timestamp
        | DataType::MySqlDateTime { .. }
        | DataType::MySqlTimestamp { .. }
        | DataType::TimestampTz => 8,
        DataType::Uuid => 16,
        DataType::Text
        | DataType::MySqlText { .. }
        | DataType::VarChar(_)
        | DataType::Char(_)
        | DataType::Interval
        | DataType::Numeric { .. }
        | DataType::Bytea
        | DataType::Binary(_)
        | DataType::VarBinary(_)
        | DataType::Blob { .. }
        | DataType::Json
        | DataType::Jsonb
        | DataType::Array(_)
        | DataType::Domain(_)
        | DataType::Enum(_)
        | DataType::Set(_)
        | DataType::Geometry(_) => -1,
    }
}

pub(super) fn pg_type_byval(dt: &DataType) -> bool {
    !matches!(
        dt,
        DataType::Text
            | DataType::MySqlText { .. }
            | DataType::VarChar(_)
            | DataType::Char(_)
            | DataType::Interval
            | DataType::Numeric { .. }
            | DataType::Bytea
            | DataType::Binary(_)
            | DataType::VarBinary(_)
            | DataType::Blob { .. }
            | DataType::Json
            | DataType::Jsonb
            | DataType::Array(_)
            | DataType::Domain(_)
            | DataType::Enum(_)
            | DataType::Set(_)
            | DataType::Geometry(_)
    )
}

pub(super) fn pg_type_align(dt: &DataType) -> &'static str {
    match dt {
        DataType::Boolean => "c",
        DataType::Int16 => "s",
        DataType::Int32 => "i",
        DataType::MySqlInt {
            kind: crate::types::MySqlIntKind::Tiny | crate::types::MySqlIntKind::Small,
            unsigned: false,
        } => "s",
        DataType::MySqlInt {
            kind: crate::types::MySqlIntKind::Int | crate::types::MySqlIntKind::Big,
            ..
        } => "d",
        DataType::MySqlInt { .. } | DataType::Bit(_) | DataType::Year => "i",
        DataType::Int64
        | DataType::Float64
        | DataType::Time
        | DataType::MySqlTime { .. }
        | DataType::TimeTz
        | DataType::Timestamp
        | DataType::MySqlDateTime { .. }
        | DataType::MySqlTimestamp { .. }
        | DataType::TimestampTz
        | DataType::Interval => "d",
        DataType::Float32 => "i",
        DataType::MySqlFloat { .. } => "i",
        DataType::MySqlDouble { .. } => "d",
        DataType::Text
        | DataType::MySqlText { .. }
        | DataType::VarChar(_)
        | DataType::Char(_)
        | DataType::Numeric { .. }
        | DataType::Bytea
        | DataType::Binary(_)
        | DataType::VarBinary(_)
        | DataType::Blob { .. }
        | DataType::Json
        | DataType::Jsonb
        | DataType::Uuid
        | DataType::Date
        | DataType::Array(_)
        | DataType::Domain(_)
        | DataType::Enum(_)
        | DataType::Set(_)
        | DataType::Geometry(_) => "i",
    }
}

pub(super) fn pg_type_storage(dt: &DataType) -> &'static str {
    match dt {
        DataType::Text
        | DataType::MySqlText { .. }
        | DataType::VarChar(_)
        | DataType::Interval
        | DataType::Numeric { .. }
        | DataType::Bytea
        | DataType::Binary(_)
        | DataType::VarBinary(_)
        | DataType::Blob { .. }
        | DataType::Json
        | DataType::Jsonb
        | DataType::Array(_)
        | DataType::Domain(_)
        | DataType::Enum(_)
        | DataType::Set(_)
        | DataType::Geometry(_) => "x",
        _ => "p",
    }
}

pub(super) fn pg_type_category(dt: &DataType) -> &'static str {
    match dt {
        DataType::Boolean => "B",
        DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::MySqlInt { .. }
        | DataType::Bit(_)
        | DataType::Year
        | DataType::Float32
        | DataType::Float64
        | DataType::MySqlFloat { .. }
        | DataType::MySqlDouble { .. }
        | DataType::Numeric { .. } => "N",
        DataType::Date
        | DataType::Time
        | DataType::MySqlTime { .. }
        | DataType::TimeTz
        | DataType::Timestamp
        | DataType::MySqlDateTime { .. }
        | DataType::MySqlTimestamp { .. }
        | DataType::TimestampTz
        | DataType::Interval => "D",
        DataType::Array(_) => "A",
        DataType::Text
        | DataType::MySqlText { .. }
        | DataType::VarChar(_)
        | DataType::Char(_)
        | DataType::Uuid
        | DataType::Bytea
        | DataType::Binary(_)
        | DataType::VarBinary(_)
        | DataType::Blob { .. }
        | DataType::Json
        | DataType::Jsonb
        | DataType::Domain(_)
        | DataType::Enum(_)
        | DataType::Set(_)
        | DataType::Geometry(_) => "S",
    }
}

pub(super) fn type_collation_oid(dt: &DataType) -> i32 {
    match dt {
        DataType::Text
        | DataType::MySqlText { .. }
        | DataType::VarChar(_)
        | DataType::Char(_)
        | DataType::Domain(_)
        | DataType::Enum(_)
        | DataType::Set(_)
        | DataType::Geometry(_) => DEFAULT_COLLATION_OID,
        _ => 0,
    }
}

pub(super) fn opclass_oid(dt: &DataType) -> i32 {
    match dt {
        DataType::Boolean => BOOL_OPCLASS_OID,
        DataType::Int16 => INT4_OPCLASS_OID,
        DataType::Int32 => INT4_OPCLASS_OID,
        DataType::Int64 => INT8_OPCLASS_OID,
        DataType::MySqlInt {
            kind: crate::types::MySqlIntKind::Big,
            ..
        } => INT8_OPCLASS_OID,
        DataType::MySqlInt { .. } | DataType::Bit(_) | DataType::Year => INT4_OPCLASS_OID,
        _ => TEXT_OPCLASS_OID,
    }
}

pub(super) fn information_schema_data_type(dt: &DataType) -> &'static str {
    match dt {
        DataType::Boolean => "boolean",
        DataType::Int16 => "smallint",
        DataType::Int64 => "bigint",
        DataType::Int32 => "integer",
        DataType::MySqlInt {
            kind: crate::types::MySqlIntKind::Tiny | crate::types::MySqlIntKind::Small,
            unsigned: false,
        } => "smallint",
        DataType::MySqlInt {
            kind: crate::types::MySqlIntKind::Big,
            ..
        } => "bigint",
        DataType::MySqlInt { .. } | DataType::Bit(_) | DataType::Year => "integer",
        DataType::Float32 => "real",
        DataType::Float64 => "double precision",
        DataType::MySqlFloat { .. } => "real",
        DataType::MySqlDouble { .. } => "double precision",
        DataType::Numeric { .. } => "numeric",
        DataType::Text
        | DataType::MySqlText { .. }
        | DataType::Domain(_)
        | DataType::Enum(_)
        | DataType::Set(_)
        | DataType::Geometry(_) => "text",
        DataType::VarChar(_) => "character varying",
        DataType::Char(_) => "character",
        DataType::Date => "date",
        DataType::Time => "time without time zone",
        DataType::MySqlTime { .. } => "time without time zone",
        DataType::TimeTz => "time with time zone",
        DataType::Interval => "interval",
        DataType::Timestamp | DataType::MySqlDateTime { .. } | DataType::MySqlTimestamp { .. } => {
            "timestamp without time zone"
        }
        DataType::TimestampTz => "timestamp with time zone",
        DataType::Uuid => "uuid",
        DataType::Bytea | DataType::Binary(_) | DataType::VarBinary(_) | DataType::Blob { .. } => {
            "bytea"
        }
        DataType::Json => "json",
        DataType::Jsonb => "jsonb",
        DataType::Array(_) => "ARRAY",
    }
}

pub(super) fn index_relation_oid(index: &IndexCatalog) -> i32 {
    INDEX_RELATION_OID_OFFSET + index.index_id as i32
}

pub(super) fn table_row_type_oid(table: &TableCatalog) -> i32 {
    TABLE_ROW_TYPE_OID_OFFSET + table.schema.table_id as i32
}

pub(super) fn view_relation_oid(view: &ViewCatalog) -> i32 {
    VIEW_RELATION_OID_OFFSET + view.view_id as i32
}

pub(super) async fn load_table_stats(
    store: &dyn KvStore,
    schema: &TableSchema,
) -> PgWireResult<Option<storage_layout::TableStats>> {
    let key = storage_layout::stats_key(schema.table_id, schema.table_epoch, None);
    let Some(bytes) = store.get(&key).await.map_err(|e| user_error("XX000", e))? else {
        return Ok(None);
    };
    storage_layout::decode_table_stats(&bytes)
        .map(Some)
        .map_err(|e| user_error("XX000", format!("{e}")))
}

pub(super) fn column_attnum(table: &TableCatalog, column_name: &str) -> i32 {
    table
        .schema
        .columns
        .iter()
        .position(|column| column.name == column_name)
        .map(|idx| idx as i32 + 1)
        .unwrap_or(0)
}

pub(super) fn empty_array(dt: &ArrowDataType) -> ArrayRef {
    match dt {
        ArrowDataType::Int32 => Arc::new(Int32Array::from(Vec::<i32>::new())),
        ArrowDataType::Int64 => Arc::new(Int64Array::from(Vec::<i64>::new())),
        ArrowDataType::Float32 => Arc::new(Float32Array::from(Vec::<f32>::new())),
        ArrowDataType::Float64 => Arc::new(Float64Array::from(Vec::<f64>::new())),
        ArrowDataType::Boolean => Arc::new(BooleanArray::from(Vec::<bool>::new())),
        ArrowDataType::Utf8 => Arc::new(StringArray::from(Vec::<String>::new())),
        ArrowDataType::Binary => Arc::new(BinaryArray::from_vec(Vec::<&[u8]>::new())),
        ArrowDataType::List(_) => {
            let mut builder = ListBuilder::new(StringBuilder::new());
            Arc::new(builder.finish())
        }
        _ => Arc::new(StringArray::from(Vec::<String>::new())),
    }
}

pub(super) fn register_empty_table(
    schema_provider: &Arc<dyn SchemaProvider>,
    table_name: &str,
    fields: Vec<Field>,
) -> PgWireResult<()> {
    let schema = Arc::new(ArrowSchema::new(fields));
    let arrays = schema
        .fields()
        .iter()
        .map(|field| empty_array(field.data_type()))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| user_error("XX000", format!("failed to build {table_name} batch: {e}")))?;
    let table = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|e| user_error("XX000", format!("failed to build {table_name} table: {e}")))?;
    schema_provider
        .register_table(table_name.to_string(), Arc::new(table))
        .map_err(|e| user_error("XX000", format!("failed to register {table_name}: {e}")))?;
    Ok(())
}

pub fn register_pg_catalog_functions(ctx: &SessionContext) {
    register_pg_catalog_functions_with_view_defs(ctx, Vec::new());
}

pub fn register_pg_catalog_functions_with_view_defs(
    ctx: &SessionContext,
    view_defs: Vec<(i32, String)>,
) {
    let view_defs = Arc::new(view_defs);

    fn row_count(args: &[ColumnarValue]) -> usize {
        args.first()
            .map(|arg| match arg {
                ColumnarValue::Array(array) => array.len(),
                ColumnarValue::Scalar(_) => 1,
            })
            .unwrap_or(1)
    }

    fn utf8_result(args: &[ColumnarValue], value: String) -> DfResult<ColumnarValue> {
        if matches!(args.first(), Some(ColumnarValue::Array(_))) {
            Ok(ColumnarValue::Array(Arc::new(StringArray::from(vec![
                value;
                row_count(args)
            ]))))
        } else {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(value))))
        }
    }

    fn bool_result(args: &[ColumnarValue], value: bool) -> DfResult<ColumnarValue> {
        if matches!(args.first(), Some(ColumnarValue::Array(_))) {
            Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(vec![
                value;
                row_count(args)
            ]))))
        } else {
            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(value))))
        }
    }

    fn setting_value(name: &str) -> String {
        match name.to_ascii_lowercase().as_str() {
            "application_name" => String::new(),
            "client_encoding" | "server_encoding" => "UTF8".to_string(),
            "datestyle" => "ISO, YMD".to_string(),
            "default_transaction_isolation" | "transaction_isolation" => {
                "read committed".to_string()
            }
            "integer_datetimes" | "standard_conforming_strings" => "on".to_string(),
            "search_path" => "public".to_string(),
            "server_version" => "14.0".to_string(),
            "server_version_num" => "140000".to_string(),
            "timezone" | "time zone" => "UTC".to_string(),
            _ => String::new(),
        }
    }

    fn type_name_from_oid(oid: i32) -> String {
        match oid {
            16 => "boolean",
            17 => "bytea",
            20 => "bigint",
            23 => "integer",
            25 => "text",
            700 => "real",
            701 => "double precision",
            1082 => "date",
            1083 => "time without time zone",
            1114 => "timestamp without time zone",
            1184 => "timestamp with time zone",
            1186 => "interval",
            1266 => "time with time zone",
            1700 => "numeric",
            2950 => "uuid",
            3802 => "jsonb",
            1043 => "character varying",
            _ => "text",
        }
        .to_string()
    }

    fn type_oid_from_name(name: &str) -> Option<i32> {
        let normalized = name
            .trim()
            .trim_matches('"')
            .rsplit('.')
            .next()
            .unwrap_or(name)
            .trim_matches('"')
            .to_ascii_lowercase();
        match normalized.as_str() {
            "bool" | "boolean" => Some(16),
            "bytea" => Some(17),
            "int8" | "bigint" => Some(20),
            "int2" | "smallint" => Some(21),
            "int4" | "integer" | "int" => Some(23),
            "text" => Some(25),
            "json" => Some(114),
            "float4" | "real" => Some(700),
            "float8" | "double precision" => Some(701),
            "unknown" => Some(705),
            "bpchar" | "char" | "character" => Some(1042),
            "varchar" | "character varying" => Some(1043),
            "date" => Some(1082),
            "time" => Some(1083),
            "timestamp" => Some(1114),
            "timestamptz" => Some(1184),
            "interval" => Some(1186),
            "timetz" => Some(1266),
            "numeric" | "decimal" => Some(1700),
            "record" => Some(2249),
            "uuid" => Some(2950),
            "jsonb" => Some(3802),
            _ => None,
        }
    }

    let get_user_by_id: ScalarFunctionImplementation = Arc::new(|args: &[ColumnarValue]| {
        if matches!(args.first(), Some(ColumnarValue::Array(_))) {
            Ok(ColumnarValue::Array(Arc::new(StringArray::from(vec![
                "postgres";
                row_count(args)
            ]))))
        } else {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                "postgres".to_string(),
            ))))
        }
    });
    ctx.register_udf(create_udf(
        "pg_get_userbyid",
        vec![ArrowDataType::Int32],
        ArrowDataType::Utf8,
        Volatility::Immutable,
        get_user_by_id,
    ));

    let pg_encoding_to_char: ScalarFunctionImplementation =
        Arc::new(|args: &[ColumnarValue]| utf8_result(args, "UTF8".to_string()));
    ctx.register_udf(create_udf(
        "pg_encoding_to_char",
        vec![ArrowDataType::Int32],
        ArrowDataType::Utf8,
        Volatility::Immutable,
        pg_encoding_to_char,
    ));

    let array_to_string: ScalarFunctionImplementation =
        Arc::new(|args: &[ColumnarValue]| utf8_result(args, String::new()));
    ctx.register_udf(create_udf(
        "array_to_string",
        vec![ArrowDataType::Utf8, ArrowDataType::Utf8],
        ArrowDataType::Utf8,
        Volatility::Immutable,
        array_to_string,
    ));

    let format_type: ScalarFunctionImplementation =
        Arc::new(|args: &[ColumnarValue]| match args.first() {
            Some(ColumnarValue::Scalar(ScalarValue::Int32(Some(oid)))) => Ok(
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(type_name_from_oid(*oid)))),
            ),
            Some(ColumnarValue::Array(array)) => {
                let array = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "format_type expects int4 oid".to_string(),
                    )
                })?;
                Ok(ColumnarValue::Array(Arc::new(StringArray::from(
                    (0..array.len())
                        .map(|idx| {
                            if array.is_null(idx) {
                                None
                            } else {
                                Some(type_name_from_oid(array.value(idx)))
                            }
                        })
                        .collect::<Vec<_>>(),
                ))))
            }
            _ => utf8_result(args, "text".to_string()),
        });
    ctx.register_udf(create_udf(
        "format_type",
        vec![ArrowDataType::Int32, ArrowDataType::Int32],
        ArrowDataType::Utf8,
        Volatility::Immutable,
        format_type,
    ));

    let to_regtype: ScalarFunctionImplementation =
        Arc::new(|args: &[ColumnarValue]| match args.first() {
            Some(ColumnarValue::Scalar(ScalarValue::Utf8(Some(name)))) => Ok(
                ColumnarValue::Scalar(ScalarValue::Int32(type_oid_from_name(name))),
            ),
            Some(ColumnarValue::Scalar(ScalarValue::Utf8(None))) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Int32(None)))
            }
            Some(ColumnarValue::Array(array)) => {
                let names = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Execution(
                            "to_regtype expects text".to_string(),
                        )
                    })?;
                Ok(ColumnarValue::Array(Arc::new(Int32Array::from(
                    (0..names.len())
                        .map(|idx| {
                            if names.is_null(idx) {
                                None
                            } else {
                                type_oid_from_name(names.value(idx))
                            }
                        })
                        .collect::<Vec<_>>(),
                ))))
            }
            _ => Ok(ColumnarValue::Scalar(ScalarValue::Int32(None))),
        });
    ctx.register_udf(create_udf(
        "to_regtype",
        vec![ArrowDataType::Utf8],
        ArrowDataType::Int32,
        Volatility::Stable,
        to_regtype,
    ));

    let pg_get_expr: ScalarFunctionImplementation =
        Arc::new(|args: &[ColumnarValue]| utf8_result(args, String::new()));
    ctx.register_udf(create_udf(
        "pg_get_expr",
        vec![ArrowDataType::Utf8, ArrowDataType::Int32],
        ArrowDataType::Utf8,
        Volatility::Immutable,
        pg_get_expr,
    ));

    let pg_get_viewdef_defs = view_defs.clone();
    let pg_get_viewdef: ScalarFunctionImplementation =
        Arc::new(move |args: &[ColumnarValue]| match args.first() {
            Some(ColumnarValue::Scalar(ScalarValue::Int32(Some(oid)))) => {
                let definition = pg_get_viewdef_defs
                    .iter()
                    .find_map(|(view_oid, definition)| {
                        (*view_oid == *oid).then(|| definition.clone())
                    })
                    .unwrap_or_default();
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(definition))))
            }
            Some(ColumnarValue::Array(array)) => {
                let array = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "pg_get_viewdef expects int4 oid".to_string(),
                    )
                })?;
                Ok(ColumnarValue::Array(Arc::new(StringArray::from(
                    (0..array.len())
                        .map(|idx| {
                            if array.is_null(idx) {
                                None
                            } else {
                                Some(
                                    pg_get_viewdef_defs
                                        .iter()
                                        .find_map(|(view_oid, definition)| {
                                            (*view_oid == array.value(idx))
                                                .then(|| definition.clone())
                                        })
                                        .unwrap_or_default(),
                                )
                            }
                        })
                        .collect::<Vec<_>>(),
                ))))
            }
            _ => utf8_result(args, String::new()),
        });
    ctx.register_udf(create_udf(
        "pg_get_viewdef",
        vec![ArrowDataType::Int32],
        ArrowDataType::Utf8,
        Volatility::Immutable,
        pg_get_viewdef,
    ));

    for (name, args) in [
        ("obj_description", vec![ArrowDataType::Int32]),
        (
            "col_description",
            vec![ArrowDataType::Int32, ArrowDataType::Int32],
        ),
    ] {
        let implementation: ScalarFunctionImplementation =
            Arc::new(|args: &[ColumnarValue]| utf8_result(args, String::new()));
        ctx.register_udf(create_udf(
            name,
            args,
            ArrowDataType::Utf8,
            Volatility::Immutable,
            implementation,
        ));
    }

    for (name, value) in [
        ("current_schema", "public"),
        ("current_database", DEFAULT_DATABASE_NAME),
        ("version", "PostgreSQL 14.0 (pg_gateway)"),
    ] {
        let value = value.to_string();
        let implementation: ScalarFunctionImplementation =
            Arc::new(move |args: &[ColumnarValue]| utf8_result(args, value.clone()));
        ctx.register_udf(create_udf(
            name,
            Vec::new(),
            ArrowDataType::Utf8,
            Volatility::Stable,
            implementation,
        ));
    }

    let current_setting: ScalarFunctionImplementation =
        Arc::new(|args: &[ColumnarValue]| match args.first() {
            Some(ColumnarValue::Scalar(ScalarValue::Utf8(Some(name)))) => {
                utf8_result(args, setting_value(name))
            }
            Some(ColumnarValue::Array(array)) => {
                let names = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Execution(
                            "current_setting expects text".to_string(),
                        )
                    })?;
                Ok(ColumnarValue::Array(Arc::new(StringArray::from(
                    (0..names.len())
                        .map(|idx| {
                            if names.is_null(idx) {
                                None
                            } else {
                                Some(setting_value(names.value(idx)))
                            }
                        })
                        .collect::<Vec<_>>(),
                ))))
            }
            _ => utf8_result(args, String::new()),
        });
    ctx.register_udf(create_udf(
        "current_setting",
        vec![ArrowDataType::Utf8],
        ArrowDataType::Utf8,
        Volatility::Stable,
        current_setting,
    ));

    for name in [
        "has_table_privilege",
        "has_schema_privilege",
        "has_database_privilege",
        "has_column_privilege",
    ] {
        let implementation: ScalarFunctionImplementation =
            Arc::new(|args: &[ColumnarValue]| bool_result(args, true));
        ctx.register_udf(create_udf(
            name,
            vec![ArrowDataType::Int32, ArrowDataType::Utf8],
            ArrowDataType::Boolean,
            Volatility::Stable,
            implementation,
        ));
    }
    for name in [
        "pg_table_is_visible",
        "pg_type_is_visible",
        "pg_function_is_visible",
    ] {
        let implementation: ScalarFunctionImplementation =
            Arc::new(|args: &[ColumnarValue]| bool_result(args, true));
        ctx.register_udf(create_udf(
            name,
            vec![ArrowDataType::Int32],
            ArrowDataType::Boolean,
            Volatility::Stable,
            implementation,
        ));
    }
}

// ── KvTableProvider ──────────────────────────────────────────────────
