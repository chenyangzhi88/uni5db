use std::sync::Arc;

use futures::stream;
use pgwire::api::Type;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;

use crate::error::unsupported;
use crate::types::{ColumnValue, ReturningProjection, RowMap, TableSchema};

pub fn empty_query_response() -> Response {
    Response::Query(QueryResponse::new(
        Arc::new(Vec::new()),
        stream::iter(Vec::<PgWireResult<pgwire::messages::data::DataRow>>::new()),
    ))
}

pub fn command_complete(tag: &str) -> Response {
    Response::Execution(Tag::new(tag))
}

pub fn command_complete_rows(tag: &str, rows: usize) -> Response {
    Response::Execution(Tag::new(tag).with_rows(rows))
}

pub fn spoofed_scalar_response(normalized_sql: &str) -> Option<PgWireResult<Vec<Response>>> {
    match normalized_sql {
        "select version()" | "select version();" => Some(single_text_row_response(
            "version",
            "PostgreSQL 14.0 (pg_gateway)",
        )),
        "select current_schema()" | "select current_schema();" => {
            Some(single_text_row_response("current_schema", "public"))
        }
        "select current_database()" | "select current_database();" => {
            Some(single_text_row_response("current_database", "defaultdb"))
        }
        "select current_user" | "select current_user;" => {
            Some(single_text_row_response("current_user", "postgres"))
        }
        "select session_user" | "select session_user;" => {
            Some(single_text_row_response("session_user", "postgres"))
        }
        _ => None,
    }
}

pub fn single_text_row_response(column_name: &str, value: &str) -> PgWireResult<Vec<Response>> {
    let fields = Arc::new(vec![FieldInfo::new(
        column_name.into(),
        None,
        None,
        Type::TEXT,
        FieldFormat::Text,
    )]);
    let schema = fields.clone();
    let row = (|| -> PgWireResult<_> {
        let mut encoder = DataRowEncoder::new(schema);
        encoder.encode_field(&value)?;
        encoder.finish()
    })();
    Ok(vec![Response::Query(QueryResponse::new(
        fields,
        stream::iter(vec![row]),
    ))])
}

pub fn single_int4_row_response(column_name: &str, value: i32) -> PgWireResult<Vec<Response>> {
    let fields = Arc::new(vec![FieldInfo::new(
        column_name.into(),
        None,
        None,
        Type::INT4,
        FieldFormat::Text,
    )]);
    let schema = fields.clone();
    let row = (|| -> PgWireResult<_> {
        let mut encoder = DataRowEncoder::new(schema);
        encoder.encode_field(&value)?;
        encoder.finish()
    })();
    Ok(vec![Response::Query(QueryResponse::new(
        fields,
        stream::iter(vec![row]),
    ))])
}

pub fn single_int8_row_response(column_name: &str, value: i64) -> PgWireResult<Vec<Response>> {
    let fields = Arc::new(vec![FieldInfo::new(
        column_name.into(),
        None,
        None,
        Type::INT8,
        FieldFormat::Text,
    )]);
    let schema = fields.clone();
    let row = (|| -> PgWireResult<_> {
        let mut encoder = DataRowEncoder::new(schema);
        encoder.encode_field(&value)?;
        encoder.finish()
    })();
    Ok(vec![Response::Query(QueryResponse::new(
        fields,
        stream::iter(vec![row]),
    ))])
}

pub fn multi_text_row_response(
    column_names: &[&str],
    rows: &[Vec<Option<String>>],
) -> PgWireResult<Vec<Response>> {
    let fields = Arc::new(
        column_names
            .iter()
            .map(|name| FieldInfo::new((*name).into(), None, None, Type::TEXT, FieldFormat::Text))
            .collect::<Vec<_>>(),
    );
    let data_rows = rows
        .iter()
        .map(|row| {
            let mut encoder = DataRowEncoder::new(fields.clone());
            for value in row {
                match value {
                    Some(value) => encoder.encode_field(value)?,
                    None => encoder.encode_field(&None::<String>)?,
                }
            }
            encoder.finish()
        })
        .collect::<Vec<_>>();
    Ok(vec![Response::Query(QueryResponse::new(
        fields,
        stream::iter(data_rows),
    ))])
}

pub fn field_infos_for_projection(
    schema: &TableSchema,
    projection: &[String],
    format: FieldFormat,
) -> Vec<FieldInfo> {
    projection
        .iter()
        .map(|name| {
            let pg_type = schema
                .find_column(name)
                .map(|c| c.data_type.to_pg_type())
                .unwrap_or(Type::TEXT);
            FieldInfo::new(name.clone(), None, None, pg_type, format)
        })
        .collect()
}

pub fn field_infos_for_returning_projection(
    schema: &TableSchema,
    projection: &[ReturningProjection],
    format: FieldFormat,
) -> Vec<FieldInfo> {
    projection
        .iter()
        .flat_map(|item| match item {
            ReturningProjection::Wildcard => schema
                .column_names()
                .into_iter()
                .map(|name| {
                    let pg_type = schema
                        .find_column(&name)
                        .map(|c| c.data_type.to_pg_type())
                        .unwrap_or(Type::TEXT);
                    FieldInfo::new(name, None, None, pg_type, format)
                })
                .collect::<Vec<_>>(),
            ReturningProjection::Column(name) => {
                vec![FieldInfo::new(
                    name.clone(),
                    None,
                    None,
                    schema
                        .find_column(name)
                        .map(|c| c.data_type.to_pg_type())
                        .unwrap_or(Type::TEXT),
                    format,
                )]
            }
            ReturningProjection::Expr { output_name, .. } => {
                vec![FieldInfo::new(
                    output_name.clone(),
                    None,
                    None,
                    Type::TEXT,
                    format,
                )]
            }
        })
        .collect()
}

pub fn build_pg_response(
    rows: Vec<RowMap>,
    schema: &TableSchema,
    projection: &[String],
) -> PgWireResult<Response> {
    build_pg_response_with_format(rows, schema, projection, FieldFormat::Text)
}

pub fn build_pg_response_with_format(
    rows: Vec<RowMap>,
    schema: &TableSchema,
    projection: &[String],
    format: FieldFormat,
) -> PgWireResult<Response> {
    let fields = Arc::new(field_infos_for_projection(schema, projection, format));

    let data_rows = rows
        .into_iter()
        .map(|row| {
            let mut encoder = DataRowEncoder::new(fields.clone());
            for (idx, col_name) in projection.iter().enumerate() {
                encode_column_value(&mut encoder, row.get(col_name), fields[idx].format())?;
            }
            encoder.finish()
        })
        .collect::<Vec<_>>();

    Ok(Response::Query(QueryResponse::new(
        fields,
        stream::iter(data_rows),
    )))
}

pub fn build_pg_returning_response(
    rows: Vec<Vec<ColumnValue>>,
    schema: &TableSchema,
    projection: &[ReturningProjection],
    format: FieldFormat,
) -> PgWireResult<Response> {
    let fields = Arc::new(field_infos_for_returning_projection(
        schema, projection, format,
    ));

    let data_rows = rows
        .into_iter()
        .map(|row| {
            let mut encoder = DataRowEncoder::new(fields.clone());
            for (idx, value) in row.iter().enumerate() {
                encode_column_value(&mut encoder, Some(value), fields[idx].format())?;
            }
            encoder.finish()
        })
        .collect::<Vec<_>>();

    Ok(Response::Query(QueryResponse::new(
        fields,
        stream::iter(data_rows),
    )))
}

fn encode_column_value(
    encoder: &mut DataRowEncoder,
    value: Option<&crate::types::ColumnValue>,
    format: FieldFormat,
) -> PgWireResult<()> {
    use crate::types::ColumnValue;

    match value {
        Some(ColumnValue::Null) | None => encoder.encode_field(&None::<String>),
        Some(ColumnValue::Int16(v)) => encoder.encode_field(v),
        Some(ColumnValue::Int32(v)) => encoder.encode_field(v),
        Some(ColumnValue::Int64(v)) => encoder.encode_field(v),
        Some(ColumnValue::Float32(v)) => encoder.encode_field(v),
        Some(ColumnValue::Float64(v)) => encoder.encode_field(v),
        Some(ColumnValue::Boolean(v)) => encoder.encode_field(v),
        Some(ColumnValue::Bytea(v)) => encoder.encode_field(v),
        Some(ColumnValue::Numeric(_))
        | Some(ColumnValue::Date(_))
        | Some(ColumnValue::Timestamp(_))
        | Some(ColumnValue::TimestampTz(_))
        | Some(ColumnValue::Uuid(_))
        | Some(ColumnValue::Json(_))
        | Some(ColumnValue::Jsonb(_))
        | Some(ColumnValue::Array(_))
            if matches!(format, FieldFormat::Binary) =>
        {
            Err(unsupported(
                "binary result format is not supported for this column type yet",
            ))
        }
        Some(value) => match value.to_text() {
            Some(text) => encoder.encode_field(&text),
            None => encoder.encode_field(&None::<String>),
        },
    }
}
