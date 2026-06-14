use super::*;

pub(super) fn arrow_type_to_pg(dt: &ArrowDataType) -> Type {
    match dt {
        ArrowDataType::Int8 | ArrowDataType::Int16 | ArrowDataType::Int32 => Type::INT4,
        ArrowDataType::Int64 => Type::INT8,
        ArrowDataType::Float32 => Type::FLOAT4,
        ArrowDataType::Float64 => Type::FLOAT8,
        ArrowDataType::Boolean => Type::BOOL,
        ArrowDataType::Binary | ArrowDataType::LargeBinary => Type::BYTEA,
        ArrowDataType::Date32 | ArrowDataType::Date64 => Type::DATE,
        ArrowDataType::Timestamp(_, _) => Type::TIMESTAMP,
        ArrowDataType::List(_) => Type::TEXT_ARRAY,
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => Type::TEXT,
        _ => Type::TEXT,
    }
}

pub fn arrow_to_pgwire_response(batches: Vec<RecordBatch>) -> PgWireResult<Response> {
    if batches.is_empty() {
        let fields = Arc::new(Vec::new());
        return Ok(Response::Query(QueryResponse::new(
            fields,
            stream::iter(Vec::new()),
        )));
    }

    let arrow_schema = batches[0].schema();
    let fields = Arc::new(
        arrow_schema
            .fields()
            .iter()
            .map(|f| {
                FieldInfo::new(
                    f.name().clone(),
                    None,
                    None,
                    arrow_type_to_pg(f.data_type()),
                    FieldFormat::Text,
                )
            })
            .collect::<Vec<_>>(),
    );

    let mut data_rows = Vec::new();
    for batch in &batches {
        for row_idx in 0..batch.num_rows() {
            let mut encoder = DataRowEncoder::new(fields.clone());
            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                if col.is_null(row_idx) {
                    encoder.encode_field(&None::<String>)?;
                } else {
                    let text = arrow_array_value_to_string(col, row_idx);
                    encoder.encode_field(&text)?;
                }
            }
            data_rows.push(encoder.finish());
        }
    }

    Ok(Response::Query(QueryResponse::new(
        fields,
        stream::iter(data_rows),
    )))
}

pub fn arrow_array_value_to_string(array: &ArrayRef, idx: usize) -> String {
    if array.is_null(idx) {
        return String::new();
    }
    match array.data_type() {
        ArrowDataType::Int8 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .unwrap();
            a.value(idx).to_string()
        }
        ArrowDataType::Int16 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .unwrap();
            a.value(idx).to_string()
        }
        ArrowDataType::Int32 => {
            let a = array.as_any().downcast_ref::<Int32Array>().unwrap();
            a.value(idx).to_string()
        }
        ArrowDataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
            a.value(idx).to_string()
        }
        ArrowDataType::UInt8 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::UInt8Array>()
                .unwrap();
            a.value(idx).to_string()
        }
        ArrowDataType::UInt16 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::UInt16Array>()
                .unwrap();
            a.value(idx).to_string()
        }
        ArrowDataType::UInt32 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::UInt32Array>()
                .unwrap();
            a.value(idx).to_string()
        }
        ArrowDataType::UInt64 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .unwrap();
            a.value(idx).to_string()
        }
        ArrowDataType::Float32 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .unwrap();
            a.value(idx).to_string()
        }
        ArrowDataType::Float64 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();
            a.value(idx).to_string()
        }
        ArrowDataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            a.value(idx).to_string()
        }
        ArrowDataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>().unwrap();
            a.value(idx).to_string()
        }
        ArrowDataType::LargeUtf8 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::LargeStringArray>()
                .unwrap();
            a.value(idx).to_string()
        }
        ArrowDataType::Binary => {
            let a = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            format!("\\x{}", hex_encode(a.value(idx)))
        }
        ArrowDataType::Timestamp(_, _) => format_timestamp_text(array, idx),
        ArrowDataType::List(_) => {
            let a = array.as_any().downcast_ref::<ListArray>().unwrap();
            format_list_text(a.value(idx))
        }
        _ => arrow::util::display::array_value_to_string(array, idx)
            .unwrap_or_else(|_| String::new()),
    }
}

fn format_timestamp_text(array: &ArrayRef, idx: usize) -> String {
    arrow::util::display::array_value_to_string(array, idx)
        .unwrap_or_else(|_| String::new())
        .replace('T', " ")
}

fn format_list_text(values: ArrayRef) -> String {
    let mut rendered = Vec::with_capacity(values.len());
    for idx in 0..values.len() {
        if values.is_null(idx) {
            rendered.push("NULL".to_string());
        } else {
            rendered.push(escape_pg_array_element(arrow_array_value_to_string(
                &values, idx,
            )));
        }
    }
    format!("{{{}}}", rendered.join(","))
}

fn escape_pg_array_element(value: String) -> String {
    if value.is_empty()
        || value.contains(',')
        || value.contains('{')
        || value.contains('}')
        || value.contains('"')
        || value.contains('\\')
        || value.chars().any(char::is_whitespace)
    {
        let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
        format!("\"{escaped}\"")
    } else {
        value
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}
