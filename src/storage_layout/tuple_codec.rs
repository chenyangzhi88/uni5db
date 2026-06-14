use super::*;

pub(super) fn encode_zone_maps(buf: &mut Vec<u8>, zones: &[ColumnZoneMap]) {
    buf.extend_from_slice(&(zones.len() as u32).to_be_bytes());
    for zone in zones {
        buf.extend_from_slice(&zone.column_id.to_be_bytes());
        buf.extend_from_slice(&zone.null_count.to_be_bytes());
        match &zone.min {
            Some(value) => {
                buf.push(1);
                encode_tuple_value(buf, value);
            }
            None => buf.push(0),
        }
        match &zone.max {
            Some(value) => {
                buf.push(1);
                encode_tuple_value(buf, value);
            }
            None => buf.push(0),
        }
    }
}

pub(super) fn decode_zone_maps(cursor: &mut Cursor<'_>) -> PgWireResult<Vec<ColumnZoneMap>> {
    let count = cursor.u32()? as usize;
    let mut zones = Vec::with_capacity(count);
    for _ in 0..count {
        let column_id = cursor.u32()?;
        let null_count = cursor.u64()?;
        let min = match cursor.u8()? {
            0 => None,
            1 => Some(decode_tuple_value(cursor)?),
            _ => return Err(user_error("XX000", "unknown zone min tag")),
        };
        let max = match cursor.u8()? {
            0 => None,
            1 => Some(decode_tuple_value(cursor)?),
            _ => return Err(user_error("XX000", "unknown zone max tag")),
        };
        zones.push(ColumnZoneMap {
            column_id,
            null_count,
            min,
            max,
        });
    }
    Ok(zones)
}

pub(super) fn fixed_width(data_type: &DataType) -> Option<usize> {
    match data_type {
        DataType::Boolean => Some(1),
        DataType::Int16 => Some(2),
        DataType::MySqlInt {
            kind: MySqlIntKind::Tiny | MySqlIntKind::Small,
            unsigned: false,
        } => Some(2),
        DataType::Int32 => Some(4),
        DataType::MySqlInt {
            kind: MySqlIntKind::Int,
            unsigned: true,
        } => Some(8),
        DataType::MySqlInt {
            kind: MySqlIntKind::Tiny | MySqlIntKind::Small | MySqlIntKind::Medium,
            unsigned: _,
        } => Some(4),
        DataType::Int64 => Some(8),
        DataType::MySqlInt {
            kind: MySqlIntKind::Big,
            unsigned: false,
        } => Some(8),
        DataType::MySqlInt {
            kind: MySqlIntKind::Big,
            unsigned: true,
        } => None,
        DataType::Year => Some(4),
        DataType::Bit(_) => Some(8),
        DataType::Float32 => Some(4),
        DataType::MySqlFloat { .. } => Some(4),
        DataType::Float64 => Some(8),
        DataType::MySqlDouble { .. } => Some(8),
        _ => None,
    }
}

pub(super) fn fixed_offsets(schema: &TableSchema) -> Vec<Option<usize>> {
    fixed_offsets_for_columns(&schema.columns)
}

pub(super) fn fixed_offsets_for_columns(columns: &[ColumnSchema]) -> Vec<Option<usize>> {
    let mut offset = 0usize;
    columns
        .iter()
        .map(|column| {
            fixed_width(&column.data_type).map(|width| {
                let current = offset;
                offset += width;
                current
            })
        })
        .collect()
}

pub(super) fn variable_positions_for_columns(columns: &[ColumnSchema]) -> Vec<Option<usize>> {
    let mut position = 0usize;
    columns
        .iter()
        .map(|column| {
            if fixed_width(&column.data_type).is_some() {
                None
            } else {
                let current = position;
                position += 1;
                Some(current)
            }
        })
        .collect()
}

pub(super) fn encode_fixed_value(dst: &mut [u8], value: &ColumnValue) {
    match value {
        ColumnValue::Boolean(value) if dst.len() == 1 => dst[0] = u8::from(*value),
        ColumnValue::Int16(value) if dst.len() == 2 => dst.copy_from_slice(&value.to_be_bytes()),
        ColumnValue::Int32(value) if dst.len() == 4 => dst.copy_from_slice(&value.to_be_bytes()),
        ColumnValue::Int64(value) if dst.len() == 8 => dst.copy_from_slice(&value.to_be_bytes()),
        ColumnValue::Float32(value) if dst.len() == 4 => dst.copy_from_slice(&value.to_be_bytes()),
        ColumnValue::Float64(value) if dst.len() == 8 => dst.copy_from_slice(&value.to_be_bytes()),
        _ => {}
    }
}

pub(super) fn decode_fixed_value(data_type: &DataType, bytes: &[u8]) -> PgWireResult<ColumnValue> {
    match data_type {
        DataType::Boolean if bytes.len() == 1 => Ok(ColumnValue::Boolean(bytes[0] != 0)),
        DataType::MySqlInt { kind, unsigned } => decode_mysql_fixed_int(*kind, *unsigned, bytes),
        DataType::Bit(_) if bytes.len() == 8 => Ok(ColumnValue::Int64(i64::from_be_bytes(
            bytes.try_into().unwrap(),
        ))),
        DataType::Year if bytes.len() == 4 => Ok(ColumnValue::Int32(i32::from_be_bytes(
            bytes.try_into().unwrap(),
        ))),
        DataType::Int16 if bytes.len() == 2 => Ok(ColumnValue::Int16(i16::from_be_bytes(
            bytes.try_into().unwrap(),
        ))),
        DataType::Int32 if bytes.len() == 4 => Ok(ColumnValue::Int32(i32::from_be_bytes(
            bytes.try_into().unwrap(),
        ))),
        DataType::Int64 if bytes.len() == 8 => Ok(ColumnValue::Int64(i64::from_be_bytes(
            bytes.try_into().unwrap(),
        ))),
        DataType::Float32 | DataType::MySqlFloat { .. } if bytes.len() == 4 => Ok(
            ColumnValue::Float32(f32::from_be_bytes(bytes.try_into().unwrap())),
        ),
        DataType::Float64 | DataType::MySqlDouble { .. } if bytes.len() == 8 => Ok(
            ColumnValue::Float64(f64::from_be_bytes(bytes.try_into().unwrap())),
        ),
        _ => Err(user_error("XX000", "fixed value type/width mismatch")),
    }
}

pub(super) fn decode_mysql_fixed_int(
    kind: MySqlIntKind,
    unsigned: bool,
    bytes: &[u8],
) -> PgWireResult<ColumnValue> {
    match (kind, unsigned, bytes.len()) {
        (MySqlIntKind::Tiny, false, 2) | (MySqlIntKind::Small, false, 2) => Ok(ColumnValue::Int16(
            i16::from_be_bytes(bytes.try_into().unwrap()),
        )),
        (MySqlIntKind::Tiny, true, 4)
        | (MySqlIntKind::Small, true, 4)
        | (MySqlIntKind::Medium, _, 4)
        | (MySqlIntKind::Int, false, 4) => Ok(ColumnValue::Int32(i32::from_be_bytes(
            bytes.try_into().unwrap(),
        ))),
        (MySqlIntKind::Int, true, 8) | (MySqlIntKind::Big, false, 8) => Ok(ColumnValue::Int64(
            i64::from_be_bytes(bytes.try_into().unwrap()),
        )),
        _ => Err(user_error(
            "XX000",
            "fixed MySQL integer type/width mismatch",
        )),
    }
}

pub(super) fn decode_fixed_numeric_value(
    data_type: &DataType,
    bytes: &[u8],
) -> PgWireResult<FastNumericValue> {
    match data_type {
        DataType::MySqlInt { kind, unsigned } => decode_mysql_fixed_int(*kind, *unsigned, bytes)
            .and_then(|value| match value {
                ColumnValue::Int16(v) => Ok(FastNumericValue::I64(v as i64)),
                ColumnValue::Int32(v) => Ok(FastNumericValue::I64(v as i64)),
                ColumnValue::Int64(v) => Ok(FastNumericValue::I64(v)),
                _ => Err(user_error("XX000", "fixed numeric type/width mismatch")),
            }),
        DataType::Bit(_) if bytes.len() == 8 => Ok(FastNumericValue::I64(i64::from_be_bytes(
            bytes.try_into().unwrap(),
        ))),
        DataType::Year if bytes.len() == 4 => Ok(FastNumericValue::I64(i32::from_be_bytes(
            bytes.try_into().unwrap(),
        ) as i64)),
        DataType::Int16 if bytes.len() == 2 => Ok(FastNumericValue::I64(i16::from_be_bytes(
            bytes.try_into().unwrap(),
        ) as i64)),
        DataType::Int32 if bytes.len() == 4 => Ok(FastNumericValue::I64(i32::from_be_bytes(
            bytes.try_into().unwrap(),
        ) as i64)),
        DataType::Int64 if bytes.len() == 8 => Ok(FastNumericValue::I64(i64::from_be_bytes(
            bytes.try_into().unwrap(),
        ))),
        DataType::Float32 | DataType::MySqlFloat { .. } if bytes.len() == 4 => Ok(
            FastNumericValue::F64(f32::from_be_bytes(bytes.try_into().unwrap()) as f64),
        ),
        DataType::Float64 | DataType::MySqlDouble { .. } if bytes.len() == 8 => Ok(
            FastNumericValue::F64(f64::from_be_bytes(bytes.try_into().unwrap())),
        ),
        _ => Err(user_error("XX000", "fixed numeric type/width mismatch")),
    }
}

pub(super) fn encode_tuple_value(buf: &mut Vec<u8>, value: &ColumnValue) {
    match value {
        ColumnValue::Null => buf.push(0),
        ColumnValue::Boolean(v) => buf.extend_from_slice(&[1, u8::from(*v)]),
        ColumnValue::Int16(v) => {
            buf.push(16);
            buf.extend_from_slice(&v.to_be_bytes());
        }
        ColumnValue::Int32(v) => {
            buf.push(2);
            buf.extend_from_slice(&v.to_be_bytes());
        }
        ColumnValue::Int64(v) => {
            buf.push(3);
            buf.extend_from_slice(&v.to_be_bytes());
        }
        ColumnValue::Text(v) => {
            buf.push(4);
            push_bytes_segment(buf, v.as_bytes());
        }
        ColumnValue::Float32(v) => {
            buf.push(5);
            buf.extend_from_slice(&v.to_be_bytes());
        }
        ColumnValue::Float64(v) => {
            buf.push(6);
            buf.extend_from_slice(&v.to_be_bytes());
        }
        ColumnValue::Bytea(v) => {
            buf.push(7);
            push_bytes_segment(buf, v);
        }
        ColumnValue::Array(values) => {
            buf.push(8);
            buf.extend_from_slice(&(values.len() as u32).to_be_bytes());
            for value in values {
                encode_tuple_value(buf, value);
            }
        }
        ColumnValue::Numeric(v) => {
            buf.push(9);
            push_bytes_segment(buf, v.as_bytes());
        }
        ColumnValue::Date(v) => {
            buf.push(10);
            push_bytes_segment(buf, v.as_bytes());
        }
        ColumnValue::Timestamp(v) => {
            buf.push(11);
            push_bytes_segment(buf, v.as_bytes());
        }
        ColumnValue::TimestampTz(v) => {
            buf.push(12);
            push_bytes_segment(buf, v.as_bytes());
        }
        ColumnValue::Uuid(v) => {
            buf.push(13);
            push_bytes_segment(buf, v.as_bytes());
        }
        ColumnValue::Json(v) => {
            buf.push(14);
            push_bytes_segment(buf, v.as_bytes());
        }
        ColumnValue::Jsonb(v) => {
            buf.push(15);
            push_bytes_segment(buf, v.as_bytes());
        }
    }
}

pub(super) fn decode_tuple_value(cursor: &mut Cursor<'_>) -> PgWireResult<ColumnValue> {
    match cursor.u8()? {
        0 => Ok(ColumnValue::Null),
        1 => Ok(ColumnValue::Boolean(cursor.u8()? != 0)),
        16 => Ok(ColumnValue::Int16(cursor.i16()?)),
        2 => Ok(ColumnValue::Int32(cursor.i32()?)),
        3 => Ok(ColumnValue::Int64(cursor.i64()?)),
        4 => {
            let bytes = cursor.bytes_segment()?;
            let text = String::from_utf8(bytes)
                .map_err(|e| user_error("XX000", format!("invalid UTF-8 in tuple: {e}")))?;
            Ok(ColumnValue::Text(text))
        }
        5 => Ok(ColumnValue::Float32(cursor.f32()?)),
        6 => Ok(ColumnValue::Float64(cursor.f64()?)),
        7 => Ok(ColumnValue::Bytea(cursor.bytes_segment()?)),
        8 => {
            let count = cursor.u32()? as usize;
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                values.push(decode_tuple_value(cursor)?);
            }
            Ok(ColumnValue::Array(values))
        }
        9 => decode_tuple_string(cursor).map(ColumnValue::Numeric),
        10 => decode_tuple_string(cursor).map(ColumnValue::Date),
        11 => decode_tuple_string(cursor).map(ColumnValue::Timestamp),
        12 => decode_tuple_string(cursor).map(ColumnValue::TimestampTz),
        13 => decode_tuple_string(cursor).map(ColumnValue::Uuid),
        14 => decode_tuple_string(cursor).map(ColumnValue::Json),
        15 => decode_tuple_string(cursor).map(ColumnValue::Jsonb),
        _ => Err(user_error("XX000", "unknown tuple value tag")),
    }
}

pub(super) fn decode_tuple_numeric_value(
    cursor: &mut Cursor<'_>,
) -> PgWireResult<FastNumericValue> {
    match cursor.u8()? {
        0 => Ok(FastNumericValue::Null),
        16 => Ok(FastNumericValue::I64(cursor.i16()? as i64)),
        2 => Ok(FastNumericValue::I64(cursor.i32()? as i64)),
        3 => Ok(FastNumericValue::I64(cursor.i64()?)),
        5 => Ok(FastNumericValue::F64(cursor.f32()? as f64)),
        6 => Ok(FastNumericValue::F64(cursor.f64()?)),
        _ => Err(user_error("XX000", "tuple value is not numeric")),
    }
}

pub(super) fn skip_tuple_value(cursor: &mut Cursor<'_>) -> PgWireResult<()> {
    match cursor.u8()? {
        0 => Ok(()),
        1 => cursor.skip(1),
        16 => cursor.skip(2),
        2 => cursor.skip(4),
        3 => cursor.skip(8),
        4 | 7 | 9 | 10 | 11 | 12 | 13 | 14 | 15 => cursor.skip_bytes_segment(),
        5 => cursor.skip(4),
        6 => cursor.skip(8),
        8 => {
            let count = cursor.u32()? as usize;
            for _ in 0..count {
                skip_tuple_value(cursor)?;
            }
            Ok(())
        }
        _ => Err(user_error("XX000", "unknown tuple value tag")),
    }
}

pub(super) fn decode_tuple_string(cursor: &mut Cursor<'_>) -> PgWireResult<String> {
    let bytes = cursor.bytes_segment()?;
    String::from_utf8(bytes)
        .map_err(|e| user_error("XX000", format!("invalid UTF-8 in tuple: {e}")))
}

pub(super) fn encode_ordered_f32(value: f32) -> [u8; 4] {
    let bits = value.to_bits();
    let ordered = if bits & 0x8000_0000 == 0 {
        bits ^ 0x8000_0000
    } else {
        !bits
    };
    ordered.to_be_bytes()
}

pub(super) fn decode_ordered_f32(bytes: [u8; 4]) -> f32 {
    let ordered = u32::from_be_bytes(bytes);
    let bits = if ordered & 0x8000_0000 != 0 {
        ordered ^ 0x8000_0000
    } else {
        !ordered
    };
    f32::from_bits(bits)
}

pub(super) fn encode_ordered_f64(value: f64) -> [u8; 8] {
    let bits = value.to_bits();
    let ordered = if bits & 0x8000_0000_0000_0000 == 0 {
        bits ^ 0x8000_0000_0000_0000
    } else {
        !bits
    };
    ordered.to_be_bytes()
}

pub(super) fn decode_ordered_f64(bytes: [u8; 8]) -> f64 {
    let ordered = u64::from_be_bytes(bytes);
    let bits = if ordered & 0x8000_0000_0000_0000 != 0 {
        ordered ^ 0x8000_0000_0000_0000
    } else {
        !ordered
    };
    f64::from_bits(bits)
}

pub(super) fn push_bytes_segment(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    buf.extend_from_slice(bytes);
}

pub(super) fn read_bytes_segment(bytes: &[u8]) -> PgWireResult<(Vec<u8>, usize)> {
    if bytes.len() < 4 {
        return Err(user_error("XX000", "truncated bytes segment length"));
    }
    let len = u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize;
    if bytes.len() < 4 + len {
        return Err(user_error("XX000", "truncated bytes segment"));
    }
    Ok((bytes[4..4 + len].to_vec(), 4 + len))
}

pub(super) fn next_prefix(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    for idx in (0..upper.len()).rev() {
        if upper[idx] != u8::MAX {
            upper[idx] += 1;
            upper.truncate(idx + 1);
            return Some(upper);
        }
    }
    None
}
