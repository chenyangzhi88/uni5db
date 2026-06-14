use pgwire::error::PgWireResult;

use crate::error::user_error;
use crate::types::{ColumnValue, DataType, MySqlIntKind};

pub const SCHEMA_PREFIX: &str = "__schema__:";
const TABLE_DATA_PREFIX: &str = "__table__";
const INDEX_DATA_PREFIX: &str = "__index__";
const ROW_MARKER_COLUMN: &str = "__row__";

pub fn encode_cell_value(value: &ColumnValue) -> Vec<u8> {
    let mut buf = Vec::new();
    match value {
        ColumnValue::Null => buf.push(0),
        other => {
            buf.push(1);
            encode_non_null_column_value(&mut buf, other);
        }
    }
    buf
}

pub fn decode_cell_value(data_type: &DataType, bytes: &[u8]) -> PgWireResult<ColumnValue> {
    if bytes.is_empty() {
        return Err(user_error("XX000", "empty cell data"));
    }
    if bytes[0] == 0 {
        return Ok(ColumnValue::Null);
    }
    let (value, consumed) = decode_non_null_column_value(data_type, &bytes[1..])?;
    if consumed + 1 != bytes.len() {
        return Err(user_error("XX000", "malformed cell data"));
    }
    Ok(value)
}

fn encode_non_null_column_value(buf: &mut Vec<u8>, value: &ColumnValue) {
    match value {
        ColumnValue::Int16(v) => buf.extend_from_slice(&v.to_be_bytes()),
        ColumnValue::Int32(v) => buf.extend_from_slice(&v.to_be_bytes()),
        ColumnValue::Int64(v) => buf.extend_from_slice(&v.to_be_bytes()),
        ColumnValue::Float32(v) => buf.extend_from_slice(&v.to_be_bytes()),
        ColumnValue::Float64(v) => buf.extend_from_slice(&v.to_be_bytes()),
        ColumnValue::Numeric(v)
        | ColumnValue::Text(v)
        | ColumnValue::Date(v)
        | ColumnValue::Timestamp(v)
        | ColumnValue::TimestampTz(v)
        | ColumnValue::Uuid(v)
        | ColumnValue::Json(v)
        | ColumnValue::Jsonb(v) => push_bytes(buf, v.as_bytes()),
        ColumnValue::Bytea(v) => push_bytes(buf, v),
        ColumnValue::Boolean(v) => buf.push(u8::from(*v)),
        ColumnValue::Array(values) => {
            buf.extend_from_slice(&(values.len() as u32).to_be_bytes());
            for value in values {
                let encoded = encode_cell_value(value);
                push_bytes(buf, &encoded);
            }
        }
        ColumnValue::Null => {}
    }
}

fn decode_non_null_column_value(
    data_type: &DataType,
    bytes: &[u8],
) -> PgWireResult<(ColumnValue, usize)> {
    match data_type {
        DataType::MySqlInt { kind, unsigned } => decode_mysql_int_value(*kind, *unsigned, bytes),
        DataType::Bit(_) => {
            if bytes.len() < 8 {
                return Err(user_error("XX000", "truncated BIT value"));
            }
            Ok((
                ColumnValue::Int64(i64::from_be_bytes(bytes[..8].try_into().unwrap())),
                8,
            ))
        }
        DataType::Year => {
            if bytes.len() < 4 {
                return Err(user_error("XX000", "truncated YEAR value"));
            }
            Ok((
                ColumnValue::Int32(i32::from_be_bytes(bytes[..4].try_into().unwrap())),
                4,
            ))
        }
        DataType::Int16 => {
            if bytes.len() < 2 {
                return Err(user_error("XX000", "truncated INT2 value"));
            }
            Ok((
                ColumnValue::Int16(i16::from_be_bytes(bytes[..2].try_into().unwrap())),
                2,
            ))
        }
        DataType::Int32 => {
            if bytes.len() < 4 {
                return Err(user_error("XX000", "truncated INT4 value"));
            }
            Ok((
                ColumnValue::Int32(i32::from_be_bytes(bytes[..4].try_into().unwrap())),
                4,
            ))
        }
        DataType::Int64 => {
            if bytes.len() < 8 {
                return Err(user_error("XX000", "truncated INT8 value"));
            }
            Ok((
                ColumnValue::Int64(i64::from_be_bytes(bytes[..8].try_into().unwrap())),
                8,
            ))
        }
        DataType::Float32 | DataType::MySqlFloat { .. } => {
            if bytes.len() < 4 {
                return Err(user_error("XX000", "truncated FLOAT4 value"));
            }
            Ok((
                ColumnValue::Float32(f32::from_be_bytes(bytes[..4].try_into().unwrap())),
                4,
            ))
        }
        DataType::Float64 | DataType::MySqlDouble { .. } => {
            if bytes.len() < 8 {
                return Err(user_error("XX000", "truncated FLOAT8 value"));
            }
            Ok((
                ColumnValue::Float64(f64::from_be_bytes(bytes[..8].try_into().unwrap())),
                8,
            ))
        }
        DataType::Numeric { .. }
        | DataType::Text
        | DataType::MySqlText { .. }
        | DataType::VarChar(_)
        | DataType::Char(_)
        | DataType::Date
        | DataType::Time
        | DataType::MySqlTime { .. }
        | DataType::TimeTz
        | DataType::Interval
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
        | DataType::Geometry(_) => {
            let (s, consumed) = read_string(bytes)?;
            let value = match data_type {
                DataType::Numeric { .. } => ColumnValue::Numeric(s),
                DataType::Date => ColumnValue::Date(s),
                DataType::Timestamp
                | DataType::MySqlDateTime { .. }
                | DataType::MySqlTimestamp { .. } => ColumnValue::Timestamp(s),
                DataType::TimestampTz => ColumnValue::TimestampTz(s),
                DataType::Uuid => ColumnValue::Uuid(s),
                DataType::Json => ColumnValue::Json(s),
                DataType::Jsonb => ColumnValue::Jsonb(s),
                _ => ColumnValue::Text(s),
            };
            Ok((value, consumed))
        }
        DataType::Boolean => {
            if bytes.is_empty() {
                return Err(user_error("XX000", "truncated BOOL value"));
            }
            Ok((ColumnValue::Boolean(bytes[0] != 0), 1))
        }
        DataType::Bytea | DataType::Binary(_) | DataType::VarBinary(_) | DataType::Blob { .. } => {
            let (value, consumed) = read_bytes(bytes)?;
            Ok((ColumnValue::Bytea(value), consumed))
        }
        DataType::Array(inner) => {
            if bytes.len() < 4 {
                return Err(user_error("XX000", "truncated ARRAY length"));
            }
            let count = u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize;
            let mut values = Vec::with_capacity(count);
            let mut offset = 4;
            for _ in 0..count {
                let (encoded, consumed) = read_bytes(&bytes[offset..])?;
                values.push(decode_cell_value(inner, &encoded)?);
                offset += consumed;
            }
            Ok((ColumnValue::Array(values), offset))
        }
    }
}

fn push_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    buf.extend_from_slice(bytes);
}

fn read_bytes(bytes: &[u8]) -> PgWireResult<(Vec<u8>, usize)> {
    if bytes.len() < 4 {
        return Err(user_error("XX000", "truncated value length"));
    }
    let len = u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize;
    if bytes.len() < 4 + len {
        return Err(user_error("XX000", "truncated value bytes"));
    }
    Ok((bytes[4..4 + len].to_vec(), 4 + len))
}

fn read_string(bytes: &[u8]) -> PgWireResult<(String, usize)> {
    let (bytes, consumed) = read_bytes(bytes)?;
    let s =
        String::from_utf8(bytes).map_err(|e| user_error("XX000", format!("invalid UTF-8: {e}")))?;
    Ok((s, consumed))
}

pub fn table_data_prefix(database_name: &str, schema_name: &str, table_name: &str) -> Vec<u8> {
    format!("{TABLE_DATA_PREFIX}:{database_name}:{schema_name}:{table_name}:").into_bytes()
}

pub fn row_marker_prefix(database_name: &str, schema_name: &str, table_name: &str) -> Vec<u8> {
    format!("{TABLE_DATA_PREFIX}:{database_name}:{schema_name}:{table_name}:{ROW_MARKER_COLUMN}:")
        .into_bytes()
}

pub fn row_marker_key(
    database_name: &str,
    schema_name: &str,
    table_name: &str,
    pk_value: &ColumnValue,
) -> Vec<u8> {
    let mut key = row_marker_prefix(database_name, schema_name, table_name);
    key.extend_from_slice(&encode_primary_key_segment(pk_value));
    key
}

pub fn cell_prefix(
    database_name: &str,
    schema_name: &str,
    table_name: &str,
    column_name: &str,
) -> Vec<u8> {
    format!("{TABLE_DATA_PREFIX}:{database_name}:{schema_name}:{table_name}:{column_name}:")
        .into_bytes()
}

pub fn cell_key(
    database_name: &str,
    schema_name: &str,
    table_name: &str,
    column_name: &str,
    pk_value: &ColumnValue,
) -> Vec<u8> {
    let mut key = cell_prefix(database_name, schema_name, table_name, column_name);
    key.extend_from_slice(&encode_primary_key_segment(pk_value));
    key
}

pub fn index_entry_prefix(
    database_name: &str,
    schema_name: &str,
    table_name: &str,
    index_name: &str,
    index_value: &ColumnValue,
) -> Vec<u8> {
    let mut key =
        format!("{INDEX_DATA_PREFIX}:{database_name}:{schema_name}:{table_name}:{index_name}:")
            .into_bytes();
    key.extend_from_slice(&encode_index_value_segment(index_value));
    key.push(b':');
    key
}

pub fn index_table_prefix(database_name: &str, schema_name: &str, table_name: &str) -> Vec<u8> {
    format!("{INDEX_DATA_PREFIX}:{database_name}:{schema_name}:{table_name}:").into_bytes()
}

pub fn index_entry_key(
    database_name: &str,
    schema_name: &str,
    table_name: &str,
    index_name: &str,
    index_value: &ColumnValue,
    pk_value: &ColumnValue,
) -> Vec<u8> {
    let mut key = index_entry_prefix(
        database_name,
        schema_name,
        table_name,
        index_name,
        index_value,
    );
    key.extend_from_slice(&encode_primary_key_segment(pk_value));
    key
}

pub fn schema_key(table_name: &str) -> String {
    format!("{SCHEMA_PREFIX}{table_name}")
}

pub fn decode_pk_from_row_marker_key(
    key: &[u8],
    database_name: &str,
    schema_name: &str,
    table_name: &str,
    data_type: &DataType,
) -> PgWireResult<ColumnValue> {
    let prefix = row_marker_prefix(database_name, schema_name, table_name);
    let suffix = key
        .strip_prefix(prefix.as_slice())
        .ok_or_else(|| user_error("XX000", "row marker key missing expected prefix"))?;
    decode_primary_key_segment(data_type, suffix)
}

pub fn decode_pk_from_cell_key(
    key: &[u8],
    database_name: &str,
    schema_name: &str,
    table_name: &str,
    column_name: &str,
    data_type: &DataType,
) -> PgWireResult<ColumnValue> {
    let prefix = cell_prefix(database_name, schema_name, table_name, column_name);
    let suffix = key
        .strip_prefix(prefix.as_slice())
        .ok_or_else(|| user_error("XX000", "cell key missing expected prefix"))?;
    decode_primary_key_segment(data_type, suffix)
}

pub fn decode_pk_from_index_entry_key(
    key: &[u8],
    database_name: &str,
    schema_name: &str,
    table_name: &str,
    index_name: &str,
    index_value: &ColumnValue,
    pk_data_type: &DataType,
) -> PgWireResult<ColumnValue> {
    let prefix = index_entry_prefix(
        database_name,
        schema_name,
        table_name,
        index_name,
        index_value,
    );
    let suffix = key
        .strip_prefix(prefix.as_slice())
        .ok_or_else(|| user_error("XX000", "index entry key missing expected prefix"))?;
    decode_primary_key_segment(pk_data_type, suffix)
}

fn encode_primary_key_segment(pk_value: &ColumnValue) -> Vec<u8> {
    match pk_value {
        ColumnValue::Int32(v) => ((*v as u32) ^ 0x80000000).to_be_bytes().to_vec(),
        ColumnValue::Int16(v) => ((*v as u16) ^ 0x8000).to_be_bytes().to_vec(),
        ColumnValue::Int64(v) => ((*v as u64) ^ 0x8000000000000000).to_be_bytes().to_vec(),
        ColumnValue::Float32(v) => encode_ordered_f32(*v),
        ColumnValue::Float64(v) => encode_ordered_f64(*v),
        ColumnValue::Text(v)
        | ColumnValue::Numeric(v)
        | ColumnValue::Date(v)
        | ColumnValue::Timestamp(v)
        | ColumnValue::TimestampTz(v)
        | ColumnValue::Uuid(v)
        | ColumnValue::Json(v)
        | ColumnValue::Jsonb(v) => {
            let mut bytes = v.as_bytes().to_vec();
            bytes.push(0);
            bytes
        }
        ColumnValue::Bytea(v) => {
            let mut bytes = v.clone();
            bytes.push(0);
            bytes
        }
        other => panic!("unsupported primary key type: {other:?}"),
    }
}

fn encode_index_value_segment(value: &ColumnValue) -> Vec<u8> {
    let mut buf = Vec::new();
    match value {
        ColumnValue::Null => buf.push(0),
        other => {
            buf.push(1);
            encode_non_null_column_value(&mut buf, other);
        }
    }
    buf
}

fn decode_primary_key_segment(data_type: &DataType, bytes: &[u8]) -> PgWireResult<ColumnValue> {
    match data_type {
        DataType::MySqlInt { kind, unsigned } => decode_mysql_int_key(*kind, *unsigned, bytes),
        DataType::Int32 => {
            if bytes.len() != 4 {
                return Err(user_error("XX000", "malformed INT4 primary key segment"));
            }
            let raw = u32::from_be_bytes(bytes.try_into().unwrap()) ^ 0x80000000;
            Ok(ColumnValue::Int32(raw as i32))
        }
        DataType::Int16 => {
            if bytes.len() != 2 {
                return Err(user_error("XX000", "malformed INT2 primary key segment"));
            }
            let raw = u16::from_be_bytes(bytes.try_into().unwrap()) ^ 0x8000;
            Ok(ColumnValue::Int16(raw as i16))
        }
        DataType::Int64 => {
            if bytes.len() != 8 {
                return Err(user_error("XX000", "malformed INT8 primary key segment"));
            }
            let raw = u64::from_be_bytes(bytes.try_into().unwrap()) ^ 0x8000000000000000;
            Ok(ColumnValue::Int64(raw as i64))
        }
        DataType::Float32 | DataType::MySqlFloat { .. } => {
            if bytes.len() != 4 {
                return Err(user_error("XX000", "malformed FLOAT4 primary key segment"));
            }
            Ok(ColumnValue::Float32(decode_ordered_f32(
                bytes.try_into().unwrap(),
            )))
        }
        DataType::Float64 | DataType::MySqlDouble { .. } => {
            if bytes.len() != 8 {
                return Err(user_error("XX000", "malformed FLOAT8 primary key segment"));
            }
            Ok(ColumnValue::Float64(decode_ordered_f64(
                bytes.try_into().unwrap(),
            )))
        }
        DataType::Text
        | DataType::MySqlText { .. }
        | DataType::VarChar(_)
        | DataType::Char(_)
        | DataType::Numeric { .. }
        | DataType::Date
        | DataType::Time
        | DataType::MySqlTime { .. }
        | DataType::TimeTz
        | DataType::Interval
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
        | DataType::Geometry(_) => {
            let text = bytes
                .strip_suffix(&[0])
                .ok_or_else(|| user_error("XX000", "malformed TEXT primary key segment"))?;
            let s = String::from_utf8(text.to_vec())
                .map_err(|e| user_error("XX000", format!("invalid UTF-8 in key: {e}")))?;
            Ok(match data_type {
                DataType::Numeric { .. } => ColumnValue::Numeric(s),
                DataType::Date => ColumnValue::Date(s),
                DataType::Timestamp
                | DataType::MySqlDateTime { .. }
                | DataType::MySqlTimestamp { .. } => ColumnValue::Timestamp(s),
                DataType::TimestampTz => ColumnValue::TimestampTz(s),
                DataType::Uuid => ColumnValue::Uuid(s),
                DataType::Json => ColumnValue::Json(s),
                DataType::Jsonb => ColumnValue::Jsonb(s),
                _ => ColumnValue::Text(s),
            })
        }
        DataType::Bytea | DataType::Binary(_) | DataType::VarBinary(_) | DataType::Blob { .. } => {
            let value = bytes
                .strip_suffix(&[0])
                .ok_or_else(|| user_error("XX000", "malformed BYTEA primary key segment"))?;
            Ok(ColumnValue::Bytea(value.to_vec()))
        }
        DataType::Bit(_) => decode_primary_key_segment(&DataType::Int64, bytes),
        DataType::Year => decode_primary_key_segment(&DataType::Int32, bytes),
        DataType::Boolean | DataType::Array(_) => Err(user_error(
            "XX000",
            "primary key type is not supported for key encoding",
        )),
    }
}

fn decode_mysql_int_value(
    kind: MySqlIntKind,
    unsigned: bool,
    bytes: &[u8],
) -> PgWireResult<(ColumnValue, usize)> {
    match (kind, unsigned) {
        (MySqlIntKind::Tiny, false) | (MySqlIntKind::Small, false) => {
            if bytes.len() < 2 {
                return Err(user_error("XX000", "truncated MySQL integer value"));
            }
            Ok((
                ColumnValue::Int16(i16::from_be_bytes(bytes[..2].try_into().unwrap())),
                2,
            ))
        }
        (MySqlIntKind::Tiny, true)
        | (MySqlIntKind::Small, true)
        | (MySqlIntKind::Medium, _)
        | (MySqlIntKind::Int, false) => {
            if bytes.len() < 4 {
                return Err(user_error("XX000", "truncated MySQL integer value"));
            }
            Ok((
                ColumnValue::Int32(i32::from_be_bytes(bytes[..4].try_into().unwrap())),
                4,
            ))
        }
        (MySqlIntKind::Int, true) | (MySqlIntKind::Big, false) => {
            if bytes.len() < 8 {
                return Err(user_error("XX000", "truncated MySQL integer value"));
            }
            Ok((
                ColumnValue::Int64(i64::from_be_bytes(bytes[..8].try_into().unwrap())),
                8,
            ))
        }
        (MySqlIntKind::Big, true) => {
            let (s, consumed) = read_string(bytes)?;
            Ok((ColumnValue::Numeric(s), consumed))
        }
    }
}

fn decode_mysql_int_key(
    kind: MySqlIntKind,
    unsigned: bool,
    bytes: &[u8],
) -> PgWireResult<ColumnValue> {
    match (kind, unsigned) {
        (MySqlIntKind::Tiny, false) | (MySqlIntKind::Small, false) => {
            decode_primary_key_segment(&DataType::Int16, bytes)
        }
        (MySqlIntKind::Tiny, true)
        | (MySqlIntKind::Small, true)
        | (MySqlIntKind::Medium, _)
        | (MySqlIntKind::Int, false) => decode_primary_key_segment(&DataType::Int32, bytes),
        (MySqlIntKind::Int, true) | (MySqlIntKind::Big, false) => {
            decode_primary_key_segment(&DataType::Int64, bytes)
        }
        (MySqlIntKind::Big, true) => decode_primary_key_segment(
            &DataType::Numeric {
                precision: None,
                scale: None,
            },
            bytes,
        ),
    }
}

fn encode_ordered_f32(value: f32) -> Vec<u8> {
    let bits = value.to_bits();
    let ordered = if bits & 0x8000_0000 == 0 {
        bits ^ 0x8000_0000
    } else {
        !bits
    };
    ordered.to_be_bytes().to_vec()
}

fn decode_ordered_f32(bytes: [u8; 4]) -> f32 {
    let ordered = u32::from_be_bytes(bytes);
    let bits = if ordered & 0x8000_0000 != 0 {
        ordered ^ 0x8000_0000
    } else {
        !ordered
    };
    f32::from_bits(bits)
}

fn encode_ordered_f64(value: f64) -> Vec<u8> {
    let bits = value.to_bits();
    let ordered = if bits & 0x8000_0000_0000_0000 == 0 {
        bits ^ 0x8000_0000_0000_0000
    } else {
        !bits
    };
    ordered.to_be_bytes().to_vec()
}

fn decode_ordered_f64(bytes: [u8; 8]) -> f64 {
    let ordered = u64::from_be_bytes(bytes);
    let bits = if ordered & 0x8000_0000_0000_0000 != 0 {
        ordered ^ 0x8000_0000_0000_0000
    } else {
        !ordered
    };
    f64::from_bits(bits)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_cell_int32() {
        let bytes = encode_cell_value(&ColumnValue::Int32(42));
        assert_eq!(
            decode_cell_value(&DataType::Int32, &bytes).unwrap(),
            ColumnValue::Int32(42)
        );
    }

    #[test]
    fn roundtrip_cell_text_and_null() {
        let bytes = encode_cell_value(&ColumnValue::Text("alice".into()));
        assert_eq!(
            decode_cell_value(&DataType::Text, &bytes).unwrap(),
            ColumnValue::Text("alice".into())
        );
        let null_bytes = encode_cell_value(&ColumnValue::Null);
        assert_eq!(
            decode_cell_value(&DataType::Text, &null_bytes).unwrap(),
            ColumnValue::Null
        );
    }

    #[test]
    fn row_marker_int_keys_remain_ordered() {
        let k1 = row_marker_key("db", "public", "t", &ColumnValue::Int32(-1));
        let k2 = row_marker_key("db", "public", "t", &ColumnValue::Int32(0));
        let k3 = row_marker_key("db", "public", "t", &ColumnValue::Int32(10));
        assert!(k1 < k2);
        assert!(k2 < k3);
    }

    #[test]
    fn decode_pk_from_marker_roundtrips() {
        let key = row_marker_key("db", "public", "t", &ColumnValue::Text("alpha".into()));
        let decoded =
            decode_pk_from_row_marker_key(&key, "db", "public", "t", &DataType::Text).unwrap();
        assert_eq!(decoded, ColumnValue::Text("alpha".into()));
    }
}
