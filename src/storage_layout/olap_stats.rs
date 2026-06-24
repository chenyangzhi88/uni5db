use pgwire::error::PgWireResult;

use super::cursor::Cursor;
use super::keys::{OlapChunkMeta, RowRecord, TableStats, VERSION};
use super::tuple_codec::{
    decode_ordered_f32, decode_ordered_f64, decode_tuple_value, decode_zone_maps,
    encode_ordered_f32, encode_ordered_f64, encode_tuple_value, encode_zone_maps,
    push_bytes_segment, read_bytes_segment,
};
use crate::error::user_error;
use crate::types::{ColumnValue, DataType, MySqlIntKind};

pub fn is_row_visible(record: &RowRecord, snapshot: u64) -> bool {
    if is_row_tombstone(record) {
        return record.mvcc.xmin <= snapshot;
    }
    record.mvcc.xmin <= snapshot && (record.mvcc.xmax == 0 || record.mvcc.xmax > snapshot)
}

pub fn is_row_tombstone(record: &RowRecord) -> bool {
    record.mvcc.xmax != 0 && record.values.is_empty()
}

pub fn encode_olap_chunk_meta(meta: &OlapChunkMeta) -> Vec<u8> {
    let mut buf = vec![VERSION];
    buf.extend_from_slice(&meta.chunk_id.to_be_bytes());
    buf.extend_from_slice(&meta.row_count.to_be_bytes());
    encode_zone_maps(&mut buf, &meta.zones);
    buf
}

pub fn decode_olap_chunk_meta(bytes: &[u8]) -> PgWireResult<OlapChunkMeta> {
    let mut cursor = Cursor::new(bytes);
    let version = cursor.u8()?;
    if version != VERSION {
        return Err(user_error("XX000", "unsupported OLAP chunk meta version"));
    }
    let chunk_id = cursor.u64()?;
    let row_count = cursor.u32()?;
    let zones = decode_zone_maps(&mut cursor)?;
    if !cursor.is_done() {
        return Err(user_error("XX000", "trailing bytes in OLAP chunk meta"));
    }
    Ok(OlapChunkMeta {
        chunk_id,
        row_count,
        zones,
    })
}

pub fn encode_olap_column_chunk(values: &[ColumnValue]) -> Vec<u8> {
    let mut buf = vec![VERSION];
    buf.extend_from_slice(&(values.len() as u32).to_be_bytes());
    for value in values {
        encode_tuple_value(&mut buf, value);
    }
    buf
}

pub fn decode_olap_column_chunk(bytes: &[u8]) -> PgWireResult<Vec<ColumnValue>> {
    let mut cursor = Cursor::new(bytes);
    let version = cursor.u8()?;
    if version != VERSION {
        return Err(user_error("XX000", "unsupported OLAP column chunk version"));
    }
    let count = cursor.u32()? as usize;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(decode_tuple_value(&mut cursor)?);
    }
    if !cursor.is_done() {
        return Err(user_error("XX000", "trailing bytes in OLAP column chunk"));
    }
    Ok(values)
}

pub fn encode_table_stats(stats: &TableStats) -> Vec<u8> {
    let mut buf = vec![VERSION];
    buf.extend_from_slice(&stats.row_count.to_be_bytes());
    encode_zone_maps(&mut buf, &stats.zones);
    buf
}

pub fn decode_table_stats(bytes: &[u8]) -> PgWireResult<TableStats> {
    let mut cursor = Cursor::new(bytes);
    let version = cursor.u8()?;
    if version != VERSION {
        return Err(user_error("XX000", "unsupported table stats version"));
    }
    let row_count = cursor.u64()?;
    let zones = decode_zone_maps(&mut cursor)?;
    if !cursor.is_done() {
        return Err(user_error("XX000", "trailing bytes in table stats"));
    }
    Ok(TableStats { row_count, zones })
}

pub fn encode_key_value(value: &ColumnValue) -> Vec<u8> {
    let mut buf = Vec::new();
    match value {
        ColumnValue::Null => buf.push(0),
        ColumnValue::Boolean(false) => buf.extend_from_slice(&[1, 0]),
        ColumnValue::Boolean(true) => buf.extend_from_slice(&[1, 1]),
        ColumnValue::Int16(v) => {
            buf.push(16);
            buf.extend_from_slice(&((*v as u16) ^ 0x8000).to_be_bytes());
        }
        ColumnValue::Int32(v) => {
            buf.push(2);
            buf.extend_from_slice(&((*v as u32) ^ 0x8000_0000).to_be_bytes());
        }
        ColumnValue::Int64(v) => {
            buf.push(3);
            buf.extend_from_slice(&((*v as u64) ^ 0x8000_0000_0000_0000).to_be_bytes());
        }
        ColumnValue::Float32(v) => {
            buf.push(5);
            buf.extend_from_slice(&encode_ordered_f32(*v));
        }
        ColumnValue::Float64(v) => {
            buf.push(6);
            buf.extend_from_slice(&encode_ordered_f64(*v));
        }
        ColumnValue::Numeric(v)
        | ColumnValue::Text(v)
        | ColumnValue::Date(v)
        | ColumnValue::Timestamp(v)
        | ColumnValue::TimestampTz(v)
        | ColumnValue::Uuid(v)
        | ColumnValue::Json(v)
        | ColumnValue::Jsonb(v) => {
            buf.push(4);
            push_bytes_segment(&mut buf, v.as_bytes());
        }
        ColumnValue::Bytea(v) => {
            buf.push(7);
            push_bytes_segment(&mut buf, v);
        }
        ColumnValue::Array(values) => {
            buf.push(8);
            buf.extend_from_slice(&(values.len() as u32).to_be_bytes());
            for value in values {
                let encoded = encode_key_value(value);
                push_bytes_segment(&mut buf, &encoded);
            }
        }
    }
    buf
}

pub fn decode_key_value(data_type: &DataType, bytes: &[u8]) -> PgWireResult<(ColumnValue, usize)> {
    let Some(tag) = bytes.first() else {
        return Err(user_error("XX000", "empty typed key value"));
    };
    match (tag, data_type) {
        (0, _) => Ok((ColumnValue::Null, 1)),
        (1, DataType::Boolean) => {
            let byte = *bytes
                .get(1)
                .ok_or_else(|| user_error("XX000", "truncated BOOL key value"))?;
            Ok((ColumnValue::Boolean(byte != 0), 2))
        }
        (tag, DataType::MySqlInt { kind, unsigned }) => {
            decode_mysql_int_key_value(*tag, *kind, *unsigned, bytes)
        }
        (16, DataType::Int16) => {
            if bytes.len() < 3 {
                return Err(user_error("XX000", "truncated INT2 key value"));
            }
            let raw = u16::from_be_bytes(bytes[1..3].try_into().unwrap()) ^ 0x8000;
            Ok((ColumnValue::Int16(raw as i16), 3))
        }
        (2, DataType::Int32) => {
            if bytes.len() < 5 {
                return Err(user_error("XX000", "truncated INT4 key value"));
            }
            let raw = u32::from_be_bytes(bytes[1..5].try_into().unwrap()) ^ 0x8000_0000;
            Ok((ColumnValue::Int32(raw as i32), 5))
        }
        (3, DataType::Int64) => {
            if bytes.len() < 9 {
                return Err(user_error("XX000", "truncated INT8 key value"));
            }
            let raw = u64::from_be_bytes(bytes[1..9].try_into().unwrap()) ^ 0x8000_0000_0000_0000;
            Ok((ColumnValue::Int64(raw as i64), 9))
        }
        (
            4,
            DataType::Text
            | DataType::MySqlText { .. }
            | DataType::VarChar(_)
            | DataType::Char(_)
            | DataType::Time
            | DataType::TimeTz
            | DataType::Interval,
        ) => {
            let (text, consumed) = read_bytes_segment(&bytes[1..])?;
            let text = String::from_utf8(text)
                .map_err(|e| user_error("XX000", format!("invalid UTF-8 in v2 key: {e}")))?;
            Ok((ColumnValue::Text(text), consumed + 1))
        }
        (4, data_type)
            if matches!(
                data_type,
                DataType::Numeric { .. }
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
                    | DataType::Geometry(_)
            ) =>
        {
            let (text, consumed) = read_bytes_segment(&bytes[1..])?;
            let text = String::from_utf8(text)
                .map_err(|e| user_error("XX000", format!("invalid UTF-8 in v2 key: {e}")))?;
            let value = match data_type {
                DataType::Numeric { .. } => ColumnValue::Numeric(text),
                DataType::Date => ColumnValue::Date(text),
                DataType::Timestamp
                | DataType::MySqlDateTime { .. }
                | DataType::MySqlTimestamp { .. } => ColumnValue::Timestamp(text),
                DataType::TimestampTz => ColumnValue::TimestampTz(text),
                DataType::Uuid => ColumnValue::Uuid(text),
                DataType::Json => ColumnValue::Json(text),
                DataType::Jsonb => ColumnValue::Jsonb(text),
                _ => ColumnValue::Text(text),
            };
            Ok((value, consumed + 1))
        }
        (5, DataType::Float32 | DataType::MySqlFloat { .. }) => {
            if bytes.len() < 5 {
                return Err(user_error("XX000", "truncated FLOAT4 key value"));
            }
            Ok((
                ColumnValue::Float32(decode_ordered_f32(bytes[1..5].try_into().unwrap())),
                5,
            ))
        }
        (6, DataType::Float64 | DataType::MySqlDouble { .. }) => {
            if bytes.len() < 9 {
                return Err(user_error("XX000", "truncated FLOAT8 key value"));
            }
            Ok((
                ColumnValue::Float64(decode_ordered_f64(bytes[1..9].try_into().unwrap())),
                9,
            ))
        }
        (
            7,
            DataType::Bytea | DataType::Binary(_) | DataType::VarBinary(_) | DataType::Blob { .. },
        ) => {
            let (value, consumed) = read_bytes_segment(&bytes[1..])?;
            Ok((ColumnValue::Bytea(value), consumed + 1))
        }
        (8, DataType::Array(inner)) => {
            if bytes.len() < 5 {
                return Err(user_error("XX000", "truncated ARRAY key value"));
            }
            let count = u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize;
            let mut values = Vec::with_capacity(count);
            let mut offset = 5;
            for _ in 0..count {
                let (encoded, consumed) = read_bytes_segment(&bytes[offset..])?;
                let (value, value_consumed) = decode_key_value(inner, &encoded)?;
                if value_consumed != encoded.len() {
                    return Err(user_error("XX000", "trailing bytes in ARRAY key element"));
                }
                values.push(value);
                offset += consumed;
            }
            Ok((ColumnValue::Array(values), offset))
        }
        _ => Err(user_error(
            "XX000",
            "typed key value does not match column type",
        )),
    }
}

pub(super) fn decode_mysql_int_key_value(
    tag: u8,
    kind: MySqlIntKind,
    unsigned: bool,
    bytes: &[u8],
) -> PgWireResult<(ColumnValue, usize)> {
    match (kind, unsigned) {
        (MySqlIntKind::Tiny, false) | (MySqlIntKind::Small, false) if tag == 16 => {
            decode_key_value(&DataType::Int16, bytes)
        }
        (MySqlIntKind::Tiny, true)
        | (MySqlIntKind::Small, true)
        | (MySqlIntKind::Medium, _)
        | (MySqlIntKind::Int, false)
            if tag == 2 =>
        {
            decode_key_value(&DataType::Int32, bytes)
        }
        (MySqlIntKind::Int, true) | (MySqlIntKind::Big, false) if tag == 3 => {
            decode_key_value(&DataType::Int64, bytes)
        }
        (MySqlIntKind::Big, true) if tag == 4 => decode_key_value(
            &DataType::Numeric {
                precision: None,
                scale: None,
            },
            bytes,
        ),
        _ => Err(user_error(
            "XX000",
            "typed key value does not match MySQL integer type",
        )),
    }
}
