use super::*;

pub(super) const VERSION: u8 = 2;
pub(super) const FAST_ROW_VERSION: u8 = 3;
pub(super) const FIXED_ROW_VERSION: u8 = 4;
pub(super) const ROW_PREFIX: u8 = b'r';
pub(super) const ROW_VERSION_PREFIX: u8 = b'v';
pub(super) const INDEX_PREFIX: u8 = b'i';
pub(super) const OLAP_PREFIX: u8 = b'o';
pub(super) const STATS_PREFIX: u8 = b's';
pub(super) const CATALOG_PREFIX: u8 = b'c';
pub(super) const DESC_VERSION_MAX: u64 = u64::MAX;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvccMeta {
    pub xmin: u64,
    pub xmax: u64,
}

impl Default for MvccMeta {
    fn default() -> Self {
        Self { xmin: 1, xmax: 0 }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RowRecord {
    pub schema_version: u32,
    pub table_epoch: u64,
    pub mvcc: MvccMeta,
    pub values: BTreeMap<u32, ColumnValue>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ColumnZoneMap {
    pub column_id: u32,
    pub null_count: u64,
    pub min: Option<ColumnValue>,
    pub max: Option<ColumnValue>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OlapChunkMeta {
    pub chunk_id: u64,
    pub row_count: u32,
    pub zones: Vec<ColumnZoneMap>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TableStats {
    pub row_count: u64,
    pub zones: Vec<ColumnZoneMap>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeScan {
    pub start: Vec<u8>,
    pub end: Option<Vec<u8>>,
    pub limit: Option<usize>,
    pub reverse: bool,
}

pub fn catalog_descriptor_key(kind: &str, id: u32) -> Vec<u8> {
    let mut key = vec![VERSION, CATALOG_PREFIX];
    push_bytes_segment(&mut key, kind.as_bytes());
    key.extend_from_slice(&id.to_be_bytes());
    key
}

pub fn row_prefix(table_id: u32, table_epoch: u64) -> Vec<u8> {
    let mut key = vec![VERSION, ROW_PREFIX];
    key.extend_from_slice(&table_id.to_be_bytes());
    key.extend_from_slice(&table_epoch.to_be_bytes());
    key
}

pub fn row_key(table_id: u32, table_epoch: u64, pk_value: &ColumnValue) -> Vec<u8> {
    let mut key = row_prefix(table_id, table_epoch);
    key.extend_from_slice(&encode_key_value(pk_value));
    key
}

pub fn row_version_prefix(table_id: u32, table_epoch: u64, pk_value: &ColumnValue) -> Vec<u8> {
    let mut key = vec![VERSION, ROW_VERSION_PREFIX];
    key.extend_from_slice(&table_id.to_be_bytes());
    key.extend_from_slice(&table_epoch.to_be_bytes());
    key.extend_from_slice(&encode_key_value(pk_value));
    key
}

pub fn row_version_key(
    table_id: u32,
    table_epoch: u64,
    pk_value: &ColumnValue,
    version: u64,
) -> Vec<u8> {
    let mut key = row_version_prefix(table_id, table_epoch, pk_value);
    key.extend_from_slice(&(DESC_VERSION_MAX - version).to_be_bytes());
    key
}

pub fn row_version_range(
    table_id: u32,
    table_epoch: u64,
    pk_value: &ColumnValue,
    limit: Option<usize>,
) -> RangeScan {
    let start = row_version_prefix(table_id, table_epoch, pk_value);
    RangeScan {
        end: next_prefix(&start),
        start,
        limit,
        reverse: false,
    }
}

pub fn row_versions_range(table_id: u32, table_epoch: u64, limit: Option<usize>) -> RangeScan {
    let mut start = vec![VERSION, ROW_VERSION_PREFIX];
    start.extend_from_slice(&table_id.to_be_bytes());
    start.extend_from_slice(&table_epoch.to_be_bytes());
    RangeScan {
        end: next_prefix(&start),
        start,
        limit,
        reverse: false,
    }
}

pub fn decode_pk_from_row_version_key(
    key: &[u8],
    table_id: u32,
    table_epoch: u64,
    data_type: &DataType,
) -> PgWireResult<(ColumnValue, u64)> {
    let mut prefix = vec![VERSION, ROW_VERSION_PREFIX];
    prefix.extend_from_slice(&table_id.to_be_bytes());
    prefix.extend_from_slice(&table_epoch.to_be_bytes());
    let suffix = key
        .strip_prefix(prefix.as_slice())
        .ok_or_else(|| user_error("XX000", "v2 row version key missing expected prefix"))?;
    let (value, consumed) = decode_key_value(data_type, suffix)?;
    if suffix.len() != consumed + 8 {
        return Err(user_error("XX000", "malformed v2 row version key suffix"));
    }
    let raw = u64::from_be_bytes(suffix[consumed..consumed + 8].try_into().unwrap());
    Ok((value, DESC_VERSION_MAX - raw))
}

pub fn decode_pk_from_row_key(
    key: &[u8],
    table_id: u32,
    table_epoch: u64,
    data_type: &DataType,
) -> PgWireResult<ColumnValue> {
    let prefix = row_prefix(table_id, table_epoch);
    let suffix = key
        .strip_prefix(prefix.as_slice())
        .ok_or_else(|| user_error("XX000", "v2 row key missing expected prefix"))?;
    let (value, consumed) = decode_key_value(data_type, suffix)?;
    if consumed != suffix.len() {
        return Err(user_error("XX000", "malformed v2 row key suffix"));
    }
    Ok(value)
}

pub fn row_range(table_id: u32, table_epoch: u64, limit: Option<usize>) -> RangeScan {
    let start = row_prefix(table_id, table_epoch);
    RangeScan {
        end: next_prefix(&start),
        start,
        limit,
        reverse: false,
    }
}

pub fn row_range_between(
    table_id: u32,
    table_epoch: u64,
    lower: Option<(&ColumnValue, bool)>,
    upper: Option<(&ColumnValue, bool)>,
    limit: Option<usize>,
) -> RangeScan {
    let start = match lower {
        Some((value, true)) => row_key(table_id, table_epoch, value),
        Some((value, false)) => {
            let mut key = row_key(table_id, table_epoch, value);
            key.push(0);
            key
        }
        None => row_prefix(table_id, table_epoch),
    };
    let end = match upper {
        Some((value, true)) => {
            let mut key = row_key(table_id, table_epoch, value);
            key.push(0);
            Some(key)
        }
        Some((value, false)) => Some(row_key(table_id, table_epoch, value)),
        None => next_prefix(&row_prefix(table_id, table_epoch)),
    };
    RangeScan {
        start,
        end,
        limit,
        reverse: false,
    }
}

pub fn index_prefix(index_id: u32, index_value: &ColumnValue) -> Vec<u8> {
    let mut key = vec![VERSION, INDEX_PREFIX];
    key.extend_from_slice(&index_id.to_be_bytes());
    key.extend_from_slice(&encode_key_value(index_value));
    key
}

pub fn index_all_prefix(index_id: u32) -> Vec<u8> {
    let mut key = vec![VERSION, INDEX_PREFIX];
    key.extend_from_slice(&index_id.to_be_bytes());
    key
}

pub fn index_entry_key(
    index_id: u32,
    index_value: &ColumnValue,
    pk_value: &ColumnValue,
) -> Vec<u8> {
    let mut key = index_prefix(index_id, index_value);
    key.extend_from_slice(&encode_key_value(pk_value));
    key
}

pub fn decode_pk_from_index_entry_key(
    key: &[u8],
    index_id: u32,
    index_value: &ColumnValue,
    pk_data_type: &DataType,
) -> PgWireResult<ColumnValue> {
    let prefix = index_prefix(index_id, index_value);
    let suffix = key
        .strip_prefix(prefix.as_slice())
        .ok_or_else(|| user_error("XX000", "v2 index key missing expected prefix"))?;
    let (value, consumed) = decode_key_value(pk_data_type, suffix)?;
    if consumed != suffix.len() {
        return Err(user_error("XX000", "malformed v2 index key suffix"));
    }
    Ok(value)
}

pub fn olap_chunk_meta_key(table_id: u32, table_epoch: u64, chunk_id: u64) -> Vec<u8> {
    let mut key = vec![VERSION, OLAP_PREFIX, b'm'];
    key.extend_from_slice(&table_id.to_be_bytes());
    key.extend_from_slice(&table_epoch.to_be_bytes());
    key.extend_from_slice(&chunk_id.to_be_bytes());
    key
}

pub fn olap_chunk_meta_prefix(table_id: u32, table_epoch: u64) -> Vec<u8> {
    let mut key = vec![VERSION, OLAP_PREFIX, b'm'];
    key.extend_from_slice(&table_id.to_be_bytes());
    key.extend_from_slice(&table_epoch.to_be_bytes());
    key
}

pub fn olap_chunk_column_prefix(table_id: u32, table_epoch: u64) -> Vec<u8> {
    let mut key = vec![VERSION, OLAP_PREFIX, b'c'];
    key.extend_from_slice(&table_id.to_be_bytes());
    key.extend_from_slice(&table_epoch.to_be_bytes());
    key
}

pub fn olap_chunk_meta_range(table_id: u32, table_epoch: u64) -> RangeScan {
    let start = olap_chunk_meta_prefix(table_id, table_epoch);
    RangeScan {
        end: next_prefix(&start),
        start,
        limit: None,
        reverse: false,
    }
}

pub fn olap_chunk_column_key(
    table_id: u32,
    table_epoch: u64,
    chunk_id: u64,
    column_id: u32,
) -> Vec<u8> {
    let mut key = vec![VERSION, OLAP_PREFIX, b'c'];
    key.extend_from_slice(&table_id.to_be_bytes());
    key.extend_from_slice(&table_epoch.to_be_bytes());
    key.extend_from_slice(&chunk_id.to_be_bytes());
    key.extend_from_slice(&column_id.to_be_bytes());
    key
}

pub fn stats_prefix(table_id: u32, table_epoch: u64) -> Vec<u8> {
    let mut key = vec![VERSION, STATS_PREFIX];
    key.extend_from_slice(&table_id.to_be_bytes());
    key.extend_from_slice(&table_epoch.to_be_bytes());
    key
}

pub fn stats_key(table_id: u32, table_epoch: u64, column_id: Option<u32>) -> Vec<u8> {
    let mut key = vec![VERSION, STATS_PREFIX];
    key.extend_from_slice(&table_id.to_be_bytes());
    key.extend_from_slice(&table_epoch.to_be_bytes());
    key.extend_from_slice(&column_id.unwrap_or(0).to_be_bytes());
    key
}
