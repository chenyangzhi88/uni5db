use super::*;

pub fn encode_row_record(schema: &TableSchema, row: &RowMap) -> Vec<u8> {
    encode_row_record_with_mvcc(schema, row, MvccMeta::default())
}

pub fn encode_row_record_with_mvcc(schema: &TableSchema, row: &RowMap, mvcc: MvccMeta) -> Vec<u8> {
    let mut buf = vec![FIXED_ROW_VERSION];
    buf.extend_from_slice(&schema.schema_version.to_be_bytes());
    buf.extend_from_slice(&schema.table_epoch.to_be_bytes());
    buf.extend_from_slice(&mvcc.xmin.to_be_bytes());
    buf.extend_from_slice(&mvcc.xmax.to_be_bytes());
    buf.extend_from_slice(&(schema.columns.len() as u32).to_be_bytes());

    let null_bitmap_len = schema.columns.len().div_ceil(8);
    let null_bitmap_start = buf.len();
    buf.resize(null_bitmap_start + null_bitmap_len, 0);

    let fixed_region_len = schema
        .columns
        .iter()
        .filter_map(|column| fixed_width(&column.data_type))
        .sum::<usize>();
    let fixed_region_start = buf.len();
    buf.resize(fixed_region_start + fixed_region_len, 0);

    let variable_columns = schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| fixed_width(&column.data_type).is_none())
        .collect::<Vec<_>>();
    let variable_directory_start = buf.len();
    buf.resize(variable_directory_start + variable_columns.len() * 8, 0);

    let mut fixed_offset = 0usize;
    let mut variable_payload = Vec::new();
    let mut variable_idx = 0usize;
    for (column_idx, column) in schema.columns.iter().enumerate() {
        let value = row.get(&column.name).unwrap_or(&ColumnValue::Null);
        if value.is_null() {
            buf[null_bitmap_start + column_idx / 8] |= 1 << (column_idx % 8);
        }
        if let Some(width) = fixed_width(&column.data_type) {
            if !value.is_null() {
                encode_fixed_value(
                    &mut buf[fixed_region_start + fixed_offset
                        ..fixed_region_start + fixed_offset + width],
                    value,
                );
            }
            fixed_offset += width;
        } else {
            let offset = variable_payload.len() as u32;
            if !value.is_null() {
                encode_tuple_value(&mut variable_payload, value);
            }
            let len = (variable_payload.len() as u32).saturating_sub(offset);
            let entry_offset = variable_directory_start + variable_idx * 8;
            buf[entry_offset..entry_offset + 4].copy_from_slice(&offset.to_be_bytes());
            buf[entry_offset + 4..entry_offset + 8].copy_from_slice(&len.to_be_bytes());
            variable_idx += 1;
        }
    }
    buf.extend_from_slice(&variable_payload);
    buf
}

pub fn encode_row_tombstone(schema: &TableSchema, version: u64) -> Vec<u8> {
    let mut buf = vec![VERSION];
    buf.extend_from_slice(&schema.schema_version.to_be_bytes());
    buf.extend_from_slice(&schema.table_epoch.to_be_bytes());
    buf.extend_from_slice(&version.to_be_bytes());
    buf.extend_from_slice(&version.to_be_bytes());
    buf.extend_from_slice(&0_u32.to_be_bytes());
    buf
}

pub fn decode_row_record(schema: &TableSchema, bytes: &[u8]) -> PgWireResult<RowMap> {
    if bytes.first().copied() == Some(FIXED_ROW_VERSION) {
        return decode_fixed_row_record(schema, bytes);
    }
    let record = decode_row_record_raw(bytes)?;
    let mut row = RowMap::new();
    for column in &schema.columns {
        row.insert(
            column.name.clone(),
            record
                .values
                .get(&column.column_id)
                .cloned()
                .unwrap_or(ColumnValue::Null),
        );
    }
    Ok(row)
}

pub fn decode_row_record_projected(
    schema: &TableSchema,
    bytes: &[u8],
    column_indices: &[usize],
) -> PgWireResult<RowMap> {
    let mut wanted = column_indices
        .iter()
        .filter_map(|idx| schema.columns.get(*idx))
        .map(|column| (column.column_id, column.name.as_str()))
        .collect::<Vec<_>>();
    wanted.sort_by_key(|(column_id, _)| *column_id);
    wanted.dedup_by_key(|(column_id, _)| *column_id);

    let mut cursor = Cursor::new(bytes);
    let version = cursor.u8()?;
    if version == FIXED_ROW_VERSION {
        return decode_fixed_row_record_projected(schema, &wanted, bytes);
    }
    if version == FAST_ROW_VERSION {
        return decode_fast_row_record_projected(&wanted, bytes);
    }
    if version != VERSION {
        return Err(user_error("XX000", "unsupported row record version"));
    }
    let _schema_version = cursor.u32()?;
    let _table_epoch = cursor.u64()?;
    let _xmin = cursor.u64()?;
    let _xmax = cursor.u64()?;
    let count = cursor.u32()? as usize;
    let mut row = RowMap::with_capacity(wanted.len());
    for _ in 0..count {
        let column_id = cursor.u32()?;
        let value = decode_tuple_value(&mut cursor)?;
        if let Ok(idx) = wanted.binary_search_by_key(&column_id, |(wanted_id, _)| *wanted_id) {
            row.insert(wanted[idx].1.to_string(), value);
        }
    }
    if !cursor.is_done() {
        return Err(user_error("XX000", "trailing bytes in v2 row record"));
    }
    Ok(row)
}

pub fn decode_row_record_projected_values(
    schema: &TableSchema,
    bytes: &[u8],
    column_indices: &[usize],
) -> PgWireResult<Vec<ColumnValue>> {
    let mut values = Vec::with_capacity(column_indices.len());
    let projector = RowValueProjector::new(schema, column_indices);
    decode_row_record_projected_values_with(&projector, bytes, &mut values)?;
    Ok(values)
}

#[derive(Clone, Debug)]
pub struct RowValueProjector {
    pub(super) schema: TableSchema,
    column_id_to_output: Vec<Option<usize>>,
    pub(super) column_indices: Vec<usize>,
    pub(super) output_len: usize,
}

impl RowValueProjector {
    pub fn new(schema: &TableSchema, column_indices: &[usize]) -> Self {
        let max_column_id = schema
            .columns
            .iter()
            .map(|column| column.column_id)
            .max()
            .unwrap_or(0) as usize;
        let mut column_id_to_output = vec![None; max_column_id.saturating_add(1)];
        for (output_idx, column_idx) in column_indices.iter().enumerate() {
            if let Some(column) = schema.columns.get(*column_idx) {
                let column_id = column.column_id as usize;
                if column_id >= column_id_to_output.len() {
                    column_id_to_output.resize(column_id.saturating_add(1), None);
                }
                column_id_to_output[column_id] = Some(output_idx);
            }
        }
        Self {
            schema: schema.clone(),
            column_id_to_output,
            column_indices: column_indices.to_vec(),
            output_len: column_indices.len(),
        }
    }

    pub(super) fn output_idx(&self, column_id: u32) -> Option<usize> {
        self.column_id_to_output
            .get(column_id as usize)
            .and_then(|idx| *idx)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum FastNumericValue {
    Null,
    I64(i64),
    F64(f64),
}

#[derive(Clone, Debug)]
pub struct FastNumericProjector {
    column_id_to_output: Vec<Option<usize>>,
    pub(super) direct_slots: Vec<FastNumericDirectSlot>,
    output_len: usize,
}

#[derive(Clone, Debug)]
pub(super) struct FastNumericDirectSlot {
    pub(super) column_id: u32,
    pub(super) directory_idx: usize,
    pub(super) fixed_value_start: Option<usize>,
    pub(super) fixed_width: Option<usize>,
    pub(super) null_byte_offset: usize,
    pub(super) null_bit: u8,
    pub(super) data_type: DataType,
    pub(super) output_idx: usize,
}

impl FastNumericProjector {
    pub fn new(schema: &TableSchema, column_indices: &[usize]) -> Self {
        let max_column_id = schema
            .columns
            .iter()
            .map(|column| column.column_id)
            .max()
            .unwrap_or(0) as usize;
        let mut column_id_to_output = vec![None; max_column_id.saturating_add(1)];
        for (output_idx, column_idx) in column_indices.iter().enumerate() {
            if let Some(column) = schema.columns.get(*column_idx) {
                let column_id = column.column_id as usize;
                if column_id >= column_id_to_output.len() {
                    column_id_to_output.resize(column_id.saturating_add(1), None);
                }
                column_id_to_output[column_id] = Some(output_idx);
            }
        }
        let fixed_offsets = fixed_offsets(schema);
        let fixed_region_start = 1 + 4 + 8 + 8 + 8 + 4 + schema.columns.len().div_ceil(8);
        let direct_slots = column_indices
            .iter()
            .enumerate()
            .filter_map(|(output_idx, column_idx)| {
                schema.columns.get(*column_idx).map(|column| {
                    let fixed_offset = fixed_offsets.get(*column_idx).copied().flatten();
                    FastNumericDirectSlot {
                        column_id: column.column_id,
                        directory_idx: *column_idx,
                        fixed_value_start: fixed_offset.map(|offset| fixed_region_start + offset),
                        fixed_width: fixed_width(&column.data_type),
                        null_byte_offset: 1 + 4 + 8 + 8 + 8 + 4 + (*column_idx / 8),
                        null_bit: 1 << (*column_idx % 8),
                        data_type: column.data_type.clone(),
                        output_idx,
                    }
                })
            })
            .collect::<Vec<_>>();
        Self {
            column_id_to_output,
            direct_slots,
            output_len: column_indices.len(),
        }
    }

    pub fn output_len(&self) -> usize {
        self.output_len
    }

    pub(super) fn output_idx(&self, column_id: u32) -> Option<usize> {
        self.column_id_to_output
            .get(column_id as usize)
            .and_then(|idx| *idx)
    }
}

pub fn is_fast_row_record(bytes: &[u8]) -> bool {
    matches!(
        bytes.first().copied(),
        Some(FAST_ROW_VERSION) | Some(FIXED_ROW_VERSION)
    )
}

pub fn reencode_row_record_fast(
    schema: &TableSchema,
    bytes: &[u8],
) -> PgWireResult<Option<Vec<u8>>> {
    if bytes.first().copied() == Some(FIXED_ROW_VERSION) {
        return Ok(None);
    }
    let record = decode_row_record_raw(bytes)?;
    if is_row_tombstone(&record) {
        return Ok(None);
    }
    let mut row = RowMap::new();
    for column in &schema.columns {
        row.insert(
            column.name.clone(),
            record
                .values
                .get(&column.column_id)
                .cloned()
                .unwrap_or(ColumnValue::Null),
        );
    }
    Ok(Some(encode_row_record_with_mvcc(schema, &row, record.mvcc)))
}

pub fn decode_row_record_fast_numeric_with(
    projector: &FastNumericProjector,
    bytes: &[u8],
    values: &mut [FastNumericValue],
) -> PgWireResult<()> {
    if values.len() != projector.output_len {
        return Err(user_error(
            "XX000",
            "fast numeric output length does not match projector",
        ));
    }
    match bytes.first().copied() {
        Some(FIXED_ROW_VERSION) => {
            decode_fixed_row_record_fast_numeric_with(projector, bytes, values)
        }
        Some(version) => {
            values.fill(FastNumericValue::Null);
            let mut cursor = Cursor::new(bytes);
            let _version = cursor.u8()?;
            match version {
                VERSION => {
                    decode_legacy_row_record_fast_numeric_with(projector, &mut cursor, values)
                }
                FAST_ROW_VERSION => {
                    decode_fast_row_record_fast_numeric_with(projector, bytes, values)
                }
                _ => Err(user_error("XX000", "unsupported row record version")),
            }
        }
        None => Err(user_error("XX000", "empty row record")),
    }
}

pub fn decode_row_record_fast_numeric_slots_with(
    projector: &FastNumericProjector,
    bytes: &[u8],
    values: &mut [FastNumericValue],
    output_slots: &[usize],
) -> PgWireResult<()> {
    if values.len() != projector.output_len {
        return Err(user_error(
            "XX000",
            "fast numeric output length does not match projector",
        ));
    }
    if bytes.first().copied() != Some(FIXED_ROW_VERSION) {
        return decode_row_record_fast_numeric_with(projector, bytes, values);
    }
    for output_slot in output_slots {
        let Some(slot) = projector
            .direct_slots
            .iter()
            .find(|slot| slot.output_idx == *output_slot)
        else {
            continue;
        };
        decode_fixed_row_numeric_slot(bytes, values, slot)?;
    }
    Ok(())
}

pub fn decode_row_record_projected_values_into(
    schema: &TableSchema,
    bytes: &[u8],
    column_indices: &[usize],
    values: &mut Vec<ColumnValue>,
) -> PgWireResult<()> {
    let projector = RowValueProjector::new(schema, column_indices);
    decode_row_record_projected_values_with(&projector, bytes, values)
}

pub fn decode_row_record_projected_values_with(
    projector: &RowValueProjector,
    bytes: &[u8],
    values: &mut Vec<ColumnValue>,
) -> PgWireResult<()> {
    let mut cursor = Cursor::new(bytes);
    let version = cursor.u8()?;
    if version == FIXED_ROW_VERSION {
        return decode_fixed_row_record_projected_values_with(projector, bytes, values);
    }
    if version == FAST_ROW_VERSION {
        return decode_fast_row_record_projected_values_with(projector, bytes, values);
    }
    if version != VERSION {
        return Err(user_error("XX000", "unsupported row record version"));
    }
    let _schema_version = cursor.u32()?;
    let _table_epoch = cursor.u64()?;
    let _xmin = cursor.u64()?;
    let _xmax = cursor.u64()?;
    let count = cursor.u32()? as usize;
    if values.len() == projector.output_len {
        for value in values.iter_mut() {
            *value = ColumnValue::Null;
        }
    } else {
        values.clear();
        values.resize_with(projector.output_len, || ColumnValue::Null);
    }
    for _ in 0..count {
        let column_id = cursor.u32()?;
        if let Some(output_idx) = projector.output_idx(column_id) {
            let value = decode_tuple_value(&mut cursor)?;
            values[output_idx] = value;
        } else {
            skip_tuple_value(&mut cursor)?;
        }
    }
    if !cursor.is_done() {
        return Err(user_error("XX000", "trailing bytes in v2 row record"));
    }
    Ok(())
}
