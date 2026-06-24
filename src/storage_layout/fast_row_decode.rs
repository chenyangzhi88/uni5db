use std::collections::BTreeMap;

use pgwire::error::PgWireResult;

use super::cursor::Cursor;
use super::keys::{FAST_ROW_VERSION, FIXED_ROW_VERSION, MvccMeta, RowRecord, VERSION};
use super::row_codec::{
    FastNumericDirectSlot, FastNumericProjector, FastNumericValue, RowValueProjector,
};
use super::tuple_codec::{
    decode_fixed_numeric_value, decode_fixed_value, decode_tuple_numeric_value, decode_tuple_value,
    fixed_offsets_for_columns, fixed_width, skip_tuple_value, variable_positions_for_columns,
};
use crate::error::user_error;
use crate::types::{ColumnValue, RowMap, TableSchema};

pub fn decode_row_record_raw(bytes: &[u8]) -> PgWireResult<RowRecord> {
    let mut cursor = Cursor::new(bytes);
    let version = cursor.u8()?;
    if version == FAST_ROW_VERSION {
        return decode_fast_row_record_raw(bytes);
    }
    if version != VERSION {
        return Err(user_error("XX000", "unsupported row record version"));
    }
    let schema_version = cursor.u32()?;
    let table_epoch = cursor.u64()?;
    let xmin = cursor.u64()?;
    let xmax = cursor.u64()?;
    let count = cursor.u32()? as usize;
    let mut values = BTreeMap::new();
    for _ in 0..count {
        let column_id = cursor.u32()?;
        let value = decode_tuple_value(&mut cursor)?;
        values.insert(column_id, value);
    }
    if !cursor.is_done() {
        return Err(user_error("XX000", "trailing bytes in v2 row record"));
    }
    Ok(RowRecord {
        schema_version,
        table_epoch,
        mvcc: MvccMeta { xmin, xmax },
        values,
    })
}

pub(super) fn fast_row_header(
    bytes: &[u8],
) -> PgWireResult<(u32, u64, MvccMeta, usize, usize, usize)> {
    let mut cursor = Cursor::new(bytes);
    let version = cursor.u8()?;
    if version != FAST_ROW_VERSION {
        return Err(user_error("XX000", "unsupported fast row record version"));
    }
    let schema_version = cursor.u32()?;
    let table_epoch = cursor.u64()?;
    let xmin = cursor.u64()?;
    let xmax = cursor.u64()?;
    let count = cursor.u32()? as usize;
    let directory_start = cursor.pos;
    let payload_start = directory_start
        .checked_add(count.saturating_mul(12))
        .ok_or_else(|| user_error("XX000", "fast row directory overflow"))?;
    if bytes.len() < payload_start {
        return Err(user_error("XX000", "truncated fast row directory"));
    }
    Ok((
        schema_version,
        table_epoch,
        MvccMeta { xmin, xmax },
        count,
        directory_start,
        payload_start,
    ))
}

pub(super) fn fast_row_directory_entry(
    bytes: &[u8],
    directory_start: usize,
    entry_idx: usize,
    payload_start: usize,
) -> PgWireResult<(u32, usize, usize)> {
    let start = directory_start + entry_idx * 12;
    if bytes.len() < start + 12 {
        return Err(user_error("XX000", "truncated fast row directory entry"));
    }
    let column_id = u32::from_be_bytes(bytes[start..start + 4].try_into().unwrap());
    let offset = u32::from_be_bytes(bytes[start + 4..start + 8].try_into().unwrap()) as usize;
    let len = u32::from_be_bytes(bytes[start + 8..start + 12].try_into().unwrap()) as usize;
    let value_start = payload_start
        .checked_add(offset)
        .ok_or_else(|| user_error("XX000", "fast row payload offset overflow"))?;
    let value_end = value_start
        .checked_add(len)
        .ok_or_else(|| user_error("XX000", "fast row payload length overflow"))?;
    if bytes.len() < value_end {
        return Err(user_error("XX000", "truncated fast row payload"));
    }
    Ok((column_id, value_start, value_end))
}

pub(super) fn decode_fast_row_record_projected(
    wanted: &[(u32, &str)],
    bytes: &[u8],
) -> PgWireResult<RowMap> {
    let (_, _, _, count, directory_start, payload_start) = fast_row_header(bytes)?;
    let mut row = RowMap::with_capacity(wanted.len());
    for entry_idx in 0..count {
        let (column_id, value_start, value_end) =
            fast_row_directory_entry(bytes, directory_start, entry_idx, payload_start)?;
        if let Ok(idx) = wanted.binary_search_by_key(&column_id, |(wanted_id, _)| *wanted_id) {
            let mut cursor = Cursor::new(&bytes[value_start..value_end]);
            row.insert(wanted[idx].1.to_string(), decode_tuple_value(&mut cursor)?);
            if !cursor.is_done() {
                return Err(user_error("XX000", "trailing bytes in fast row value"));
            }
        }
    }
    Ok(row)
}

pub(super) fn decode_fast_row_record_projected_values_with(
    projector: &RowValueProjector,
    bytes: &[u8],
    values: &mut Vec<ColumnValue>,
) -> PgWireResult<()> {
    let (_, _, _, count, directory_start, payload_start) = fast_row_header(bytes)?;
    if values.len() == projector.output_len {
        for value in values.iter_mut() {
            *value = ColumnValue::Null;
        }
    } else {
        values.clear();
        values.resize_with(projector.output_len, || ColumnValue::Null);
    }
    for entry_idx in 0..count {
        let (column_id, value_start, value_end) =
            fast_row_directory_entry(bytes, directory_start, entry_idx, payload_start)?;
        if let Some(output_idx) = projector.output_idx(column_id) {
            let mut cursor = Cursor::new(&bytes[value_start..value_end]);
            values[output_idx] = decode_tuple_value(&mut cursor)?;
            if !cursor.is_done() {
                return Err(user_error("XX000", "trailing bytes in fast row value"));
            }
        }
    }
    Ok(())
}

pub(super) fn decode_fast_row_record_raw(bytes: &[u8]) -> PgWireResult<RowRecord> {
    let (schema_version, table_epoch, mvcc, count, directory_start, payload_start) =
        fast_row_header(bytes)?;
    let mut values = BTreeMap::new();
    for entry_idx in 0..count {
        let (column_id, value_start, value_end) =
            fast_row_directory_entry(bytes, directory_start, entry_idx, payload_start)?;
        let mut cursor = Cursor::new(&bytes[value_start..value_end]);
        let value = decode_tuple_value(&mut cursor)?;
        if !cursor.is_done() {
            return Err(user_error("XX000", "trailing bytes in fast row value"));
        }
        values.insert(column_id, value);
    }
    Ok(RowRecord {
        schema_version,
        table_epoch,
        mvcc,
        values,
    })
}

pub(super) fn decode_legacy_row_record_fast_numeric_with(
    projector: &FastNumericProjector,
    cursor: &mut Cursor<'_>,
    values: &mut [FastNumericValue],
) -> PgWireResult<()> {
    let _schema_version = cursor.u32()?;
    let _table_epoch = cursor.u64()?;
    let _xmin = cursor.u64()?;
    let _xmax = cursor.u64()?;
    let count = cursor.u32()? as usize;
    for _ in 0..count {
        let column_id = cursor.u32()?;
        if let Some(output_idx) = projector.output_idx(column_id) {
            values[output_idx] = decode_tuple_numeric_value(cursor)?;
        } else {
            skip_tuple_value(cursor)?;
        }
    }
    if !cursor.is_done() {
        return Err(user_error("XX000", "trailing bytes in v2 row record"));
    }
    Ok(())
}

pub(super) fn decode_fast_row_record_fast_numeric_with(
    projector: &FastNumericProjector,
    bytes: &[u8],
    values: &mut [FastNumericValue],
) -> PgWireResult<()> {
    let (_, _, _, count, directory_start, payload_start) = fast_row_header(bytes)?;
    if projector
        .direct_slots
        .iter()
        .all(|slot| slot.directory_idx < count)
    {
        let mut direct = true;
        for slot in &projector.direct_slots {
            let (column_id, value_start, value_end) = fast_row_directory_entry(
                bytes,
                directory_start,
                slot.directory_idx,
                payload_start,
            )?;
            if column_id != slot.column_id {
                direct = false;
                break;
            }
            let mut cursor = Cursor::new(&bytes[value_start..value_end]);
            values[slot.output_idx] = decode_tuple_numeric_value(&mut cursor)?;
            if !cursor.is_done() {
                return Err(user_error("XX000", "trailing bytes in fast row value"));
            }
        }
        if direct {
            return Ok(());
        }
        values.fill(FastNumericValue::Null);
    }

    for entry_idx in 0..count {
        let (column_id, value_start, value_end) =
            fast_row_directory_entry(bytes, directory_start, entry_idx, payload_start)?;
        if let Some(output_idx) = projector.output_idx(column_id) {
            let mut cursor = Cursor::new(&bytes[value_start..value_end]);
            values[output_idx] = decode_tuple_numeric_value(&mut cursor)?;
            if !cursor.is_done() {
                return Err(user_error("XX000", "trailing bytes in fast row value"));
            }
        }
    }
    Ok(())
}

pub(super) struct FixedRowLayout {
    stored_column_count: usize,
    null_bitmap_start: usize,
    fixed_region_start: usize,
    variable_directory_start: usize,
    variable_payload_start: usize,
    fixed_offsets: Vec<Option<usize>>,
    variable_positions: Vec<Option<usize>>,
}

pub(super) fn fixed_row_layout(schema: &TableSchema, bytes: &[u8]) -> PgWireResult<FixedRowLayout> {
    let mut cursor = Cursor::new(bytes);
    let version = cursor.u8()?;
    if version != FIXED_ROW_VERSION {
        return Err(user_error("XX000", "unsupported fixed row record version"));
    }
    let _schema_version = cursor.u32()?;
    let _table_epoch = cursor.u64()?;
    let _xmin = cursor.u64()?;
    let _xmax = cursor.u64()?;
    let count = cursor.u32()? as usize;
    if count > schema.columns.len() {
        return Err(user_error(
            "XX000",
            "fixed row schema column count mismatch",
        ));
    }
    let null_bitmap_start = cursor.pos;
    let stored_columns = &schema.columns[..count];
    let null_bitmap_len = count.div_ceil(8);
    let fixed_region_start = null_bitmap_start + null_bitmap_len;
    let fixed_offsets = fixed_offsets_for_columns(stored_columns);
    let fixed_region_len = schema
        .columns
        .iter()
        .take(count)
        .filter_map(|column| fixed_width(&column.data_type))
        .sum::<usize>();
    let variable_directory_start = fixed_region_start + fixed_region_len;
    let variable_positions = variable_positions_for_columns(stored_columns);
    let variable_count = variable_positions
        .iter()
        .filter(|position| position.is_some())
        .count();
    let variable_payload_start = variable_directory_start + variable_count * 8;
    if bytes.len() < variable_payload_start {
        return Err(user_error("XX000", "truncated fixed row header"));
    }
    Ok(FixedRowLayout {
        stored_column_count: count,
        null_bitmap_start,
        fixed_region_start,
        variable_directory_start,
        variable_payload_start,
        fixed_offsets,
        variable_positions,
    })
}

pub(super) fn fixed_row_is_null(
    bytes: &[u8],
    layout: &FixedRowLayout,
    column_idx: usize,
) -> PgWireResult<bool> {
    if column_idx >= layout.stored_column_count {
        return Ok(true);
    }
    let byte = bytes
        .get(layout.null_bitmap_start + column_idx / 8)
        .ok_or_else(|| user_error("XX000", "truncated fixed row null bitmap"))?;
    Ok(byte & (1 << (column_idx % 8)) != 0)
}

pub(super) fn decode_fixed_row_record(schema: &TableSchema, bytes: &[u8]) -> PgWireResult<RowMap> {
    let layout = fixed_row_layout(schema, bytes)?;
    let mut row = RowMap::with_capacity(schema.columns.len());
    for (column_idx, column) in schema.columns.iter().enumerate() {
        row.insert(
            column.name.clone(),
            decode_fixed_row_column(schema, bytes, &layout, column_idx)?,
        );
    }
    Ok(row)
}

pub(super) fn decode_fixed_row_record_projected(
    schema: &TableSchema,
    wanted: &[(u32, &str)],
    bytes: &[u8],
) -> PgWireResult<RowMap> {
    let layout = fixed_row_layout(schema, bytes)?;
    let mut row = RowMap::with_capacity(wanted.len());
    for (column_idx, column) in schema.columns.iter().enumerate() {
        if let Ok(idx) = wanted.binary_search_by_key(&column.column_id, |(wanted_id, _)| *wanted_id)
        {
            row.insert(
                wanted[idx].1.to_string(),
                decode_fixed_row_column(schema, bytes, &layout, column_idx)?,
            );
        }
    }
    Ok(row)
}

pub(super) fn decode_fixed_row_record_projected_values_with(
    projector: &RowValueProjector,
    bytes: &[u8],
    values: &mut Vec<ColumnValue>,
) -> PgWireResult<()> {
    if values.len() == projector.output_len {
        for value in values.iter_mut() {
            *value = ColumnValue::Null;
        }
    } else {
        values.clear();
        values.resize_with(projector.output_len, || ColumnValue::Null);
    }
    let layout = fixed_row_layout(&projector.schema, bytes)?;
    for (output_idx, column_idx) in projector.column_indices.iter().enumerate() {
        values[output_idx] =
            decode_fixed_row_column(&projector.schema, bytes, &layout, *column_idx)?;
    }
    Ok(())
}

pub(super) fn decode_fixed_row_column(
    schema: &TableSchema,
    bytes: &[u8],
    layout: &FixedRowLayout,
    column_idx: usize,
) -> PgWireResult<ColumnValue> {
    let column = schema
        .columns
        .get(column_idx)
        .ok_or_else(|| user_error("XX000", "fixed row column index out of bounds"))?;
    if column_idx >= layout.stored_column_count {
        return Ok(ColumnValue::Null);
    }
    if fixed_row_is_null(bytes, layout, column_idx)? {
        return Ok(ColumnValue::Null);
    }
    if let Some(offset) = layout
        .fixed_offsets
        .get(column_idx)
        .and_then(|offset| *offset)
    {
        let width = fixed_width(&column.data_type).unwrap_or(0);
        let start = layout.fixed_region_start + offset;
        let end = start + width;
        if bytes.len() < end {
            return Err(user_error("XX000", "truncated fixed row fixed value"));
        }
        return decode_fixed_value(&column.data_type, &bytes[start..end]);
    }
    let variable_position = layout
        .variable_positions
        .get(column_idx)
        .and_then(|position| *position)
        .ok_or_else(|| user_error("XX000", "fixed row missing variable column position"))?;
    let entry_start = layout.variable_directory_start + variable_position * 8;
    if bytes.len() < entry_start + 8 {
        return Err(user_error(
            "XX000",
            "truncated fixed row variable directory",
        ));
    }
    let offset =
        u32::from_be_bytes(bytes[entry_start..entry_start + 4].try_into().unwrap()) as usize;
    let len =
        u32::from_be_bytes(bytes[entry_start + 4..entry_start + 8].try_into().unwrap()) as usize;
    let value_start = layout
        .variable_payload_start
        .checked_add(offset)
        .ok_or_else(|| user_error("XX000", "fixed row variable offset overflow"))?;
    let value_end = value_start
        .checked_add(len)
        .ok_or_else(|| user_error("XX000", "fixed row variable length overflow"))?;
    if bytes.len() < value_end {
        return Err(user_error("XX000", "truncated fixed row variable value"));
    }
    let mut cursor = Cursor::new(&bytes[value_start..value_end]);
    let value = decode_tuple_value(&mut cursor)?;
    if !cursor.is_done() {
        return Err(user_error(
            "XX000",
            "trailing bytes in fixed row variable value",
        ));
    }
    Ok(value)
}

pub(super) fn decode_fixed_row_record_fast_numeric_with(
    projector: &FastNumericProjector,
    bytes: &[u8],
    values: &mut [FastNumericValue],
) -> PgWireResult<()> {
    if bytes.first().copied() != Some(FIXED_ROW_VERSION) {
        return Err(user_error("XX000", "unsupported fixed row record version"));
    }
    for slot in &projector.direct_slots {
        decode_fixed_row_numeric_slot(bytes, values, slot)?;
    }
    Ok(())
}

pub(super) fn decode_fixed_row_numeric_slot(
    bytes: &[u8],
    values: &mut [FastNumericValue],
    slot: &FastNumericDirectSlot,
) -> PgWireResult<()> {
    let null_byte = bytes
        .get(slot.null_byte_offset)
        .ok_or_else(|| user_error("XX000", "truncated fixed row null bitmap"))?;
    if null_byte & slot.null_bit != 0 {
        values[slot.output_idx] = FastNumericValue::Null;
        return Ok(());
    }
    let Some(start) = slot.fixed_value_start else {
        return Err(user_error(
            "XX000",
            "fixed row fast numeric column is not fixed-width",
        ));
    };
    let width = slot.fixed_width.unwrap_or(0);
    let end = start + width;
    if bytes.len() < end {
        return Err(user_error("XX000", "truncated fixed row numeric value"));
    }
    values[slot.output_idx] = decode_fixed_numeric_value(&slot.data_type, &bytes[start..end])?;
    Ok(())
}
