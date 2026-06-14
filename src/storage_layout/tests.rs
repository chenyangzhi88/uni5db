use super::*;
use crate::types::{ColumnSchema, RowMap};

fn schema() -> TableSchema {
    TableSchema {
        table_name: "users".into(),
        table_id: 7,
        schema_version: 2,
        table_epoch: 3,
        primary_key: "id".into(),
        check_constraints: Vec::new(),
        unique_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        columns: vec![
            ColumnSchema {
                column_id: 1,
                name: "id".into(),
                data_type: DataType::Int32,
                primary_key: true,
                nullable: false,
                default: None,

                on_update: None,

                character_set: None,

                collation: None,
            },
            ColumnSchema {
                column_id: 2,
                name: "name".into(),
                data_type: DataType::Text,
                primary_key: false,
                nullable: true,
                default: None,

                on_update: None,

                character_set: None,

                collation: None,
            },
        ],
    }
}

#[test]
fn typed_key_encoding_preserves_integer_order() {
    assert!(encode_key_value(&ColumnValue::Int32(-1)) < encode_key_value(&ColumnValue::Int32(0)));
    assert!(encode_key_value(&ColumnValue::Int64(-1)) < encode_key_value(&ColumnValue::Int64(0)));
}

#[test]
fn row_record_roundtrips_by_column_id() {
    let schema = schema();
    let mut row = RowMap::new();
    row.insert("id".into(), ColumnValue::Int32(42));
    row.insert("name".into(), ColumnValue::Text("alice".into()));
    let bytes = encode_row_record(&schema, &row);
    assert_eq!(bytes.first().copied(), Some(FIXED_ROW_VERSION));
    let decoded = decode_row_record(&schema, &bytes).unwrap();
    assert_eq!(decoded.get("id"), Some(&ColumnValue::Int32(42)));
    assert_eq!(
        decoded.get("name"),
        Some(&ColumnValue::Text("alice".into()))
    );
}

#[test]
fn fast_row_projected_decode_reads_only_requested_columns() {
    let schema = schema();
    let mut row = RowMap::new();
    row.insert("id".into(), ColumnValue::Int32(42));
    row.insert("name".into(), ColumnValue::Text("alice".into()));
    let bytes = encode_row_record(&schema, &row);
    let decoded = decode_row_record_projected(&schema, &bytes, &[0]).unwrap();
    assert_eq!(decoded.get("id"), Some(&ColumnValue::Int32(42)));
    assert_eq!(decoded.get("name"), None);
}

#[test]
fn fast_numeric_projector_reads_new_and_legacy_rows() {
    let schema = schema();
    let projector = FastNumericProjector::new(&schema, &[0]);
    let mut values = vec![FastNumericValue::Null; projector.output_len()];

    let mut row = RowMap::new();
    row.insert("id".into(), ColumnValue::Int32(42));
    row.insert("name".into(), ColumnValue::Text("alice".into()));
    let fast_bytes = encode_row_record(&schema, &row);
    decode_row_record_fast_numeric_with(&projector, &fast_bytes, &mut values).unwrap();
    assert_eq!(values, vec![FastNumericValue::I64(42)]);

    let mut legacy = vec![VERSION];
    legacy.extend_from_slice(&schema.schema_version.to_be_bytes());
    legacy.extend_from_slice(&schema.table_epoch.to_be_bytes());
    legacy.extend_from_slice(&1_u64.to_be_bytes());
    legacy.extend_from_slice(&0_u64.to_be_bytes());
    legacy.extend_from_slice(&2_u32.to_be_bytes());
    legacy.extend_from_slice(&1_u32.to_be_bytes());
    encode_tuple_value(&mut legacy, &ColumnValue::Int32(7));
    legacy.extend_from_slice(&2_u32.to_be_bytes());
    encode_tuple_value(&mut legacy, &ColumnValue::Text("legacy".into()));
    decode_row_record_fast_numeric_with(&projector, &legacy, &mut values).unwrap();
    assert_eq!(values, vec![FastNumericValue::I64(7)]);
}

#[test]
fn row_and_index_keys_are_id_based() {
    let row = row_key(7, 3, &ColumnValue::Int32(42));
    assert_eq!(
        decode_pk_from_row_key(&row, 7, 3, &DataType::Int32).unwrap(),
        ColumnValue::Int32(42)
    );
    let index = index_entry_key(9, &ColumnValue::Text("a".into()), &ColumnValue::Int32(42));
    assert_eq!(
        decode_pk_from_index_entry_key(&index, 9, &ColumnValue::Text("a".into()), &DataType::Int32)
            .unwrap(),
        ColumnValue::Int32(42)
    );
}
