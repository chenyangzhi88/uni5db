use std::cmp::Ordering;

use super::{
    ColumnSchema, ColumnValue, DataType, MySqlIntKind, RowMap, TableSchema, apply_assignments,
    parse_column_schema,
};

#[test]
fn data_type_from_sql_int_variants() {
    assert_eq!(DataType::from_sql("INT"), DataType::Int32);
    assert_eq!(DataType::from_sql("int4"), DataType::Int32);
    assert_eq!(DataType::from_sql("INTEGER"), DataType::Int32);
    assert_eq!(DataType::from_sql("SERIAL"), DataType::Int32);
}

#[test]
fn data_type_from_sql_bigint_variants() {
    assert_eq!(DataType::from_sql("BIGINT"), DataType::Int64);
    assert_eq!(DataType::from_sql("int8"), DataType::Int64);
    assert_eq!(DataType::from_sql("BIGSERIAL"), DataType::Int64);
}

#[test]
fn data_type_from_sql_bool_variants() {
    assert_eq!(DataType::from_sql("BOOL"), DataType::Boolean);
    assert_eq!(DataType::from_sql("boolean"), DataType::Boolean);
}

#[test]
fn data_type_from_sql_mysql_type_variants() {
    assert_eq!(
        DataType::from_sql("TINYINT UNSIGNED"),
        DataType::MySqlInt {
            kind: MySqlIntKind::Tiny,
            unsigned: true,
        }
    );
    assert_eq!(
        DataType::from_sql("MEDIUMINT"),
        DataType::MySqlInt {
            kind: MySqlIntKind::Medium,
            unsigned: false,
        }
    );
    assert_eq!(DataType::from_sql("BIT(8)"), DataType::Bit(Some(8)));
    assert_eq!(DataType::from_sql("YEAR"), DataType::Year);
    assert_eq!(
        DataType::from_sql("ENUM('new','paid')"),
        DataType::Enum(vec!["new".into(), "paid".into()])
    );
    assert_eq!(
        DataType::from_sql("SET('red','blue')"),
        DataType::Set(vec!["red".into(), "blue".into()])
    );
    assert_eq!(DataType::from_sql("BINARY(4)"), DataType::Binary(Some(4)));
    assert_eq!(
        DataType::from_sql("VARBINARY(12)"),
        DataType::VarBinary(Some(12))
    );
    assert_eq!(
        DataType::from_sql("DATETIME(6)"),
        DataType::MySqlDateTime { fsp: Some(6) }
    );
}

#[test]
fn data_type_from_sql_text_fallback() {
    assert_eq!(DataType::from_sql("TEXT"), DataType::Text);
    assert_eq!(DataType::from_sql("VARCHAR"), DataType::VarChar(None));
    assert_eq!(
        DataType::from_sql("VARCHAR(12)"),
        DataType::VarChar(Some(12))
    );
    assert_eq!(DataType::from_sql("CHAR"), DataType::Char(Some(1)));
    assert_eq!(
        DataType::from_sql("unknown_type"),
        DataType::Domain("unknown_type".into())
    );
}

#[test]
fn data_type_from_sql_phase2_and_arrays() {
    assert_eq!(DataType::from_sql("REAL"), DataType::Float32);
    assert_eq!(DataType::from_sql("DOUBLE PRECISION"), DataType::Float64);
    assert_eq!(
        DataType::from_sql("NUMERIC(10,2)"),
        DataType::Numeric {
            precision: Some(10),
            scale: Some(2)
        }
    );
    assert_eq!(DataType::from_sql("DATE"), DataType::Date);
    assert_eq!(DataType::from_sql("TIMESTAMP"), DataType::Timestamp);
    assert_eq!(
        DataType::from_sql("TIMESTAMP WITH TIME ZONE"),
        DataType::TimestampTz
    );
    assert_eq!(DataType::from_sql("UUID"), DataType::Uuid);
    assert_eq!(DataType::from_sql("BYTEA"), DataType::Bytea);
    assert_eq!(DataType::from_sql("JSONB"), DataType::Jsonb);
    assert_eq!(
        DataType::from_sql("INT[]"),
        DataType::Array(Box::new(DataType::Int32))
    );
}

#[test]
fn data_type_to_pg_type() {
    assert_eq!(DataType::Int32.to_pg_type(), pgwire::api::Type::INT4);
    assert_eq!(DataType::Int64.to_pg_type(), pgwire::api::Type::INT8);
    assert_eq!(DataType::Float32.to_pg_type(), pgwire::api::Type::FLOAT4);
    assert_eq!(DataType::Float64.to_pg_type(), pgwire::api::Type::FLOAT8);
    assert_eq!(
        DataType::Numeric {
            precision: None,
            scale: None
        }
        .to_pg_type(),
        pgwire::api::Type::NUMERIC
    );
    assert_eq!(DataType::Text.to_pg_type(), pgwire::api::Type::TEXT);
    assert_eq!(DataType::Boolean.to_pg_type(), pgwire::api::Type::BOOL);
    assert_eq!(DataType::Jsonb.to_pg_type(), pgwire::api::Type::JSONB);
    assert_eq!(
        DataType::Array(Box::new(DataType::Int32)).to_pg_type(),
        pgwire::api::Type::INT4_ARRAY
    );
}

#[test]
fn data_type_roundtrip_str() {
    for dt in [
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Numeric {
            precision: None,
            scale: None,
        },
        DataType::Text,
        DataType::Boolean,
        DataType::Date,
        DataType::Timestamp,
        DataType::TimestampTz,
        DataType::Uuid,
        DataType::Bytea,
        DataType::Json,
        DataType::Jsonb,
        DataType::Array(Box::new(DataType::Int32)),
    ] {
        assert_eq!(DataType::from_sql(&dt.to_str()), dt);
    }
}

#[test]
fn column_value_to_text() {
    assert_eq!(ColumnValue::Null.to_text(), None);
    assert_eq!(ColumnValue::Int32(42).to_text(), Some("42".into()));
    assert_eq!(ColumnValue::Int64(-100).to_text(), Some("-100".into()));
    assert_eq!(ColumnValue::Float64(1.5).to_text(), Some("1.5".into()));
    assert_eq!(
        ColumnValue::Bytea(vec![0xde, 0xad]).to_text(),
        Some("\\xdead".into())
    );
    assert_eq!(ColumnValue::Text("hi".into()).to_text(), Some("hi".into()));
    assert_eq!(ColumnValue::Boolean(true).to_text(), Some("true".into()));
}

#[test]
fn column_value_is_null() {
    assert!(ColumnValue::Null.is_null());
    assert!(!ColumnValue::Int32(0).is_null());
    assert!(!ColumnValue::Text("".into()).is_null());
}

#[test]
fn column_value_ordering_int32() {
    assert!(ColumnValue::Int32(-1) < ColumnValue::Int32(0));
    assert!(ColumnValue::Int32(0) < ColumnValue::Int32(1));
    assert!(ColumnValue::Int32(2) < ColumnValue::Int32(10));
    assert_eq!(
        ColumnValue::Int32(5).partial_cmp(&ColumnValue::Int32(5)),
        Some(Ordering::Equal)
    );
}

#[test]
fn column_value_ordering_cross_int() {
    assert!(ColumnValue::Int32(1) < ColumnValue::Int64(2));
    assert!(ColumnValue::Int64(1) < ColumnValue::Int32(2));
    assert_eq!(
        ColumnValue::Int32(42).partial_cmp(&ColumnValue::Int64(42)),
        Some(Ordering::Equal)
    );
}

#[test]
fn column_value_ordering_text() {
    assert!(ColumnValue::Text("a".into()) < ColumnValue::Text("b".into()));
    assert!(ColumnValue::Text("abc".into()) < ColumnValue::Text("abd".into()));
}

#[test]
fn column_value_ordering_incompatible_returns_none() {
    assert_eq!(
        ColumnValue::Int32(1).partial_cmp(&ColumnValue::Text("1".into())),
        None
    );
    assert_eq!(
        ColumnValue::Boolean(true).partial_cmp(&ColumnValue::Int32(1)),
        None
    );
}

#[test]
fn numeric_ordering_uses_decimal_precision() {
    assert!(
        ColumnValue::Numeric("9007199254740993".into())
            > ColumnValue::Numeric("9007199254740992".into())
    );
    assert!(
        ColumnValue::Numeric("0.00000000000000000000000000000000000002".into())
            > ColumnValue::Numeric("0.00000000000000000000000000000000000001".into())
    );
    assert_eq!(
        ColumnValue::Numeric("1.2300".into()).partial_cmp(&ColumnValue::Numeric("1.23".into())),
        Some(Ordering::Equal)
    );
    assert!(ColumnValue::Numeric("-1.24".into()) < ColumnValue::Numeric("-1.23".into()));
    assert!(ColumnValue::Numeric("1.2e3".into()) > ColumnValue::Numeric("1199.99".into()));
}

fn test_schema() -> TableSchema {
    TableSchema {
        table_name: "users".into(),
        table_id: 1,
        schema_version: 1,
        table_epoch: 1,
        primary_key: "id".into(),
        check_constraints: Vec::new(),
        unique_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        columns: vec![
            ColumnSchema {
                column_id: 0,
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
                column_id: 0,
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
fn schema_pk_data_type() {
    let schema = test_schema();
    assert_eq!(schema.pk_data_type(), &DataType::Int32);
}

#[test]
fn schema_find_column() {
    let schema = test_schema();
    assert!(schema.find_column("id").unwrap().primary_key);
    assert!(schema.find_column("name").is_some());
    assert!(schema.find_column("missing").is_none());
}

#[test]
fn schema_column_names() {
    let schema = test_schema();
    assert_eq!(schema.column_names(), vec!["id", "name"]);
}

#[test]
fn apply_assignments_overwrites_existing() {
    let mut row = RowMap::new();
    row.insert("name".into(), ColumnValue::Text("old".into()));

    apply_assignments(
        &mut row,
        &[("name".into(), ColumnValue::Text("new".into()))],
    );

    assert_eq!(row.get("name"), Some(&ColumnValue::Text("new".into())));
}

#[test]
fn apply_assignments_adds_new_column() {
    let mut row = RowMap::new();
    apply_assignments(&mut row, &[("age".into(), ColumnValue::Int32(25))]);
    assert_eq!(row.get("age"), Some(&ColumnValue::Int32(25)));
}

#[test]
fn parse_column_schema_full() {
    let json = serde_json::json!({
        "name": "age",
        "data_type": "INT4",
        "primary_key": false,
        "nullable": true,
    });
    let col = parse_column_schema(&json).unwrap();
    assert_eq!(col.name, "age");
    assert_eq!(col.data_type, DataType::Int32);
    assert!(!col.primary_key);
    assert!(col.nullable);
}

#[test]
fn parse_column_schema_defaults() {
    let json = serde_json::json!({ "name": "x" });
    let col = parse_column_schema(&json).unwrap();
    assert_eq!(col.data_type, DataType::Text);
    assert!(!col.primary_key);
    assert!(col.nullable);
}

#[test]
fn parse_column_schema_missing_name_errors() {
    let json = serde_json::json!({ "data_type": "INT4" });
    assert!(parse_column_schema(&json).is_err());
}
