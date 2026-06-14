use super::*;
use crate::types::ColumnSchema;

fn make_expr_number(n: &str) -> Expr {
    Expr::Value(SqlValue::Number(n.into(), false).into())
}

fn make_expr_string(s: &str) -> Expr {
    Expr::Value(SqlValue::SingleQuotedString(s.into()).into())
}

fn make_expr_bool(b: bool) -> Expr {
    Expr::Value(SqlValue::Boolean(b).into())
}

fn make_expr_null() -> Expr {
    Expr::Value(SqlValue::Null.into())
}

#[test]
fn mysql_numeric_truthiness_and_null_safe_compare() {
    assert_eq!(mysql_text_to_number("  -12.5abc"), -12.5);
    assert_eq!(mysql_text_to_number("abc"), 0.0);
    assert!(compare_row_values(
        &ColumnValue::Text("42x".into()),
        &ColumnValue::Int32(42),
        &BinaryOperator::Eq,
    ));
    assert!(!compare_row_values(
        &ColumnValue::Null,
        &ColumnValue::Null,
        &BinaryOperator::Eq,
    ));
    assert!(compare_row_values(
        &ColumnValue::Null,
        &ColumnValue::Null,
        &BinaryOperator::Spaceship,
    ));
}

// ── sql_expr_to_column_value ──────────────────────────────────────

#[test]
fn convert_number_to_int32() {
    let v = sql_expr_to_column_value(&make_expr_number("42"), &DataType::Int32).unwrap();
    assert_eq!(v, ColumnValue::Int32(42));
}

#[test]
fn convert_number_to_int64() {
    let v = sql_expr_to_column_value(&make_expr_number("9999999999"), &DataType::Int64).unwrap();
    assert_eq!(v, ColumnValue::Int64(9_999_999_999));
}

#[test]
fn convert_number_to_text() {
    let v = sql_expr_to_column_value(&make_expr_number("42"), &DataType::Text).unwrap();
    assert_eq!(v, ColumnValue::Text("42".into()));
}

#[test]
fn convert_string_to_text() {
    let v = sql_expr_to_column_value(&make_expr_string("hello"), &DataType::Text).unwrap();
    assert_eq!(v, ColumnValue::Text("hello".into()));
}

#[test]
fn convert_string_to_int32() {
    let v = sql_expr_to_column_value(&make_expr_string("123"), &DataType::Int32).unwrap();
    assert_eq!(v, ColumnValue::Int32(123));
}

#[test]
fn convert_string_to_int32_error() {
    assert!(sql_expr_to_column_value(&make_expr_string("abc"), &DataType::Int32).is_err());
}

#[test]
fn convert_string_to_bool_variants() {
    for (input, expected) in [
        ("true", true),
        ("t", true),
        ("1", true),
        ("yes", true),
        ("on", true),
        ("false", false),
        ("f", false),
        ("0", false),
        ("no", false),
        ("off", false),
    ] {
        let v = sql_expr_to_column_value(&make_expr_string(input), &DataType::Boolean).unwrap();
        assert_eq!(v, ColumnValue::Boolean(expected), "input: {input}");
    }
}

#[test]
fn convert_string_to_bool_error() {
    assert!(sql_expr_to_column_value(&make_expr_string("maybe"), &DataType::Boolean).is_err());
}

#[test]
fn convert_bool_literal() {
    let v = sql_expr_to_column_value(&make_expr_bool(true), &DataType::Boolean).unwrap();
    assert_eq!(v, ColumnValue::Boolean(true));
}

#[test]
fn convert_null() {
    let v = sql_expr_to_column_value(&make_expr_null(), &DataType::Text).unwrap();
    assert_eq!(v, ColumnValue::Null);
}

#[test]
fn convert_negative_number() {
    let expr = Expr::UnaryOp {
        op: sqlparser::ast::UnaryOperator::Minus,
        expr: Box::new(make_expr_number("42")),
    };
    let v = sql_expr_to_column_value(&expr, &DataType::Int32).unwrap();
    assert_eq!(v, ColumnValue::Int32(-42));
}

#[test]
fn convert_casted_string_to_int32() {
    let expr = Expr::Cast {
        expr: Box::new(make_expr_string("42")),
        data_type: sqlparser::ast::DataType::Int(None),
        kind: sqlparser::ast::CastKind::Cast,
        array: false,
        format: None,
    };
    let v = sql_expr_to_column_value(&expr, &DataType::Int32).unwrap();
    assert_eq!(v, ColumnValue::Int32(42));
}

#[test]
fn convert_typed_string_bool() {
    let expr = Expr::TypedString(sqlparser::ast::TypedString {
        data_type: sqlparser::ast::DataType::Boolean,
        value: SqlValue::SingleQuotedString("true".into()).into(),
        uses_odbc_syntax: false,
    });
    let v = sql_expr_to_column_value(&expr, &DataType::Boolean).unwrap();
    assert_eq!(v, ColumnValue::Boolean(true));
}

#[test]
fn convert_int32_overflow_errors() {
    let big = format!("{}", i32::MAX as i64 + 1);
    assert!(sql_expr_to_column_value(&make_expr_number(&big), &DataType::Int32).is_err());
}

#[test]
fn convert_mysql_unsigned_bit_and_year_bounds() {
    let tiny_unsigned = DataType::MySqlInt {
        kind: MySqlIntKind::Tiny,
        unsigned: true,
    };
    assert_eq!(
        sql_expr_to_column_value(&make_expr_number("255"), &tiny_unsigned).unwrap(),
        ColumnValue::Int32(255)
    );
    assert!(sql_expr_to_column_value(&make_expr_number("256"), &tiny_unsigned).is_err());
    assert!(sql_expr_to_column_value(&make_expr_number("-1"), &tiny_unsigned).is_err());

    assert_eq!(
        sql_expr_to_column_value(&make_expr_number("255"), &DataType::Bit(Some(8))).unwrap(),
        ColumnValue::Int64(255)
    );
    assert!(sql_expr_to_column_value(&make_expr_number("256"), &DataType::Bit(Some(8))).is_err());

    assert_eq!(
        sql_expr_to_column_value(&make_expr_number("2026"), &DataType::Year).unwrap(),
        ColumnValue::Int32(2026)
    );
    assert!(sql_expr_to_column_value(&make_expr_number("10000"), &DataType::Year).is_err());
}

// ── build_insert_row ──────────────────────────────────────────────

fn test_schema() -> TableSchema {
    TableSchema {
        table_name: "t".into(),
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
fn build_insert_row_with_explicit_columns() {
    let schema = test_schema();
    let columns = vec![Ident::new("id"), Ident::new("name")];
    let values = vec![make_expr_number("1"), make_expr_string("alice")];
    let row = build_insert_row(&schema, &columns, values).unwrap();

    assert_eq!(row.get("id"), Some(&ColumnValue::Int32(1)));
    assert_eq!(row.get("name"), Some(&ColumnValue::Text("alice".into())));
}

#[test]
fn build_insert_row_implicit_columns() {
    let schema = test_schema();
    let values = vec![make_expr_number("1"), make_expr_string("bob")];
    let row = build_insert_row(&schema, &[], values).unwrap();

    assert_eq!(row.get("id"), Some(&ColumnValue::Int32(1)));
    assert_eq!(row.get("name"), Some(&ColumnValue::Text("bob".into())));
}

#[test]
fn build_insert_row_column_count_mismatch_errors() {
    let schema = test_schema();
    let values = vec![make_expr_number("1")]; // only 1 value, 2 columns
    assert!(build_insert_row(&schema, &[], values).is_err());
}

#[test]
fn build_insert_row_unknown_column_errors() {
    let schema = test_schema();
    let columns = vec![Ident::new("id"), Ident::new("nonexistent")];
    let values = vec![make_expr_number("1"), make_expr_string("x")];
    assert!(build_insert_row(&schema, &columns, values).is_err());
}

// ── extract_primary_key_filter ────────────────────────────────────

#[test]
fn pk_filter_none_when_no_selection() {
    let schema = test_schema();
    let result = extract_primary_key_filter(None, &schema).unwrap();
    assert!(result.is_none());
}

#[test]
fn pk_filter_matches_eq_on_pk() {
    let schema = test_schema();
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Identifier(Ident::new("id"))),
        op: BinaryOperator::Eq,
        right: Box::new(make_expr_number("42")),
    };
    let result = extract_primary_key_filter(Some(&expr), &schema).unwrap();
    assert_eq!(result, Some(ColumnValue::Int32(42)));
}

#[test]
fn pk_filter_ignores_non_pk_column() {
    let schema = test_schema();
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Identifier(Ident::new("name"))),
        op: BinaryOperator::Eq,
        right: Box::new(make_expr_string("alice")),
    };
    let result = extract_primary_key_filter(Some(&expr), &schema).unwrap();
    assert!(result.is_none());
}

#[test]
fn pk_filter_unwraps_nested() {
    let schema = test_schema();
    let inner = Expr::BinaryOp {
        left: Box::new(Expr::Identifier(Ident::new("id"))),
        op: BinaryOperator::Eq,
        right: Box::new(make_expr_number("7")),
    };
    let expr = Expr::Nested(Box::new(inner));
    let result = extract_primary_key_filter(Some(&expr), &schema).unwrap();
    assert_eq!(result, Some(ColumnValue::Int32(7)));
}

// ── expr_identifier_name ──────────────────────────────────────────

#[test]
fn identifier_simple() {
    let expr = Expr::Identifier(Ident::new("col1"));
    assert_eq!(expr_identifier_name(&expr).unwrap(), "col1");
}

#[test]
fn identifier_compound() {
    let expr = Expr::CompoundIdentifier(vec![Ident::new("t"), Ident::new("col1")]);
    assert_eq!(expr_identifier_name(&expr).unwrap(), "col1");
}

#[test]
fn identifier_unsupported_errors() {
    let expr = make_expr_number("42");
    assert!(expr_identifier_name(&expr).is_err());
}

#[test]
fn identifier_name_unwraps_cast() {
    let expr = Expr::Cast {
        expr: Box::new(Expr::Identifier(Ident::new("id"))),
        data_type: sqlparser::ast::DataType::Text,
        kind: sqlparser::ast::CastKind::Cast,
        array: false,
        format: None,
    };
    assert_eq!(expr_identifier_name(&expr).unwrap(), "id");
}

#[test]
fn fast_path_filter_allows_casted_literal() {
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Identifier(Ident::new("id"))),
        op: BinaryOperator::Eq,
        right: Box::new(Expr::Cast {
            expr: Box::new(make_expr_string("1")),
            data_type: sqlparser::ast::DataType::Int(None),
            kind: sqlparser::ast::CastKind::Cast,
            array: false,
            format: None,
        }),
    };
    assert!(supports_fast_path_filter(&expr));
}

#[test]
fn column_range_filter_handles_between_and_bound_merging() {
    let schema = test_schema();
    let between = Expr::Between {
        expr: Box::new(Expr::Identifier(Ident::new("id"))),
        negated: false,
        low: Box::new(make_expr_number("10")),
        high: Box::new(make_expr_number("20")),
    };
    assert_eq!(
        extract_primary_key_range_filter(Some(&between), &schema).unwrap(),
        Some((
            Some((ColumnValue::Int32(10), true)),
            Some((ColumnValue::Int32(20), true))
        ))
    );

    let merged = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("id"))),
            op: BinaryOperator::Gt,
            right: Box::new(make_expr_number("1")),
        }),
        op: BinaryOperator::And,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("id"))),
            op: BinaryOperator::GtEq,
            right: Box::new(make_expr_number("5")),
        }),
    };
    assert_eq!(
        extract_primary_key_range_filter(Some(&merged), &schema).unwrap(),
        Some((Some((ColumnValue::Int32(5), true)), None))
    );
}

#[test]
fn evaluate_row_expression_covers_arithmetic_bool_and_division_errors() {
    let schema = test_schema();
    let mut row = RowMap::new();
    row.insert("id".into(), ColumnValue::Int32(9));
    row.insert("name".into(), ColumnValue::Text("alice".into()));
    let ctx = EvalContext {
        row: &row,
        excluded_row: None,
    };

    let div = Expr::BinaryOp {
        left: Box::new(Expr::Identifier(Ident::new("id"))),
        op: BinaryOperator::MyIntegerDivide,
        right: Box::new(make_expr_number("2")),
    };
    assert_eq!(
        evaluate_row_expression(&div, &schema, &ctx).unwrap(),
        ColumnValue::Int64(4)
    );

    let xor = Expr::BinaryOp {
        left: Box::new(make_expr_bool(true)),
        op: BinaryOperator::Xor,
        right: Box::new(make_expr_bool(false)),
    };
    assert_eq!(
        evaluate_row_expression(&xor, &schema, &ctx).unwrap(),
        ColumnValue::Boolean(true)
    );

    let div_zero = Expr::BinaryOp {
        left: Box::new(Expr::Identifier(Ident::new("id"))),
        op: BinaryOperator::Divide,
        right: Box::new(make_expr_number("0")),
    };
    assert!(evaluate_row_expression(&div_zero, &schema, &ctx).is_err());
}
