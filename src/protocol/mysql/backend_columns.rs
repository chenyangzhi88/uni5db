use opensrv_mysql::{Column, ColumnFlags, ColumnType};
use pgwire::api::Type;
use pgwire::api::results::FieldInfo;
use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement};

use crate::core::server::MySqlColumnMetadata;
use crate::dialect::parser::{self, SqlDialect};

use super::{MYSQL_CHARSET_BINARY, MYSQL_CHARSET_UTF8_GENERAL_CI, MySqlBackend};

impl MySqlBackend {
    pub(super) fn text_column(name: &str) -> Column {
        Column {
            table: String::new(),
            column: name.to_string(),
            collen: 65_535,
            charset: Some(MYSQL_CHARSET_UTF8_GENERAL_CI),
            coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
            colflags: ColumnFlags::empty(),
            decimals: Some(0),
        }
    }

    pub(super) fn int_column(name: &str) -> Column {
        Column {
            table: String::new(),
            column: name.to_string(),
            collen: 11,
            charset: Some(MYSQL_CHARSET_BINARY),
            coltype: ColumnType::MYSQL_TYPE_LONG,
            colflags: ColumnFlags::NOT_NULL_FLAG | ColumnFlags::NUM_FLAG,
            decimals: Some(0),
        }
    }

    pub(super) fn mysql_column(field: &FieldInfo) -> Column {
        Column {
            table: String::new(),
            column: field.name().to_string(),
            collen: Self::mysql_column_length(field.datatype()),
            charset: Some(Self::mysql_column_charset(field.datatype())),
            coltype: Self::mysql_column_type(field.datatype()),
            colflags: Self::mysql_column_flags(field.datatype()),
            decimals: Some(Self::mysql_column_decimals(field.datatype())),
        }
    }

    pub(super) fn mysql_column_from_metadata(
        table: &str,
        metadata: &MySqlColumnMetadata,
    ) -> Column {
        let (coltype, collen, charset, mut colflags, decimals) =
            Self::mysql_type_from_metadata(&metadata.column_type);
        if metadata.nullable.eq_ignore_ascii_case("NO") {
            colflags.insert(ColumnFlags::NOT_NULL_FLAG);
        }
        match metadata.key.to_ascii_uppercase().as_str() {
            "PRI" => colflags.insert(ColumnFlags::PRI_KEY_FLAG),
            "UNI" => colflags.insert(ColumnFlags::UNIQUE_KEY_FLAG),
            "MUL" => colflags.insert(ColumnFlags::MULTIPLE_KEY_FLAG),
            _ => {}
        }
        if metadata
            .extra
            .split_whitespace()
            .any(|part| part.eq_ignore_ascii_case("auto_increment"))
        {
            colflags.insert(ColumnFlags::AUTO_INCREMENT_FLAG);
        }
        Column {
            table: table.to_string(),
            column: metadata.field.clone(),
            collen,
            charset: Some(charset),
            coltype,
            colflags,
            decimals: Some(decimals),
        }
    }

    pub(super) fn mysql_type_from_metadata(
        column_type: &str,
    ) -> (ColumnType, u32, u16, ColumnFlags, u8) {
        let lower = column_type.to_ascii_lowercase();
        if lower.starts_with("tinyint") {
            (
                ColumnType::MYSQL_TYPE_TINY,
                4,
                MYSQL_CHARSET_BINARY,
                ColumnFlags::NUM_FLAG,
                0,
            )
        } else if lower.starts_with("smallint") {
            (
                ColumnType::MYSQL_TYPE_SHORT,
                6,
                MYSQL_CHARSET_BINARY,
                ColumnFlags::NUM_FLAG,
                0,
            )
        } else if lower.starts_with("mediumint") {
            (
                ColumnType::MYSQL_TYPE_INT24,
                9,
                MYSQL_CHARSET_BINARY,
                ColumnFlags::NUM_FLAG,
                0,
            )
        } else if lower.starts_with("int") || lower.starts_with("integer") {
            (
                ColumnType::MYSQL_TYPE_LONG,
                11,
                MYSQL_CHARSET_BINARY,
                ColumnFlags::NUM_FLAG,
                0,
            )
        } else if lower.starts_with("bigint") {
            (
                ColumnType::MYSQL_TYPE_LONGLONG,
                20,
                MYSQL_CHARSET_BINARY,
                ColumnFlags::NUM_FLAG,
                0,
            )
        } else if lower.starts_with("float") {
            (
                ColumnType::MYSQL_TYPE_FLOAT,
                12,
                MYSQL_CHARSET_BINARY,
                ColumnFlags::NUM_FLAG,
                31,
            )
        } else if lower.starts_with("double") {
            (
                ColumnType::MYSQL_TYPE_DOUBLE,
                22,
                MYSQL_CHARSET_BINARY,
                ColumnFlags::NUM_FLAG,
                31,
            )
        } else if lower.starts_with("decimal") {
            (
                ColumnType::MYSQL_TYPE_NEWDECIMAL,
                65,
                MYSQL_CHARSET_BINARY,
                ColumnFlags::NUM_FLAG,
                0,
            )
        } else if lower.starts_with("date") {
            (
                ColumnType::MYSQL_TYPE_DATE,
                10,
                MYSQL_CHARSET_BINARY,
                ColumnFlags::empty(),
                0,
            )
        } else if lower.starts_with("datetime") || lower.starts_with("timestamp") {
            (
                ColumnType::MYSQL_TYPE_DATETIME,
                26,
                MYSQL_CHARSET_BINARY,
                ColumnFlags::TIMESTAMP_FLAG,
                6,
            )
        } else if lower.contains("blob")
            || lower.starts_with("binary")
            || lower.starts_with("varbinary")
        {
            (
                ColumnType::MYSQL_TYPE_BLOB,
                65_535,
                MYSQL_CHARSET_BINARY,
                ColumnFlags::BLOB_FLAG | ColumnFlags::BINARY_FLAG,
                0,
            )
        } else {
            (
                ColumnType::MYSQL_TYPE_VAR_STRING,
                65_535,
                MYSQL_CHARSET_UTF8_GENERAL_CI,
                ColumnFlags::empty(),
                0,
            )
        }
    }

    pub(super) fn mysql_list_fields_table(payload: &[u8]) -> String {
        let table = payload.split(|byte| *byte == 0).next().unwrap_or_default();
        String::from_utf8_lossy(table)
            .trim()
            .trim_matches('`')
            .to_string()
    }

    pub(super) fn mysql_param_column(idx: usize) -> Column {
        Column {
            table: String::new(),
            column: format!("?{}", idx + 1),
            collen: 65_535,
            charset: Some(MYSQL_CHARSET_UTF8_GENERAL_CI),
            coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
            colflags: ColumnFlags::empty(),
            decimals: Some(0),
        }
    }

    pub(super) fn mysql_column_type(data_type: &Type) -> ColumnType {
        if data_type == &Type::BOOL {
            ColumnType::MYSQL_TYPE_TINY
        } else if data_type == &Type::INT2 {
            ColumnType::MYSQL_TYPE_SHORT
        } else if data_type == &Type::INT4 {
            ColumnType::MYSQL_TYPE_LONG
        } else if data_type == &Type::INT8 {
            ColumnType::MYSQL_TYPE_LONGLONG
        } else if data_type == &Type::FLOAT4 {
            ColumnType::MYSQL_TYPE_FLOAT
        } else if data_type == &Type::FLOAT8 {
            ColumnType::MYSQL_TYPE_DOUBLE
        } else if data_type == &Type::BYTEA {
            ColumnType::MYSQL_TYPE_BLOB
        } else if data_type == &Type::DATE {
            ColumnType::MYSQL_TYPE_DATE
        } else if data_type == &Type::TIMESTAMP || data_type == &Type::TIMESTAMPTZ {
            ColumnType::MYSQL_TYPE_DATETIME
        } else if data_type == &Type::NUMERIC {
            ColumnType::MYSQL_TYPE_NEWDECIMAL
        } else {
            ColumnType::MYSQL_TYPE_VAR_STRING
        }
    }

    pub(super) fn mysql_column_length(data_type: &Type) -> u32 {
        if data_type == &Type::BOOL {
            1
        } else if data_type == &Type::INT2 {
            6
        } else if data_type == &Type::INT4 {
            11
        } else if data_type == &Type::INT8 {
            20
        } else if data_type == &Type::FLOAT4 {
            12
        } else if data_type == &Type::FLOAT8 {
            22
        } else if data_type == &Type::NUMERIC {
            65
        } else if data_type == &Type::DATE {
            10
        } else if data_type == &Type::TIMESTAMP || data_type == &Type::TIMESTAMPTZ {
            26
        } else {
            65_535
        }
    }

    pub(super) fn mysql_column_flags(data_type: &Type) -> ColumnFlags {
        if [
            Type::BOOL,
            Type::INT2,
            Type::INT4,
            Type::INT8,
            Type::FLOAT4,
            Type::FLOAT8,
            Type::NUMERIC,
        ]
        .contains(data_type)
        {
            ColumnFlags::NUM_FLAG
        } else if data_type == &Type::BYTEA {
            ColumnFlags::BLOB_FLAG | ColumnFlags::BINARY_FLAG
        } else if data_type == &Type::TIMESTAMP || data_type == &Type::TIMESTAMPTZ {
            ColumnFlags::TIMESTAMP_FLAG
        } else {
            ColumnFlags::empty()
        }
    }

    pub(super) fn mysql_column_charset(data_type: &Type) -> u16 {
        if data_type == &Type::BYTEA
            || data_type == &Type::BOOL
            || data_type == &Type::INT2
            || data_type == &Type::INT4
            || data_type == &Type::INT8
            || data_type == &Type::FLOAT4
            || data_type == &Type::FLOAT8
            || data_type == &Type::NUMERIC
            || data_type == &Type::DATE
            || data_type == &Type::TIMESTAMP
            || data_type == &Type::TIMESTAMPTZ
        {
            MYSQL_CHARSET_BINARY
        } else {
            MYSQL_CHARSET_UTF8_GENERAL_CI
        }
    }

    pub(super) fn mysql_column_decimals(data_type: &Type) -> u8 {
        if data_type == &Type::FLOAT4 || data_type == &Type::FLOAT8 {
            31
        } else if data_type == &Type::TIMESTAMP || data_type == &Type::TIMESTAMPTZ {
            6
        } else {
            0
        }
    }

    pub(super) fn prepared_result_columns(query: &str) -> Vec<Column> {
        let Ok(statements) = parser::parse_sql(SqlDialect::MySql, query) else {
            return Vec::new();
        };
        let Some(Statement::Query(query)) = statements.into_iter().next() else {
            return Vec::new();
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            return Vec::new();
        };
        select
            .projection
            .iter()
            .filter_map(Self::prepared_select_item_column)
            .collect()
    }

    pub(super) fn prepared_select_item_column(item: &SelectItem) -> Option<Column> {
        let (name, data_type) = match item {
            SelectItem::UnnamedExpr(expr) => (
                Self::prepared_expr_name(expr),
                Self::prepared_expr_mysql_type(expr),
            ),
            SelectItem::ExprWithAlias { expr, alias } => {
                (alias.value.clone(), Self::prepared_expr_mysql_type(expr))
            }
            SelectItem::ExprWithAliases { expr, aliases } => (
                aliases
                    .first()
                    .map(|alias| alias.value.clone())
                    .unwrap_or_else(|| Self::prepared_expr_name(expr)),
                Self::prepared_expr_mysql_type(expr),
            ),
            SelectItem::QualifiedWildcard(prefix, _) => {
                (format!("{prefix}.*"), ColumnType::MYSQL_TYPE_VAR_STRING)
            }
            SelectItem::Wildcard(_) => ("*".to_string(), ColumnType::MYSQL_TYPE_VAR_STRING),
        };
        Some(Column {
            table: String::new(),
            column: name,
            collen: 65_535,
            charset: Some(Self::mysql_prepared_column_charset(data_type)),
            coltype: data_type,
            colflags: match data_type {
                ColumnType::MYSQL_TYPE_TINY
                | ColumnType::MYSQL_TYPE_SHORT
                | ColumnType::MYSQL_TYPE_LONG
                | ColumnType::MYSQL_TYPE_LONGLONG
                | ColumnType::MYSQL_TYPE_FLOAT
                | ColumnType::MYSQL_TYPE_DOUBLE
                | ColumnType::MYSQL_TYPE_NEWDECIMAL => ColumnFlags::NUM_FLAG,
                _ => ColumnFlags::empty(),
            },
            decimals: Some(Self::mysql_prepared_column_decimals(data_type)),
        })
    }

    pub(super) fn mysql_prepared_column_charset(data_type: ColumnType) -> u16 {
        match data_type {
            ColumnType::MYSQL_TYPE_STRING
            | ColumnType::MYSQL_TYPE_VAR_STRING
            | ColumnType::MYSQL_TYPE_VARCHAR
            | ColumnType::MYSQL_TYPE_ENUM
            | ColumnType::MYSQL_TYPE_SET
            | ColumnType::MYSQL_TYPE_JSON => MYSQL_CHARSET_UTF8_GENERAL_CI,
            _ => MYSQL_CHARSET_BINARY,
        }
    }

    pub(super) fn mysql_prepared_column_decimals(data_type: ColumnType) -> u8 {
        match data_type {
            ColumnType::MYSQL_TYPE_FLOAT | ColumnType::MYSQL_TYPE_DOUBLE => 31,
            ColumnType::MYSQL_TYPE_TIMESTAMP | ColumnType::MYSQL_TYPE_DATETIME => 6,
            _ => 0,
        }
    }

    pub(super) fn prepared_expr_name(expr: &Expr) -> String {
        match expr {
            Expr::Identifier(ident) => ident.value.clone(),
            Expr::CompoundIdentifier(parts) => parts
                .last()
                .map(|ident| ident.value.clone())
                .unwrap_or_else(|| expr.to_string()),
            Expr::Value(value) => value.to_string(),
            Expr::Function(function) => function.name.to_string(),
            _ => expr.to_string(),
        }
    }

    pub(super) fn prepared_expr_mysql_type(expr: &Expr) -> ColumnType {
        match expr {
            Expr::Value(value) => match &value.value {
                sqlparser::ast::Value::Number(_, _) => ColumnType::MYSQL_TYPE_NEWDECIMAL,
                sqlparser::ast::Value::Boolean(_) => ColumnType::MYSQL_TYPE_TINY,
                sqlparser::ast::Value::Null => ColumnType::MYSQL_TYPE_NULL,
                _ => ColumnType::MYSQL_TYPE_VAR_STRING,
            },
            Expr::Cast { data_type, .. } => {
                let type_name = data_type.to_string().to_ascii_lowercase();
                if type_name.contains("bigint") {
                    ColumnType::MYSQL_TYPE_LONGLONG
                } else if type_name.contains("int")
                    || type_name.contains("year")
                    || type_name.contains("signed")
                    || type_name.contains("unsigned")
                {
                    ColumnType::MYSQL_TYPE_LONG
                } else if type_name.contains("double") || type_name.contains("real") {
                    ColumnType::MYSQL_TYPE_DOUBLE
                } else if type_name.contains("float") {
                    ColumnType::MYSQL_TYPE_FLOAT
                } else if type_name.contains("decimal") || type_name.contains("numeric") {
                    ColumnType::MYSQL_TYPE_NEWDECIMAL
                } else if type_name.contains("date") || type_name.contains("time") {
                    ColumnType::MYSQL_TYPE_DATETIME
                } else {
                    ColumnType::MYSQL_TYPE_VAR_STRING
                }
            }
            _ => ColumnType::MYSQL_TYPE_VAR_STRING,
        }
    }
}
