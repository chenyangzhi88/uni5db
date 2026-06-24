use pgwire::error::PgWireResult;

use super::GatewayServer;
use crate::catalog::IndexCatalog;
use crate::error::unsupported;
use crate::types::{ColumnValue, QueryPlan, ReadAccess, TableSchema};

impl GatewayServer {
    pub(super) async fn mysql_explain_rows(
        &self,
        plan: &QueryPlan,
    ) -> PgWireResult<Vec<Vec<Option<String>>>> {
        match plan {
            QueryPlan::SelectRows {
                schema,
                access,
                limit,
                ..
            } => {
                let indexes = self.catalog.list_indexes_for_table(schema.table_id).await?;
                let (access_type, key, possible_keys, rows, extra) =
                    self.mysql_explain_access(schema, access, &indexes, *limit);
                Ok(vec![vec![
                    Some("1".to_string()),
                    Some("SIMPLE".to_string()),
                    Some(schema.table_name.clone()),
                    None,
                    Some(access_type),
                    possible_keys,
                    key,
                    None,
                    None,
                    Some(rows.to_string()),
                    Some("100.00".to_string()),
                    extra,
                ]])
            }
            _ => Err(unsupported(
                "EXPLAIN currently supports SELECT statements in MySQL mode",
            )),
        }
    }

    pub(super) async fn postgres_explain_rows(
        &self,
        plan: &QueryPlan,
    ) -> PgWireResult<Vec<Vec<Option<String>>>> {
        let QueryPlan::SelectRows {
            schema,
            access,
            limit,
            offset,
            ..
        } = plan
        else {
            return Err(unsupported(
                "EXPLAIN currently supports SELECT statements in PostgreSQL mode",
            ));
        };

        let indexes = self.catalog.list_indexes_for_table(schema.table_id).await?;
        let mut lines = match access {
            ReadAccess::PointLookup { key } => vec![
                format!(
                    "Index Scan using {}_pkey on {}",
                    schema.table_name, schema.table_name
                ),
                format!(
                    "  Index Cond: ({} = {})",
                    schema.primary_key,
                    Self::postgres_explain_value(key)
                ),
            ],
            ReadAccess::PrimaryKeyInLookup { keys } => vec![
                format!(
                    "Index Scan using {}_pkey on {}",
                    schema.table_name, schema.table_name
                ),
                format!(
                    "  Index Cond: ({} = ANY ({{{}}}))",
                    schema.primary_key,
                    keys.iter()
                        .map(Self::postgres_explain_value)
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            ],
            ReadAccess::PrimaryKeyRangeScan {
                lower,
                upper,
                filter,
            } => {
                let mut lines = vec![format!(
                    "Index Scan using {}_pkey on {}",
                    schema.table_name, schema.table_name
                )];
                if let Some(cond) =
                    Self::postgres_explain_range_condition(&schema.primary_key, lower, upper)
                {
                    lines.push(format!("  Index Cond: {cond}"));
                }
                if let Some(filter) = filter {
                    lines.push(format!("  Filter: ({filter})"));
                }
                lines
            }
            ReadAccess::SecondaryIndexLookup {
                index_name,
                column_name,
                key,
                filter,
            } => {
                let mut lines = vec![format!(
                    "Index Scan using {index_name} on {}",
                    schema.table_name
                )];
                lines.push(format!(
                    "  Index Cond: ({column_name} = {})",
                    Self::postgres_explain_value(key)
                ));
                if let Some(filter) = filter {
                    lines.push(format!("  Filter: ({filter})"));
                }
                lines
            }
            ReadAccess::SecondaryIndexRangeScan {
                index_name,
                column_name,
                lower,
                upper,
                filter,
            } => {
                let mut lines = vec![format!(
                    "Index Scan using {index_name} on {}",
                    schema.table_name
                )];
                if let Some(cond) =
                    Self::postgres_explain_range_condition(column_name, lower, upper)
                {
                    lines.push(format!("  Index Cond: {cond}"));
                }
                if let Some(filter) = filter {
                    lines.push(format!("  Filter: ({filter})"));
                }
                lines
            }
            ReadAccess::PrefixScan { filter } => {
                let possible_index = indexes.iter().find(|index| {
                    index.unique && index.column_names == vec![schema.primary_key.clone()]
                });
                let mut lines = if possible_index.is_some() && schema.has_user_primary_key() {
                    vec![format!("Seq Scan on {}", schema.table_name)]
                } else {
                    vec![format!("Seq Scan on {}", schema.table_name)]
                };
                if let Some(filter) = filter {
                    lines.push(format!("  Filter: ({filter})"));
                }
                lines
            }
        };

        if let Some(limit) = limit {
            lines.insert(0, format!("Limit: {limit}"));
        }
        if *offset > 0 {
            lines.insert(0, format!("Offset: {offset}"));
        }

        Ok(lines.into_iter().map(|line| vec![Some(line)]).collect())
    }

    pub(super) fn postgres_explain_value(value: &ColumnValue) -> String {
        match value {
            ColumnValue::Null => "NULL".to_string(),
            ColumnValue::Text(value)
            | ColumnValue::Date(value)
            | ColumnValue::Timestamp(value)
            | ColumnValue::TimestampTz(value)
            | ColumnValue::Uuid(value)
            | ColumnValue::Json(value)
            | ColumnValue::Jsonb(value)
            | ColumnValue::Numeric(value) => format!("'{}'", value.replace('\'', "''")),
            ColumnValue::Boolean(value) => value.to_string(),
            ColumnValue::Bytea(bytes) => format!("'\\x{}'", Self::postgres_explain_hex(bytes)),
            ColumnValue::Array(values) => format!(
                "{{{}}}",
                values
                    .iter()
                    .map(Self::postgres_explain_value)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            _ => value.to_text().unwrap_or_else(|| format!("{value:?}")),
        }
    }

    pub(super) fn postgres_explain_hex(bytes: &[u8]) -> String {
        let mut out = String::with_capacity(bytes.len().saturating_mul(2));
        for byte in bytes {
            use std::fmt::Write as _;
            let _ = write!(&mut out, "{byte:02x}");
        }
        out
    }

    pub(super) fn postgres_explain_range_condition(
        column_name: &str,
        lower: &Option<(ColumnValue, bool)>,
        upper: &Option<(ColumnValue, bool)>,
    ) -> Option<String> {
        let mut parts = Vec::new();
        if let Some((value, inclusive)) = lower {
            parts.push(format!(
                "({column_name} {} {})",
                if *inclusive { ">=" } else { ">" },
                Self::postgres_explain_value(value)
            ));
        }
        if let Some((value, inclusive)) = upper {
            parts.push(format!(
                "({column_name} {} {})",
                if *inclusive { "<=" } else { "<" },
                Self::postgres_explain_value(value)
            ));
        }
        (!parts.is_empty()).then(|| parts.join(" AND "))
    }

    pub(super) fn mysql_explain_access(
        &self,
        schema: &TableSchema,
        access: &ReadAccess,
        indexes: &[IndexCatalog],
        limit: Option<usize>,
    ) -> (
        String,
        Option<String>,
        Option<String>,
        usize,
        Option<String>,
    ) {
        let possible_keys = self.mysql_possible_keys(schema, indexes);
        match access {
            ReadAccess::PointLookup { .. } | ReadAccess::PrimaryKeyInLookup { keys: _ } => (
                "const".to_string(),
                Some("PRIMARY".to_string()),
                possible_keys,
                1,
                None,
            ),
            ReadAccess::PrimaryKeyRangeScan { filter, .. } => (
                "range".to_string(),
                Some("PRIMARY".to_string()),
                possible_keys,
                limit.unwrap_or(1).max(1),
                filter.as_ref().map(|_| "Using where".to_string()),
            ),
            ReadAccess::SecondaryIndexLookup {
                index_name, filter, ..
            } => (
                "ref".to_string(),
                Some(index_name.clone()),
                possible_keys,
                limit.unwrap_or(1).max(1),
                filter.as_ref().map(|_| "Using where".to_string()),
            ),
            ReadAccess::SecondaryIndexRangeScan {
                index_name, filter, ..
            } => (
                "range".to_string(),
                Some(index_name.clone()),
                possible_keys,
                limit.unwrap_or(1).max(1),
                filter.as_ref().map(|_| "Using where".to_string()),
            ),
            ReadAccess::PrefixScan { filter } => (
                "ALL".to_string(),
                None,
                possible_keys,
                limit.unwrap_or(1000).max(1),
                filter.as_ref().map(|_| "Using where".to_string()),
            ),
        }
    }

    pub(super) fn mysql_possible_keys(
        &self,
        schema: &TableSchema,
        indexes: &[IndexCatalog],
    ) -> Option<String> {
        let mut keys = Vec::new();
        if schema.has_user_primary_key() {
            keys.push("PRIMARY".to_string());
        }
        keys.extend(indexes.iter().map(|index| index.index_name.clone()));
        (!keys.is_empty()).then(|| keys.join(","))
    }
}
