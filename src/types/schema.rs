use pgwire::error::PgWireResult;
use serde_json::Value;

use crate::error::user_error;

use super::DataType;

const INTERNAL_ROWID_TYPE: DataType = DataType::Int64;

#[derive(Clone, Debug)]
pub struct ColumnSchema {
    pub column_id: u32,
    pub name: String,
    pub data_type: DataType,
    pub primary_key: bool,
    pub nullable: bool,
    pub default: Option<String>,
    pub on_update: Option<String>,
    pub character_set: Option<String>,
    pub collation: Option<String>,
}

#[derive(Clone, Debug)]
pub struct TableSchema {
    pub table_name: String,
    pub table_id: u32,
    pub schema_version: u32,
    pub table_epoch: u64,
    pub primary_key: String,
    pub check_constraints: Vec<CheckConstraintSchema>,
    pub unique_constraints: Vec<UniqueConstraintSchema>,
    pub foreign_keys: Vec<ForeignKeyConstraintSchema>,
    pub columns: Vec<ColumnSchema>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckConstraintSchema {
    pub name: String,
    pub expr: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UniqueConstraintSchema {
    pub name: String,
    pub columns: Vec<String>,
    pub primary_key: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForeignKeyConstraintSchema {
    pub name: String,
    pub columns: Vec<String>,
    pub foreign_table: String,
    pub referred_columns: Vec<String>,
}

impl TableSchema {
    pub fn normalize_descriptor(&mut self) {
        if self.schema_version == 0 {
            self.schema_version = 1;
        }
        if self.table_epoch == 0 {
            self.table_epoch = 1;
        }
        for (idx, column) in self.columns.iter_mut().enumerate() {
            if column.column_id == 0 {
                column.column_id = idx as u32 + 1;
            }
        }
    }

    pub fn pk_data_type(&self) -> &DataType {
        self.columns
            .iter()
            .find(|c| c.primary_key)
            .map(|c| &c.data_type)
            .unwrap_or(&INTERNAL_ROWID_TYPE)
    }

    pub fn find_column(&self, name: &str) -> Option<&ColumnSchema> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn find_column_by_id(&self, column_id: u32) -> Option<&ColumnSchema> {
        self.columns.iter().find(|c| c.column_id == column_id)
    }

    pub fn column_names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }

    pub fn has_user_primary_key(&self) -> bool {
        self.columns.iter().any(|c| c.primary_key)
    }
}

pub fn parse_column_schema(value: &Value) -> PgWireResult<ColumnSchema> {
    Ok(ColumnSchema {
        column_id: value.get("column_id").and_then(Value::as_u64).unwrap_or(0) as u32,
        name: value
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| user_error("XX000", "schema column name is malformed"))?
            .to_string(),
        data_type: DataType::from_sql(
            value
                .get("data_type")
                .and_then(Value::as_str)
                .unwrap_or("TEXT"),
        ),
        primary_key: value
            .get("primary_key")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        nullable: value
            .get("nullable")
            .and_then(Value::as_bool)
            .unwrap_or(true),
        default: value
            .get("default")
            .and_then(Value::as_str)
            .map(str::to_string),
        on_update: value
            .get("on_update")
            .and_then(Value::as_str)
            .map(str::to_string),
        character_set: value
            .get("character_set")
            .and_then(Value::as_str)
            .map(str::to_string),
        collation: value
            .get("collation")
            .and_then(Value::as_str)
            .map(str::to_string),
    })
}
