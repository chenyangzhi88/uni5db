use pgwire::error::PgWireResult;
use sqlparser::ast::{
    AlterTableOperation, ColumnDef, ColumnOption, ColumnOptionDef, Expr, Ident, ObjectName, Query,
    SchemaName, SequenceOptions, TableConstraint,
};

use super::GatewayServer;
use crate::catalog::{object_name_to_string, resolve_table_reference, schema_name_to_string};
use crate::error::{unsupported, user_error};
use crate::sql::{expr_identifier_name, sql_expr_to_column_value};
use crate::types::{
    CheckConstraintSchema, ColumnSchema, ColumnValue, DataType, ForeignKeyConstraintSchema,
    QueryPlan, TableAlterOperation, UniqueConstraintSchema,
};

impl GatewayServer {
    pub(super) async fn plan_create_database(
        &self,
        db_name: ObjectName,
        if_not_exists: bool,
    ) -> PgWireResult<QueryPlan> {
        let database_name = object_name_to_string(&db_name)?;
        Ok(QueryPlan::CreateDatabase {
            database_name,
            if_not_exists,
        })
    }

    pub(super) async fn plan_create_schema(
        &self,
        database_name: &str,
        schema_name: SchemaName,
        if_not_exists: bool,
    ) -> PgWireResult<QueryPlan> {
        let schema_name = schema_name_to_string(&schema_name)?;
        Ok(QueryPlan::CreateSchema {
            database_name: database_name.to_string(),
            schema_name,
            if_not_exists,
        })
    }

    pub(super) async fn plan_create_sequence(
        &self,
        database_name: &str,
        default_schema_name: &str,
        name: ObjectName,
        if_not_exists: bool,
        sequence_options: Vec<SequenceOptions>,
    ) -> PgWireResult<QueryPlan> {
        let qualified_name = object_name_to_string(&name)?;
        let (schema_name, sequence_name) = if qualified_name.contains('.') {
            resolve_table_reference(&qualified_name)?
        } else {
            (default_schema_name.to_string(), qualified_name)
        };
        let database = self
            .catalog
            .get_database(database_name)
            .await?
            .ok_or_else(|| {
                user_error(
                    "3D000",
                    format!("database '{database_name}' does not exist"),
                )
            })?;
        if self
            .catalog
            .get_schema(database.database_id, &schema_name)
            .await?
            .is_none()
        {
            return Err(user_error(
                "3F000",
                format!("schema '{schema_name}' does not exist"),
            ));
        }

        let mut start = 1_i64;
        let mut increment = 1_i64;
        for option in sequence_options {
            match option {
                SequenceOptions::StartWith(expr, _) => start = Self::sequence_option_i64(&expr)?,
                SequenceOptions::IncrementBy(expr, _) => {
                    increment = Self::sequence_option_i64(&expr)?;
                    if increment == 0 {
                        return Err(user_error(
                            "22023",
                            "INCREMENT must not be zero for CREATE SEQUENCE",
                        ));
                    }
                }
                SequenceOptions::MinValue(_)
                | SequenceOptions::MaxValue(_)
                | SequenceOptions::Cache(_)
                | SequenceOptions::Cycle(_) => {}
            }
        }

        Ok(QueryPlan::CreateSequence {
            database_name: database_name.to_string(),
            schema_name,
            sequence_name,
            if_not_exists,
            start,
            increment,
        })
    }

    pub(super) fn sequence_option_i64(expr: &Expr) -> PgWireResult<i64> {
        match sql_expr_to_column_value(expr, &DataType::Int64)? {
            ColumnValue::Int64(value) => Ok(value),
            _ => Err(user_error("22023", "sequence option must be an integer")),
        }
    }

    pub(super) async fn plan_create_view(
        &self,
        database_name: &str,
        default_schema_name: &str,
        name: ObjectName,
        query: Query,
        or_replace: bool,
        if_not_exists: bool,
    ) -> PgWireResult<QueryPlan> {
        let qualified_name = object_name_to_string(&name)?;
        let (schema_name, view_name) = if qualified_name.contains('.') {
            resolve_table_reference(&qualified_name)?
        } else {
            (default_schema_name.to_string(), qualified_name)
        };
        Ok(QueryPlan::CreateView {
            database_name: database_name.to_string(),
            schema_name,
            view_name,
            definition: query.to_string(),
            or_replace,
            if_not_exists,
        })
    }

    pub(super) async fn plan_drop_table(
        &self,
        database_name: &str,
        search_path: &[String],
        names: Vec<ObjectName>,
        if_exists: bool,
    ) -> PgWireResult<QueryPlan> {
        let mut tables = Vec::with_capacity(names.len());
        for name in names {
            let qualified_name = object_name_to_string(&name)?;
            let (schema_name, table_name) = if qualified_name.contains('.') {
                resolve_table_reference(&qualified_name)?
            } else {
                let (resolved_schema, existing) = self
                    .resolve_table_schema(database_name, search_path, &qualified_name)
                    .await?;
                if existing.is_none() && !if_exists {
                    return Err(user_error(
                        "42P01",
                        format!("table '{qualified_name}' does not exist"),
                    ));
                }
                (resolved_schema, qualified_name)
            };
            tables.push((schema_name, table_name));
        }
        Ok(QueryPlan::DropTables {
            database_name: database_name.to_string(),
            tables,
            if_exists,
        })
    }

    pub(super) async fn plan_drop_index(
        &self,
        database_name: &str,
        search_path: &[String],
        names: Vec<ObjectName>,
        if_exists: bool,
    ) -> PgWireResult<QueryPlan> {
        let mut indexes = Vec::with_capacity(names.len());
        let database = self
            .catalog
            .get_database(database_name)
            .await?
            .ok_or_else(|| {
                user_error(
                    "3D000",
                    format!("database '{database_name}' does not exist"),
                )
            })?;
        for name in names {
            let qualified_name = object_name_to_string(&name)?;
            let (schema_name, index_name) = if qualified_name.contains('.') {
                resolve_table_reference(&qualified_name)?
            } else {
                let mut resolved = None;
                for schema in search_path {
                    let Some(schema_meta) = self
                        .catalog
                        .get_schema(database.database_id, schema)
                        .await?
                    else {
                        continue;
                    };
                    if self
                        .catalog
                        .get_index(database.database_id, schema_meta.schema_id, &qualified_name)
                        .await?
                        .is_some()
                    {
                        resolved = Some((schema.clone(), qualified_name.clone()));
                        break;
                    }
                }
                match resolved {
                    Some(value) => value,
                    None if if_exists => (search_path[0].clone(), qualified_name),
                    None => {
                        return Err(user_error(
                            "42P01",
                            format!("index '{qualified_name}' does not exist"),
                        ));
                    }
                }
            };
            indexes.push((schema_name, index_name));
        }
        Ok(QueryPlan::DropIndexes {
            database_name: database_name.to_string(),
            indexes,
            if_exists,
        })
    }

    pub(super) async fn plan_drop_sequence(
        &self,
        database_name: &str,
        default_schema_name: &str,
        names: Vec<ObjectName>,
        if_exists: bool,
    ) -> PgWireResult<QueryPlan> {
        let mut sequences = Vec::with_capacity(names.len());
        for name in names {
            let qualified_name = object_name_to_string(&name)?;
            let (schema_name, sequence_name) = if qualified_name.contains('.') {
                resolve_table_reference(&qualified_name)?
            } else {
                (default_schema_name.to_string(), qualified_name)
            };
            sequences.push((schema_name, sequence_name));
        }
        Ok(QueryPlan::DropSequences {
            database_name: database_name.to_string(),
            sequences,
            if_exists,
        })
    }

    pub(super) async fn plan_drop_view(
        &self,
        database_name: &str,
        default_schema_name: &str,
        names: Vec<ObjectName>,
        if_exists: bool,
    ) -> PgWireResult<QueryPlan> {
        let mut views = Vec::with_capacity(names.len());
        for name in names {
            let qualified_name = object_name_to_string(&name)?;
            let (schema_name, view_name) = if qualified_name.contains('.') {
                resolve_table_reference(&qualified_name)?
            } else {
                (default_schema_name.to_string(), qualified_name)
            };
            views.push((schema_name, view_name));
        }
        Ok(QueryPlan::DropViews {
            database_name: database_name.to_string(),
            views,
            if_exists,
        })
    }

    pub(super) async fn plan_drop_schema(
        &self,
        database_name: &str,
        names: Vec<ObjectName>,
        if_exists: bool,
    ) -> PgWireResult<QueryPlan> {
        Ok(QueryPlan::DropSchemas {
            database_name: database_name.to_string(),
            schemas: names
                .into_iter()
                .map(|name| object_name_to_string(&name))
                .collect::<PgWireResult<Vec<_>>>()?,
            if_exists,
        })
    }

    pub(super) fn column_schema_from_alter_parts(
        &self,
        schema_name: &str,
        table_name: &str,
        ordinal: usize,
        name: Ident,
        data_type: sqlparser::ast::DataType,
        options: Vec<ColumnOption>,
    ) -> PgWireResult<(
        ColumnSchema,
        Vec<CheckConstraintSchema>,
        Option<UniqueConstraintSchema>,
        Option<ForeignKeyConstraintSchema>,
    )> {
        self.column_schema_from_def(
            schema_name,
            table_name,
            ordinal,
            ColumnDef {
                name,
                data_type,
                options: options
                    .into_iter()
                    .map(|option| ColumnOptionDef { name: None, option })
                    .collect(),
            },
        )
    }

    pub(super) async fn plan_alter_table(
        &self,
        database_name: &str,
        search_path: &[String],
        name: ObjectName,
        operations: Vec<AlterTableOperation>,
    ) -> PgWireResult<QueryPlan> {
        let table_name = object_name_to_string(&name)?;
        let (schema_name, schema) = self
            .resolve_table_schema(database_name, search_path, &table_name)
            .await?;
        let schema = schema
            .ok_or_else(|| user_error("42P01", format!("table '{table_name}' does not exist")))?;

        let operation_count = operations.len();
        if operation_count == 0 {
            return Ok(QueryPlan::Noop {
                tag: "ALTER TABLE".to_string(),
            });
        }

        let mut planned = Vec::new();
        for operation in operations {
            match operation {
                AlterTableOperation::AddColumn {
                    column_def,
                    if_not_exists,
                    ..
                } => {
                    let (column, checks, unique, fk) = self.column_schema_from_def(
                        &schema_name,
                        &schema.table_name,
                        schema.columns.len() + planned.len() + 1,
                        column_def,
                    )?;
                    if !checks.is_empty() || unique.is_some() || fk.is_some() {
                        return Err(unsupported(
                            "ALTER TABLE ADD COLUMN supports column type/default/nullability; inline CHECK/UNIQUE/REFERENCES should be added separately",
                        ));
                    }
                    planned.push(TableAlterOperation::AddColumn {
                        column,
                        if_not_exists,
                    });
                }
                AlterTableOperation::ModifyColumn {
                    col_name,
                    data_type,
                    options,
                    ..
                } => {
                    let ordinal = schema
                        .columns
                        .iter()
                        .position(|column| column.name == col_name.value)
                        .ok_or_else(|| {
                            user_error("42703", format!("column '{}' not found", col_name.value))
                        })?
                        + 1;
                    let (column, checks, unique, fk) = self.column_schema_from_alter_parts(
                        &schema_name,
                        &schema.table_name,
                        ordinal,
                        col_name.clone(),
                        data_type,
                        options,
                    )?;
                    if !checks.is_empty() || unique.is_some() || fk.is_some() {
                        return Err(unsupported(
                            "ALTER TABLE MODIFY COLUMN supports type/default/nullability/charset/collation/on update; constraints should be added separately",
                        ));
                    }
                    planned.push(TableAlterOperation::ModifyColumn {
                        column_name: col_name.value,
                        column,
                    });
                }
                AlterTableOperation::ChangeColumn {
                    old_name,
                    new_name,
                    data_type,
                    options,
                    ..
                } => {
                    let ordinal = schema
                        .columns
                        .iter()
                        .position(|column| column.name == old_name.value)
                        .ok_or_else(|| {
                            user_error("42703", format!("column '{}' not found", old_name.value))
                        })?
                        + 1;
                    let (column, checks, unique, fk) = self.column_schema_from_alter_parts(
                        &schema_name,
                        &schema.table_name,
                        ordinal,
                        new_name,
                        data_type,
                        options,
                    )?;
                    if !checks.is_empty() || unique.is_some() || fk.is_some() {
                        return Err(unsupported(
                            "ALTER TABLE CHANGE COLUMN supports type/default/nullability/charset/collation/on update; constraints should be added separately",
                        ));
                    }
                    planned.push(TableAlterOperation::ModifyColumn {
                        column_name: old_name.value,
                        column,
                    });
                }
                AlterTableOperation::DropColumn {
                    column_names,
                    if_exists,
                    ..
                } => {
                    for column_name in column_names {
                        planned.push(TableAlterOperation::DropColumn {
                            column_name: column_name.value,
                            if_exists,
                        });
                    }
                }
                AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                } => planned.push(TableAlterOperation::RenameColumn {
                    old_name: old_column_name.value,
                    new_name: new_column_name.value,
                }),
                AlterTableOperation::RenameTable { table_name } => {
                    if operation_count != 1 {
                        return Err(unsupported(
                            "ALTER TABLE RENAME TO cannot be combined with other operations",
                        ));
                    }
                    let table_name = match table_name {
                        sqlparser::ast::RenameTableNameKind::As(name)
                        | sqlparser::ast::RenameTableNameKind::To(name) => name,
                    };
                    let new_name = object_name_to_string(&table_name)?;
                    if new_name.contains('.') {
                        return Err(unsupported(
                            "ALTER TABLE RENAME TO only supports unqualified table names",
                        ));
                    }
                    planned.push(TableAlterOperation::RenameTable { new_name });
                }
                AlterTableOperation::AlterColumn { column_name, op } => match op {
                    sqlparser::ast::AlterColumnOperation::SetDefault { value } => {
                        planned.push(TableAlterOperation::SetDefault {
                            column_name: column_name.value,
                            default: Some(value.to_string()),
                        });
                    }
                    sqlparser::ast::AlterColumnOperation::DropDefault => {
                        planned.push(TableAlterOperation::SetDefault {
                            column_name: column_name.value,
                            default: None,
                        });
                    }
                    sqlparser::ast::AlterColumnOperation::SetNotNull => {
                        planned.push(TableAlterOperation::SetNotNull {
                            column_name: column_name.value,
                            nullable: false,
                        });
                    }
                    sqlparser::ast::AlterColumnOperation::DropNotNull => {
                        planned.push(TableAlterOperation::SetNotNull {
                            column_name: column_name.value,
                            nullable: true,
                        });
                    }
                    _ => {
                        return Err(unsupported(
                            "ALTER TABLE ALTER COLUMN only supports DEFAULT and NOT NULL changes",
                        ));
                    }
                },
                AlterTableOperation::AddConstraint {
                    constraint: TableConstraint::PrimaryKey(primary),
                    ..
                } if primary.columns.len() == 1 => {
                    if operation_count != 1 {
                        return Err(unsupported(
                            "ALTER TABLE ADD PRIMARY KEY cannot be combined with other operations",
                        ));
                    }
                    let column_name = Self::index_column_name(primary.columns[0].clone())?;
                    schema.find_column(&column_name).ok_or_else(|| {
                        user_error("42703", format!("column '{column_name}' not found"))
                    })?;
                    return Ok(QueryPlan::AlterTableAddPrimaryKey {
                        database_name: database_name.to_string(),
                        schema_name,
                        table_name: schema.table_name,
                        column_name,
                    });
                }
                AlterTableOperation::AddConstraint {
                    constraint: TableConstraint::Unique(unique),
                    ..
                } => {
                    let name = unique.name;
                    let column_names = Self::index_column_names(unique.columns)?;
                    if column_names.is_empty() {
                        return Err(user_error("42601", "UNIQUE constraint requires columns"));
                    }
                    planned.push(TableAlterOperation::AddUnique {
                        constraint: UniqueConstraintSchema {
                            name: name.map(|ident| ident.value).unwrap_or_else(|| {
                                format!("{}_{}_key", schema.table_name, column_names.join("_"))
                            }),
                            columns: column_names,
                            primary_key: false,
                        },
                    });
                }
                AlterTableOperation::AddConstraint {
                    constraint: TableConstraint::Check(check),
                    ..
                } => planned.push(TableAlterOperation::AddCheck {
                    constraint: CheckConstraintSchema {
                        name: check
                            .name
                            .map(|ident| ident.value)
                            .unwrap_or_else(|| format!("{}_check", schema.table_name)),
                        expr: check.expr.to_string(),
                    },
                }),
                AlterTableOperation::AddConstraint {
                    constraint: TableConstraint::ForeignKey(foreign_key),
                    ..
                } => planned.push(TableAlterOperation::AddForeignKey {
                    constraint: ForeignKeyConstraintSchema {
                        name: foreign_key
                            .name
                            .or(foreign_key.index_name)
                            .map(|ident| ident.value)
                            .unwrap_or_else(|| format!("{}_fkey", schema.table_name)),
                        columns: foreign_key
                            .columns
                            .into_iter()
                            .map(|ident| ident.value)
                            .collect(),
                        foreign_table: object_name_to_string(&foreign_key.foreign_table)?,
                        referred_columns: foreign_key
                            .referred_columns
                            .into_iter()
                            .map(|ident| ident.value)
                            .collect(),
                    },
                }),
                AlterTableOperation::AddConstraint {
                    constraint: TableConstraint::Index(index),
                    ..
                } => {
                    let column_names = Self::index_column_names(index.columns)?;
                    let index_name = index.name.map(|ident| ident.value).unwrap_or_else(|| {
                        format!("{}_{}_idx", schema.table_name, column_names.join("_"))
                    });
                    planned.push(TableAlterOperation::AddIndex {
                        index_name,
                        column_names,
                        unique: false,
                        if_not_exists: false,
                    });
                }
                AlterTableOperation::AddConstraint {
                    constraint: TableConstraint::FulltextOrSpatial(index),
                    ..
                } => {
                    let column_names = Self::index_column_names(index.columns)?;
                    let index_name = index
                        .opt_index_name
                        .map(|ident| ident.value)
                        .unwrap_or_else(|| {
                            format!("{}_{}_idx", schema.table_name, column_names.join("_"))
                        });
                    planned.push(TableAlterOperation::AddIndex {
                        index_name,
                        column_names,
                        unique: false,
                        if_not_exists: false,
                    });
                }
                AlterTableOperation::DropForeignKey { name, .. } => {
                    planned.push(TableAlterOperation::DropForeignKey {
                        name: name.value,
                        if_exists: false,
                    });
                }
                AlterTableOperation::DropConstraint {
                    name, if_exists, ..
                } => planned.push(TableAlterOperation::DropForeignKey {
                    name: name.value,
                    if_exists,
                }),
                AlterTableOperation::DropIndex { name } => {
                    planned.push(TableAlterOperation::DropIndex {
                        index_name: name.value,
                        if_exists: false,
                    });
                }
                AlterTableOperation::AutoIncrement { value, .. } => {
                    let value = value.to_string().parse::<i64>().map_err(|_| {
                        user_error("22023", "AUTO_INCREMENT value must be an integer")
                    })?;
                    planned.push(TableAlterOperation::SetAutoIncrement { value });
                }
                AlterTableOperation::Algorithm { .. } | AlterTableOperation::Lock { .. } => {}
                _ => {
                    return Err(unsupported(
                        "fast-path ALTER TABLE operation is not supported yet",
                    ));
                }
            }
        }

        Ok(QueryPlan::AlterTable {
            database_name: database_name.to_string(),
            schema_name,
            table_name: schema.table_name,
            operations: planned,
        })
    }

    pub(super) fn index_column_name(column: sqlparser::ast::IndexColumn) -> PgWireResult<String> {
        if column.column.options.asc == Some(false) || column.column.options.nulls_first.is_some() {
            return Err(unsupported(
                "index sort direction and NULLS ordering are not supported yet",
            ));
        }
        match &column.column.expr {
            Expr::Substring { expr, .. } => expr_identifier_name(expr),
            Expr::Function(function) => object_name_to_string(&function.name),
            expr => expr_identifier_name(expr),
        }
    }

    pub(super) fn index_column_names(
        columns: Vec<sqlparser::ast::IndexColumn>,
    ) -> PgWireResult<Vec<String>> {
        columns
            .into_iter()
            .map(Self::index_column_name)
            .collect::<PgWireResult<Vec<_>>>()
    }

    pub(super) async fn plan_create_index(
        &self,
        database_name: &str,
        search_path: &[String],
        name: Option<ObjectName>,
        table_name: ObjectName,
        using: Option<sqlparser::ast::IndexType>,
        columns: Vec<sqlparser::ast::IndexColumn>,
        unique: bool,
        concurrently: bool,
        if_not_exists: bool,
        include: Vec<Ident>,
        predicate: Option<Expr>,
    ) -> PgWireResult<QueryPlan> {
        if concurrently {
            return Err(unsupported(
                "CREATE INDEX CONCURRENTLY is not supported yet",
            ));
        }
        if using
            .as_ref()
            .is_some_and(|ident| !ident.to_string().eq_ignore_ascii_case("btree"))
        {
            return Err(unsupported("only btree indexes are supported"));
        }
        if !include.is_empty() {
            return Err(unsupported(
                "CREATE INDEX INCLUDE columns are not supported yet",
            ));
        }
        if predicate.is_some() {
            return Err(unsupported("partial indexes are not supported yet"));
        }
        if columns.is_empty() {
            return Err(user_error(
                "42601",
                "CREATE INDEX requires at least one column",
            ));
        }
        let mut column_names = Vec::with_capacity(columns.len());
        for column in columns {
            column_names.push(Self::index_column_name(column)?);
        }

        let table_name = object_name_to_string(&table_name)?;
        let (schema_name, schema) = self
            .resolve_table_schema(database_name, search_path, &table_name)
            .await?;
        let schema = schema
            .ok_or_else(|| user_error("42P01", format!("table '{table_name}' does not exist")))?;
        for column_name in &column_names {
            schema
                .find_column(column_name)
                .ok_or_else(|| user_error("42703", format!("column '{column_name}' not found")))?;
        }

        let index_name = match name {
            Some(name) => {
                let index_name = object_name_to_string(&name)?;
                if index_name.contains('.') {
                    let (index_schema, index_name) = resolve_table_reference(&index_name)?;
                    if index_schema != schema_name {
                        return Err(user_error(
                            "3F000",
                            "index schema must match the indexed table schema",
                        ));
                    }
                    index_name
                } else {
                    index_name
                }
            }
            None => format!("{}_{}_idx", schema.table_name, column_names.join("_")),
        };

        Ok(QueryPlan::CreateIndex {
            database_name: database_name.to_string(),
            schema_name,
            table_name: schema.table_name,
            index_name,
            column_names,
            unique,
            if_not_exists,
        })
    }
}
