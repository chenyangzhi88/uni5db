use pgwire::error::PgWireResult;
use sqlparser::ast::{
    ColumnDef, ColumnOption, CreateTableLikeKind, ObjectName, Query, TableConstraint,
};

use super::GatewayServer;
use super::shared::{
    SessionCatalog, is_auto_increment_option, is_serial_type, mysql_mode_data_type,
    serial_sequence_name,
};
use crate::catalog::{object_name_to_string, resolve_table_reference};
use crate::error::{unsupported, user_error};
use crate::mode::GatewayMode;
use crate::sql::nextval_sequence_name;
use crate::types::{
    CheckConstraintSchema, ColumnSchema, DataType, ForeignKeyConstraintSchema,
    INTERNAL_ROWID_COLUMN, QueryPlan, TableSchema, UniqueConstraintSchema,
};

impl GatewayServer {
    pub(super) fn column_schema_from_def(
        &self,
        schema_name: &str,
        table_name: &str,
        ordinal: usize,
        column: ColumnDef,
    ) -> PgWireResult<(
        ColumnSchema,
        Vec<CheckConstraintSchema>,
        Option<UniqueConstraintSchema>,
        Option<ForeignKeyConstraintSchema>,
    )> {
        let mut is_primary = false;
        let mut is_unique = false;
        let mut nullable = true;
        let mut default = None;
        let mut on_update = None;
        let mut character_set = None;
        let mut collation = None;
        let mut checks = Vec::new();
        let mut foreign_key = None;
        let column_name = column.name.value.clone();
        let auto_increment = column
            .options
            .iter()
            .any(|option| is_auto_increment_option(&option.option));

        for option in &column.options {
            match &option.option {
                ColumnOption::PrimaryKey(_) => {
                    is_primary = true;
                    nullable = false;
                }
                ColumnOption::Unique(_) => is_unique = true,
                ColumnOption::NotNull => nullable = false,
                ColumnOption::Default(expr) => default = Some(expr.to_string()),
                ColumnOption::OnUpdate(expr) => on_update = Some(expr.to_string()),
                ColumnOption::CharacterSet(name) => character_set = Some(name.to_string()),
                ColumnOption::Collation(name) => collation = Some(name.to_string()),
                ColumnOption::Check(expr) => checks.push(CheckConstraintSchema {
                    name: option
                        .name
                        .as_ref()
                        .map(|ident| ident.value.clone())
                        .unwrap_or_else(|| format!("{table_name}_{column_name}_check")),
                    expr: expr.to_string(),
                }),
                ColumnOption::ForeignKey(fk_constraint) => {
                    foreign_key = Some(ForeignKeyConstraintSchema {
                        name: option
                            .name
                            .as_ref()
                            .map(|ident| ident.value.clone())
                            .unwrap_or_else(|| format!("{table_name}_{column_name}_fkey")),
                        columns: vec![column_name.clone()],
                        foreign_table: object_name_to_string(&fk_constraint.foreign_table)?,
                        referred_columns: fk_constraint
                            .referred_columns
                            .iter()
                            .map(|ident| ident.value.clone())
                            .collect(),
                    });
                }
                _ => {}
            }
        }
        if auto_increment {
            nullable = false;
        }

        let raw_data_type = column.data_type.to_string();
        if default.is_none() && (is_serial_type(&raw_data_type) || auto_increment) {
            default = Some(format!(
                "nextval('{}')",
                serial_sequence_name(schema_name, table_name, &column_name)
            ));
        }
        let unique_constraint = (is_unique || is_primary).then(|| UniqueConstraintSchema {
            name: if is_primary {
                format!("{table_name}_pkey")
            } else {
                format!("{table_name}_{column_name}_key")
            },
            columns: vec![column_name.clone()],
            primary_key: is_primary,
        });
        Ok((
            ColumnSchema {
                column_id: ordinal as u32,
                name: column_name,
                data_type: if self.mode == GatewayMode::MySql {
                    mysql_mode_data_type(&raw_data_type)
                } else {
                    DataType::from_sql(&raw_data_type)
                },
                primary_key: is_primary,
                nullable,
                default,
                on_update,
                character_set,
                collation,
            },
            checks,
            unique_constraint,
            foreign_key,
        ))
    }

    pub(super) async fn plan_create_table(
        &self,
        database_name: &str,
        default_schema_name: &str,
        _search_path: &[String],
        name: ObjectName,
        columns: Vec<ColumnDef>,
        constraints: Vec<TableConstraint>,
        if_not_exists: bool,
        auto_increment_start: Option<i64>,
    ) -> PgWireResult<QueryPlan> {
        let qualified_name = object_name_to_string(&name)?;
        let (schema_name, table_name) = if qualified_name.contains('.') {
            resolve_table_reference(&qualified_name)?
        } else {
            (default_schema_name.to_string(), qualified_name.clone())
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
        if self
            .catalog
            .load_table(database_name, &schema_name, &table_name)
            .await?
            .is_some()
        {
            if if_not_exists {
                return Ok(QueryPlan::Noop {
                    tag: "CREATE TABLE".to_string(),
                });
            }
            return Err(user_error(
                "42P07",
                format!("table '{qualified_name}' already exists"),
            ));
        }

        let table_id = self.catalog.allocate_table_id().await?;
        let mut primary_key = None;
        let mut column_schemas = Vec::with_capacity(columns.len());

        let mut check_constraints = Vec::new();
        let mut unique_constraints = Vec::new();
        let mut foreign_keys = Vec::new();
        let mut indexes = Vec::new();

        for column in columns {
            let (column_schema, column_checks, column_unique, column_fk) = self
                .column_schema_from_def(
                    &schema_name,
                    &table_name,
                    column_schemas.len() + 1,
                    column,
                )?;
            if column_schema.primary_key {
                primary_key = Some(column_schema.name.clone());
            }
            check_constraints.extend(column_checks);
            if let Some(unique) = column_unique {
                unique_constraints.push(unique);
            }
            if let Some(fk) = column_fk {
                foreign_keys.push(fk);
            }
            column_schemas.push(column_schema);
        }

        for constraint in constraints {
            match constraint {
                TableConstraint::Unique(unique) => {
                    let name = unique.name;
                    let is_primary = false;
                    let columns = unique
                        .columns
                        .into_iter()
                        .map(Self::index_column_name)
                        .collect::<PgWireResult<Vec<_>>>()?;
                    if columns.is_empty() {
                        return Err(user_error("42601", "UNIQUE constraint requires columns"));
                    }
                    let column_names = columns;
                    for column_name in &column_names {
                        if column_schemas
                            .iter()
                            .all(|column| &column.name != column_name)
                        {
                            return Err(user_error(
                                "42703",
                                format!("column '{column_name}' not found"),
                            ));
                        }
                    }
                    unique_constraints.push(UniqueConstraintSchema {
                        name: name.map(|ident| ident.value).unwrap_or_else(|| {
                            format!("{}_{}_key", table_name, column_names.join("_"))
                        }),
                        columns: column_names,
                        primary_key: is_primary,
                    });
                }
                TableConstraint::PrimaryKey(primary) => {
                    let name = primary.name;
                    let columns = primary
                        .columns
                        .into_iter()
                        .map(Self::index_column_name)
                        .collect::<PgWireResult<Vec<_>>>()?;
                    if columns.is_empty() {
                        return Err(user_error("42601", "UNIQUE constraint requires columns"));
                    }
                    let column_names = columns;
                    for column_name in &column_names {
                        if column_schemas
                            .iter()
                            .all(|column| &column.name != column_name)
                        {
                            return Err(user_error(
                                "42703",
                                format!("column '{column_name}' not found"),
                            ));
                        }
                    }
                    if column_names.len() != 1 {
                        return Err(unsupported("composite primary keys are not supported yet"));
                    }
                    primary_key = Some(column_names[0].clone());
                    unique_constraints.push(UniqueConstraintSchema {
                        name: name
                            .map(|ident| ident.value)
                            .unwrap_or_else(|| format!("{table_name}_pkey")),
                        columns: column_names,
                        primary_key: true,
                    });
                }
                TableConstraint::Check(check) => {
                    check_constraints.push(CheckConstraintSchema {
                        name: check
                            .name
                            .map(|ident| ident.value)
                            .unwrap_or_else(|| format!("{table_name}_check")),
                        expr: check.expr.to_string(),
                    });
                }
                TableConstraint::ForeignKey(foreign_key) => {
                    foreign_keys.push(ForeignKeyConstraintSchema {
                        name: foreign_key
                            .name
                            .map(|ident| ident.value)
                            .unwrap_or_else(|| format!("{table_name}_fkey")),
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
                    });
                }
                TableConstraint::Index(index) => {
                    let column_names = Self::index_column_names(index.columns)?;
                    if !column_names.is_empty() {
                        let index_name = index.name.map(|ident| ident.value).unwrap_or_else(|| {
                            format!("{}_{}_idx", table_name, column_names.join("_"))
                        });
                        indexes.push((index_name, column_names, false));
                    }
                }
                TableConstraint::FulltextOrSpatial(index) => {
                    let column_names = Self::index_column_names(index.columns)?;
                    if !column_names.is_empty() {
                        let index_name = index
                            .opt_index_name
                            .map(|ident| ident.value)
                            .unwrap_or_else(|| {
                                format!("{}_{}_idx", table_name, column_names.join("_"))
                            });
                        indexes.push((index_name, column_names, false));
                    }
                }
                _ => {}
            }
        }

        let primary_key = primary_key
            .or_else(|| {
                column_schemas
                    .iter()
                    .find(|c| c.name == "id")
                    .map(|c| c.name.clone())
            })
            .unwrap_or_else(|| INTERNAL_ROWID_COLUMN.to_string());

        for col in &mut column_schemas {
            if col.name == primary_key {
                col.primary_key = true;
                col.nullable = false;
            }
        }

        let mut schema = TableSchema {
            table_name,
            table_id,
            schema_version: 1,
            table_epoch: 1,
            primary_key,
            check_constraints,
            unique_constraints,
            foreign_keys,
            columns: column_schemas,
        };
        schema.normalize_descriptor();

        Ok(QueryPlan::CreateTable {
            database_name: database_name.to_string(),
            schema_name,
            schema,
            auto_increment_start,
            indexes,
        })
    }

    pub(super) async fn plan_create_table_like(
        &self,
        database_name: &str,
        default_schema_name: &str,
        search_path: &[String],
        name: ObjectName,
        if_not_exists: bool,
        like: CreateTableLikeKind,
        auto_increment_start: Option<i64>,
    ) -> PgWireResult<QueryPlan> {
        let qualified_name = object_name_to_string(&name)?;
        let (schema_name, table_name) = if qualified_name.contains('.') {
            resolve_table_reference(&qualified_name)?
        } else {
            (default_schema_name.to_string(), qualified_name.clone())
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
        if self
            .catalog
            .load_table(database_name, &schema_name, &table_name)
            .await?
            .is_some()
        {
            if if_not_exists {
                return Ok(QueryPlan::Noop {
                    tag: "CREATE TABLE".to_string(),
                });
            }
            return Err(user_error(
                "42P07",
                format!("table '{qualified_name}' already exists"),
            ));
        }

        let source_name = match like {
            CreateTableLikeKind::Plain(like) | CreateTableLikeKind::Parenthesized(like) => {
                object_name_to_string(&like.name)?
            }
        };
        let (_, source_schema) = self
            .resolve_table_schema(database_name, search_path, &source_name)
            .await?;
        let source_schema = source_schema
            .ok_or_else(|| user_error("42P01", format!("table '{source_name}' does not exist")))?;

        let mut schema = source_schema.clone();
        let source_table_id = source_schema.table_id;
        schema.table_name = table_name;
        schema.table_id = self.catalog.allocate_table_id().await?;
        schema.schema_version = 1;
        schema.table_epoch = 1;
        schema.foreign_keys.clear();
        for column in &mut schema.columns {
            if column
                .default
                .as_deref()
                .and_then(nextval_sequence_name)
                .is_some()
            {
                column.default = Some(format!(
                    "nextval('{}')",
                    serial_sequence_name(&schema_name, &schema.table_name, &column.name)
                ));
            }
        }
        schema.normalize_descriptor();
        for constraint in &mut schema.unique_constraints {
            if constraint.primary_key {
                constraint.name = format!("{}_pkey", schema.table_name);
            } else {
                constraint.name =
                    format!("{}_{}_key", schema.table_name, constraint.columns.join("_"));
            }
        }
        for check in &mut schema.check_constraints {
            check.name = format!("{}_check", schema.table_name);
        }
        let indexes = self
            .catalog
            .list_indexes_for_table(source_table_id)
            .await?
            .into_iter()
            .map(|index| {
                (
                    format!("{}_{}_idx", schema.table_name, index.column_names.join("_")),
                    index.column_names,
                    index.unique,
                )
            })
            .collect();

        Ok(QueryPlan::CreateTable {
            database_name: database_name.to_string(),
            schema_name,
            schema,
            auto_increment_start,
            indexes,
        })
    }

    pub(super) async fn plan_create_table_as(
        &self,
        database_name: &str,
        default_schema_name: &str,
        search_path: &[String],
        name: ObjectName,
        if_not_exists: bool,
        query: Query,
    ) -> PgWireResult<QueryPlan> {
        let qualified_name = object_name_to_string(&name)?;
        let (schema_name, table_name) = if qualified_name.contains('.') {
            resolve_table_reference(&qualified_name)?
        } else {
            (default_schema_name.to_string(), qualified_name)
        };
        if self
            .catalog
            .load_table(database_name, &schema_name, &table_name)
            .await?
            .is_some()
        {
            if if_not_exists {
                return Ok(QueryPlan::Noop {
                    tag: "CREATE TABLE".to_string(),
                });
            }
            return Err(user_error(
                "42P07",
                format!("table '{table_name}' already exists"),
            ));
        }
        let session = SessionCatalog {
            database_name: database_name.to_string(),
            schema_name: schema_name.clone(),
            search_path: search_path.to_vec(),
        };
        let batches = self
            .collect_datafusion_batches(&query.to_string(), &session)
            .await?;
        let arrow_schema = batches.first().map(|batch| batch.schema());
        let fields = arrow_schema
            .as_ref()
            .map(|schema| schema.fields().clone())
            .unwrap_or_default();
        let columns = fields
            .iter()
            .enumerate()
            .map(|(idx, field)| ColumnSchema {
                column_id: idx as u32 + 1,
                name: field.name().clone(),
                data_type: Self::arrow_type_to_gateway_type(field.data_type()),
                primary_key: false,
                nullable: true,
                default: None,
                on_update: None,
                character_set: None,
                collation: None,
            })
            .collect::<Vec<_>>();
        let mut schema = TableSchema {
            table_name,
            table_id: self.catalog.allocate_table_id().await?,
            schema_version: 1,
            table_epoch: 1,
            primary_key: INTERNAL_ROWID_COLUMN.to_string(),
            check_constraints: Vec::new(),
            unique_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            columns,
        };
        schema.normalize_descriptor();
        let target_columns = schema.column_names();
        let rows = self.datafusion_rows_for_schema(&batches, &schema, &target_columns)?;
        Ok(QueryPlan::CreateTableAs {
            database_name: database_name.to_string(),
            schema_name,
            schema,
            rows,
        })
    }
}
