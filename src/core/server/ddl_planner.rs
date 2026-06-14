use super::*;

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
