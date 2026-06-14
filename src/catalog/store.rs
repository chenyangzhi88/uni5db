use super::*;

impl CatalogStore {
    pub fn new(store: Arc<dyn KvStore>) -> Self {
        Self { store }
    }

    pub async fn ensure_bootstrap(&self) -> PgWireResult<()> {
        if self
            .store
            .get(database_by_name_key(DEFAULT_DATABASE_NAME).as_bytes())
            .await
            .map_err(|e| user_error("XX000", e))?
            .is_some()
        {
            return Ok(());
        }

        self.store
            .put(NEXT_DATABASE_ID_KEY, &2u32.to_be_bytes())
            .await
            .map_err(|e| user_error("XX000", e))?;
        self.store
            .put(NEXT_SCHEMA_ID_KEY, &2u32.to_be_bytes())
            .await
            .map_err(|e| user_error("XX000", e))?;
        self.store
            .put(NEXT_TABLE_ID_KEY, &1u32.to_be_bytes())
            .await
            .map_err(|e| user_error("XX000", e))?;
        self.store
            .put(NEXT_INDEX_ID_KEY, &1u32.to_be_bytes())
            .await
            .map_err(|e| user_error("XX000", e))?;
        self.store
            .put(NEXT_VIEW_ID_KEY, &1u32.to_be_bytes())
            .await
            .map_err(|e| user_error("XX000", e))?;

        let default_db = DatabaseCatalog {
            database_id: 1,
            database_name: DEFAULT_DATABASE_NAME.to_string(),
        };
        self.write_database(&default_db).await?;

        let default_schema = SchemaCatalog {
            schema_id: 1,
            database_id: default_db.database_id,
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
        };
        self.write_schema(&default_schema).await?;

        Ok(())
    }

    pub async fn ensure_bootstrap_for_mode(&self, mode: GatewayMode) -> PgWireResult<()> {
        self.ensure_bootstrap().await?;
        match self.compat_mode().await? {
            Some(existing) if existing == mode => Ok(()),
            Some(existing) => Err(user_error(
                "XX000",
                format!(
                    "catalog was created in {existing} mode and cannot be opened in {mode} mode"
                ),
            )),
            None => {
                self.store
                    .put(COMPAT_MODE_KEY, mode.as_str().as_bytes())
                    .await
                    .map_err(|e| user_error("XX000", e))?;
                Ok(())
            }
        }
    }

    pub async fn compat_mode(&self) -> PgWireResult<Option<GatewayMode>> {
        let Some(bytes) = self
            .store
            .get(COMPAT_MODE_KEY)
            .await
            .map_err(|e| user_error("XX000", e))?
        else {
            return Ok(None);
        };
        let value = std::str::from_utf8(&bytes)
            .map_err(|e| user_error("XX000", format!("catalog compat mode is malformed: {e}")))?;
        value.parse().map(Some).map_err(|_| {
            user_error(
                "XX000",
                format!("catalog compat mode '{value}' is not supported"),
            )
        })
    }

    pub async fn create_database(
        &self,
        database_name: &str,
        if_not_exists: bool,
    ) -> PgWireResult<DatabaseCatalog> {
        self.ensure_bootstrap().await?;

        if let Some(existing) = self.get_database(database_name).await? {
            if if_not_exists {
                return Ok(existing);
            }
            return Err(user_error(
                "42P04",
                format!("database '{database_name}' already exists"),
            ));
        }

        let database_id = self.allocate_id(NEXT_DATABASE_ID_KEY).await?;
        let database = DatabaseCatalog {
            database_id,
            database_name: database_name.to_string(),
        };
        self.write_database(&database).await?;
        self.write_schema(&SchemaCatalog {
            schema_id: self.allocate_id(NEXT_SCHEMA_ID_KEY).await?,
            database_id,
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
        })
        .await?;
        Ok(database)
    }

    pub async fn get_database(&self, database_name: &str) -> PgWireResult<Option<DatabaseCatalog>> {
        self.ensure_bootstrap().await?;

        let Some(id_bytes) = self
            .store
            .get(database_by_name_key(database_name).as_bytes())
            .await
            .map_err(|e| user_error("XX000", e))?
        else {
            return Ok(None);
        };
        let database_id = decode_u32(&id_bytes, "database id")?;
        let Some(payload) = self
            .store
            .get(database_by_id_key(database_id).as_bytes())
            .await
            .map_err(|e| user_error("XX000", e))?
        else {
            return Err(user_error("XX000", "catalog database payload missing"));
        };
        Ok(Some(decode_database_catalog(&payload)?))
    }

    pub async fn create_schema(
        &self,
        database_name: &str,
        schema_name: &str,
        if_not_exists: bool,
    ) -> PgWireResult<SchemaCatalog> {
        self.ensure_bootstrap().await?;
        let database = self.get_database(database_name).await?.ok_or_else(|| {
            user_error(
                "3D000",
                format!("database '{database_name}' does not exist"),
            )
        })?;

        if let Some(existing) = self.get_schema(database.database_id, schema_name).await? {
            if if_not_exists {
                return Ok(existing);
            }
            return Err(user_error(
                "42P06",
                format!("schema '{schema_name}' already exists"),
            ));
        }

        let schema = SchemaCatalog {
            schema_id: self.allocate_id(NEXT_SCHEMA_ID_KEY).await?,
            database_id: database.database_id,
            schema_name: schema_name.to_string(),
        };
        self.write_schema(&schema).await?;
        Ok(schema)
    }

    pub async fn get_schema(
        &self,
        database_id: u32,
        schema_name: &str,
    ) -> PgWireResult<Option<SchemaCatalog>> {
        self.ensure_bootstrap().await?;

        let Some(id_bytes) = self
            .store
            .get(schema_by_name_key(database_id, schema_name).as_bytes())
            .await
            .map_err(|e| user_error("XX000", e))?
        else {
            return Ok(None);
        };
        let schema_id = decode_u32(&id_bytes, "schema id")?;
        let Some(payload) = self
            .store
            .get(schema_by_id_key(schema_id).as_bytes())
            .await
            .map_err(|e| user_error("XX000", e))?
        else {
            return Err(user_error("XX000", "catalog schema payload missing"));
        };
        Ok(Some(decode_schema_catalog(&payload)?))
    }

    pub async fn allocate_table_id(&self) -> PgWireResult<u32> {
        self.ensure_bootstrap().await?;
        self.allocate_id(NEXT_TABLE_ID_KEY).await
    }

    pub async fn store_table(
        &self,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
    ) -> PgWireResult<()> {
        self.ensure_bootstrap().await?;
        let database = self.get_database(database_name).await?.ok_or_else(|| {
            user_error(
                "3D000",
                format!("database '{database_name}' does not exist"),
            )
        })?;
        let schema_meta = self
            .get_schema(database.database_id, schema_name)
            .await?
            .ok_or_else(|| user_error("3F000", format!("schema '{schema_name}' does not exist")))?;

        let mut normalized_schema = schema.clone();
        normalized_schema.normalize_descriptor();
        let table_catalog = TableCatalog {
            database_id: database.database_id,
            schema_id: schema_meta.schema_id,
            schema_name: schema_name.to_string(),
            table_name: normalized_schema.table_name.clone(),
            schema: normalized_schema.clone(),
        };
        let payload = encode_table_catalog(&table_catalog)?;
        self.store
            .put(
                table_by_id_key(normalized_schema.table_id).as_bytes(),
                &payload,
            )
            .await
            .map_err(|e| user_error("XX000", e))?;
        self.store
            .put(
                table_by_name_key(
                    database.database_id,
                    schema_meta.schema_id,
                    &normalized_schema.table_name,
                )
                .as_bytes(),
                &normalized_schema.table_id.to_be_bytes(),
            )
            .await
            .map_err(|e| user_error("XX000", e))?;

        if database_name == DEFAULT_DATABASE_NAME && schema_name == DEFAULT_SCHEMA_NAME {
            let legacy = encode_legacy_table_schema(&normalized_schema)?;
            self.store
                .put(
                    schema_key(&normalized_schema.table_name).as_bytes(),
                    &legacy,
                )
                .await
                .map_err(|e| user_error("XX000", e))?;
        }

        Ok(())
    }

    pub async fn load_table(
        &self,
        database_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> PgWireResult<Option<TableCatalog>> {
        self.ensure_bootstrap().await?;
        let Some(database) = self.get_database(database_name).await? else {
            return Ok(None);
        };
        let Some(schema_meta) = self.get_schema(database.database_id, schema_name).await? else {
            return Ok(None);
        };
        let Some(id_bytes) = self
            .store
            .get(
                table_by_name_key(database.database_id, schema_meta.schema_id, table_name)
                    .as_bytes(),
            )
            .await
            .map_err(|e| user_error("XX000", e))?
        else {
            if database_name == DEFAULT_DATABASE_NAME && schema_name == DEFAULT_SCHEMA_NAME {
                return self.load_legacy_default_table(table_name).await;
            }
            return Ok(None);
        };
        let table_id = decode_u32(&id_bytes, "table id")?;
        let Some(payload) = self
            .store
            .get(table_by_id_key(table_id).as_bytes())
            .await
            .map_err(|e| user_error("XX000", e))?
        else {
            return Err(user_error("XX000", "catalog table payload missing"));
        };
        Ok(Some(decode_table_catalog(&payload)?))
    }

    pub async fn list_tables(&self, database_name: &str) -> PgWireResult<Vec<TableCatalog>> {
        self.ensure_bootstrap().await?;
        let Some(database) = self.get_database(database_name).await? else {
            return Ok(Vec::new());
        };
        let entries = self
            .store
            .scan_prefix(b"__catalog__/tables/by-id/")
            .await
            .map_err(|e| user_error("XX000", e))?;

        let mut tables = Vec::new();
        let catalog_entry_count = entries.len();
        for (_, value) in entries {
            let table = decode_table_catalog(&value)?;
            if table.database_id == database.database_id {
                tables.push(table);
            }
        }
        let catalog_table_count = tables.len();
        if database_name == DEFAULT_DATABASE_NAME {
            self.append_legacy_default_tables(&mut tables).await?;
        }
        if matches!(
            std::env::var("PG_GATEWAY_PROFILE_SCAN").as_deref(),
            Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES")
        ) {
            let table_summary = tables
                .iter()
                .map(|table| {
                    format!(
                        "{}.{}:{}:{}:{}",
                        table.schema_name,
                        table.table_name,
                        table.schema.table_id,
                        table.schema.table_epoch,
                        table.schema.schema_version
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            log::info!(
                "catalog.list_tables database={} catalog_by_id_entries={} catalog_table_count={} final_table_count={} tables={}",
                database_name,
                catalog_entry_count,
                catalog_table_count,
                tables.len(),
                table_summary
            );
        }
        Ok(tables)
    }

    async fn load_legacy_default_table(
        &self,
        table_name: &str,
    ) -> PgWireResult<Option<TableCatalog>> {
        let Some(payload) = self
            .store
            .get(schema_key(table_name).as_bytes())
            .await
            .map_err(|e| user_error("XX000", e))?
        else {
            return Ok(None);
        };
        Ok(Some(decode_table_catalog(&payload)?))
    }

    async fn append_legacy_default_tables(
        &self,
        tables: &mut Vec<TableCatalog>,
    ) -> PgWireResult<()> {
        let entries = self
            .store
            .scan_prefix(SCHEMA_PREFIX.as_bytes())
            .await
            .map_err(|e| user_error("XX000", e))?;
        let mut decoded = 0usize;
        let legacy_entry_count = entries.len();
        for (_, value) in entries {
            let table = decode_table_catalog(&value)?;
            decoded += 1;
            if table.database_id != 1 || table.schema_name != DEFAULT_SCHEMA_NAME {
                continue;
            }
            if tables.iter().any(|existing| {
                existing.schema.table_id == table.schema.table_id
                    || existing.table_name == table.table_name
            }) {
                continue;
            }
            tables.push(table);
        }
        if matches!(
            std::env::var("PG_GATEWAY_PROFILE_SCAN").as_deref(),
            Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES")
        ) {
            log::info!(
                "catalog.legacy_schema_scan prefix={} entries={} decoded={} appended_final_count={}",
                SCHEMA_PREFIX,
                legacy_entry_count,
                decoded,
                tables.len()
            );
        }
        Ok(())
    }

    pub async fn list_schemas(&self, database_name: &str) -> PgWireResult<Vec<SchemaCatalog>> {
        self.ensure_bootstrap().await?;
        let Some(database) = self.get_database(database_name).await? else {
            return Ok(Vec::new());
        };
        let entries = self
            .store
            .scan_prefix(b"__catalog__/schemas/by-id/")
            .await
            .map_err(|e| user_error("XX000", e))?;

        let mut schemas = Vec::new();
        for (_, value) in entries {
            let schema = decode_schema_catalog(&value)?;
            if schema.database_id == database.database_id {
                schemas.push(schema);
            }
        }
        schemas.sort_by(|left, right| left.schema_name.cmp(&right.schema_name));
        Ok(schemas)
    }

    pub async fn list_databases(&self) -> PgWireResult<Vec<DatabaseCatalog>> {
        self.ensure_bootstrap().await?;
        let entries = self
            .store
            .scan_prefix(b"__catalog__/databases/by-id/")
            .await
            .map_err(|e| user_error("XX000", e))?;

        let mut databases = Vec::with_capacity(entries.len());
        for (_, value) in entries {
            databases.push(decode_database_catalog(&value)?);
        }
        databases.sort_by(|left, right| left.database_name.cmp(&right.database_name));
        Ok(databases)
    }

    pub async fn drop_table(
        &self,
        database_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> PgWireResult<Option<TableCatalog>> {
        self.ensure_bootstrap().await?;
        let Some(table) = self
            .load_table(database_name, schema_name, table_name)
            .await?
        else {
            return Ok(None);
        };

        self.store
            .delete(table_by_id_key(table.schema.table_id).as_bytes())
            .await
            .map_err(|e| user_error("XX000", e))?;
        self.store
            .delete(table_by_name_key(table.database_id, table.schema_id, table_name).as_bytes())
            .await
            .map_err(|e| user_error("XX000", e))?;
        for index in self.list_indexes_for_table(table.schema.table_id).await? {
            self.drop_index_by_id(&index).await?;
        }

        if database_name == DEFAULT_DATABASE_NAME && schema_name == DEFAULT_SCHEMA_NAME {
            self.store
                .delete(schema_key(table_name).as_bytes())
                .await
                .map_err(|e| user_error("XX000", e))?;
        }

        Ok(Some(table))
    }

    pub async fn store_view(
        &self,
        database_name: &str,
        schema_name: &str,
        view_name: &str,
        definition: &str,
        or_replace: bool,
        if_not_exists: bool,
    ) -> PgWireResult<Option<ViewCatalog>> {
        self.ensure_bootstrap().await?;
        let database = self.get_database(database_name).await?.ok_or_else(|| {
            user_error(
                "3D000",
                format!("database '{database_name}' does not exist"),
            )
        })?;
        let schema = self
            .get_schema(database.database_id, schema_name)
            .await?
            .ok_or_else(|| user_error("3F000", format!("schema '{schema_name}' does not exist")))?;
        if self
            .load_table(database_name, schema_name, view_name)
            .await?
            .is_some()
        {
            return Err(user_error(
                "42P07",
                format!("relation '{view_name}' already exists"),
            ));
        }
        if let Some(mut existing) = self
            .load_view(database_name, schema_name, view_name)
            .await?
        {
            if if_not_exists {
                return Ok(None);
            }
            if !or_replace {
                return Err(user_error(
                    "42P07",
                    format!("view '{view_name}' already exists"),
                ));
            }
            existing.definition = definition.to_string();
            self.write_view(&existing).await?;
            return Ok(Some(existing));
        }

        let view = ViewCatalog {
            view_id: self.allocate_id(NEXT_VIEW_ID_KEY).await?,
            database_id: database.database_id,
            schema_id: schema.schema_id,
            schema_name: schema_name.to_string(),
            view_name: view_name.to_string(),
            definition: definition.to_string(),
        };
        self.write_view(&view).await?;
        Ok(Some(view))
    }

    pub async fn load_view(
        &self,
        database_name: &str,
        schema_name: &str,
        view_name: &str,
    ) -> PgWireResult<Option<ViewCatalog>> {
        self.ensure_bootstrap().await?;
        let Some(database) = self.get_database(database_name).await? else {
            return Ok(None);
        };
        let Some(schema) = self.get_schema(database.database_id, schema_name).await? else {
            return Ok(None);
        };
        let Some(id_bytes) = self
            .store
            .get(view_by_name_key(database.database_id, schema.schema_id, view_name).as_bytes())
            .await
            .map_err(|e| user_error("XX000", e))?
        else {
            return Ok(None);
        };
        let view_id = decode_u32(&id_bytes, "view id")?;
        let Some(payload) = self
            .store
            .get(view_by_id_key(view_id).as_bytes())
            .await
            .map_err(|e| user_error("XX000", e))?
        else {
            return Err(user_error("XX000", "catalog view payload missing"));
        };
        Ok(Some(decode_view_catalog(&payload)?))
    }

    pub async fn list_views(&self, database_name: &str) -> PgWireResult<Vec<ViewCatalog>> {
        self.ensure_bootstrap().await?;
        let Some(database) = self.get_database(database_name).await? else {
            return Ok(Vec::new());
        };
        let entries = self
            .store
            .scan_prefix(b"__catalog__/views/by-id/")
            .await
            .map_err(|e| user_error("XX000", e))?;
        let mut views = Vec::new();
        for (_, value) in entries {
            let view = decode_view_catalog(&value)?;
            if view.database_id == database.database_id {
                views.push(view);
            }
        }
        views.sort_by(|left, right| {
            left.schema_name
                .cmp(&right.schema_name)
                .then(left.view_name.cmp(&right.view_name))
        });
        Ok(views)
    }

    pub async fn drop_view(
        &self,
        database_name: &str,
        schema_name: &str,
        view_name: &str,
    ) -> PgWireResult<Option<ViewCatalog>> {
        self.ensure_bootstrap().await?;
        let Some(view) = self
            .load_view(database_name, schema_name, view_name)
            .await?
        else {
            return Ok(None);
        };
        self.store
            .delete(view_by_id_key(view.view_id).as_bytes())
            .await
            .map_err(|e| user_error("XX000", e))?;
        self.store
            .delete(view_by_name_key(view.database_id, view.schema_id, view_name).as_bytes())
            .await
            .map_err(|e| user_error("XX000", e))?;
        Ok(Some(view))
    }

    pub async fn drop_schema(
        &self,
        database_name: &str,
        schema_name: &str,
    ) -> PgWireResult<Option<SchemaCatalog>> {
        self.ensure_bootstrap().await?;
        let Some(database) = self.get_database(database_name).await? else {
            return Ok(None);
        };
        let Some(schema) = self.get_schema(database.database_id, schema_name).await? else {
            return Ok(None);
        };
        self.store
            .delete(schema_by_id_key(schema.schema_id).as_bytes())
            .await
            .map_err(|e| user_error("XX000", e))?;
        self.store
            .delete(schema_by_name_key(database.database_id, schema_name).as_bytes())
            .await
            .map_err(|e| user_error("XX000", e))?;
        Ok(Some(schema))
    }

    pub async fn drop_database(
        &self,
        database_name: &str,
    ) -> PgWireResult<Option<DatabaseCatalog>> {
        self.ensure_bootstrap().await?;
        let Some(database) = self.get_database(database_name).await? else {
            return Ok(None);
        };
        self.store
            .delete(database_by_id_key(database.database_id).as_bytes())
            .await
            .map_err(|e| user_error("XX000", e))?;
        self.store
            .delete(database_by_name_key(database_name).as_bytes())
            .await
            .map_err(|e| user_error("XX000", e))?;
        Ok(Some(database))
    }

    pub async fn rename_table(
        &self,
        database_name: &str,
        schema_name: &str,
        old_table_name: &str,
        new_table_name: &str,
    ) -> PgWireResult<TableCatalog> {
        self.ensure_bootstrap().await?;
        let database = self.get_database(database_name).await?.ok_or_else(|| {
            user_error(
                "3D000",
                format!("database '{database_name}' does not exist"),
            )
        })?;
        let schema_meta = self
            .get_schema(database.database_id, schema_name)
            .await?
            .ok_or_else(|| user_error("3F000", format!("schema '{schema_name}' does not exist")))?;
        if self
            .load_table(database_name, schema_name, new_table_name)
            .await?
            .is_some()
        {
            return Err(user_error(
                "42P07",
                format!("table '{new_table_name}' already exists"),
            ));
        }
        let mut table = self
            .load_table(database_name, schema_name, old_table_name)
            .await?
            .ok_or_else(|| {
                user_error("42P01", format!("table '{old_table_name}' does not exist"))
            })?;
        self.store
            .delete(
                table_by_name_key(database.database_id, schema_meta.schema_id, old_table_name)
                    .as_bytes(),
            )
            .await
            .map_err(|e| user_error("XX000", e))?;
        if database_name == DEFAULT_DATABASE_NAME && schema_name == DEFAULT_SCHEMA_NAME {
            self.store
                .delete(schema_key(old_table_name).as_bytes())
                .await
                .map_err(|e| user_error("XX000", e))?;
        }
        table.table_name = new_table_name.to_string();
        table.schema.table_name = new_table_name.to_string();
        self.store_table(database_name, schema_name, &table.schema)
            .await?;
        Ok(table)
    }

    pub async fn bump_table_epoch(
        &self,
        database_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> PgWireResult<TableCatalog> {
        let mut table = self
            .load_table(database_name, schema_name, table_name)
            .await?
            .ok_or_else(|| user_error("42P01", format!("table '{table_name}' does not exist")))?;
        table.schema.table_epoch = table.schema.table_epoch.saturating_add(1).max(1);
        table.schema.schema_version = table.schema.schema_version.max(1);
        self.store_table(database_name, schema_name, &table.schema)
            .await?;
        Ok(table)
    }

    pub async fn create_index(
        &self,
        database_name: &str,
        schema_name: &str,
        table_name: &str,
        index_name: &str,
        column_names: &[String],
        unique: bool,
        if_not_exists: bool,
    ) -> PgWireResult<Option<IndexCatalog>> {
        self.ensure_bootstrap().await?;
        let table = self
            .load_table(database_name, schema_name, table_name)
            .await?
            .ok_or_else(|| user_error("42P01", format!("table '{table_name}' does not exist")))?;
        if self
            .get_index(table.database_id, table.schema_id, index_name)
            .await?
            .is_some()
        {
            if if_not_exists {
                return Ok(None);
            }
            return Err(user_error(
                "42P07",
                format!("relation '{index_name}' already exists"),
            ));
        }
        if column_names.is_empty() {
            return Err(user_error(
                "42601",
                "CREATE INDEX requires at least one column",
            ));
        }
        let index = IndexCatalog {
            index_id: self.allocate_id(NEXT_INDEX_ID_KEY).await?,
            database_id: table.database_id,
            schema_id: table.schema_id,
            table_id: table.schema.table_id,
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
            index_name: index_name.to_string(),
            column_name: column_names[0].clone(),
            column_names: column_names.to_vec(),
            unique,
        };
        self.write_index(&index).await?;
        Ok(Some(index))
    }

    pub async fn get_index(
        &self,
        database_id: u32,
        schema_id: u32,
        index_name: &str,
    ) -> PgWireResult<Option<IndexCatalog>> {
        self.ensure_bootstrap().await?;
        let Some(id_bytes) = self
            .store
            .get(index_by_name_key(database_id, schema_id, index_name).as_bytes())
            .await
            .map_err(|e| user_error("XX000", e))?
        else {
            return Ok(None);
        };
        let index_id = decode_u32(&id_bytes, "index id")?;
        let Some(payload) = self
            .store
            .get(index_by_id_key(index_id).as_bytes())
            .await
            .map_err(|e| user_error("XX000", e))?
        else {
            return Err(user_error("XX000", "catalog index payload missing"));
        };
        Ok(Some(decode_index_catalog(&payload)?))
    }

    pub async fn list_indexes_for_table(&self, table_id: u32) -> PgWireResult<Vec<IndexCatalog>> {
        self.ensure_bootstrap().await?;
        let entries = self
            .store
            .scan_prefix(index_by_table_prefix(table_id).as_bytes())
            .await
            .map_err(|e| user_error("XX000", e))?;
        let mut indexes = Vec::with_capacity(entries.len());
        for (_, id_bytes) in entries {
            let index_id = decode_u32(&id_bytes, "index id")?;
            let Some(payload) = self
                .store
                .get(index_by_id_key(index_id).as_bytes())
                .await
                .map_err(|e| user_error("XX000", e))?
            else {
                return Err(user_error("XX000", "catalog index payload missing"));
            };
            indexes.push(decode_index_catalog(&payload)?);
        }
        indexes.sort_by(|left, right| left.index_name.cmp(&right.index_name));
        Ok(indexes)
    }

    pub async fn drop_index(
        &self,
        database_id: u32,
        schema_id: u32,
        index_name: &str,
    ) -> PgWireResult<Option<IndexCatalog>> {
        self.ensure_bootstrap().await?;
        let Some(index) = self.get_index(database_id, schema_id, index_name).await? else {
            return Ok(None);
        };
        self.drop_index_by_id(&index).await?;
        Ok(Some(index))
    }
}
