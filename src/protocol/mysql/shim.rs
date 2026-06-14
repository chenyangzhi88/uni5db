use super::*;

#[async_trait]
impl<W> AsyncMysqlShim<W> for MySqlBackend
where
    W: AsyncWrite + Send + Unpin,
{
    type Error = std::io::Error;

    fn version(&self) -> String {
        MYSQL_SERVER_VERSION.to_string()
    }

    fn server_capabilities(&self, base: CapabilityFlags) -> CapabilityFlags {
        base | CapabilityFlags::CLIENT_FOUND_ROWS
            | CapabilityFlags::CLIENT_MULTI_STATEMENTS
            | CapabilityFlags::CLIENT_MULTI_RESULTS
            | CapabilityFlags::CLIENT_CONNECT_ATTRS
    }

    fn on_client_capabilities(
        &mut self,
        capabilities: CapabilityFlags,
        connection_attrs: &[(String, String)],
    ) {
        self.client.client_capabilities = capabilities;
        self.client.connection_attrs = connection_attrs.iter().cloned().collect();
    }

    fn default_auth_plugin(&self) -> &str {
        "mysql_native_password"
    }

    async fn auth_plugin_for_username(&self, _user: &[u8]) -> &'static str {
        "mysql_native_password"
    }

    async fn authenticate(
        &self,
        auth_plugin: &str,
        _username: &[u8],
        _salt: &[u8],
        _auth_data: &[u8],
    ) -> bool {
        matches!(
            auth_plugin,
            "" | "mysql_native_password" | "caching_sha2_password" | "mysql_clear_password"
        )
    }

    async fn on_prepare<'a>(
        &'a mut self,
        query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        let param_count = Self::count_placeholders(query);
        let validation_sql = if param_count == 0 {
            query.to_string()
        } else {
            let defaults = vec!["NULL".to_string(); param_count];
            match Self::bind_prepared_sql(query, &defaults) {
                Ok(sql) => sql,
                Err(error) => {
                    return info
                        .error(ErrorKind::ER_PARSE_ERROR, error.as_bytes())
                        .await;
                }
            }
        };
        if let Err(error) = parser::parse_sql(SqlDialect::MySql, &validation_sql) {
            return info
                .error(ErrorKind::ER_PARSE_ERROR, error.to_string().as_bytes())
                .await;
        }

        let statement_id = self.next_statement_id;
        self.next_statement_id = self.next_statement_id.saturating_add(1).max(1);
        let params = (0..param_count)
            .map(Self::mysql_param_column)
            .collect::<Vec<_>>();
        let columns = Self::prepared_result_columns(&validation_sql);
        self.prepared.insert(
            statement_id,
            MySqlPreparedStatement {
                sql: query.to_string(),
                param_count,
            },
        );
        info.reply(statement_id, params.iter(), columns.iter())
            .await
    }

    async fn on_execute<'a>(
        &'a mut self,
        id: u32,
        params: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        let Some(prepared) = self.prepared.get(&id) else {
            return results
                .error(
                    ErrorKind::ER_UNKNOWN_STMT_HANDLER,
                    b"unknown prepared statement",
                )
                .await;
        };
        let prepared_sql = prepared.sql.clone();
        let param_count = prepared.param_count;
        let rendered_params = params
            .into_iter()
            .map(|param| Self::render_mysql_param_value(param.value, param.coltype))
            .collect::<Vec<_>>();
        if rendered_params.len() != param_count {
            return results
                .error(
                    ErrorKind::ER_WRONG_ARGUMENTS,
                    b"incorrect number of prepared statement parameters",
                )
                .await;
        }
        let query = match Self::bind_prepared_sql(&prepared_sql, &rendered_params) {
            Ok(query) => query,
            Err(error) => {
                return results
                    .error(ErrorKind::ER_WRONG_ARGUMENTS, error.as_bytes())
                    .await;
            }
        };
        self.handle_query(&query, results).await
    }

    async fn on_close<'a>(&'a mut self, statement_id: u32) {
        self.prepared.remove(&statement_id);
    }

    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        log::debug!("mysql query: {query}");
        self.handle_query(query, results).await
    }

    async fn on_list_fields<'a>(
        &'a mut self,
        payload: &'a [u8],
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        let table = Self::mysql_list_fields_table(payload);
        log::debug!(
            "mysql list fields: table={table:?} payload_len={}",
            payload.len()
        );
        if table.is_empty() {
            return results
                .error(ErrorKind::ER_BAD_TABLE_ERROR, b"missing table name")
                .await;
        }
        let metadata = match self
            .server
            .mysql_describe_table_fast(self.current_database(), &table)
            .await
        {
            Ok(metadata) => metadata,
            Err(error) => {
                let kind = Self::mysql_error_kind_for_pgwire(&error);
                return results.error(kind, error.to_string().as_bytes()).await;
            }
        };
        let columns = metadata
            .iter()
            .map(|column| Self::mysql_column_from_metadata(&table, column))
            .collect::<Vec<_>>();
        let rows = results.start(&columns).await?;
        rows.finish().await
    }

    fn local_infile_request(&self, query: &str) -> Option<String> {
        GatewayServer::mysql_load_data_local_filename(query)
    }

    async fn on_local_infile_start<'a>(&'a mut self, query: &'a str) -> Result<(), Self::Error> {
        self.server
            .begin_mysql_load_data_local(&self.client, query)
            .await
            .map_err(|error| io::Error::other(error.to_string()))
    }

    async fn on_local_infile_data<'a>(&'a mut self, data: &'a [u8]) -> Result<(), Self::Error> {
        self.server
            .push_mysql_load_data_local(&self.client, data)
            .await
            .map_err(|error| io::Error::other(error.to_string()))
    }

    async fn on_local_infile_end<'a>(
        &'a mut self,
        _query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        let inserted = self
            .server
            .finish_mysql_load_data_local(&self.client)
            .await
            .map_err(|error| io::Error::other(error.to_string()))?;
        results
            .completed(OkResponse {
                affected_rows: inserted as u64,
                info: format!("Records: {inserted}"),
                ..OkResponse::default()
            })
            .await
    }

    async fn on_init<'a>(
        &'a mut self,
        database: &'a str,
        writer: InitWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        log::debug!("mysql init database: {database}");
        self.client
            .metadata
            .insert(METADATA_DATABASE.to_string(), database.to_string());
        match self.ensure_session().await {
            Ok(()) => writer.ok().await,
            Err(error) => {
                let kind = Self::mysql_error_kind_for_pgwire(&error);
                writer.error(kind, error.to_string().as_bytes()).await
            }
        }
    }
}
