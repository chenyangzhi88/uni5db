use super::*;

impl MySqlBackend {
    pub(super) fn new(server: Arc<GatewayServer>) -> Self {
        Self {
            server,
            client: MySqlClientState::default(),
            next_statement_id: 1,
            prepared: std::collections::HashMap::new(),
        }
    }

    pub(super) fn normalize_query(query: &str) -> String {
        let mut query = query.trim();
        loop {
            let Some(rest) = query.strip_prefix("/*") else {
                break;
            };
            let Some(end) = rest.find("*/") else {
                break;
            };
            query = rest[end + 2..].trim_start();
        }
        query
            .trim_end_matches(';')
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_lowercase()
    }

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

    pub(super) fn mysql_error_kind_for_sqlstate(sqlstate: &str, message: &str) -> ErrorKind {
        let message_lower = message.to_ascii_lowercase();
        match sqlstate {
            "42601" => ErrorKind::ER_PARSE_ERROR,
            "42P01" => ErrorKind::ER_NO_SUCH_TABLE,
            "42P07" => ErrorKind::ER_TABLE_EXISTS_ERROR,
            "42703" => ErrorKind::ER_BAD_FIELD_ERROR,
            "3D000" => ErrorKind::ER_BAD_DB_ERROR,
            "23505" => ErrorKind::ER_DUP_ENTRY,
            "23502" => ErrorKind::ER_BAD_NULL_ERROR,
            "23503" if message_lower.contains("referenced") => ErrorKind::ER_ROW_IS_REFERENCED_2,
            "23503" => ErrorKind::ER_NO_REFERENCED_ROW_2,
            "22001" => ErrorKind::ER_DATA_TOO_LONG,
            "22003" => ErrorKind::ER_WARN_DATA_OUT_OF_RANGE,
            "22007" | "22008" | "22018" => ErrorKind::ER_TRUNCATED_WRONG_VALUE,
            "25006" => ErrorKind::ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION,
            "40001" if message_lower.contains("timeout") => ErrorKind::ER_LOCK_WAIT_TIMEOUT,
            "40001" => ErrorKind::ER_LOCK_DEADLOCK,
            "0A000" => ErrorKind::ER_NOT_SUPPORTED_YET,
            _ if message_lower.contains("lock wait timeout") => ErrorKind::ER_LOCK_WAIT_TIMEOUT,
            _ if message_lower.contains("deadlock") => ErrorKind::ER_LOCK_DEADLOCK,
            _ if message_lower.contains("duplicate") => ErrorKind::ER_DUP_ENTRY,
            _ if message_lower.contains("unknown database") => ErrorKind::ER_BAD_DB_ERROR,
            _ if message_lower.contains("does not exist") => ErrorKind::ER_NO_SUCH_TABLE,
            _ if message_lower.contains("already exists") => ErrorKind::ER_TABLE_EXISTS_ERROR,
            _ => ErrorKind::ER_UNKNOWN_ERROR,
        }
    }

    pub(super) fn mysql_error_kind_for_pgwire(error: &PgWireError) -> ErrorKind {
        match error {
            PgWireError::UserError(info) => {
                Self::mysql_error_kind_for_sqlstate(&info.code, &info.message)
            }
            PgWireError::InvalidOptionValue(_) => ErrorKind::ER_WRONG_ARGUMENTS,
            PgWireError::UnsupportedProtocolVersion(_, _)
            | PgWireError::UnsupportedSASLAuthMethod(_) => ErrorKind::ER_NOT_SUPPORTED_YET,
            _ => Self::mysql_error_kind_for_sqlstate("", &error.to_string()),
        }
    }

    pub(super) async fn ensure_session(&mut self) -> Result<(), PgWireError> {
        if self.client.bootstrapped {
            return Ok(());
        }
        self.server.post_startup(&mut self.client).await?;
        self.client.bootstrapped = true;
        Ok(())
    }

    pub(super) async fn write_gateway_responses<'a, W>(
        &'a mut self,
        responses: Vec<Response>,
        results: QueryResultWriter<'a, W>,
    ) -> std::io::Result<()>
    where
        W: AsyncWrite + Send + Unpin,
    {
        let Some(response) = responses.into_iter().next() else {
            return results.completed(OkResponse::default()).await;
        };
        match response {
            Response::EmptyQuery => results.completed(OkResponse::default()).await,
            Response::Execution(tag)
            | Response::TransactionStart(tag)
            | Response::TransactionEnd(tag) => {
                let complete: CommandComplete = tag.into();
                let mysql_insert = mysql_insert_result_from_tag(&complete.tag);
                let affected_rows = mysql_insert
                    .map(|(affected_rows, _)| affected_rows)
                    .unwrap_or_else(|| affected_rows_from_tag(&complete.tag));
                let last_insert_id = mysql_insert.map(|(_, id)| id).unwrap_or(0);
                if last_insert_id > 0 {
                    self.client.last_insert_id = last_insert_id;
                }
                let ok = OkResponse {
                    affected_rows,
                    last_insert_id,
                    warnings: self.client.warnings.len().min(u16::MAX as usize) as u16,
                    info: complete.tag,
                    ..OkResponse::default()
                };
                results.completed(ok).await
            }
            Response::Query(mut query) => {
                let columns = query
                    .row_schema()
                    .iter()
                    .map(Self::mysql_column)
                    .collect::<Vec<_>>();
                let mut rows = results.start(&columns).await?;
                while let Some(row) = query.data_rows().next().await {
                    let row = row.map_err(io::Error::from)?;
                    for value in decode_pg_text_row(&row)? {
                        rows.write_col(value.as_deref())?;
                    }
                    rows.end_row().await?;
                }
                rows.finish().await
            }
            Response::Error(error) => {
                let kind = Self::mysql_error_kind_for_sqlstate(&error.code, &error.message);
                self.client
                    .record_warning(kind as u16, error.message.clone());
                results.error(kind, error.message.as_bytes()).await
            }
            Response::CopyIn(_) | Response::CopyOut(_) | Response::CopyBoth(_) => {
                results
                    .error(
                        ErrorKind::ER_NOT_SUPPORTED_YET,
                        b"COPY is not supported by mysql protocol",
                    )
                    .await
            }
        }
    }

    pub(super) async fn single_text_row<'a, W>(
        column_name: &'static str,
        value: &str,
        results: QueryResultWriter<'a, W>,
    ) -> std::io::Result<()>
    where
        W: AsyncWrite + Send + Unpin,
    {
        let columns = [Self::text_column(column_name)];
        let mut rows = results.start(&columns).await?;
        rows.write_col(value)?;
        rows.end_row().await?;
        rows.finish().await
    }

    pub(super) async fn single_int_row<'a, W>(
        column_name: &'static str,
        value: i32,
        results: QueryResultWriter<'a, W>,
    ) -> std::io::Result<()>
    where
        W: AsyncWrite + Send + Unpin,
    {
        let columns = [Self::int_column(column_name)];
        let mut rows = results.start(&columns).await?;
        rows.write_col(value)?;
        rows.end_row().await?;
        rows.finish().await
    }

    pub(super) async fn single_text_pair_row<'a, W>(
        column_names: [&str; 2],
        values: [String; 2],
        results: QueryResultWriter<'a, W>,
    ) -> std::io::Result<()>
    where
        W: AsyncWrite + Send + Unpin,
    {
        let columns = [
            Self::text_column(column_names[0]),
            Self::text_column(column_names[1]),
        ];
        let mut rows = results.start(&columns).await?;
        rows.write_col(values[0].as_str())?;
        rows.write_col(values[1].as_str())?;
        rows.end_row().await?;
        rows.finish().await
    }

    pub(super) async fn mysql_system_variable_row<'a, W>(
        columns: Vec<Column>,
        values: Vec<MySqlSystemVariableValue>,
        results: QueryResultWriter<'a, W>,
    ) -> std::io::Result<()>
    where
        W: AsyncWrite + Send + Unpin,
    {
        let mut rows = results.start(&columns).await?;
        for value in values {
            match value {
                MySqlSystemVariableValue::Int(value) => rows.write_col(value)?,
                MySqlSystemVariableValue::Text(value) => rows.write_col(value.as_str())?,
            }
        }
        rows.end_row().await?;
        rows.finish().await
    }

    pub(super) async fn write_describe_rows<'a, W>(
        rows_data: Vec<MySqlColumnMetadata>,
        results: QueryResultWriter<'a, W>,
    ) -> std::io::Result<()>
    where
        W: AsyncWrite + Send + Unpin,
    {
        let columns = [
            Self::text_column("Field"),
            Self::text_column("Type"),
            Self::text_column("Null"),
            Self::text_column("Key"),
            Self::text_column("Default"),
            Self::text_column("Extra"),
        ];
        let mut rows = results.start(&columns).await?;
        for item in rows_data {
            rows.write_col(item.field.as_str())?;
            rows.write_col(item.column_type.as_str())?;
            rows.write_col(item.nullable.as_str())?;
            rows.write_col(item.key.as_str())?;
            rows.write_col(item.default_value.as_deref())?;
            rows.write_col(item.extra.as_str())?;
            rows.end_row().await?;
        }
        rows.finish().await
    }

    pub(super) async fn write_show_tables<'a, W>(
        table_column: &str,
        tables: Vec<String>,
        full: bool,
        results: QueryResultWriter<'a, W>,
    ) -> std::io::Result<()>
    where
        W: AsyncWrite + Send + Unpin,
    {
        let table_column = Self::text_column(table_column);
        if full {
            let columns = [table_column, Self::text_column("Table_type")];
            let mut rows = results.start(&columns).await?;
            for table in tables {
                rows.write_col(table.as_str())?;
                rows.write_col("BASE TABLE")?;
                rows.end_row().await?;
            }
            rows.finish().await
        } else {
            let columns = [table_column];
            let mut rows = results.start(&columns).await?;
            for table in tables {
                rows.write_col(table.as_str())?;
                rows.end_row().await?;
            }
            rows.finish().await
        }
    }

    pub(super) async fn write_show_databases<'a, W>(
        databases: Vec<String>,
        results: QueryResultWriter<'a, W>,
    ) -> std::io::Result<()>
    where
        W: AsyncWrite + Send + Unpin,
    {
        let columns = [Self::text_column("Database")];
        let mut rows = results.start(&columns).await?;
        for database in databases {
            rows.write_col(database.as_str())?;
            rows.end_row().await?;
        }
        rows.finish().await
    }

    pub(super) async fn run_gateway_query<'a, W>(
        &'a mut self,
        query: &str,
        results: QueryResultWriter<'a, W>,
    ) -> std::io::Result<()>
    where
        W: AsyncWrite + Send + Unpin,
    {
        if let Err(error) = self.ensure_session().await {
            let kind = Self::mysql_error_kind_for_pgwire(&error);
            return results.error(kind, error.to_string().as_bytes()).await;
        }
        self.client.clear_warnings();
        match self.server.run_query(&mut self.client, query).await {
            Ok(responses) => self.write_gateway_responses(responses, results).await,
            Err(error) => {
                let kind = Self::mysql_error_kind_for_pgwire(&error);
                self.client.record_warning(kind as u16, error.to_string());
                results.error(kind, error.to_string().as_bytes()).await
            }
        }
    }

    pub(super) async fn switch_database<'a, W>(
        &'a mut self,
        database: &str,
        results: QueryResultWriter<'a, W>,
    ) -> std::io::Result<()>
    where
        W: AsyncWrite + Send + Unpin,
    {
        self.client
            .metadata
            .insert(METADATA_DATABASE.to_string(), database.to_string());
        match self.ensure_session().await {
            Ok(()) => results.completed(OkResponse::default()).await,
            Err(error) => {
                let kind = Self::mysql_error_kind_for_pgwire(&error);
                results.error(kind, error.to_string().as_bytes()).await
            }
        }
    }

    pub(super) fn current_database(&self) -> &str {
        self.client
            .metadata
            .get(METADATA_DATABASE)
            .map(String::as_str)
            .unwrap_or(DEFAULT_DATABASE_NAME)
    }

    pub(super) fn session_value(&self, key: &str, default: &'static str) -> String {
        self.client
            .metadata
            .get(key)
            .cloned()
            .unwrap_or_else(|| default.to_string())
    }

    pub(super) fn mysql_system_variable_select(
        &self,
        normalized: &str,
    ) -> Option<(Vec<Column>, Vec<MySqlSystemVariableValue>)> {
        let mut select_list = normalized.strip_prefix("select ")?;
        if !select_list.starts_with("@@") {
            return None;
        }
        if let Some((before_from, from_target)) = select_list.rsplit_once(" from ") {
            if from_target.trim() != "dual" {
                return None;
            }
            select_list = before_from.trim();
        }
        if let Some((before_limit, limit)) = select_list.rsplit_once(" limit ")
            && limit.trim().parse::<u64>().ok() == Some(1)
        {
            select_list = before_limit.trim();
        }

        let mut columns = Vec::new();
        let mut values = Vec::new();
        for expr in select_list.split(',') {
            let expr = expr.trim();
            let (variable_expr, alias) = Self::mysql_select_expr_alias(expr);
            let value = self.mysql_system_variable_value(variable_expr)?;
            let column_name = alias.unwrap_or(variable_expr);
            columns.push(match value {
                MySqlSystemVariableValue::Int(_) => Self::int_column(column_name),
                MySqlSystemVariableValue::Text(_) => Self::text_column(column_name),
            });
            values.push(value);
        }
        (!columns.is_empty()).then_some((columns, values))
    }

    pub(super) fn mysql_select_expr_alias(expr: &str) -> (&str, Option<&str>) {
        if let Some((left, right)) = expr.rsplit_once(" as ") {
            let alias = right.trim().trim_matches('`').trim_matches('"');
            if !alias.is_empty() {
                return (left.trim(), Some(alias));
            }
        }
        (expr, None)
    }

    pub(super) fn mysql_system_variable_value(
        &self,
        variable_expr: &str,
    ) -> Option<MySqlSystemVariableValue> {
        let variable = variable_expr
            .trim()
            .trim_start_matches("@@")
            .strip_prefix("session.")
            .or_else(|| {
                variable_expr
                    .trim()
                    .trim_start_matches("@@")
                    .strip_prefix("global.")
            })
            .unwrap_or_else(|| variable_expr.trim().trim_start_matches("@@"));

        match variable {
            "auto_increment_increment" => Some(MySqlSystemVariableValue::Int(1)),
            "autocommit" => Some(MySqlSystemVariableValue::Int(
                self.session_value(MYSQL_AUTOCOMMIT, "1")
                    .parse::<i32>()
                    .unwrap_or(1),
            )),
            "character_set_client" => Some(MySqlSystemVariableValue::Text(
                self.session_value(MYSQL_CHARACTER_SET_CLIENT, "utf8mb4"),
            )),
            "character_set_connection" => Some(MySqlSystemVariableValue::Text(
                self.session_value(MYSQL_CHARACTER_SET_CONNECTION, "utf8mb4"),
            )),
            "character_set_results" => Some(MySqlSystemVariableValue::Text(
                self.session_value(MYSQL_CHARACTER_SET_RESULTS, "utf8mb4"),
            )),
            "collation_connection" => Some(MySqlSystemVariableValue::Text(
                self.session_value(MYSQL_COLLATION_CONNECTION, "utf8mb4_0900_ai_ci"),
            )),
            "innodb_lock_wait_timeout" => Some(MySqlSystemVariableValue::Int(
                self.session_value(MYSQL_LOCK_WAIT_TIMEOUT, "50")
                    .parse::<i32>()
                    .unwrap_or(50),
            )),
            "license" => Some(MySqlSystemVariableValue::Text("GPL".to_string())),
            "lower_case_table_names" => Some(MySqlSystemVariableValue::Int(0)),
            "max_allowed_packet" => Some(MySqlSystemVariableValue::Int(67_108_864)),
            "net_buffer_length" => Some(MySqlSystemVariableValue::Int(16_384)),
            "net_write_timeout" => Some(MySqlSystemVariableValue::Int(60)),
            "query_cache_size" => Some(MySqlSystemVariableValue::Int(0)),
            "read_only" | "transaction_read_only" => Some(MySqlSystemVariableValue::Int(0)),
            "sql_mode" => Some(MySqlSystemVariableValue::Text(
                self.session_value(MYSQL_SQL_MODE, MYSQL_DEFAULT_SQL_MODE),
            )),
            "system_time_zone" => Some(MySqlSystemVariableValue::Text("UTC".to_string())),
            "time_zone" => Some(MySqlSystemVariableValue::Text(
                self.session_value(MYSQL_TIME_ZONE, "SYSTEM"),
            )),
            "transaction_isolation" | "tx_isolation" => Some(MySqlSystemVariableValue::Text(
                self.session_value(MYSQL_TRANSACTION_ISOLATION, "REPEATABLE-READ"),
            )),
            "version" => Some(MySqlSystemVariableValue::Text(
                MYSQL_SERVER_VERSION.to_string(),
            )),
            "version_comment" => Some(MySqlSystemVariableValue::Text(
                "UniDB MySQL compatibility layer".to_string(),
            )),
            "wait_timeout" => Some(MySqlSystemVariableValue::Int(28_800)),
            "warning_count" => Some(MySqlSystemVariableValue::Int(
                self.client.warnings.len().min(i32::MAX as usize) as i32,
            )),
            _ => None,
        }
    }

    pub(super) fn strip_mysql_identifier(value: &str) -> String {
        value
            .trim()
            .trim_end_matches(';')
            .trim_matches('`')
            .trim_matches('"')
            .trim_matches('\'')
            .to_string()
    }

    pub(super) fn first_identifier_after<'a>(query: &'a str, prefixes: &[&str]) -> Option<String> {
        let trimmed = query.trim().trim_end_matches(';').trim();
        let lower = trimmed.to_ascii_lowercase();
        for prefix in prefixes {
            if let Some(rest) = lower.strip_prefix(prefix) {
                let raw = &trimmed[trimmed.len() - rest.len()..];
                let ident = raw.split_whitespace().next()?;
                return Some(Self::strip_mysql_identifier(ident));
            }
        }
        None
    }

    pub(super) fn sql_string(value: &str) -> String {
        format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
    }

    pub(super) fn show_like_pattern(query: &str) -> Option<String> {
        let trimmed = query.trim().trim_end_matches(';').trim();
        let lower = trimmed.to_ascii_lowercase();
        let idx = lower.find(" like ")?;
        Some(Self::strip_mysql_identifier(&trimmed[idx + 6..]))
    }

    pub(super) fn set_mysql_session_variable(&mut self, normalized: &str, original: &str) -> bool {
        if normalized.starts_with("set names ") {
            let charset = original
                .trim()
                .trim_end_matches(';')
                .split_whitespace()
                .nth(2)
                .map(Self::strip_mysql_identifier)
                .unwrap_or_else(|| "utf8mb4".to_string());
            for key in [
                MYSQL_CHARACTER_SET_CLIENT,
                MYSQL_CHARACTER_SET_CONNECTION,
                MYSQL_CHARACTER_SET_RESULTS,
            ] {
                self.client
                    .metadata
                    .insert(key.to_string(), charset.clone());
            }
            return true;
        }
        if normalized.starts_with("set character set ") {
            let charset = original
                .trim()
                .trim_end_matches(';')
                .split_whitespace()
                .nth(3)
                .map(Self::strip_mysql_identifier)
                .unwrap_or_else(|| "utf8mb4".to_string());
            self.client
                .metadata
                .insert(MYSQL_CHARACTER_SET_CLIENT.to_string(), charset.clone());
            self.client
                .metadata
                .insert(MYSQL_CHARACTER_SET_CONNECTION.to_string(), charset.clone());
            self.client
                .metadata
                .insert(MYSQL_CHARACTER_SET_RESULTS.to_string(), charset);
            return true;
        }
        for (prefix, key) in [
            ("set autocommit", MYSQL_AUTOCOMMIT),
            ("set session autocommit", MYSQL_AUTOCOMMIT),
            ("set @@autocommit", MYSQL_AUTOCOMMIT),
            ("set sql_mode", MYSQL_SQL_MODE),
            ("set session sql_mode", MYSQL_SQL_MODE),
            ("set time_zone", MYSQL_TIME_ZONE),
            ("set session time_zone", MYSQL_TIME_ZONE),
            ("set innodb_lock_wait_timeout", MYSQL_LOCK_WAIT_TIMEOUT),
            (
                "set session innodb_lock_wait_timeout",
                MYSQL_LOCK_WAIT_TIMEOUT,
            ),
            ("set @@innodb_lock_wait_timeout", MYSQL_LOCK_WAIT_TIMEOUT),
        ] {
            if normalized.starts_with(prefix) {
                let mut value = original
                    .split_once('=')
                    .map(|(_, value)| Self::strip_mysql_identifier(value))
                    .unwrap_or_default();
                if key == MYSQL_AUTOCOMMIT {
                    value = if value == "0" || value.eq_ignore_ascii_case("off") {
                        "0".to_string()
                    } else {
                        "1".to_string()
                    };
                } else if key == MYSQL_SQL_MODE && value.eq_ignore_ascii_case("default") {
                    value = MYSQL_DEFAULT_SQL_MODE.to_string();
                }
                self.client.metadata.insert(key.to_string(), value);
                return true;
            }
        }
        false
    }

    pub(super) fn mysql_transaction_isolation_sql(normalized: &str) -> Option<&'static str> {
        if !normalized.contains("transaction isolation level") {
            return None;
        }
        if normalized.contains("read committed") {
            Some("read committed")
        } else if normalized.contains("repeatable read") {
            Some("repeatable read")
        } else if normalized.contains("serializable") {
            Some("serializable")
        } else {
            None
        }
    }

    pub(super) fn is_transaction_control(normalized: &str) -> bool {
        matches!(
            normalized,
            "begin" | "start transaction" | "commit" | "end" | "rollback"
        )
    }

    pub(super) fn autocommit_disabled(&self) -> bool {
        self.client
            .metadata
            .get(MYSQL_AUTOCOMMIT)
            .is_some_and(|value| value == "0" || value.eq_ignore_ascii_case("off"))
    }

    pub(super) fn count_placeholders(sql: &str) -> usize {
        let mut count = 0usize;
        let mut chars = sql.chars().peekable();
        let mut quote = None;
        while let Some(ch) = chars.next() {
            if let Some(active) = quote {
                if ch == '\\' {
                    let _ = chars.next();
                } else if ch == active {
                    quote = None;
                }
                continue;
            }
            match ch {
                '\'' | '"' | '`' => quote = Some(ch),
                '?' => count += 1,
                '-' if chars.peek() == Some(&'-') => {
                    for next in chars.by_ref() {
                        if next == '\n' {
                            break;
                        }
                    }
                }
                '#' => {
                    for next in chars.by_ref() {
                        if next == '\n' {
                            break;
                        }
                    }
                }
                _ => {}
            }
        }
        count
    }

    pub(super) fn bind_prepared_sql(sql: &str, params: &[String]) -> Result<String, String> {
        let mut output = String::with_capacity(sql.len() + params.len() * 8);
        let mut param_idx = 0usize;
        let mut chars = sql.chars().peekable();
        let mut quote = None;
        while let Some(ch) = chars.next() {
            if let Some(active) = quote {
                output.push(ch);
                if ch == '\\' {
                    if let Some(next) = chars.next() {
                        output.push(next);
                    }
                } else if ch == active {
                    quote = None;
                }
                continue;
            }
            match ch {
                '\'' | '"' | '`' => {
                    quote = Some(ch);
                    output.push(ch);
                }
                '?' => {
                    let Some(value) = params.get(param_idx) else {
                        return Err("not enough prepared statement parameters".to_string());
                    };
                    output.push_str(value);
                    param_idx += 1;
                }
                '-' if chars.peek() == Some(&'-') => {
                    output.push(ch);
                    if let Some(next) = chars.next() {
                        output.push(next);
                    }
                    for next in chars.by_ref() {
                        output.push(next);
                        if next == '\n' {
                            break;
                        }
                    }
                }
                '#' => {
                    output.push(ch);
                    for next in chars.by_ref() {
                        output.push(next);
                        if next == '\n' {
                            break;
                        }
                    }
                }
                _ => output.push(ch),
            }
        }
        if param_idx != params.len() {
            return Err("too many prepared statement parameters".to_string());
        }
        Ok(output)
    }

    pub(super) fn render_mysql_param_value(value: Value<'_>, coltype: ColumnType) -> String {
        match value.into_inner() {
            ValueInner::NULL => "NULL".to_string(),
            ValueInner::Bytes(bytes) if Self::mysql_binary_param_type(coltype) => {
                Self::render_mysql_hex_param(bytes)
            }
            ValueInner::Bytes(bytes) => Self::render_mysql_bytes_param(bytes),
            ValueInner::Int(value) => value.to_string(),
            ValueInner::UInt(value) => value.to_string(),
            ValueInner::Double(value) => {
                if value.is_finite() {
                    value.to_string()
                } else {
                    "NULL".to_string()
                }
            }
            ValueInner::Date(bytes) => render_mysql_date(bytes),
            ValueInner::Datetime(bytes) => render_mysql_datetime(bytes),
            ValueInner::Time(bytes) => render_mysql_time(bytes),
        }
    }

    pub(super) fn mysql_binary_param_type(coltype: ColumnType) -> bool {
        matches!(
            coltype,
            ColumnType::MYSQL_TYPE_BLOB
                | ColumnType::MYSQL_TYPE_TINY_BLOB
                | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
                | ColumnType::MYSQL_TYPE_LONG_BLOB
                | ColumnType::MYSQL_TYPE_BIT
                | ColumnType::MYSQL_TYPE_GEOMETRY
        )
    }

    pub(super) fn render_mysql_bytes_param(bytes: &[u8]) -> String {
        if let Ok(value) = std::str::from_utf8(bytes)
            && !value.contains('\0')
        {
            return format!("'{}'", escape_sql_string(value));
        }
        Self::render_mysql_hex_param(bytes)
    }

    pub(super) fn render_mysql_hex_param(bytes: &[u8]) -> String {
        let mut hex = String::with_capacity(bytes.len() * 2 + 3);
        hex.push_str("X'");
        for byte in bytes {
            use std::fmt::Write as _;
            let _ = write!(&mut hex, "{byte:02x}");
        }
        hex.push('\'');
        hex
    }
}
