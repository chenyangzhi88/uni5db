use std::time::Instant;

use pgwire::api::ClientInfo;
use pgwire::api::results::{CopyResponse, Response};
use pgwire::error::PgWireResult;
use sqlparser::ast::{CopyLegacyCsvOption, CopyLegacyOption, CopyOption, Ident, ObjectName};

use super::GatewayServer;
use super::shared::{
    COPY_WRITE_BATCH_MAX_ENTRIES, COPY_WRITE_BATCH_MAX_ROWS, CopyInFormat, CopyInOptions,
    CopyInState, MySqlLoadDataLocalSpec, SessionCatalog, log_copy_profile,
    parse_mysql_identifier_token, parse_mysql_quoted_literal, single_char_literal,
    strip_mysql_identifier_token,
};
use crate::catalog::object_name_to_string;
use crate::codec::{index_entry_key, index_entry_prefix};
use crate::error::{unsupported, user_error};
use crate::storage_layout;
use crate::types::{ColumnValue, DataType, RowMap};

impl GatewayServer {
    pub(super) fn parse_copy_in_options(
        options: &[CopyOption],
        legacy_options: &[CopyLegacyOption],
    ) -> PgWireResult<CopyInOptions> {
        let mut parsed = CopyInOptions::default();
        for option in options {
            match option {
                CopyOption::Format(format) if format.value.eq_ignore_ascii_case("text") => {
                    parsed.format = CopyInFormat::Text;
                    parsed.delimiter = '\t';
                }
                CopyOption::Format(format) if format.value.eq_ignore_ascii_case("csv") => {
                    parsed.format = CopyInFormat::Csv;
                    parsed.delimiter = ',';
                    parsed.null = String::new();
                }
                CopyOption::Format(format) if format.value.eq_ignore_ascii_case("binary") => {
                    return Err(unsupported("binary COPY is not supported yet"));
                }
                CopyOption::Format(format) => {
                    return Err(unsupported(format!(
                        "COPY format '{}' is not supported",
                        format.value
                    )));
                }
                CopyOption::Freeze(_) => {}
                CopyOption::Delimiter(delimiter) => parsed.delimiter = *delimiter,
                CopyOption::Null(null) => parsed.null = null.clone(),
                CopyOption::Header(header) => parsed.header = *header,
                CopyOption::Quote(quote) => parsed.quote = *quote,
                CopyOption::Escape(escape) => parsed.escape = *escape,
                CopyOption::Encoding(encoding)
                    if encoding.eq_ignore_ascii_case("utf8")
                        || encoding.eq_ignore_ascii_case("utf-8") => {}
                CopyOption::Encoding(_) => {
                    return Err(unsupported(
                        "COPY ENCODING other than UTF8 is not supported yet",
                    ));
                }
                CopyOption::ForceQuote(_)
                | CopyOption::ForceNotNull(_)
                | CopyOption::ForceNull(_) => {
                    return Err(unsupported("COPY FORCE_* options are not supported yet"));
                }
            }
        }

        for option in legacy_options {
            match option {
                CopyLegacyOption::Binary => {
                    return Err(unsupported("binary COPY is not supported yet"));
                }
                CopyLegacyOption::Delimiter(delimiter) => parsed.delimiter = *delimiter,
                CopyLegacyOption::Null(null) => parsed.null = null.clone(),
                CopyLegacyOption::Csv(csv_options) => {
                    parsed.format = CopyInFormat::Csv;
                    parsed.delimiter = ',';
                    if parsed.null == "\\N" {
                        parsed.null = String::new();
                    }
                    for csv_option in csv_options {
                        match csv_option {
                            CopyLegacyCsvOption::Header => parsed.header = true,
                            CopyLegacyCsvOption::Quote(quote) => parsed.quote = *quote,
                            CopyLegacyCsvOption::Escape(escape) => parsed.escape = *escape,
                            CopyLegacyCsvOption::ForceQuote(_)
                            | CopyLegacyCsvOption::ForceNotNull(_) => {
                                return Err(unsupported(
                                    "COPY CSV FORCE options are not supported yet",
                                ));
                            }
                        }
                    }
                }
                _ => return Err(unsupported("COPY legacy option is not supported yet")),
            }
        }

        if parsed.delimiter.len_utf8() != 1 {
            return Err(user_error(
                "22023",
                "COPY delimiter must be a single-byte character",
            ));
        }
        Ok(parsed)
    }

    pub(crate) fn mysql_load_data_local_filename(query: &str) -> Option<String> {
        Self::parse_mysql_load_data_local(query)
            .ok()
            .map(|spec| spec.filename)
    }

    pub(super) fn parse_mysql_load_data_local(query: &str) -> PgWireResult<MySqlLoadDataLocalSpec> {
        let trimmed = query.trim().trim_end_matches(';').trim();
        let lower = trimmed.to_ascii_lowercase();
        let prefix = "load data local infile";
        if !lower.starts_with(prefix) {
            return Err(unsupported("not a LOAD DATA LOCAL INFILE statement"));
        }

        let mut rest = trimmed[prefix.len()..].trim_start();
        let (filename, after_filename) = parse_mysql_quoted_literal(rest)
            .ok_or_else(|| user_error("42000", "LOAD DATA LOCAL INFILE requires a filename"))?;
        rest = after_filename.trim_start();
        let rest_lower = rest.to_ascii_lowercase();
        if !rest_lower.starts_with("into table") {
            return Err(user_error(
                "42000",
                "LOAD DATA LOCAL INFILE requires INTO TABLE",
            ));
        }
        rest = rest["into table".len()..].trim_start();
        let (table_name, after_table) = parse_mysql_identifier_token(rest)
            .ok_or_else(|| user_error("42000", "LOAD DATA requires a table name"))?;
        rest = after_table.trim_start();

        let mut options = CopyInOptions::default();
        let mut columns = Vec::new();
        loop {
            let rest_trimmed = rest.trim_start();
            if rest_trimmed.is_empty() {
                break;
            }
            if let Some(column_start) = rest_trimmed.strip_prefix('(') {
                let Some(end_idx) = column_start.rfind(')') else {
                    return Err(user_error("42000", "LOAD DATA column list is unterminated"));
                };
                columns = column_start[..end_idx]
                    .split(',')
                    .map(|name| strip_mysql_identifier_token(name.trim()).to_string())
                    .filter(|name| !name.is_empty())
                    .collect();
                rest = column_start[end_idx + 1..].trim_start();
                continue;
            }

            let lower_rest = rest_trimmed.to_ascii_lowercase();
            if lower_rest.starts_with("fields terminated by")
                || lower_rest.starts_with("columns terminated by")
            {
                let keyword_len = if lower_rest.starts_with("fields terminated by") {
                    "fields terminated by".len()
                } else {
                    "columns terminated by".len()
                };
                let literal_rest = rest_trimmed[keyword_len..].trim_start();
                let (value, after) = parse_mysql_quoted_literal(literal_rest).ok_or_else(|| {
                    user_error("42000", "FIELDS TERMINATED BY requires a string literal")
                })?;
                options.delimiter = single_char_literal(&value, "FIELDS TERMINATED BY")?;
                rest = after;
                continue;
            }
            if lower_rest.starts_with("lines terminated by") {
                let literal_rest = rest_trimmed["lines terminated by".len()..].trim_start();
                let (value, after) = parse_mysql_quoted_literal(literal_rest).ok_or_else(|| {
                    user_error("42000", "LINES TERMINATED BY requires a string literal")
                })?;
                if value != "\n" {
                    return Err(unsupported(
                        "LOAD DATA currently supports only LINES TERMINATED BY '\\n'",
                    ));
                }
                rest = after;
                continue;
            }
            if lower_rest.starts_with("ignore ") {
                let after_ignore = rest_trimmed["ignore ".len()..].trim_start();
                let digits = after_ignore
                    .chars()
                    .take_while(|ch| ch.is_ascii_digit())
                    .collect::<String>();
                let count = digits.parse::<usize>().unwrap_or(0);
                let after_digits = after_ignore[digits.len()..].trim_start();
                if !after_digits.to_ascii_lowercase().starts_with("lines") {
                    return Err(user_error("42000", "IGNORE requires LINES"));
                }
                if count > 1 {
                    return Err(unsupported(
                        "LOAD DATA currently supports only IGNORE 1 LINES",
                    ));
                }
                options.header = count == 1;
                rest = after_digits["lines".len()..].trim_start();
                continue;
            }
            return Err(unsupported(format!(
                "unsupported LOAD DATA clause near '{}'",
                rest_trimmed.chars().take(48).collect::<String>()
            )));
        }

        Ok(MySqlLoadDataLocalSpec {
            filename,
            table_name,
            columns,
            options,
        })
    }

    pub(super) async fn begin_copy_from_stdin<C>(
        &self,
        client: &C,
        session: &SessionCatalog,
        table_name: ObjectName,
        columns: Vec<Ident>,
        mut options: CopyInOptions,
        use_session_transaction: bool,
    ) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo,
    {
        let table_name = object_name_to_string(&table_name)?;
        let (schema_name, schema) = self
            .resolve_table_schema(&session.database_name, &session.search_path, &table_name)
            .await?;
        let schema = schema
            .ok_or_else(|| user_error("42P01", format!("table '{table_name}' does not exist")))?;
        let columns = if columns.is_empty() {
            schema.column_names()
        } else {
            columns
                .into_iter()
                .map(|ident| ident.value)
                .collect::<Vec<_>>()
        };

        for column_name in &columns {
            if schema.find_column(column_name).is_none() {
                return Err(user_error(
                    "42703",
                    format!("column '{column_name}' not found"),
                ));
            }
        }

        let indexes = self.catalog.list_indexes_for_table(schema.table_id).await?;
        options.header_pending = options.header;
        let session_id = self.session_id(client);
        self.active_copy_in.lock().await.insert(
            session_id,
            CopyInState {
                session_id,
                use_session_transaction,
                database_name: session.database_name.clone(),
                schema_name,
                schema,
                indexes,
                columns: columns.clone(),
                options,
                buffer: Vec::new(),
                pending_writes: Vec::new(),
                pending_rows: 0,
                inserted_rows: 0,
            },
        );

        Ok(vec![Response::CopyIn(CopyResponse::new(
            0,
            columns.len(),
            vec![0; columns.len()],
        ))])
    }

    pub(crate) async fn begin_mysql_load_data_local<C>(
        &self,
        client: &C,
        query: &str,
    ) -> PgWireResult<()>
    where
        C: ClientInfo,
    {
        let spec = Self::parse_mysql_load_data_local(query)?;
        let session = self.session_catalog(client);
        let table_name = spec
            .table_name
            .rsplit_once('.')
            .map(|(_, table)| table.to_string())
            .unwrap_or_else(|| spec.table_name.clone());
        let (schema_name, schema) = self
            .resolve_table_schema(&session.database_name, &session.search_path, &table_name)
            .await?;
        let schema = schema
            .ok_or_else(|| user_error("42P01", format!("table '{table_name}' does not exist")))?;
        let columns = if spec.columns.is_empty() {
            schema.column_names()
        } else {
            spec.columns
        };
        for column_name in &columns {
            if schema.find_column(column_name).is_none() {
                return Err(user_error(
                    "42703",
                    format!("column '{column_name}' not found"),
                ));
            }
        }
        let indexes = self.catalog.list_indexes_for_table(schema.table_id).await?;
        let session_id = self.session_id(client);
        let mut options = spec.options;
        options.header_pending = options.header;
        self.active_copy_in.lock().await.insert(
            session_id,
            CopyInState {
                session_id,
                use_session_transaction: self.has_active_transaction(client).await,
                database_name: session.database_name,
                schema_name,
                schema,
                indexes,
                columns,
                options,
                buffer: Vec::new(),
                pending_writes: Vec::new(),
                pending_rows: 0,
                inserted_rows: 0,
            },
        );
        Ok(())
    }

    pub(crate) async fn push_mysql_load_data_local<C>(
        &self,
        client: &C,
        data: &[u8],
    ) -> PgWireResult<()>
    where
        C: ClientInfo,
    {
        let session_id = self.session_id(client);
        let mut state = self
            .active_copy_in
            .lock()
            .await
            .remove(&session_id)
            .ok_or_else(|| user_error("XX000", "LOAD DATA LOCAL state not initialized"))?;
        state.buffer.extend_from_slice(data);
        let result = self.flush_copy_buffer(session_id, &mut state, false).await;
        self.active_copy_in.lock().await.insert(session_id, state);
        result
    }

    pub(crate) async fn finish_mysql_load_data_local<C>(&self, client: &C) -> PgWireResult<usize>
    where
        C: ClientInfo,
    {
        let session_id = self.session_id(client);
        let mut state = self
            .active_copy_in
            .lock()
            .await
            .remove(&session_id)
            .ok_or_else(|| user_error("XX000", "LOAD DATA LOCAL state not initialized"))?;
        self.flush_copy_buffer(session_id, &mut state, true).await?;
        self.flush_copy_writes(&mut state).await?;
        if !state.use_session_transaction {
            self.store
                .sync_wal()
                .await
                .map_err(|e| user_error("XX000", e))?;
        }
        Ok(state.inserted_rows)
    }

    pub(super) fn decode_copy_field(
        &self,
        raw: &str,
        data_type: &DataType,
        options: &CopyInOptions,
    ) -> PgWireResult<ColumnValue> {
        if raw == options.null {
            return Ok(ColumnValue::Null);
        }

        match data_type {
            DataType::Int16 => raw
                .parse::<i16>()
                .map(ColumnValue::Int16)
                .map_err(|e| user_error("22P02", format!("invalid INT2 value '{raw}': {e}"))),
            DataType::Int32 => raw
                .parse::<i32>()
                .map(ColumnValue::Int32)
                .map_err(|e| user_error("22P02", format!("invalid INT4 value '{raw}': {e}"))),
            DataType::Int64 => raw
                .parse::<i64>()
                .map(ColumnValue::Int64)
                .map_err(|e| user_error("22P02", format!("invalid INT8 value '{raw}': {e}"))),
            DataType::Boolean => match raw.to_ascii_lowercase().as_str() {
                "t" | "true" | "1" => Ok(ColumnValue::Boolean(true)),
                "f" | "false" | "0" => Ok(ColumnValue::Boolean(false)),
                _ => Err(user_error("22P02", format!("invalid BOOL value '{raw}'"))),
            },
            _ => crate::sql::parse_text_for_type(raw, data_type),
        }
    }

    pub(super) fn split_copy_fields(
        text: &str,
        options: &CopyInOptions,
    ) -> PgWireResult<Vec<String>> {
        match options.format {
            CopyInFormat::Text => Ok(text
                .split(options.delimiter)
                .map(str::to_string)
                .collect::<Vec<_>>()),
            CopyInFormat::Csv => Self::split_csv_copy_fields(text, options),
        }
    }

    pub(super) fn split_csv_copy_fields(
        text: &str,
        options: &CopyInOptions,
    ) -> PgWireResult<Vec<String>> {
        let mut fields = Vec::new();
        let mut field = String::new();
        let mut in_quotes = false;
        let mut chars = text.chars().peekable();
        while let Some(ch) = chars.next() {
            if in_quotes {
                if ch == options.escape && chars.peek() == Some(&options.quote) {
                    field.push(options.quote);
                    chars.next();
                } else if ch == options.quote {
                    in_quotes = false;
                } else {
                    field.push(ch);
                }
            } else if ch == options.quote {
                in_quotes = true;
            } else if ch == options.delimiter {
                fields.push(std::mem::take(&mut field));
            } else {
                field.push(ch);
            }
        }
        if in_quotes {
            return Err(user_error("22P04", "unterminated CSV quoted field"));
        }
        fields.push(field);
        Ok(fields)
    }

    pub(super) async fn flush_copy_buffer(
        &self,
        session_id: i32,
        state: &mut CopyInState,
        final_flush: bool,
    ) -> PgWireResult<()> {
        let mut start = 0usize;
        let mut end = 0usize;
        while end < state.buffer.len() {
            if state.buffer[end] != b'\n' {
                end += 1;
                continue;
            }

            let line = state.buffer[start..end].to_vec();
            self.apply_copy_line(session_id, state, &line).await?;
            start = end + 1;
            end += 1;
        }

        if final_flush && start < state.buffer.len() {
            let line = state.buffer[start..].to_vec();
            self.apply_copy_line(session_id, state, &line).await?;
            start = state.buffer.len();
        }

        if start > 0 {
            state.buffer.drain(..start);
        }
        Ok(())
    }

    pub(super) async fn apply_copy_line(
        &self,
        _session_id: i32,
        state: &mut CopyInState,
        line: &[u8],
    ) -> PgWireResult<()> {
        if line.is_empty() {
            return Ok(());
        }

        let line = if let Some(stripped) = line.strip_suffix(b"\r") {
            stripped
        } else {
            line
        };
        if line == br"\." {
            return Ok(());
        }
        let text = std::str::from_utf8(line)
            .map_err(|e| user_error("22021", format!("COPY input is not valid UTF-8: {e}")))?;
        if state.options.header_pending {
            state.options.header_pending = false;
            return Ok(());
        }
        let fields = Self::split_copy_fields(text, &state.options)?;
        if fields.len() != state.columns.len() {
            return Err(user_error(
                "22P04",
                format!(
                    "COPY row has {} fields but target expects {}",
                    fields.len(),
                    state.columns.len()
                ),
            ));
        }

        let mut row = RowMap::new();
        for (column_name, raw) in state.columns.iter().zip(fields.into_iter()) {
            let column = state
                .schema
                .find_column(column_name)
                .ok_or_else(|| user_error("42703", format!("column '{column_name}' not found")))?;
            row.insert(
                column_name.clone(),
                self.decode_copy_field(&raw, &column.data_type, &state.options)?,
            );
        }

        let pk_value = if state.schema.has_user_primary_key() {
            row.get(&state.schema.primary_key)
                .cloned()
                .unwrap_or(ColumnValue::Null)
        } else {
            self.allocate_internal_row_id(state.schema.table_id).await?
        };
        if pk_value.is_null() {
            return Err(user_error(
                "23502",
                format!("null value for primary key '{}'", state.schema.primary_key),
            ));
        }
        Self::validate_row_constraints(&state.schema, &row)?;
        state.pending_writes.push((
            storage_layout::row_key(state.schema.table_id, state.schema.table_epoch, &pk_value),
            storage_layout::encode_row_record(&state.schema, &row),
        ));
        for index in state.indexes.iter() {
            let index_value = Self::indexed_value(&row, index);
            let v2_key = storage_layout::index_entry_key(index.index_id, &index_value, &pk_value);
            let key = index_entry_key(
                &state.database_name,
                &state.schema_name,
                &state.schema.table_name,
                &index.index_name,
                &index_value,
                &pk_value,
            );
            if index.unique && !Self::index_value_has_null(&index_value) {
                let v2_prefix = storage_layout::index_prefix(index.index_id, &index_value);
                let v2_range = storage_layout::RangeScan {
                    end: {
                        let mut upper = v2_prefix.clone();
                        for idx in (0..upper.len()).rev() {
                            if upper[idx] != u8::MAX {
                                upper[idx] += 1;
                                upper.truncate(idx + 1);
                                break;
                            }
                        }
                        Some(upper)
                    },
                    start: v2_prefix.clone(),
                    limit: None,
                    reverse: false,
                };
                for (key, _) in self
                    .txn_scan_range(
                        state.use_session_transaction.then_some(state.session_id),
                        &v2_range,
                    )
                    .await
                    .map_err(|e| user_error("XX000", e))?
                {
                    let existing_pk = storage_layout::decode_pk_from_index_entry_key(
                        &key,
                        index.index_id,
                        &index_value,
                        state.schema.pk_data_type(),
                    )?;
                    if existing_pk != pk_value {
                        return Err(user_error(
                            "23505",
                            format!(
                                "duplicate key value violates unique constraint '{}'",
                                index.index_name
                            ),
                        ));
                    }
                }
                if state.pending_writes.iter().any(|(existing_key, _)| {
                    existing_key.starts_with(&v2_prefix) && *existing_key != v2_key
                }) {
                    return Err(user_error(
                        "23505",
                        format!(
                            "duplicate key value violates unique constraint '{}'",
                            index.index_name
                        ),
                    ));
                }
                let prefix = index_entry_prefix(
                    &state.database_name,
                    &state.schema_name,
                    &state.schema.table_name,
                    &index.index_name,
                    &index_value,
                );
                if state.pending_writes.iter().any(|(existing_key, _)| {
                    existing_key.starts_with(&prefix) && *existing_key != key
                }) || !self
                    .txn_scan_prefix(
                        state.use_session_transaction.then_some(state.session_id),
                        &prefix,
                    )
                    .await
                    .map_err(|e| user_error("XX000", e))?
                    .is_empty()
                {
                    return Err(user_error(
                        "23505",
                        format!(
                            "duplicate key value violates unique constraint '{}'",
                            index.index_name
                        ),
                    ));
                }
            }
            state.pending_writes.push((v2_key, vec![1]));
            state.pending_writes.push((key, vec![1]));
        }
        state.pending_rows += 1;
        if state.pending_rows >= COPY_WRITE_BATCH_MAX_ROWS
            || state.pending_writes.len() >= COPY_WRITE_BATCH_MAX_ENTRIES
        {
            self.flush_copy_writes(state).await?;
        }
        state.inserted_rows += 1;
        Ok(())
    }

    pub(super) async fn flush_copy_writes(&self, state: &mut CopyInState) -> PgWireResult<()> {
        if state.pending_writes.is_empty() {
            return Ok(());
        }
        let writes = std::mem::take(&mut state.pending_writes);
        let write_count = writes.len();
        let row_count = state.pending_rows;
        state.pending_rows = 0;
        let in_transaction = state.use_session_transaction;
        let started_at = Instant::now();
        self.txn_put_batch(
            state.use_session_transaction.then_some(state.session_id),
            writes,
        )
        .await
        .map_err(|e| user_error("XX000", e))?;
        log_copy_profile(format!(
            "flush_copy_writes session={} in_txn={} rows={} entries={} elapsed_ms={}",
            state.session_id,
            in_transaction,
            row_count,
            write_count,
            started_at.elapsed().as_millis()
        ));
        Ok(())
    }
}
