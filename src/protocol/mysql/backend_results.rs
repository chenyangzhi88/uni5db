use std::io;

use futures::StreamExt;
use opensrv_mysql::{Column, ErrorKind, OkResponse, QueryResultWriter};
use pgwire::api::METADATA_DATABASE;
use pgwire::api::results::Response;
use pgwire::error::PgWireError;
use pgwire::messages::response::CommandComplete;
use tokio::io::AsyncWrite;

use crate::catalog::DEFAULT_DATABASE_NAME;
use crate::core::server::MySqlColumnMetadata;

use super::{
    MySqlBackend, MySqlSystemVariableValue, affected_rows_from_tag, decode_pg_text_row,
    mysql_insert_result_from_tag,
};

impl MySqlBackend {
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
}
