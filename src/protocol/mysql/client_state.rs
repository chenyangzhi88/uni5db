use std::collections::HashMap;
use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Sink;
use opensrv_mysql::CapabilityFlags;
use pgwire::api::{ClientInfo, METADATA_DATABASE, METADATA_USER};
use pgwire::messages::response::TransactionStatus;
use pgwire::messages::startup::SecretKey;
use pgwire::messages::{PgWireBackendMessage, ProtocolVersion};

use crate::catalog::DEFAULT_DATABASE_NAME;

use super::{MYSQL_DEFAULT_SQL_MODE, MYSQL_SQL_MODE, MySqlBackend, MySqlWarning};

impl Drop for MySqlBackend {
    fn drop(&mut self) {
        if self.client.transaction_status != TransactionStatus::Transaction {
            return;
        }
        let server = Arc::clone(&self.server);
        let session_id = self.client.pid_and_secret_key().0;
        tokio::spawn(async move {
            let _ = server.rollback_session_by_id(session_id).await;
        });
    }
}

pub(super) struct MySqlClientState {
    pub(super) metadata: HashMap<String, String>,
    protocol_version: ProtocolVersion,
    pid_secret_key: (i32, SecretKey),
    state: pgwire::api::PgWireConnectionState,
    pub(super) transaction_status: TransactionStatus,
    pub(super) bootstrapped: bool,
    pub(super) last_insert_id: u64,
    pub(super) warnings: Vec<MySqlWarning>,
    pub(super) client_capabilities: CapabilityFlags,
    pub(super) connection_attrs: HashMap<String, String>,
}

impl Default for MySqlClientState {
    fn default() -> Self {
        let mut metadata = HashMap::new();
        metadata.insert(
            METADATA_DATABASE.to_string(),
            DEFAULT_DATABASE_NAME.to_string(),
        );
        metadata.insert(METADATA_USER.to_string(), "root".to_string());
        metadata.insert(
            MYSQL_SQL_MODE.to_string(),
            MYSQL_DEFAULT_SQL_MODE.to_string(),
        );
        Self {
            metadata,
            protocol_version: ProtocolVersion::PROTOCOL3_0,
            pid_secret_key: (next_mysql_session_id(), SecretKey::I32(0)),
            state: pgwire::api::PgWireConnectionState::ReadyForQuery,
            transaction_status: TransactionStatus::Idle,
            bootstrapped: false,
            last_insert_id: 0,
            warnings: Vec::new(),
            client_capabilities: CapabilityFlags::empty(),
            connection_attrs: HashMap::new(),
        }
    }
}

impl MySqlClientState {
    pub(super) fn clear_warnings(&mut self) {
        self.warnings.clear();
    }

    pub(super) fn record_warning(&mut self, code: u16, message: String) {
        self.warnings.push(MySqlWarning {
            level: "Warning",
            code,
            message,
        });
    }
}

impl ClientInfo for MySqlClientState {
    fn socket_addr(&self) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 3306))
    }

    fn is_secure(&self) -> bool {
        false
    }

    fn protocol_version(&self) -> ProtocolVersion {
        self.protocol_version
    }

    fn set_protocol_version(&mut self, version: ProtocolVersion) {
        self.protocol_version = version;
    }

    fn pid_and_secret_key(&self) -> (i32, SecretKey) {
        self.pid_secret_key.clone()
    }

    fn set_pid_and_secret_key(&mut self, pid: i32, secret_key: SecretKey) {
        self.pid_secret_key = (pid, secret_key);
    }

    fn state(&self) -> pgwire::api::PgWireConnectionState {
        self.state
    }

    fn set_state(&mut self, new_state: pgwire::api::PgWireConnectionState) {
        self.state = new_state;
    }

    fn transaction_status(&self) -> TransactionStatus {
        self.transaction_status
    }

    fn set_transaction_status(&mut self, new_status: TransactionStatus) {
        self.transaction_status = new_status;
    }

    fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.metadata
    }

    fn sni_server_name(&self) -> Option<&str> {
        None
    }

    fn client_certificates<'a>(&self) -> Option<&[rustls_pki_types::CertificateDer<'a>]> {
        None
    }
}

impl Sink<PgWireBackendMessage> for MySqlClientState {
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, _item: PgWireBackendMessage) -> Result<(), Self::Error> {
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

fn next_mysql_session_id() -> i32 {
    static NEXT_ID: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(10_000);
    NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

pub(super) fn affected_rows_from_tag(tag: &str) -> u64 {
    tag.split_whitespace()
        .last()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0)
}

pub(super) fn mysql_insert_result_from_tag(tag: &str) -> Option<(u64, u64)> {
    let mut parts = tag.split_whitespace();
    if parts.next()? != "MYSQL_INSERT" {
        return None;
    }
    let affected_rows = parts.next()?.parse::<u64>().ok()?;
    let last_insert_id = parts
        .next()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    Some((affected_rows, last_insert_id))
}

pub(super) fn escape_sql_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "''")
}

pub(super) fn render_mysql_date(bytes: &[u8]) -> String {
    if bytes.len() < 4 {
        return "NULL".to_string();
    }
    let year = u16::from_le_bytes([bytes[0], bytes[1]]);
    let month = bytes[2];
    let day = bytes[3];
    format!("'{year:04}-{month:02}-{day:02}'")
}

pub(super) fn render_mysql_datetime(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return "'0000-00-00 00:00:00'".to_string();
    }
    if bytes.len() != 4 && bytes.len() != 7 && bytes.len() != 11 {
        return "NULL".to_string();
    }
    let year = u16::from_le_bytes([bytes[0], bytes[1]]);
    let month = bytes[2];
    let day = bytes[3];
    let (hour, minute, second) = if bytes.len() >= 7 {
        (bytes[4], bytes[5], bytes[6])
    } else {
        (0, 0, 0)
    };
    if bytes.len() == 11 {
        let micros = u32::from_le_bytes([bytes[7], bytes[8], bytes[9], bytes[10]]);
        format!("'{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{micros:06}'")
    } else {
        format!("'{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}'")
    }
}

pub(super) fn render_mysql_time(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return "'00:00:00'".to_string();
    }
    if bytes.len() != 8 && bytes.len() != 12 {
        return "NULL".to_string();
    }
    let negative = bytes[0] != 0;
    let days = u32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
    let hours = days as u64 * 24 + bytes[5] as u64;
    let minutes = bytes[6];
    let seconds = bytes[7];
    let sign = if negative { "-" } else { "" };
    if bytes.len() == 12 {
        let micros = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
        format!("'{sign}{hours:02}:{minutes:02}:{seconds:02}.{micros:06}'")
    } else {
        format!("'{sign}{hours:02}:{minutes:02}:{seconds:02}'")
    }
}

pub(super) fn decode_pg_text_row(
    row: &pgwire::messages::data::DataRow,
) -> io::Result<Vec<Option<Vec<u8>>>> {
    let data = row.data.as_ref();
    let mut offset = 0usize;
    let mut values = Vec::with_capacity(row.field_count.max(0) as usize);
    for _ in 0..row.field_count {
        if data.len().saturating_sub(offset) < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated pgwire data row",
            ));
        }
        let len = i32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);
        offset += 4;
        if len < 0 {
            values.push(None);
            continue;
        }
        let len = len as usize;
        if data.len().saturating_sub(offset) < len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated pgwire column value",
            ));
        }
        values.push(Some(data[offset..offset + len].to_vec()));
        offset += len;
    }
    Ok(values)
}
