use std::collections::HashMap;
use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::catalog::DEFAULT_DATABASE_NAME;
use crate::core::server::{GatewayServer, MySqlColumnMetadata};
use crate::dialect::parser::{self, SqlDialect};
use crate::mem_store::KvStore;
use crate::mode::GatewayMode;

const MYSQL_SERVER_VERSION: &str = "8.0.0-unidb";
const MYSQL_DEFAULT_SQL_MODE: &str = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION";
const MYSQL_AUTOCOMMIT: &str = "mysql.autocommit";
const MYSQL_SQL_MODE: &str = "mysql.sql_mode";
const MYSQL_TIME_ZONE: &str = "mysql.time_zone";
const MYSQL_CHARACTER_SET_CLIENT: &str = "mysql.character_set_client";
const MYSQL_CHARACTER_SET_CONNECTION: &str = "mysql.character_set_connection";
const MYSQL_CHARACTER_SET_RESULTS: &str = "mysql.character_set_results";
const MYSQL_COLLATION_CONNECTION: &str = "mysql.collation_connection";
const MYSQL_TRANSACTION_ISOLATION: &str = "mysql.transaction_isolation";
const MYSQL_LOCK_WAIT_TIMEOUT: &str = "mysql.innodb_lock_wait_timeout";
const MYSQL_CHARSET_UTF8_GENERAL_CI: u16 = 0x21;
const MYSQL_CHARSET_BINARY: u16 = 0x3f;
use async_trait::async_trait;
use futures::{Sink, StreamExt};
use opensrv_mysql::{
    AsyncMysqlIntermediary, AsyncMysqlShim, CapabilityFlags, Column, ColumnFlags, ColumnType,
    ErrorKind, InitWriter, OkResponse, ParamParser, QueryResultWriter, StatementMetaWriter, Value,
    ValueInner,
};
use pgwire::api::results::{FieldInfo, Response};
use pgwire::api::{ClientInfo, METADATA_DATABASE, METADATA_USER, Type};
use pgwire::error::PgWireError;
use pgwire::messages::response::{CommandComplete, TransactionStatus};
use pgwire::messages::startup::SecretKey;
use pgwire::messages::{PgWireBackendMessage, ProtocolVersion};
use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement};
use tokio::io::{AsyncWrite, BufWriter};

mod serve;
pub use serve::serve;
use serve::*;
mod client_state;
use client_state::*;
mod backend_metadata;
mod backend_query;
mod shim;
#[cfg(test)]
mod tests;
