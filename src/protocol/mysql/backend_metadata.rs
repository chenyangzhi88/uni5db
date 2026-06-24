use std::sync::Arc;

use crate::core::server::GatewayServer;

use super::{MySqlBackend, MySqlClientState};

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
}
