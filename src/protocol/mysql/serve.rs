use super::*;

pub async fn serve(
    store: Arc<dyn KvStore>,
    listen_addr: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind(&listen_addr).await?;
    log::info!("pg_gateway mysql protocol listening on {listen_addr}");
    let server = Arc::new(GatewayServer::with_mode(store, GatewayMode::MySql));

    loop {
        let (socket, _) = listener.accept().await?;
        let server = server.clone();
        tokio::spawn(async move {
            let (reader, writer) = socket.into_split();
            let writer = BufWriter::new(writer);
            if let Err(error) =
                AsyncMysqlIntermediary::run_on(MySqlBackend::new(server), reader, writer).await
            {
                log::error!("pg_gateway mysql connection error: {error}");
            }
        });
    }
}

pub(super) struct MySqlBackend {
    pub(super) server: Arc<GatewayServer>,
    pub(super) client: MySqlClientState,
    pub(super) next_statement_id: u32,
    pub(super) prepared: std::collections::HashMap<u32, MySqlPreparedStatement>,
}

pub(super) struct MySqlPreparedStatement {
    pub(super) sql: String,
    pub(super) param_count: usize,
}

#[derive(Clone, Debug)]
pub(super) struct MySqlWarning {
    pub(super) level: &'static str,
    pub(super) code: u16,
    pub(super) message: String,
}

#[derive(Clone, Debug)]
pub(super) enum MySqlSystemVariableValue {
    Int(i32),
    Text(String),
}
