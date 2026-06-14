use std::sync::Arc;

use pgwire::tokio::tokio_rustls::TlsAcceptor;
use pgwire::tokio::tokio_rustls::rustls::ServerConfig;
use pgwire::tokio::{self as pgwire_tokio, process_socket};
use rustls_pki_types::{
    CertificateDer, PrivateKeyDer, PrivatePkcs1KeyDer, PrivatePkcs8KeyDer, PrivateSec1KeyDer,
};
use tokio::net::TcpListener;

use crate::core::server::GatewayFactory;
use crate::mem_store::KvStore;
use crate::mode::GatewayMode;

pub async fn serve(
    store: Arc<dyn KvStore>,
    listen_addr: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let factory = Arc::new(GatewayFactory::new(store, GatewayMode::Postgres));
    let listener = TcpListener::bind(&listen_addr).await?;
    let tls_acceptor = load_tls_acceptor()?;

    log::info!("pg_gateway postgres protocol listening on {listen_addr}");

    loop {
        let (socket, _) = listener.accept().await?;
        let factory = factory.clone();
        let tls_acceptor = tls_acceptor.clone();
        tokio::spawn(async move {
            if let Err(error) = process_socket(socket, tls_acceptor, factory).await {
                log::error!("pg_gateway postgres connection error: {error}");
            }
        });
    }
}

fn load_tls_acceptor() -> Result<Option<TlsAcceptor>, Box<dyn std::error::Error>> {
    let Some(cert_path) = std::env::var("PG_GATEWAY_TLS_CERT").ok() else {
        return Ok(None);
    };
    let Some(key_path) = std::env::var("PG_GATEWAY_TLS_KEY").ok() else {
        return Err("PG_GATEWAY_TLS_KEY must be set when PG_GATEWAY_TLS_CERT is set".into());
    };

    let cert_pem = std::fs::read_to_string(&cert_path)?;
    let certs = pem_payloads(&cert_pem, "CERTIFICATE")
        .into_iter()
        .map(CertificateDer::from)
        .collect::<Vec<_>>();
    if certs.is_empty() {
        return Err(format!("no CERTIFICATE block found in {cert_path}").into());
    }

    let key_pem = std::fs::read_to_string(&key_path)?;
    let key = pem_payloads(&key_pem, "PRIVATE KEY")
        .into_iter()
        .next()
        .map(|key| PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key)))
        .or_else(|| {
            pem_payloads(&key_pem, "RSA PRIVATE KEY")
                .into_iter()
                .next()
                .map(|key| PrivateKeyDer::Pkcs1(PrivatePkcs1KeyDer::from(key)))
        })
        .or_else(|| {
            pem_payloads(&key_pem, "EC PRIVATE KEY")
                .into_iter()
                .next()
                .map(|key| PrivateKeyDer::Sec1(PrivateSec1KeyDer::from(key)))
        })
        .ok_or_else(|| format!("no supported private key block found in {key_path}"))?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    Ok(Some(pgwire_tokio::TlsAcceptor::from(Arc::new(config))))
}

fn pem_payloads(input: &str, label: &str) -> Vec<Vec<u8>> {
    let begin = format!("-----BEGIN {label}-----");
    let end = format!("-----END {label}-----");
    let mut blocks = Vec::new();
    let mut rest = input;
    while let Some(start) = rest.find(&begin) {
        rest = &rest[start + begin.len()..];
        let Some(stop) = rest.find(&end) else {
            break;
        };
        let body = rest[..stop]
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .collect::<String>();
        if let Ok(decoded) = decode_base64(&body) {
            blocks.push(decoded);
        }
        rest = &rest[stop + end.len()..];
    }
    blocks
}

fn decode_base64(input: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    fn value(byte: u8) -> Option<u8> {
        match byte {
            b'A'..=b'Z' => Some(byte - b'A'),
            b'a'..=b'z' => Some(byte - b'a' + 26),
            b'0'..=b'9' => Some(byte - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }

    let bytes = input.as_bytes();
    let mut output = Vec::with_capacity(bytes.len() * 3 / 4);
    for chunk in bytes.chunks(4) {
        if chunk.len() < 4 {
            return Err("invalid base64 length".into());
        }
        let pad = chunk.iter().rev().take_while(|byte| **byte == b'=').count();
        let mut n = 0u32;
        for byte in chunk {
            n <<= 6;
            if *byte != b'=' {
                n |= value(*byte).ok_or("invalid base64 character")? as u32;
            }
        }
        output.push((n >> 16) as u8);
        if pad < 2 {
            output.push((n >> 8) as u8);
        }
        if pad == 0 {
            output.push(n as u8);
        }
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::{decode_base64, pem_payloads};

    #[test]
    fn decode_base64_handles_padding() {
        assert_eq!(decode_base64("Zm9v").unwrap(), b"foo");
        assert_eq!(decode_base64("Zm8=").unwrap(), b"fo");
        assert_eq!(decode_base64("Zg==").unwrap(), b"f");
    }

    #[test]
    fn decode_base64_rejects_invalid_input() {
        assert!(decode_base64("Zg").is_err());
        assert!(decode_base64("####").is_err());
    }

    #[test]
    fn pem_payloads_extracts_matching_blocks() {
        let input = "\
-----BEGIN CERTIFICATE-----\n\
Zm9v\n\
-----END CERTIFICATE-----\n\
-----BEGIN PRIVATE KEY-----\n\
YmFy\n\
-----END PRIVATE KEY-----\n\
-----BEGIN CERTIFICATE-----\n\
YmF6\n\
-----END CERTIFICATE-----\n";

        assert_eq!(
            pem_payloads(input, "CERTIFICATE"),
            vec![b"foo".to_vec(), b"baz".to_vec()]
        );
        assert_eq!(pem_payloads(input, "PRIVATE KEY"), vec![b"bar".to_vec()]);
    }

    #[test]
    fn pem_payloads_ignores_invalid_or_unclosed_blocks() {
        let input = "\
-----BEGIN CERTIFICATE-----\n\
####\n\
-----END CERTIFICATE-----\n\
-----BEGIN CERTIFICATE-----\n\
Zm9v\n";

        assert!(pem_payloads(input, "CERTIFICATE").is_empty());
    }
}
