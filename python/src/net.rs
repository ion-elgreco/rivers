//! Network utilities — port scanning and TCP listener binding helpers.

/// Try to bind a TcpListener on `host` starting from `start` port up to `max`.
/// Returns the first successfully bound listener.
pub async fn find_available_port(
    host: &str,
    start: u16,
    max: u16,
) -> anyhow::Result<tokio::net::TcpListener> {
    for p in start..=max {
        match tokio::net::TcpListener::bind(format!("{host}:{p}")).await {
            Ok(listener) => return Ok(listener),
            Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => continue,
            Err(e) => return Err(e.into()),
        }
    }
    anyhow::bail!("No available port in range {start}-{max}")
}
