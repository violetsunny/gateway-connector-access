//! mqtt-broker 独立进程：读 TOML 配置（可选）→ 监听 → Ctrl-C 优雅停机。
//!
//! 用法：`cargo run -p mqtt-broker -- [config.toml]`（无参数 = 0.0.0.0:1883 匿名）。

use std::sync::Arc;

use anyhow::Context;
use mqtt_broker::{Broker, Config, serve};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let config = match std::env::args().nth(1) {
        Some(path) => {
            mqtt_broker::load_config(&path).with_context(|| format!("load config {path}"))?
        }
        None => Config::default(),
    };
    let listener = TcpListener::bind(&config.listen)
        .await
        .with_context(|| format!("bind {}", config.listen))?;
    tracing::info!(listen = %config.listen, auth = config.username.is_some(), "mqtt broker listening");

    let broker = Arc::new(Broker::new(config));
    let (stop_tx, stop_rx) = watch::channel(false);
    let server = tokio::spawn(serve(listener, broker, stop_rx));
    tokio::signal::ctrl_c().await.context("wait for ctrl-c")?;
    tracing::info!("ctrl-c received, shutting down");
    let _ = stop_tx.send(true);
    let _ = server.await;
    tracing::info!("mqtt broker stopped");
    Ok(())
}
