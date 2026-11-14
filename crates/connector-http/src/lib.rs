//! http connector：axum HTTP 服务。
//!
//! - `POST /api/{pkey}/**` 通配接入：pkey→协议映射来自配置，wildcardPath/headers/query 作为
//!   params 传给 Lua `decode_multi`，解码结果经 `DeviceSink` 上行（对齐 Java `HttpConnectorStater`）。
//! - `POST /access/std/{rtg,history,event,info}` 标准报文路由：报文格式由 std 示例 Lua 脚本承载
//!   （对齐 Java `HttpStdConnector`，不移植 `EnnStandardHttpProtocol`）。
//!
//! 路由分发 / params 注入逻辑与测试见 [`router`]；配置见 [`config`]。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use connector_common::{
    CommonError, Connector, DeviceSink, LuaProtocol, Protocol, Result, StatusMap, async_trait,
};
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

mod config;
mod router;

pub use config::{HttpConfig, HttpRouteConfig};
use router::{HttpState, RouteEntry, build_router};

/// HTTP connector。构造时即加载全部协议脚本（配置/脚本错误立刻可见，不拖到 start）。
pub struct HttpConnector {
    routes: Arc<HashMap<String, RouteEntry>>,
    std_protocol: Arc<dyn Protocol>,
    std_params: serde_json::Value,
    address: String,
    port: u16,
    /// 请求/上行计数，与 handler 共享同一 `Arc`，供 `status` 读取。
    requests: Arc<AtomicUsize>,
    messages: Arc<AtomicUsize>,
    /// 优雅停机信号（start 装入，stop 触发）。
    shutdown: Mutex<Option<oneshot::Sender<()>>>,
    local_addr: Mutex<Option<SocketAddr>>,
}

impl HttpConnector {
    /// 从配置构建，加载通配路由协议与 std 协议脚本。脚本读取/语法错误 → `Err`（失败可见）。
    /// `script_dir` 为 Some 时对脚本路径做白名单校验（见 `LuaProtocol::from_script_in`）。
    pub fn new(config: HttpConfig, script_dir: Option<&str>) -> Result<Self> {
        let mut routes = HashMap::new();
        for rc in &config.configuration {
            let protocol =
                LuaProtocol::from_script_in(&rc.protocol.name, &rc.protocol.path, script_dir)?;
            routes.insert(
                rc.request_path.clone(),
                RouteEntry {
                    protocol: Arc::new(protocol),
                    params: rc.protocol.params.clone(),
                },
            );
        }
        let std_protocol: Arc<dyn Protocol> = Arc::new(LuaProtocol::from_script_in(
            &config.std_protocol.name,
            &config.std_protocol.path,
            script_dir,
        )?);

        Ok(Self {
            routes: Arc::new(routes),
            std_protocol,
            std_params: config.std_protocol.params.clone(),
            address: config.address.clone(),
            port: config.port,
            requests: Arc::new(AtomicUsize::new(0)),
            messages: Arc::new(AtomicUsize::new(0)),
            shutdown: Mutex::new(None),
            local_addr: Mutex::new(None),
        })
    }

    fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
        // 锁中毒不应 panic 整个 connector，恢复内部数据继续（对齐 common 层 MockSink 处理）。
        m.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[async_trait]
impl Connector for HttpConnector {
    async fn start(&self, sink: Arc<dyn DeviceSink>) -> Result<()> {
        let state = Arc::new(HttpState {
            routes: self.routes.clone(),
            std_protocol: self.std_protocol.clone(),
            std_params: self.std_params.clone(),
            sink,
            requests: self.requests.clone(),
            messages: self.messages.clone(),
        });
        let router = build_router(state);

        let addr = format!("{}:{}", self.address, self.port);
        let listener = TcpListener::bind(&addr)
            .await
            .map_err(|e| CommonError::Internal(format!("bind {addr} failed: {e}")))?;
        let local = listener
            .local_addr()
            .map_err(|e| CommonError::Internal(format!("local_addr failed: {e}")))?;

        let (tx, rx) = oneshot::channel::<()>();
        *Self::lock(&self.local_addr) = Some(local);
        *Self::lock(&self.shutdown) = Some(tx);

        tokio::spawn(async move {
            let server = axum::serve(listener, router).with_graceful_shutdown(async {
                let _ = rx.await;
            });
            if let Err(e) = server.await {
                tracing::error!(error = %e, "http server 异常退出");
            }
        });

        tracing::info!(%local, "http connector 已启动");
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        if let Some(tx) = Self::lock(&self.shutdown).take() {
            // 接收端已丢弃（server 已退出）时忽略发送错误。
            let _ = tx.send(());
        }
        tracing::info!("http connector 已停止");
        Ok(())
    }

    fn status(&self) -> StatusMap {
        let mut m = StatusMap::new();
        let listening = Self::lock(&self.local_addr).map(|a| a.to_string());
        m.insert("listening".into(), json!(listening));
        m.insert(
            "requests".into(),
            json!(self.requests.load(Ordering::Relaxed)),
        );
        m.insert(
            "messages".into(),
            json!(self.messages.load(Ordering::Relaxed)),
        );
        let mut pkeys: Vec<&String> = self.routes.keys().collect();
        pkeys.sort();
        m.insert("pkeys".into(), json!(pkeys));
        m
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use connector_common::{MockSink, ProtocolConfig};

    fn std_config(port: u16) -> HttpConfig {
        HttpConfig {
            address: "127.0.0.1".into(),
            port,
            configuration: vec![],
            std_protocol: ProtocolConfig {
                name: "enn-standard-http".into(),
                path: format!("{}/protocols/std_http.lua", env!("CARGO_MANIFEST_DIR")),
                params: json!({}),
                protocol_type: "script".into(),
            },
        }
    }

    /// 脚本缺失 → new 立刻报错（失败可见，不拖到 start）。
    #[test]
    fn new_reports_missing_script() {
        let mut cfg = std_config(0);
        cfg.std_protocol.path = "/definitely/not/here.lua".into();
        let err = HttpConnector::new(cfg, None)
            .err()
            .expect("expected ScriptLoad error");
        assert!(matches!(err, CommonError::ScriptLoad { .. }), "got {err:?}");
    }

    /// script_dir 白名单：std 脚本真实存在但在目录外 → new 报错（失败可见）。
    #[test]
    fn new_rejects_script_outside_dir() {
        let dir = std::env::temp_dir().join(format!("gw-scriptdir-{}-http", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let err = HttpConnector::new(std_config(0), Some(dir.to_str().unwrap()))
            .err()
            .expect("expected rejection");
        assert!(err.to_string().contains("拒绝加载"), "{err}");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// start 绑定端口（port=0 由 OS 分配）、status 反映真实监听地址、stop 优雅停机。
    #[tokio::test]
    async fn start_binds_status_reflects_addr_and_stop_shuts_down() {
        let connector = HttpConnector::new(std_config(0), None).unwrap();
        let sink: Arc<dyn DeviceSink> = Arc::new(MockSink::new());

        connector.start(sink).await.unwrap();
        let status = connector.status();
        let listening = status["listening"].as_str().unwrap();
        assert!(
            listening.starts_with("127.0.0.1:") && listening != "127.0.0.1:0",
            "expected a real bound port, got {listening}"
        );
        assert_eq!(status["requests"], json!(0));
        assert_eq!(status["messages"], json!(0));

        connector.stop().await.unwrap();
    }
}
