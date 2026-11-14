//! tcp connector（票 03 server 端 + 票 04 client 端）。
//!
//! server 链路：监听端口 → 拆包器切帧（[`codec`]，8 种，对齐 Java ParserProvider）→ Lua 协议
//! decode_multi → 登录帧走 DeviceDirectory 鉴权建会话（[`session`]）/ 数据帧仅
//! DEVICE_REPORT_REQ 且 metrics 非空上行 DeviceSink（[`server`]）；读空闲心跳超时断开。
//!
//! client 链路：外连远端 → 复用同一拆包/登录/上行链路收数 → 写空闲心跳保活 →
//! 断线按 reconnectInterval 自动重连（无限重试，[`client`]）。
//!
//! 下行不实现（Java 侧死代码）。bossGroupThread/workerGroupThread 配置忽略并 warn
//! （tokio runtime 自动调度，无 Netty boss/worker 对应物）。

pub mod client;
pub mod codec;
pub mod server;
pub mod session;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use connector_common::error::{CommonError, Result};
use connector_common::{
    Connector, DeviceDirectory, DeviceSink, ProtocolRegistry, StatusMap, TcpConfig, async_trait,
};
use serde_json::Value as JsonValue;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::client::{ClientCtx, reconnect_delay, run_client};
use crate::codec::build_codec;
use crate::server::{ConnCtx, idle_duration, serve};
use crate::session::SessionManager;

#[derive(Default)]
struct Inner {
    shutdown: Option<watch::Sender<bool>>,
    handles: Vec<JoinHandle<()>>,
    listeners: Vec<SocketAddr>,
    /// client 远端地址（status 排障用）。
    clients: Vec<String>,
}

/// tcp connector：按 `[tcp]` 配置装配 server/client。
pub struct TcpConnector {
    cfg: TcpConfig,
    registry: Arc<ProtocolRegistry>,
    directory: Arc<dyn DeviceDirectory>,
    sessions: Arc<SessionManager>,
    inner: Mutex<Inner>,
}

impl TcpConnector {
    pub fn new(
        cfg: TcpConfig,
        registry: Arc<ProtocolRegistry>,
        directory: Arc<dyn DeviceDirectory>,
    ) -> Self {
        Self {
            cfg,
            registry,
            directory,
            sessions: Arc::new(SessionManager::new()),
            inner: Mutex::new(Inner::default()),
        }
    }

    /// 实际绑定的监听地址（port=0 时测试/gateway 由此发现端口）。
    pub fn local_addrs(&self) -> Vec<SocketAddr> {
        self.lock().listeners.clone()
    }

    /// 会话表（status 与测试断言用）。
    pub fn sessions(&self) -> Arc<SessionManager> {
        Arc::clone(&self.sessions)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[async_trait]
impl Connector for TcpConnector {
    async fn start(&self, sink: Arc<dyn DeviceSink>) -> Result<()> {
        if self.lock().shutdown.is_some() {
            return Err(CommonError::Internal(
                "tcp connector already started".into(),
            ));
        }
        let servers = self
            .cfg
            .server
            .as_ref()
            .map(|s| s.configuration.as_slice())
            .unwrap_or(&[]);
        let clients = self
            .cfg
            .client
            .as_ref()
            .map(|c| c.configuration.as_slice())
            .unwrap_or(&[]);
        if servers.is_empty() && clients.is_empty() {
            tracing::warn!("tcp: no server/client configuration, connector idles");
        }
        let (tx, rx) = watch::channel(false);
        let mut handles = Vec::new();
        let mut listeners = Vec::new();
        for sc in servers {
            let where_ = format!("{}:{}", sc.address, sc.port);
            // 单个 server 装配失败即整体失败（失败可见），已 spawn 的 listener 回收
            let built: Result<()> = async {
                let pc = sc.protocol.as_ref().ok_or_else(|| {
                    CommonError::Config(format!("tcp server {where_}: protocol config missing"))
                })?;
                let protocol = self.registry.get(&pc.name)?;
                // 启动期校验拆包器配置（每连接会各自重建有状态实例）
                build_codec(sc.parser_type.as_deref(), sc.parser_params.as_ref())?;
                if sc.boss_group_thread.is_some() || sc.worker_group_thread.is_some() {
                    tracing::warn!("tcp server {where_}: bossGroupThread/workerGroupThread ignored (tokio runtime schedules automatically)");
                }
                let listener = TcpListener::bind((sc.address.as_str(), sc.port)).await?;
                let local = listener.local_addr()?;
                let ctx = Arc::new(ConnCtx {
                    protocol,
                    params: pc.params.clone(),
                    directory: Arc::clone(&self.directory),
                    sink: Arc::clone(&sink),
                    sessions: Arc::clone(&self.sessions),
                    parser_type: sc.parser_type.clone(),
                    parser_params: sc.parser_params.clone(),
                    idle: idle_duration(sc.heartbeat_interval),
                    shutdown: rx.clone(),
                });
                handles.push(tokio::spawn(serve(listener, ctx)));
                listeners.push(local);
                tracing::info!("tcp server listening on {local}");
                Ok(())
            }
            .await;
            if let Err(e) = built {
                for h in &handles {
                    h.abort();
                }
                return Err(e);
            }
        }
        let mut client_remotes = Vec::new();
        for cc in clients {
            let remote = format!("{}:{}", cc.remote_address, cc.remote_port);
            // 单个 client 装配失败即整体失败（失败可见），已 spawn 的任务回收。
            // 外连本身失败不在此列：run_client 内部无限重试（对齐 Java 异步 connect+listener）。
            let built: Result<()> = async {
                let pc = cc.protocol.as_ref().ok_or_else(|| {
                    CommonError::Config(format!("tcp client {remote}: protocol config missing"))
                })?;
                let protocol = self.registry.get(&pc.name)?;
                // 启动期校验拆包器配置（每次连接会各自重建有状态实例）
                build_codec(cc.parser_type.as_deref(), cc.parser_params.as_ref())?;
                if cc.worker_group_thread.is_some() {
                    tracing::warn!("tcp client {remote}: workerGroupThread ignored (tokio runtime schedules automatically)");
                }
                let conn = Arc::new(ConnCtx {
                    protocol,
                    params: pc.params.clone(),
                    directory: Arc::clone(&self.directory),
                    sink: Arc::clone(&sink),
                    sessions: Arc::clone(&self.sessions),
                    parser_type: cc.parser_type.clone(),
                    parser_params: cc.parser_params.clone(),
                    // Java client 仅装 WRITER_IDLE：无读空闲断连（TcpConnectionClient.java:93）
                    idle: None,
                    shutdown: rx.clone(),
                });
                let cctx = Arc::new(ClientCtx {
                    conn,
                    remote: remote.clone(),
                    heartbeat: idle_duration(cc.heartbeat_interval),
                    reconnect: reconnect_delay(cc.reconnect_enabled, cc.reconnect_interval),
                });
                handles.push(tokio::spawn(run_client(cctx)));
                client_remotes.push(remote);
                Ok(())
            }
            .await;
            if let Err(e) = built {
                for h in &handles {
                    h.abort();
                }
                return Err(e);
            }
        }
        // ponytail: start/stop 由 gateway 串行调用，check-then-act 无并发防护
        let mut inner = self.lock();
        inner.handles = handles;
        inner.listeners = listeners;
        inner.clients = client_remotes;
        inner.shutdown = Some(tx);
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        let mut inner = self.lock();
        if let Some(tx) = inner.shutdown.take() {
            // 通知 accept/client 循环与全部连接任务退出（server 连接任务不在 handles 里，靠 watch 广播）
            let _ = tx.send(true);
        }
        // ponytail: abort 兜底可能跳过 client 连接的会话清理（仅影响 stop 后 status 快照）；
        // 升级路径：先等 watch 优雅退出再 abort
        for h in inner.handles.drain(..) {
            h.abort();
        }
        inner.listeners.clear();
        inner.clients.clear();
        Ok(())
    }

    fn status(&self) -> StatusMap {
        let inner = self.lock();
        let mut m = StatusMap::new();
        m.insert("servers".into(), JsonValue::from(inner.listeners.len()));
        m.insert(
            "listeners".into(),
            JsonValue::from(
                inner
                    .listeners
                    .iter()
                    .map(SocketAddr::to_string)
                    .collect::<Vec<_>>(),
            ),
        );
        m.insert("clients".into(), JsonValue::from(inner.clients.len()));
        m.insert("remotes".into(), JsonValue::from(inner.clients.clone()));
        m.insert("connections".into(), JsonValue::from(self.sessions.len()));
        m
    }
}
