//! tcp server 连接处理：accept 循环 + 单连接任务（拆包 → Lua decode_multi → 登录/上行路由）。
//!
//! 路由部分（[`ConnCtx`]/[`ConnHandler`]）由 client（票 04，[`crate::client`]）复用。
//!
//! 行为基准（Java）：
//! - MessageHandler.java:53 —— deviceId 为空的上行丢弃 + warn
//! - MessageHandler.java:74-80 —— 仅 MetricReportRequest 且 metrics 非空才 store_message
//! - MessageHandler.java:64-65 —— 登录失败关闭连接
//! - HeartbeatEventHandler.java:42-45 —— READER_IDLE 超时关闭连接
//! - DecoderHandler.java:50 + ExceptionHandler —— decode 异常关闭连接
//!
//! 下行不实现（Java 侧死代码路径）：不回写登录响应/echo，消息带 response 字段仅 warn。
//! 与 Java 的刻意偏差（修正其 bug，spec「已知 bug 修正而非复刻」）：
//! - LoginProcesser.java:25-43 鉴权结果被无条件覆盖为 login=true（永真 bug）——
//!   此处改为 DeviceDirectory 真实鉴权（sn→deviceId + 密码校验），失败即断开。
//! - MessageHandler.java:69-73 未登录数据自动建会话——此处未登录数据丢弃 + warn（失败可见）。

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use connector_common::{DeviceDirectory, DeviceSink, Protocol};
use serde_json::Value as JsonValue;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

use crate::codec::build_codec;
use crate::session::{Session, SessionManager};

/// 上行路由用的 messageType 字符串（对齐 Java MessageType 枚举序列化值）。
const LOGIN_REQ: &str = "DEVICE_LOGIN_REQ";
const REPORT_REQ: &str = "DEVICE_REPORT_REQ";

/// 一个连接配置对应的共享上下文（server 各连接任务共享；client 复用做帧路由）。
pub struct ConnCtx {
    pub protocol: Arc<dyn Protocol>,
    /// protocol.params（Lua 入参），登录时从中取 projectId。
    pub params: JsonValue,
    pub directory: Arc<dyn DeviceDirectory>,
    pub sink: Arc<dyn DeviceSink>,
    pub sessions: Arc<SessionManager>,
    pub parser_type: Option<String>,
    pub parser_params: Option<JsonValue>,
    /// 读空闲超时（None = 不检测），等价 Java IdleStateHandler(READER_IDLE)。
    pub idle: Option<Duration>,
    pub shutdown: watch::Receiver<bool>,
}

/// heartbeatInterval（秒）→ 空闲超时/周期。对齐 Java：缺省 120s，<=0 关闭。
/// server 端用作读空闲断连（READER_IDLE）；client 端复用为写空闲心跳周期
/// （Java TcpClientConfig heartbeatInterval 缺省同为 120s，0 不发送）。
pub(crate) fn idle_duration(heartbeat_interval: Option<i32>) -> Option<Duration> {
    match heartbeat_interval {
        Some(s) if s > 0 => Some(Duration::from_secs(u64::from(s as u32))),
        Some(_) => None,
        None => Some(Duration::from_secs(120)),
    }
}

/// accept 循环，直到 listener 出错或收到 shutdown。
pub async fn serve(listener: TcpListener, ctx: Arc<ConnCtx>) {
    let local = listener
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "?".to_string());
    let mut shutdown = ctx.shutdown.clone();
    loop {
        tokio::select! {
            acc = listener.accept() => match acc {
                Ok((stream, peer)) => {
                    let conn_id = ctx.sessions.next_conn_id();
                    tokio::spawn(handle_conn(Arc::clone(&ctx), stream, peer, conn_id));
                }
                // 失败可见：accept 错误打日志后退出本 listener（不静默空转）
                Err(e) => {
                    tracing::error!(error = %e, "tcp accept failed on {local}, listener exiting");
                    return;
                }
            },
            // Ref 持 RwLock 读守卫（!Send），在 async 块内立即丢弃，避免跨 await
            _ = async {
                if shutdown.wait_for(|v| *v).await.is_err() {
                    tracing::warn!("shutdown sender dropped, tcp server {local} exiting");
                }
            } => return,
        }
    }
}

/// 单连接任务：读字节 → 拆帧 → 逐帧处理；EOF/读错/拆包错/decode 错/读空闲超时/shutdown 时退出并清理会话。
async fn handle_conn(ctx: Arc<ConnCtx>, mut stream: TcpStream, peer: SocketAddr, conn_id: u64) {
    tracing::info!(%peer, conn_id, "new tcp connection");
    let mut codec = match build_codec(ctx.parser_type.as_deref(), ctx.parser_params.as_ref()) {
        Ok(c) => c,
        // 启动时已校验过同一配置，正常不可达；失败可见
        Err(e) => {
            tracing::error!(%peer, "build frame codec failed: {e}");
            return;
        }
    };
    let mut handler = ConnHandler::new(Arc::clone(&ctx), peer, conn_id);
    let mut buf: Vec<u8> = Vec::new();
    // ponytail: 固定 8KB 读块；升级路径 BytesMut + read_buf 减少一次拷贝
    let mut tmp = [0u8; 8192];
    let mut shutdown = ctx.shutdown.clone();
    'outer: loop {
        tokio::select! {
            // Ref 持 RwLock 读守卫（!Send），在 async 块内立即丢弃，避免跨 await
            _ = async { let _ = shutdown.wait_for(|v| *v).await; } => break,
            _ = idle_sleep(ctx.idle) => {
                // 对齐 HeartbeatEventHandler READER_IDLE：超时断开
                tracing::warn!(%peer, "read idle timeout (heartbeat), closing connection");
                break;
            }
            r = stream.read(&mut tmp) => match r {
                Ok(0) => break, // 对端关闭
                Ok(n) => {
                    buf.extend_from_slice(&tmp[..n]);
                    loop {
                        match codec.decode(&mut buf) {
                            Ok(Some(frame)) => {
                                if handler.on_frame(frame).await {
                                    break 'outer;
                                }
                            }
                            Ok(None) => break,
                            Err(e) => {
                                tracing::warn!(%peer, "frame decode error: {e}, closing connection");
                                break 'outer;
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(%peer, "tcp read error: {e}");
                    break;
                }
            }
        }
    }
    ctx.sessions.remove_by_conn(conn_id);
    tracing::info!(%peer, conn_id, "tcp connection closed");
}

async fn idle_sleep(idle: Option<Duration>) {
    match idle {
        Some(d) => tokio::time::sleep(d).await,
        None => std::future::pending::<()>().await,
    }
}

/// 单连接的消息路由状态（登录前 device_id 为 None）。server/client 连接共用（票 04 复用）。
pub(crate) struct ConnHandler {
    ctx: Arc<ConnCtx>,
    peer: SocketAddr,
    conn_id: u64,
    device_id: Option<String>,
}

impl ConnHandler {
    pub(crate) fn new(ctx: Arc<ConnCtx>, peer: SocketAddr, conn_id: u64) -> Self {
        Self {
            ctx,
            peer,
            conn_id,
            device_id: None,
        }
    }

    /// 处理一个完整帧；返回 true 表示需要关闭连接。
    pub(crate) async fn on_frame(&mut self, frame: Vec<u8>) -> bool {
        let msgs = match self.ctx.protocol.decode_multi(&frame, &self.ctx.params) {
            Ok(m) => m,
            // 对齐 Java exceptionCaught：decode 失败关闭连接（失败可见）
            Err(e) => {
                tracing::warn!(peer = %self.peer, "protocol decode_multi failed: {e}, closing connection");
                return true;
            }
        };
        for msg in msgs {
            if self.route(msg).await {
                return true;
            }
        }
        false
    }

    /// 路由一条 wire message；返回 true 表示需要关闭连接。
    async fn route(&mut self, msg: JsonValue) -> bool {
        let mtype = msg
            .get("messageType")
            .and_then(JsonValue::as_str)
            .unwrap_or("");
        if mtype == LOGIN_REQ {
            return self.login(&msg);
        }
        if msg.get("response").is_some_and(|r| !r.is_null()) {
            // Java DecoderHandler.sendResponse 会把 response 回写设备——下行不实现（票 03 范围）
            tracing::warn!(peer = %self.peer, "message carries response field: downlink not implemented, ignoring");
        }
        if self.device_id.is_none() {
            // 偏差修正：Java 对未登录数据自动建会话（MessageHandler:69-73），此处丢弃 + warn
            tracing::warn!(peer = %self.peer, messageType = mtype, "data before login, dropped");
            return false;
        }
        if msg.get("deviceId").and_then(JsonValue::as_str).is_none() {
            // 对齐 MessageHandler.java:53
            tracing::warn!(peer = %self.peer, messageType = mtype, "message deviceId missing, dropped");
            return false;
        }
        if mtype == REPORT_REQ {
            let metrics_empty = msg
                .get("metrics")
                .and_then(JsonValue::as_array)
                .is_none_or(|a| a.is_empty());
            if metrics_empty {
                // 对齐 MessageHandler.java:75-79
                tracing::warn!(peer = %self.peer, "DEVICE_REPORT_REQ with empty metrics, dropped");
                return false;
            }
            if let Err(e) = self.ctx.sink.store_message(msg).await {
                tracing::error!(peer = %self.peer, "store_message failed: {e}");
            }
        } else {
            // 对齐 MessageHandler.java:74-82：仅 MetricReportRequest 上行，其余类型 Java 走
            // echo 回写（下行，不实现），此处忽略
            tracing::debug!(peer = %self.peer, messageType = mtype, "message type not uplinked, ignored");
        }
        false
    }

    /// 登录帧：DeviceDirectory 做 sn→deviceId + 密码鉴权，成功绑定会话，失败断开。
    /// 返回 true 表示需要关闭连接。
    fn login(&mut self, msg: &JsonValue) -> bool {
        let sn = msg.get("sn").and_then(JsonValue::as_str).unwrap_or("");
        let password = msg
            .get("password")
            .and_then(JsonValue::as_str)
            .unwrap_or("");
        // ponytail: projectId 取自协议 params（缺省空串）；多项目共存时升级为按监听端口配置
        let project_id = self
            .ctx
            .params
            .get("projectId")
            .and_then(JsonValue::as_str)
            .unwrap_or("");
        let Some(device_id) = self
            .ctx
            .directory
            .get_device_id_by_sn(project_id, sn)
            .filter(|d| self.ctx.directory.auth_device(d, password))
        else {
            tracing::warn!(peer = %self.peer, sn, "login auth failed, closing connection");
            return true;
        };
        self.device_id = Some(device_id.clone());
        self.ctx.sessions.add(Session {
            device_id: device_id.clone(),
            sn: sn.to_string(),
            peer: self.peer.to_string(),
            conn_id: self.conn_id,
        });
        tracing::info!(peer = %self.peer, sn, %device_id, conn_id = self.conn_id, "device logged in");
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_duration_maps_heartbeat_interval() {
        // Java 缺省 120s；0/负数不检测；正数为秒
        assert_eq!(idle_duration(None), Some(Duration::from_secs(120)));
        assert_eq!(idle_duration(Some(0)), None);
        assert_eq!(idle_duration(Some(-1)), None);
        assert_eq!(idle_duration(Some(30)), Some(Duration::from_secs(30)));
    }
}
