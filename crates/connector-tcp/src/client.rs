//! tcp client 外连（票 04）：重连状态机 + 写空闲心跳，复用票 03 的拆包（[`crate::codec`]）
//! 与登录/上行路由（[`crate::server::ConnHandler`]）链路收数上行 DeviceSink。
//!
//! 行为基准（Java）：
//! - TcpConnectionClient.java:71-117 —— Bootstrap 外连，pipeline 复用拆包器/MessageHandler
//! - TcpConnectionClient.java:109-113 —— 首连失败 → reconnectInterval 秒后重试
//! - ReconnectHandler.java:23-46 —— 断开（channelInactive）→ 按 reconnectInterval 定周期
//!   重连直到成功；reconnectInterval==0 → 不重连
//! - HeartbeatEventHandler.java:46-49 —— WRITER_IDLE 到期 → 发 Message{HEARTBEAT}
//!   （经 protocol.encode 写出字节）
//! - HeartbeatHandlerProvider.java:13-15 —— heartbeatInterval==0 → 不装心跳
//! - TcpClientConfig.java —— reconnectInterval 缺省 5s，heartbeatInterval 缺省 120s
//!
//! 刻意偏差：
//! - Java reconnectEnabled 为死配置（无任何代码读取），重连仅由 reconnectInterval==0 控制；
//!   此处 reconnectEnabled=false 显式关闭重连（配置一致性，失败可见）。
//! - Java 首连失败（TcpConnectionClient.reconnect）与断开重连（ReconnectHandler）是两份
//!   重复逻辑；此处统一为单循环：连接 → 服务 → 断开 → sleep(interval) → 重连（无限重试）。
//! - 未设 SO_KEEPALIVE（Java 设了）：应用层心跳已覆盖存活探测；升级路径 socket2。
//!
//! 下行不实现（与票 03 一致，Java 侧死代码）。

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value as JsonValue;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Instant, MissedTickBehavior, interval_at};

use crate::codec::build_codec;
use crate::server::{ConnCtx, ConnHandler};

/// 一个 client 外连配置对应的共享上下文。
pub struct ClientCtx {
    /// 帧路由上下文（protocol/params/directory/sink/sessions），复用 server 端结构。
    pub conn: Arc<ConnCtx>,
    /// 远端 "host:port"。
    pub remote: String,
    /// 写空闲心跳周期（None = 不发送，对齐 Java heartbeatInterval=0）。
    pub heartbeat: Option<Duration>,
    /// 重连间隔（None = 不重连，对齐 Java reconnectInterval=0）。
    pub reconnect: Option<Duration>,
}

/// 重连配置 → 间隔。对齐 Java：reconnectInterval 缺省 5s，<=0 不重连；
/// 偏差：reconnectEnabled=false 显式关闭（Java 死字段，见模块注释）。
pub(crate) fn reconnect_delay(enabled: Option<bool>, interval: Option<i64>) -> Option<Duration> {
    if enabled == Some(false) {
        return None;
    }
    match interval {
        Some(s) if s > 0 => Some(Duration::from_secs(s as u64)),
        Some(_) => None,
        None => Some(Duration::from_secs(5)),
    }
}

/// 心跳 wire message：仅 messageType=HEARTBEAT（对齐 HeartbeatEventHandler.java:47-48），
/// 字节内容由 protocol.encode 决定（对齐 EncodeHandler.java:30）。
fn heartbeat_message() -> JsonValue {
    serde_json::json!({ "messageType": "HEARTBEAT" })
}

/// client 主循环：连接 → 服务 → 断开 → sleep(reconnect) → 重连，直到 shutdown。
/// 连接失败/断开均打日志后重试（无限重试，失败可见）。
pub async fn run_client(ctx: Arc<ClientCtx>) {
    let mut shutdown = ctx.conn.shutdown.clone();
    loop {
        let stream = tokio::select! {
            r = TcpStream::connect(&ctx.remote) => r,
            // Ref 持 RwLock 读守卫（!Send），在 async 块内立即丢弃，避免跨 await
            _ = async { let _ = shutdown.wait_for(|v| *v).await; } => return,
        };
        match stream {
            Ok(stream) => {
                tracing::info!(remote = %ctx.remote, "tcp client connected");
                handle_conn(&ctx, stream).await;
            }
            // 对齐 TcpConnectionClient.java:110：连接失败打日志，进入重连等待
            Err(e) => tracing::warn!(remote = %ctx.remote, "tcp client connect failed: {e}"),
        }
        let Some(delay) = ctx.reconnect else {
            tracing::warn!(remote = %ctx.remote, "reconnect disabled, tcp client exiting");
            return;
        };
        tracing::info!(remote = %ctx.remote, "tcp client reconnecting in {}s", delay.as_secs());
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = async { let _ = shutdown.wait_for(|v| *v).await; } => return,
        }
    }
}

/// 单连接：读字节 → 拆帧 → ConnHandler 路由（登录/上行）；写空闲到期发心跳。
/// EOF/读错/拆包错/decode 错/心跳写错/shutdown 时退出并清理会话，由主循环决定重连。
async fn handle_conn(ctx: &ClientCtx, stream: TcpStream) {
    let Ok(peer) = stream.peer_addr() else {
        // 已连接 socket 上实际不可达；失败可见
        tracing::error!(remote = %ctx.remote, "tcp client peer_addr failed");
        return;
    };
    if let Err(e) = stream.set_nodelay(true) {
        // 非致命：仅影响延迟（对齐 Java TCP_NODELAY 意图，TcpConnectionClient.java:84）
        tracing::warn!(%peer, "set_nodelay failed: {e}");
    }
    let conn_id = ctx.conn.sessions.next_conn_id();
    let (mut rd, mut wr) = stream.into_split();
    let mut codec = match build_codec(
        ctx.conn.parser_type.as_deref(),
        ctx.conn.parser_params.as_ref(),
    ) {
        Ok(c) => c,
        // 启动时已校验过同一配置，正常不可达；失败可见
        Err(e) => {
            tracing::error!(%peer, "build frame codec failed: {e}");
            return;
        }
    };
    let mut handler = ConnHandler::new(Arc::clone(&ctx.conn), peer, conn_id);
    let mut buf: Vec<u8> = Vec::new();
    // ponytail: 固定 8KB 读块，与 server 端一致
    let mut tmp = [0u8; 8192];
    // 写空闲心跳：下行不实现，唯一写操作就是心跳本身，interval 周期等价 Netty
    // WRITER_IDLE（读不重置计时）。升级路径：新增下行写时改为写后重置的 sleep 定时器。
    let mut hb = ctx.heartbeat.map(|d| {
        let mut t = interval_at(Instant::now() + d, d);
        t.set_missed_tick_behavior(MissedTickBehavior::Delay);
        t
    });
    let mut shutdown = ctx.conn.shutdown.clone();
    'outer: loop {
        tokio::select! {
            // Ref 持 RwLock 读守卫（!Send），在 async 块内立即丢弃，避免跨 await
            _ = async { let _ = shutdown.wait_for(|v| *v).await; } => break,
            _ = async {
                match &mut hb {
                    Some(t) => {
                        t.tick().await;
                    }
                    None => std::future::pending::<()>().await,
                }
            } => {
                // 对齐 HeartbeatEventHandler WRITER_IDLE：Message{HEARTBEAT} 经 protocol.encode 写出
                let bytes = match ctx.conn.protocol.encode(&heartbeat_message(), &ctx.conn.params) {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::error!(%peer, "heartbeat encode failed: {e}, closing connection");
                        break;
                    }
                };
                if let Err(e) = wr.write_all(&bytes).await {
                    tracing::warn!(%peer, "heartbeat write failed: {e}");
                    break;
                }
                tracing::debug!(%peer, len = bytes.len(), "heartbeat sent");
            }
            r = rd.read(&mut tmp) => match r {
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
    ctx.conn.sessions.remove_by_conn(conn_id);
    tracing::info!(%peer, conn_id, "tcp client connection closed");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconnect_delay_maps_config() {
        // Java 缺省：reconnectEnabled=true、reconnectInterval=5s
        assert_eq!(reconnect_delay(None, None), Some(Duration::from_secs(5)));
        assert_eq!(
            reconnect_delay(Some(true), None),
            Some(Duration::from_secs(5))
        );
        assert_eq!(
            reconnect_delay(Some(true), Some(3)),
            Some(Duration::from_secs(3))
        );
        // interval<=0 → 不重连（对齐 Java reconnectInterval==0，ReconnectHandler.java:32）
        assert_eq!(reconnect_delay(Some(true), Some(0)), None);
        assert_eq!(reconnect_delay(None, Some(-1)), None);
        // 偏差：enabled=false 显式关闭（Java 死字段）
        assert_eq!(reconnect_delay(Some(false), Some(3)), None);
        assert_eq!(reconnect_delay(Some(false), None), None);
    }

    #[test]
    fn heartbeat_message_carries_type_only() {
        // 对齐 HeartbeatEventHandler.java:47-48：仅设置 messageType
        assert_eq!(
            heartbeat_message(),
            serde_json::json!({"messageType": "HEARTBEAT"})
        );
    }
}
