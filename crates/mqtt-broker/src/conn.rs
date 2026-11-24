//! 连接层：accept 循环 + 单连接任务（CONNECT 握手 → 读写分离 → keepalive → will）。
//!
//! 每连接一个 writer 任务（独占写半侧，串行化全部出站报文）+ reader 循环（本任务）。
//! 异常断开（EOF/读错/解码错/keepalive 超时/shutdown）发布 will；DISCONNECT 正常关闭不发。

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};

use crate::broker::{Broker, ConnectOutcome, OUTBOUND_CAP};
use crate::codec::{self, Connect, Packet};

/// 首包 CONNECT 等待上限（防空闲连接占资源）。ponytail: 固定 10s；升级路径：可配置。
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// accept 循环，直到 listener 出错或收到 shutdown。
pub async fn serve(
    listener: TcpListener,
    broker: Arc<Broker>,
    mut shutdown: watch::Receiver<bool>,
) {
    let local = listener
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "?".to_string());
    loop {
        tokio::select! {
            acc = listener.accept() => match acc {
                Ok((stream, peer)) => {
                    let conn_id = broker.next_conn_id();
                    tokio::spawn(handle(Arc::clone(&broker), stream, peer, conn_id, shutdown.clone()));
                }
                // 失败可见：accept 错误打日志后退出本 listener（不静默空转）
                Err(e) => {
                    tracing::error!(error = %e, "mqtt accept failed on {local}, listener exiting");
                    return;
                }
            },
            // Ref 守卫在 async 块内立即丢弃，避免跨 select 臂借用 shutdown
            _ = async {
                if shutdown.wait_for(|v| *v).await.is_err() {
                    tracing::warn!("shutdown sender dropped, mqtt broker {local} exiting");
                }
            } => return,
        }
    }
}

async fn handle(
    broker: Arc<Broker>,
    stream: TcpStream,
    peer: SocketAddr,
    conn_id: u64,
    mut shutdown: watch::Receiver<bool>,
) {
    let (mut rd, mut wr) = stream.into_split();
    let Some((connect, mut buf)) = read_connect(&mut rd, peer).await else {
        return;
    };
    let (tx, mut rx) = mpsc::channel::<Packet>(OUTBOUND_CAP);
    // kick sender 存入 broker；被接管/慢消费者断开时 broker 丢弃它 → changed() Err → reader 退出
    let (kick_tx, mut kick_rx) = watch::channel(false);
    let (client_id, session_present, keep_alive, pending) =
        match broker.connect(connect, conn_id, tx.clone(), kick_tx) {
            ConnectOutcome::Rejected(code) => {
                tracing::warn!(%peer, code, "CONNECT rejected");
                let mut out = Vec::new();
                codec::encode(
                    &Packet::Connack {
                        session_present: false,
                        code,
                    },
                    &mut out,
                );
                let _ = wr.write_all(&out).await;
                return;
            }
            ConnectOutcome::Accepted {
                client_id,
                session_present,
                keep_alive,
                pending,
            } => (client_id, session_present, keep_alive, pending),
        };
    tracing::info!(%peer, conn_id, %client_id, session_present, "mqtt client connected");

    let writer = tokio::spawn(async move {
        while let Some(packet) = rx.recv().await {
            let mut out = Vec::new();
            codec::encode(&packet, &mut out);
            if let Err(e) = wr.write_all(&out).await {
                tracing::debug!(%peer, "mqtt write failed: {e}");
                break;
            }
        }
        let _ = wr.shutdown().await;
    });

    if tx
        .send(Packet::Connack {
            session_present,
            code: 0,
        })
        .await
        .is_err()
    {
        writer.abort();
        return;
    }
    for m in pending {
        if tx.send(Packet::Publish(m)).await.is_err() {
            break;
        }
    }

    // keepalive 超时 = 1.5 × keep_alive（MQTT-3.1.2-24）；0 = 不检测
    let keepalive = (keep_alive > 0).then(|| Duration::from_secs(u64::from(keep_alive)) * 3 / 2);
    let mut tmp = [0u8; 8192];
    let mut normal = false;
    'outer: loop {
        loop {
            match codec::decode(&mut buf) {
                Ok(Some(packet)) => match on_packet(&broker, &client_id, &tx, packet).await {
                    Flow::Continue => {}
                    Flow::NormalClose => {
                        normal = true;
                        break 'outer;
                    }
                    Flow::Close => break 'outer,
                },
                Ok(None) => break,
                Err(e) => {
                    tracing::warn!(%peer, %client_id, "mqtt decode error: {e}, closing");
                    break 'outer;
                }
            }
        }
        tokio::select! {
            _ = shutdown.wait_for(|v| *v) => break 'outer,
            // broker 侧发起的关闭（接管/慢消费者）：sender 被丢弃 → Err 同样退出
            _ = kick_rx.changed() => break 'outer,
            r = read_chunk(&mut rd, &mut tmp, keepalive) => match r {
                Ok(0) => break 'outer,
                Ok(n) => buf.extend_from_slice(&tmp[..n]),
                Err(e) => {
                    tracing::warn!(%peer, %client_id, "mqtt read error: {e}, closing");
                    break 'outer;
                }
            }
        }
    }
    if let Some(will) = broker.disconnect(&client_id, conn_id, !normal) {
        broker.publish(will);
    }
    drop(tx);
    // ponytail: abort 而非 await——连接已关闭，避免慢消费者写半侧挂住；升级路径：带超时的 flush
    writer.abort();
    tracing::info!(%peer, conn_id, %client_id, "mqtt client disconnected");
}

/// 等待首包 CONNECT；返回剩余缓冲（可能已含后续报文，不能丢）。
async fn read_connect(rd: &mut OwnedReadHalf, peer: SocketAddr) -> Option<(Connect, Vec<u8>)> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    let deadline = tokio::time::Instant::now() + CONNECT_TIMEOUT;
    loop {
        let n = match tokio::time::timeout_at(deadline, rd.read(&mut tmp)).await {
            Ok(Ok(0)) => return None,
            Ok(Ok(n)) => n,
            Ok(Err(e)) => {
                tracing::warn!(%peer, "read before CONNECT failed: {e}");
                return None;
            }
            Err(_) => {
                tracing::warn!(%peer, "CONNECT timeout ({}s), closing", CONNECT_TIMEOUT.as_secs());
                return None;
            }
        };
        buf.extend_from_slice(&tmp[..n]);
        match codec::decode(&mut buf) {
            Ok(Some(Packet::Connect(c))) => return Some((c, buf)),
            Ok(Some(other)) => {
                tracing::warn!(%peer, "first packet is not CONNECT: {other:?}");
                return None;
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(%peer, "CONNECT decode error: {e}");
                return None;
            }
        }
    }
}

async fn read_chunk(
    rd: &mut OwnedReadHalf,
    tmp: &mut [u8],
    keepalive: Option<Duration>,
) -> std::io::Result<usize> {
    match keepalive {
        Some(d) => tokio::time::timeout(d, rd.read(tmp))
            .await
            .unwrap_or_else(|_| {
                Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "keepalive timeout",
                ))
            }),
        None => rd.read(tmp).await,
    }
}

enum Flow {
    Continue,
    /// 异常关闭（发 will）。
    Close,
    /// 收到 DISCONNECT，正常关闭（不发 will）。
    NormalClose,
}

async fn on_packet(
    broker: &Broker,
    client_id: &str,
    tx: &mpsc::Sender<Packet>,
    packet: Packet,
) -> Flow {
    match packet {
        Packet::Publish(p) => {
            match p.qos {
                0 => broker.publish(p),
                1 => {
                    let id = p.packet_id.expect("codec guarantees packet id for qos1");
                    // 先路由再 PUBACK（at-least-once）；dup 重发不做幂等去重，允许重复投递
                    broker.publish(p);
                    if tx.send(Packet::Puback(id)).await.is_err() {
                        return Flow::Close;
                    }
                }
                _ => {
                    tracing::warn!(client_id, "QoS2 PUBLISH not supported, closing");
                    return Flow::Close;
                }
            }
            Flow::Continue
        }
        Packet::Puback(id) => {
            broker.puback(client_id, id);
            Flow::Continue
        }
        Packet::Subscribe { packet_id, filters } => {
            let out = broker.subscribe(client_id, &filters);
            if tx
                .send(Packet::Suback {
                    packet_id,
                    codes: out.codes,
                })
                .await
                .is_err()
            {
                return Flow::Close;
            }
            for m in out.retained {
                if tx.send(Packet::Publish(m)).await.is_err() {
                    return Flow::Close;
                }
            }
            Flow::Continue
        }
        Packet::Unsubscribe { packet_id, filters } => {
            broker.unsubscribe(client_id, &filters);
            if tx.send(Packet::Unsuback(packet_id)).await.is_err() {
                return Flow::Close;
            }
            Flow::Continue
        }
        Packet::Pingreq => {
            if tx.send(Packet::Pingresp).await.is_err() {
                return Flow::Close;
            }
            Flow::Continue
        }
        Packet::Disconnect => Flow::NormalClose,
        other => {
            tracing::warn!(
                client_id,
                "unexpected packet from client: {other:?}, closing"
            );
            Flow::Close
        }
    }
}
