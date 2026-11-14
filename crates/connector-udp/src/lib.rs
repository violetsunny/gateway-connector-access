//! udp connector：绑定配置端口（含 SO_BROADCAST），数据报 → Lua 协议 `decode_multi` →
//! 仅 DEVICE_EVENT_REQ / DEVICE_INFO_REQ / DEVICE_REPORT_REQ 三类经 [`DeviceSink`] 上行。
//!
//! 对齐 Java `UdpConnectorStater` / `MessageHandler`，并修正其 bug：
//! - Java start 的空 catch 吞异常 → 这里绑定/协议解析失败返回 Err，运行期错误 tracing 打日志（失败可见）
//! - Java 双 `.handler()` 覆盖 LoggingHandler → 这里统一 tracing，无双挂 bug
//!
//! 刻意偏差：Java `exceptionCaught` 会 close socket（一条坏包杀死整个监听）；
//! 这里 decode/store 失败仅记日志并计数，监听器保活。

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use connector_common::{
    CommonError, Connector, DeviceSink, MessageType, Protocol, ProtocolConfig, ProtocolRegistry,
    Result, StatusMap, async_trait,
};
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};
use tokio::net::UdpSocket;
use tokio::task::JoinHandle;

fn default_log_level() -> String {
    "warn".to_string()
}

/// 对齐 Java `UdpConfig`（`ennwiot.connector-udp` 段，gateway 顶层 `[udp]`）。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UdpConfig {
    #[serde(default)]
    pub configuration: Vec<UdpServerConfig>,
}

/// 对齐 Java `UdpServerConfig`：port / description / logLevel(默认 "warn") / protocol。
///
/// protocol 在 Rust 版为必填：Java 侧 protocol 为 null 时 NPE 被空 catch 吞掉、监听静默不启动；
/// 这里缺字段直接配置解析报错（失败可见）。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UdpServerConfig {
    pub port: u16,
    #[serde(default)]
    pub description: Option<String>,
    // ponytail: logLevel 仅为配置兼容而解析，未接入 tracing 动态级别；
    // 升级路径：按 listener 挂独立 target + EnvFilter。
    #[serde(default = "default_log_level")]
    pub log_level: String,
    pub protocol: ProtocolConfig,
}

/// 单 listener 运行计数（status 暴露，测试断言用）。
#[derive(Debug, Default)]
struct Counters {
    received: AtomicU64,
    forwarded: AtomicU64,
    dropped: AtomicU64,
    decode_errors: AtomicU64,
}

struct Listener {
    local_addr: SocketAddr,
    description: Option<String>,
    counters: Arc<Counters>,
    handle: JoinHandle<()>,
}

/// UDP connector：每个配置项一个绑定 socket + 一个独立接收任务。
pub struct UdpConnector {
    config: UdpConfig,
    registry: Arc<ProtocolRegistry>,
    inner: Mutex<Vec<Listener>>,
}

impl UdpConnector {
    pub fn new(config: UdpConfig, registry: Arc<ProtocolRegistry>) -> Self {
        Self {
            config,
            registry,
            inner: Mutex::new(Vec::new()),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Vec<Listener>> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// 绑定 `0.0.0.0:port` 并开 SO_BROADCAST（对齐 Java `ChannelOption.SO_BROADCAST`）。
/// port=0 由 OS 分配临时端口，实际地址经 `status()` 查询。
fn bind_broadcast(port: u16) -> Result<UdpSocket> {
    // ponytail: 仅 IPv4（对齐 Java NioDatagramChannel 默认）；升级路径：IPv6/双栈。
    let sock = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::DGRAM, None)?;
    sock.set_broadcast(true)?;
    sock.set_nonblocking(true)?;
    sock.bind(&SocketAddr::from(([0, 0, 0, 0], port)).into())?;
    let std_sock: std::net::UdpSocket = sock.into();
    Ok(UdpSocket::from_std(std_sock)?)
}

fn spawn_listener(
    sc: &UdpServerConfig,
    registry: &ProtocolRegistry,
    sink: &Arc<dyn DeviceSink>,
) -> Result<Listener> {
    let proto = registry.get(&sc.protocol.name)?;
    let socket = bind_broadcast(sc.port)?;
    let local_addr = socket.local_addr()?;
    let counters = Arc::new(Counters::default());
    let handle = tokio::spawn(recv_loop(
        socket,
        proto,
        sc.protocol.params.clone(),
        sink.clone(),
        counters.clone(),
        local_addr.to_string(),
    ));
    Ok(Listener {
        local_addr,
        description: sc.description.clone(),
        counters,
        handle,
    })
}

/// 单 listener 接收循环。recv 失败视为 socket 级致命错误（记日志退出）；
/// decode/store 失败仅记日志计数，循环继续（修 Java 空 catch，且不因坏包杀监听）。
async fn recv_loop(
    socket: UdpSocket,
    proto: Arc<dyn Protocol>,
    params: JsonValue,
    sink: Arc<dyn DeviceSink>,
    counters: Arc<Counters>,
    label: String,
) {
    // 65535 = UDP 数据报理论上限，超长包由 OS 层截断/丢弃。
    let mut buf = vec![0u8; 65535];
    loop {
        let (len, src) = match socket.recv_from(&mut buf).await {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(listener = %label, err = %e, "udp recv failed, listener exiting");
                return;
            }
        };
        counters.received.fetch_add(1, Ordering::Relaxed);
        let msgs = match proto.decode_multi(&buf[..len], &params) {
            Ok(m) => m,
            Err(e) => {
                counters.decode_errors.fetch_add(1, Ordering::Relaxed);
                tracing::error!(listener = %label, %src, len, err = %e,
                    "udp decode failed, datagram dropped");
                continue;
            }
        };
        for msg in msgs {
            forward(&sink, &counters, &label, src, msg).await;
        }
    }
}

/// 类型过滤 + 上行（对齐 Java MessageHandler.java:38-70 的 switch）。
async fn forward(
    sink: &Arc<dyn DeviceSink>,
    counters: &Counters,
    label: &str,
    src: SocketAddr,
    msg: JsonValue,
) {
    match parse_message_type(&msg) {
        Some(mt) if is_upstream_type(mt) => {
            if let Err(e) = sink.store_message(msg).await {
                tracing::error!(listener = %label, %src, err = %e, "udp store_message failed");
            } else {
                counters.forwarded.fetch_add(1, Ordering::Relaxed);
            }
        }
        Some(mt) => {
            counters.dropped.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(listener = %label, %src, ?mt, "udp message type not upstream, dropped");
        }
        None => {
            counters.dropped.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(listener = %label, %src, messageType = ?msg.get("messageType"),
                "udp message without parsable messageType, dropped");
        }
    }
}

fn parse_message_type(msg: &JsonValue) -> Option<MessageType> {
    serde_json::from_value::<MessageType>(msg.get("messageType")?.clone()).ok()
}

/// 仅三类设备上行请求进 sink，其余 12 类丢弃。
/// 穷举匹配：common 新增 MessageType 变体时此处编译失败，强制显式决策。
fn is_upstream_type(mt: MessageType) -> bool {
    match mt {
        MessageType::DeviceEventReq | MessageType::DeviceInfoReq | MessageType::DeviceReportReq => {
            true
        }
        MessageType::Heartbeat
        | MessageType::DeviceLoginReq
        | MessageType::DeviceLoginRsp
        | MessageType::DeviceReportRsp
        | MessageType::DeviceStatusReq
        | MessageType::DeviceNtpReq
        | MessageType::DeviceNtpRsp
        | MessageType::CloudOperationReq
        | MessageType::CloudOperationRsp
        | MessageType::CloudHistoryReq
        | MessageType::CloudNtpReq
        | MessageType::CloudNtpRsp => false,
    }
}

#[async_trait]
impl Connector for UdpConnector {
    async fn start(&self, sink: Arc<dyn DeviceSink>) -> Result<()> {
        // ponytail: check-then-act 非原子，假定 gateway 装配单线程调用 start；
        // 升级路径：inner 换 tokio Mutex 或状态机枚举。
        {
            if !self.lock().is_empty() {
                return Err(CommonError::Config("udp connector already started".into()));
            }
        }
        if self.config.configuration.is_empty() {
            tracing::info!("udp: configuration is empty, nothing to bind");
            return Ok(());
        }
        let mut listeners = Vec::with_capacity(self.config.configuration.len());
        for sc in &self.config.configuration {
            match spawn_listener(sc, &self.registry, &sink) {
                Ok(l) => {
                    tracing::info!(listener = %l.local_addr, description = ?l.description,
                        "udp listener started");
                    listeners.push(l);
                }
                Err(e) => {
                    // 部分失败不留半启动的 listener（失败可见 + 不泄漏任务）。
                    for l in listeners {
                        l.handle.abort();
                    }
                    return Err(e);
                }
            }
        }
        self.lock().extend(listeners);
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        // ponytail: abort 硬停，最多丢一条在途数据报；升级路径：watch channel 优雅退出。
        let listeners: Vec<Listener> = self.lock().drain(..).collect();
        for l in listeners {
            l.handle.abort();
            let _ = l.handle.await;
        }
        Ok(())
    }

    fn status(&self) -> StatusMap {
        let inner = self.lock();
        let listeners: Vec<JsonValue> = inner
            .iter()
            .map(|l| {
                let c = &l.counters;
                json!({
                    "localAddr": l.local_addr.to_string(),
                    "description": l.description,
                    "received": c.received.load(Ordering::Relaxed),
                    "forwarded": c.forwarded.load(Ordering::Relaxed),
                    "dropped": c.dropped.load(Ordering::Relaxed),
                    "decodeErrors": c.decode_errors.load(Ordering::Relaxed),
                })
            })
            .collect();
        let mut m = StatusMap::new();
        m.insert("running".into(), json!(!inner.is_empty()));
        m.insert("listeners".into(), JsonValue::Array(listeners));
        m
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use connector_common::{GatewayConfig, LuaProtocol, MockSink};
    use std::time::Duration;

    fn protocols_dir() -> String {
        format!(
            "{}/../connector-common/protocols",
            env!("CARGO_MANIFEST_DIR")
        )
    }

    fn registry_with_json_passthrough() -> Arc<ProtocolRegistry> {
        let mut reg = ProtocolRegistry::new();
        reg.register(
            LuaProtocol::from_script(
                "json_passthrough",
                &format!("{}/json_passthrough.lua", protocols_dir()),
            )
            .unwrap(),
        );
        Arc::new(reg)
    }

    fn server_config(port: u16) -> UdpServerConfig {
        UdpServerConfig {
            port,
            description: Some("test".into()),
            log_level: default_log_level(),
            protocol: ProtocolConfig {
                name: "json_passthrough".into(),
                path: format!("{}/json_passthrough.lua", protocols_dir()),
                params: json!({}),
                protocol_type: "script".into(),
            },
        }
    }

    fn listener_port(status: &StatusMap, idx: usize) -> u16 {
        let addr = status["listeners"][idx]["localAddr"].as_str().unwrap();
        addr.rsplit(':').next().unwrap().parse().unwrap()
    }

    /// 有界轮询等 status 满足条件（真实 socket 无法用 tokio::time::pause）。
    async fn wait_until(conn: &UdpConnector, pred: impl Fn(&StatusMap) -> bool) -> StatusMap {
        for _ in 0..300 {
            let s = conn.status();
            if pred(&s) {
                return s;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!(
            "timeout waiting for status predicate; last = {:?}",
            conn.status()
        );
    }

    fn loopback(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    // ---- 配置解析（字段对齐 Java UdpConfig/UdpServerConfig）----

    const UDP_TOML: &str = r#"
[kafka]
bootstrapServers = "127.0.0.1:9092"

[udp]
[[udp.configuration]]
port = 9100
description = "udp main"
logLevel = "info"
[udp.configuration.protocol]
name = "json_passthrough"
path = "protocols/json_passthrough.lua"
type = "script"
[udp.configuration.protocol.params]
deviceId = "d1"
[[udp.configuration]]
port = 9101
[udp.configuration.protocol]
name = "hex_frame"
path = "protocols/hex_frame.lua"
"#;

    #[test]
    fn parses_udp_section_from_gateway_config() {
        let gw: GatewayConfig = toml::from_str(UDP_TOML).unwrap();
        let udp: UdpConfig = serde_json::from_value(gw.udp.unwrap()).unwrap();
        assert_eq!(udp.configuration.len(), 2);

        let a = &udp.configuration[0];
        assert_eq!(a.port, 9100);
        assert_eq!(a.description.as_deref(), Some("udp main"));
        assert_eq!(a.log_level, "info");
        assert_eq!(a.protocol.name, "json_passthrough");
        assert_eq!(a.protocol.path, "protocols/json_passthrough.lua");
        assert_eq!(a.protocol.protocol_type, "script");
        assert_eq!(a.protocol.params["deviceId"], "d1");

        let b = &udp.configuration[1];
        assert_eq!(b.port, 9101);
        // Java UdpServerConfig 默认值：logLevel = "warn"，description 缺省 null
        assert_eq!(b.log_level, "warn");
        assert!(b.description.is_none());
        assert_eq!(b.protocol.protocol_type, "script");
        assert_eq!(b.protocol.params, json!({}));
    }

    #[test]
    fn rejects_missing_port_or_protocol() {
        // 缺 port
        assert!(
            toml::from_str::<UdpConfig>(
                r#"
[[configuration]]
description = "no port"
[configuration.protocol]
name = "p"
path = "x.lua"
"#
            )
            .is_err()
        );
        // 缺 protocol（Java 侧 null protocol 会 NPE 被空 catch 吞掉，Rust 版必须报错）
        assert!(
            toml::from_str::<UdpConfig>(
                r#"
[[configuration]]
port = 9100
"#
            )
            .is_err()
        );
        // port 类型错误
        assert!(
            toml::from_str::<UdpConfig>(
                r#"
[[configuration]]
port = "not-a-number"
[configuration.protocol]
name = "p"
path = "x.lua"
"#
            )
            .is_err()
        );
    }

    // ---- 类型过滤（对齐 Java MessageHandler.java:38-70）----

    #[test]
    fn only_three_request_types_go_upstream() {
        use MessageType::*;
        assert!(is_upstream_type(DeviceEventReq));
        assert!(is_upstream_type(DeviceInfoReq));
        assert!(is_upstream_type(DeviceReportReq));
        for mt in [
            Heartbeat,
            DeviceLoginReq,
            DeviceLoginRsp,
            DeviceReportRsp,
            DeviceStatusReq,
            DeviceNtpReq,
            DeviceNtpRsp,
            CloudOperationReq,
            CloudOperationRsp,
            CloudHistoryReq,
            CloudNtpReq,
            CloudNtpRsp,
        ] {
            assert!(!is_upstream_type(mt), "{mt:?} should be dropped");
        }
    }

    #[test]
    fn parse_message_type_handles_missing_and_unknown() {
        assert_eq!(
            parse_message_type(&json!({"messageType": "HEARTBEAT"})),
            Some(MessageType::Heartbeat)
        );
        assert_eq!(parse_message_type(&json!({})), None);
        assert_eq!(
            parse_message_type(&json!({"messageType": "NOT_A_TYPE"})),
            None
        );
        assert_eq!(parse_message_type(&json!({"messageType": 42})), None);
    }

    // ---- loopback 端到端（Seam A：MockSink）----

    #[tokio::test]
    async fn loopback_end_to_end_with_type_filter() {
        let conn = UdpConnector::new(
            UdpConfig {
                configuration: vec![server_config(0)],
            },
            registry_with_json_passthrough(),
        );
        let sink = Arc::new(MockSink::new());
        conn.start(sink.clone()).await.unwrap();
        // 重复 start 必须报错（Java 版会静默双绑）
        assert!(conn.start(sink.clone()).await.is_err());

        let status = conn.status();
        assert_eq!(status["running"], json!(true));
        let addr = loopback(listener_port(&status, 0));

        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        // 上行类型：DEVICE_REPORT_REQ → sink
        client
            .send_to(
                br#"{"deviceId":"d1","sn":"s1","messageType":"DEVICE_REPORT_REQ"}"#,
                addr,
            )
            .await
            .unwrap();
        wait_until(&conn, |s| s["listeners"][0]["forwarded"] == json!(1)).await;
        assert_eq!(sink.len(), 1);
        assert_eq!(sink.messages()[0]["messageType"], "DEVICE_REPORT_REQ");
        assert_eq!(sink.messages()[0]["deviceId"], "d1");
        assert_eq!(sink.messages()[0]["sn"], "s1");

        // 过滤类型：HEARTBEAT 被丢弃；再发 EVENT 证明监听器存活（loopback 同 socket 对保序）
        client
            .send_to(
                br#"{"deviceId":"d1","sn":"s1","messageType":"HEARTBEAT"}"#,
                addr,
            )
            .await
            .unwrap();
        client
            .send_to(
                br#"{"deviceId":"d1","sn":"s1","messageType":"DEVICE_EVENT_REQ"}"#,
                addr,
            )
            .await
            .unwrap();
        let s = wait_until(&conn, |st| st["listeners"][0]["forwarded"] == json!(2)).await;
        assert_eq!(s["listeners"][0]["received"], json!(3));
        assert_eq!(s["listeners"][0]["dropped"], json!(1));
        assert_eq!(sink.len(), 2);
        assert!(
            sink.messages()
                .iter()
                .all(|m| m["messageType"] != "HEARTBEAT")
        );

        conn.stop().await.unwrap();
        assert_eq!(conn.status()["running"], json!(false));
        assert!(conn.status()["listeners"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn bad_datagram_is_logged_and_listener_survives() {
        let conn = UdpConnector::new(
            UdpConfig {
                configuration: vec![server_config(0)],
            },
            registry_with_json_passthrough(),
        );
        let sink = Arc::new(MockSink::new());
        conn.start(sink.clone()).await.unwrap();
        let addr = loopback(listener_port(&conn.status(), 0));
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        // 非法 JSON → decode 失败计数，监听器不退出（偏差于 Java exceptionCaught-close）
        client.send_to(b"not json at all", addr).await.unwrap();
        wait_until(&conn, |s| s["listeners"][0]["decodeErrors"] == json!(1)).await;

        // 后续合法报文仍能上行
        client
            .send_to(br#"{"sn":"s2","messageType":"DEVICE_INFO_REQ"}"#, addr)
            .await
            .unwrap();
        wait_until(&conn, |s| s["listeners"][0]["forwarded"] == json!(1)).await;
        assert_eq!(sink.len(), 1);
        assert_eq!(sink.messages()[0]["messageType"], "DEVICE_INFO_REQ");

        conn.stop().await.unwrap();
    }

    #[tokio::test]
    async fn each_config_entry_gets_independent_listener() {
        let conn = UdpConnector::new(
            UdpConfig {
                configuration: vec![server_config(0), server_config(0)],
            },
            registry_with_json_passthrough(),
        );
        let sink = Arc::new(MockSink::new());
        conn.start(sink.clone()).await.unwrap();

        let s = conn.status();
        assert_eq!(s["listeners"].as_array().unwrap().len(), 2);
        let (a0, a1) = (
            loopback(listener_port(&s, 0)),
            loopback(listener_port(&s, 1)),
        );
        assert_ne!(a0, a1);

        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        client
            .send_to(br#"{"sn":"a","messageType":"DEVICE_INFO_REQ"}"#, a0)
            .await
            .unwrap();
        client
            .send_to(br#"{"sn":"b","messageType":"DEVICE_REPORT_REQ"}"#, a1)
            .await
            .unwrap();
        wait_until(&conn, |st| {
            st["listeners"][0]["forwarded"] == json!(1)
                && st["listeners"][1]["forwarded"] == json!(1)
        })
        .await;
        assert_eq!(sink.len(), 2);

        conn.stop().await.unwrap();
    }

    #[tokio::test]
    async fn empty_configuration_starts_nothing() {
        let conn = UdpConnector::new(
            UdpConfig {
                configuration: vec![],
            },
            registry_with_json_passthrough(),
        );
        conn.start(Arc::new(MockSink::new())).await.unwrap();
        assert_eq!(conn.status()["running"], json!(false));
        conn.stop().await.unwrap();
    }

    #[tokio::test]
    async fn unknown_protocol_fails_start() {
        let mut sc = server_config(0);
        sc.protocol.name = "nope".into();
        let conn = UdpConnector::new(
            UdpConfig {
                configuration: vec![sc],
            },
            Arc::new(ProtocolRegistry::new()),
        );
        let err = conn.start(Arc::new(MockSink::new())).await.unwrap_err();
        assert!(
            matches!(err, CommonError::ProtocolNotFound(_)),
            "got {err:?}"
        );
    }
}
