//! snmp connector（票 08）：SNMP v2c GET 按 intervalMinute 轮询。
//!
//! 行为基准：Java `SnmpConnectorStater`（每 server 一个 Timer，`:61-62`）+ `SnmpTimerTask`
//! （每轮新建 transport、同步 GET 全部 OID、响应 map → `decodeMulti` → `storeMessage`，`:55-91`）。
//!
//! 与 Java 的刻意差异：
//! - community 来自配置（Java `SnmpTimerTask.java:47` 硬编码 `"huawei123"`）；
//! - 仅 v2c：配置出现 v3 版本或 v3 专有字段 → 启动报错（失败可见）；
//! - 每 server 一个 tokio 任务：单点超时/解码失败只打日志，不影响同轮其他 agent 与后续轮次；
//! - 值→字符串映射见 `ber::SnmpValue::to_wire_string`（TimeTicks 输出原始数值，
//!   不做 snmp4j 的 "d:h:m:s.hs" 格式化）。
//!
//! 上行链路：GET 响应 varbind → `{oid: 值字符串}` JSON → `Protocol::decode_multi` → `DeviceSink`。

mod ber;
mod config;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::time::Duration;

use ber::VarBind;
use connector_common::protocol::{Protocol, ProtocolRegistry};
use connector_common::sink::DeviceSink;
use connector_common::{CommonError, Connector, Result, StatusMap, async_trait};
use serde_json::{Map, Value as JsonValue, json};
use tokio::net::UdpSocket;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{info, warn};

pub use config::{SnmpConfig, SnmpServerConfig, parse_oids};

/// ponytail: 单个 UDP 数据报上限。SNMP GET 响应远小于此；升级路径：需要 GETBULK/大表时
/// 改为按 OID 分批请求。
const MAX_DATAGRAM: usize = 65535;

/// 运行计数，供 `status()` 与测试断言。
#[derive(Debug, Default)]
struct Counters {
    rounds_ok: AtomicU64,
    rounds_failed: AtomicU64,
    messages: AtomicU64,
}

/// 单个 SNMP agent 的轮询器：一轮 = 新建 UDP 会话 → GET 全部 OID → 响应 map → decode_multi → sink。
struct Poller {
    label: String,
    address: String,
    port: u16,
    community: String,
    oids: Vec<String>,
    timeout: Duration,
    attempts: u32,
    proto: Arc<dyn Protocol>,
    params: JsonValue,
    next_id: AtomicI32,
    counters: Arc<Counters>,
}

impl Poller {
    fn new(
        server: &SnmpServerConfig,
        oids: Vec<String>,
        proto: Arc<dyn Protocol>,
        counters: Arc<Counters>,
    ) -> Self {
        Self {
            label: server.label(),
            address: server.address.clone(),
            port: server.port,
            community: server.community.clone(),
            oids,
            timeout: server.timeout_duration(),
            attempts: server.attempts(),
            proto,
            params: server.protocol.params.clone(),
            next_id: AtomicI32::new(1),
            counters,
        }
    }

    /// ponytail: 进程内单调递增并掩掉符号位，不做跨进程唯一（单 gateway 对单 agent 足够）；
    /// 升级路径：需与外部 SNMP 管理器共存时改用随机起始值。
    fn next_request_id(&self) -> i32 {
        (self.next_id.fetch_add(1, Ordering::Relaxed) & 0x7fff_ffff).max(1)
    }

    /// 轮询一轮。传输层失败（超时/网络/BER/agent error-status）返回 Err 由调用方打日志；
    /// 协议解码失败按 Java 行为（`SnmpTimerTask.java:92-93`）就地打日志，不中断本轮。
    async fn poll_once(&self, sink: &dyn DeviceSink) -> Result<()> {
        // 每轮独立会话，对齐 Java 每轮 new DefaultUdpTransportMapping + close。
        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        socket
            .connect((self.address.as_str(), self.port))
            .await
            .map_err(|e| {
                CommonError::Internal(format!("snmp {}: 连接 agent 失败: {e}", self.label))
            })?;

        let request_id = self.next_request_id();
        let request = ber::build_get_request(request_id, &self.community, &self.oids)?;
        let mut buf = vec![0u8; MAX_DATAGRAM];

        for attempt in 1..=self.attempts {
            socket.send(&request).await.map_err(|e| {
                CommonError::Internal(format!("snmp {}: 发送 GET 失败: {e}", self.label))
            })?;
            match tokio::time::timeout(self.timeout, socket.recv(&mut buf)).await {
                Ok(Ok(n)) => {
                    let pdu =
                        ber::parse_get_response(&buf[..n], &self.community, i64::from(request_id))?;
                    let payload = serde_json::to_vec(&varbinds_to_json(&pdu.varbinds))?;
                    return self.deliver(&payload, sink).await;
                }
                Ok(Err(e)) => {
                    return Err(CommonError::Internal(format!(
                        "snmp {}: 接收响应失败: {e}",
                        self.label
                    )));
                }
                Err(_) => warn!(
                    target: "snmp",
                    "GET {} 超时（第 {}/{} 次，{}ms）",
                    self.label,
                    attempt,
                    self.attempts,
                    self.timeout.as_millis()
                ),
            }
        }
        Err(CommonError::Internal(format!(
            "snmp {}: GET 在 {} 次尝试后仍超时",
            self.label, self.attempts
        )))
    }

    /// 响应 JSON → Lua decode_multi → sink（上行唯一出口）。
    async fn deliver(&self, payload: &[u8], sink: &dyn DeviceSink) -> Result<()> {
        let messages = match self.proto.decode_multi(payload, &self.params) {
            Ok(messages) => messages,
            Err(e) => {
                // 解码失败只影响本轮本 agent，打日志后正常返回（对齐 Java catch DecodeMessageException）。
                warn!(target: "snmp", "{} 协议解码失败，本轮丢弃: {e}", self.label);
                return Ok(());
            }
        };
        for message in messages {
            sink.store_message(message).await?;
            self.counters.messages.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }
}

/// GET 响应 varbind → `{oid: 值字符串}`，对齐 Java `SnmpTimerTask.java:75-81`
/// （`map.put(oid, variable.toString())`，值一律字符串）。
fn varbinds_to_json(varbinds: &[VarBind]) -> JsonValue {
    let mut map = Map::new();
    for vb in varbinds {
        map.insert(vb.oid.clone(), JsonValue::String(vb.value.to_wire_string()));
    }
    JsonValue::Object(map)
}

/// 轮询主循环：首个 tick 立即执行（对齐 Java `timer.schedule(task, new Date(), period)`），
/// 之后每 `interval` 一轮；单轮失败只打日志、循环继续；收到 shutdown 信号退出。
async fn run_poll_loop(
    poller: Arc<Poller>,
    sink: Arc<dyn DeviceSink>,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(interval);
    // 轮次落后时不补偿堆积（agent 长时间不可达后恢复时不应瞬间打满）。
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ticker.tick() => match poller.poll_once(sink.as_ref()).await {
                Ok(()) => {
                    poller.counters.rounds_ok.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    poller.counters.rounds_failed.fetch_add(1, Ordering::Relaxed);
                    warn!(target: "snmp", "轮询失败（不中断下轮）: {e}");
                }
            },
            changed = shutdown.changed() => {
                if changed.is_err() {
                    warn!(target: "snmp", "shutdown 通道已关闭，退出轮询 {}", poller.label);
                }
                break;
            }
        }
    }
    info!(target: "snmp", "轮询任务退出 {}", poller.label);
}

/// SNMP connector：每个配置的 agent 一个轮询任务。
pub struct SnmpConnector {
    servers: Vec<SnmpServerConfig>,
    /// 脚本目录白名单（None = 不启用，向后兼容），start 构建注册表时透传。
    script_dir: Option<String>,
    counters: Arc<Counters>,
    shutdown: watch::Sender<bool>,
    handles: Mutex<Vec<JoinHandle<()>>>,
}

// `Arc<dyn Protocol>` / `JoinHandle` / `watch::Sender` 无 Debug，手写只暴露配置面。
impl std::fmt::Debug for SnmpConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnmpConnector")
            .field("servers", &self.servers)
            .finish_non_exhaustive()
    }
}

impl SnmpConnector {
    /// 构建并校验配置（community / v3 / intervalMinute / OIDS），失败即启动报错。
    /// `script_dir` 为 Some 时 start 加载协议脚本做白名单校验。
    pub fn new(config: SnmpConfig, script_dir: Option<&str>) -> Result<Self> {
        for server in &config.configuration {
            server.validate()?;
        }
        let (shutdown, _) = watch::channel(false);
        Ok(Self {
            servers: config.configuration,
            script_dir: script_dir.map(String::from),
            counters: Arc::new(Counters::default()),
            shutdown,
            handles: Mutex::new(Vec::new()),
        })
    }

    /// 从 `GatewayConfig.snmp`（一期为 JsonValue 兜底）构建。
    pub fn from_value(value: &JsonValue, script_dir: Option<&str>) -> Result<Self> {
        Self::new(SnmpConfig::from_value(value)?, script_dir)
    }

    fn lock_handles(&self) -> std::sync::MutexGuard<'_, Vec<JoinHandle<()>>> {
        self.handles.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[async_trait]
impl Connector for SnmpConnector {
    async fn start(&self, sink: Arc<dyn DeviceSink>) -> Result<()> {
        let mut handles = self.lock_handles();
        for server in &self.servers {
            // type != "script"（如 Java 的 jar）→ ProtocolRegistry 报错，启动失败可见。
            let registry = ProtocolRegistry::from_protocols_in(
                &[&server.protocol],
                self.script_dir.as_deref(),
            )?;
            let proto = registry.get(&server.protocol.name)?;
            let oids = parse_oids(&server.protocol.params)?;
            let interval = server.interval_duration();
            let poller = Arc::new(Poller::new(server, oids, proto, self.counters.clone()));
            info!(
                target: "snmp",
                "启动轮询 {}：每 {}s，OID {} 个，超时 {}ms，尝试 {} 次",
                server.label(),
                interval.as_secs(),
                poller.oids.len(),
                server.timeout_duration().as_millis(),
                server.attempts()
            );
            handles.push(tokio::spawn(run_poll_loop(
                poller,
                sink.clone(),
                interval,
                self.shutdown.subscribe(),
            )));
        }
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        // 无接收者（未 start）时 send 返回 Err，此处无需处理。
        let _ = self.shutdown.send(true);
        let handles = std::mem::take(&mut *self.lock_handles());
        for handle in handles {
            // 轮询任务内部不 panic；JoinError 只可能是外部 abort，同样视为已停止。
            let _ = handle.await;
        }
        info!(target: "snmp", "已停止");
        Ok(())
    }

    fn status(&self) -> StatusMap {
        let mut status = StatusMap::new();
        status.insert("connector".into(), json!("snmp"));
        status.insert("version".into(), json!("v2c"));
        status.insert("servers".into(), json!(self.servers.len()));
        status.insert(
            "targets".into(),
            json!(
                self.servers
                    .iter()
                    .map(SnmpServerConfig::label)
                    .collect::<Vec<_>>()
            ),
        );
        status.insert(
            "roundsOk".into(),
            json!(self.counters.rounds_ok.load(Ordering::Relaxed)),
        );
        status.insert(
            "roundsFailed".into(),
            json!(self.counters.rounds_failed.load(Ordering::Relaxed)),
        );
        status.insert(
            "messagesStored".into(),
            json!(self.counters.messages.load(Ordering::Relaxed)),
        );
        status
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ber::{SnmpValue, build_message, build_response, parse_get_request};
    use connector_common::protocol::LuaProtocol;
    use connector_common::sink::MockSink;
    use std::net::SocketAddr;

    fn lua_path() -> String {
        format!("{}/protocols/snmp_metric.lua", env!("CARGO_MANIFEST_DIR"))
    }

    /// 构造一份指向 `addr` 的 server 配置（JSON 形态，走真实解析 + 校验路径）。
    fn server_config(addr: SocketAddr, timeout_ms: u64, oids: &str) -> SnmpServerConfig {
        let value = json!({
            "configuration": [{
                "address": addr.ip().to_string(),
                "port": addr.port(),
                "community": "test-community",
                "timeout": timeout_ms,
                "retry": 0,
                "intervalMinute": 1,
                "version": "v2c",
                "protocol": {
                    "name": "snmp_metric",
                    "path": lua_path(),
                    "type": "script",
                    "params": {
                        "OIDS": oids,
                        "deviceId": "dev-1",
                        "sn": "SN001",
                        "ts": 1_700_000_000_000i64,
                        "codes": {
                            "1.3.6.1.2.1.1.3.0": "uptime",
                            "1.3.6.1.2.1.2.2.1.10.1": "ifInOctets",
                        },
                    },
                },
            }]
        });
        let cfg = SnmpConfig::from_value(&value).unwrap();
        let server = cfg.configuration.into_iter().next().unwrap();
        server.validate().unwrap();
        server
    }

    fn test_protocol(server: &SnmpServerConfig) -> Arc<dyn Protocol> {
        let registry = ProtocolRegistry::from_protocols(&[&server.protocol]).unwrap();
        registry.get(&server.protocol.name).unwrap()
    }

    /// decode_multi 必定抛错的协议（验证解码失败不中断轮询）。
    fn failing_protocol() -> Arc<dyn Protocol> {
        Arc::new(
            LuaProtocol::from_source(
                "failing",
                r#"
                function decode(input, params) return {} end
                function decode_multi(input, params) error("boom") end
                function encode(msg, params) return "" end
                function login(req, params) return true end
                "#,
            )
            .unwrap(),
        )
    }

    /// 被测的两个测点（TimeTicks + Counter32）。
    fn sample_varbinds() -> Vec<VarBind> {
        vec![
            VarBind {
                oid: "1.3.6.1.2.1.1.3.0".into(),
                value: SnmpValue::TimeTicks(12345678),
            },
            VarBind {
                oid: "1.3.6.1.2.1.2.2.1.10.1".into(),
                value: SnmpValue::Counter32(4294967295),
            },
        ]
    }

    /// 假 agent：收到 GET 请求即按固定 varbind 回响应（用本 crate 的 BER 编解码）。
    async fn spawn_fake_agent(varbinds: Vec<VarBind>) -> SocketAddr {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buf = vec![0u8; MAX_DATAGRAM];
            loop {
                let Ok((n, peer)) = socket.recv_from(&mut buf).await else {
                    break;
                };
                let Ok(pdu) = parse_get_request(&buf[..n], "test-community") else {
                    continue;
                };
                let frame = build_response(pdu.request_id, "test-community", 0, 0, &varbinds);
                if socket.send_to(&frame, peer).await.is_err() {
                    break;
                }
            }
        });
        addr
    }

    /// 只回固定帧、不解析请求的假 agent（用于 community 不匹配等异常路径）。
    async fn spawn_canned_agent(frame: Vec<u8>) -> SocketAddr {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buf = vec![0u8; MAX_DATAGRAM];
            loop {
                let Ok((_, peer)) = socket.recv_from(&mut buf).await else {
                    break;
                };
                if socket.send_to(&frame, peer).await.is_err() {
                    break;
                }
            }
        });
        addr
    }

    /// 有界等待，避免固定 sleep 造成 flaky。
    async fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !cond() {
            assert!(tokio::time::Instant::now() < deadline, "等待超时: {what}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    // ---- 响应 → Metric 映射 ----

    /// varbind → `{oid: 值字符串}`（Java `map.put(oid, variable.toString())` 等价）。
    #[test]
    fn varbinds_map_to_oid_string_json() {
        let mut varbinds = sample_varbinds();
        varbinds.push(VarBind {
            oid: "1.3.6.1.2.1.1.1.0".into(),
            value: SnmpValue::OctetString(b"Linux router".to_vec()),
        });
        let value = varbinds_to_json(&varbinds);
        assert_eq!(value["1.3.6.1.2.1.1.3.0"], "12345678");
        assert_eq!(value["1.3.6.1.2.1.2.2.1.10.1"], "4294967295");
        assert_eq!(value["1.3.6.1.2.1.1.1.0"], "Linux router");
        assert_eq!(value.as_object().unwrap().len(), 3);
    }

    /// NoSuchObject 也进 map（值为标记名），由 Lua 脚本决定是否丢弃——不静默吞。
    #[test]
    fn no_such_object_is_mapped_not_dropped() {
        let value = varbinds_to_json(&[VarBind {
            oid: "1.3.6.1.2.1.99.1.0".into(),
            value: SnmpValue::NoSuchObject,
        }]);
        assert_eq!(value["1.3.6.1.2.1.99.1.0"], "noSuchObject");
    }

    // ---- 端到端（loopback 假 agent + Lua 协议 + MockSink）----

    /// start → 首轮立即 GET → decode_multi → store_message；stop 后不再轮询。
    #[tokio::test]
    async fn polls_agent_and_stores_decoded_messages() {
        let agent = spawn_fake_agent(sample_varbinds()).await;
        let server = server_config(agent, 2000, "1.3.6.1.2.1.1.3.0,1.3.6.1.2.1.2.2.1.10.1");
        let connector = SnmpConnector::new(
            SnmpConfig {
                configuration: vec![server],
            },
            None,
        )
        .unwrap();
        let sink = Arc::new(MockSink::new());

        connector.start(sink.clone()).await.unwrap();
        wait_until("首轮上行消息", || !sink.is_empty()).await;

        let messages = sink.messages();
        assert_eq!(messages.len(), 1);
        let msg = &messages[0];
        assert_eq!(msg["messageType"], "DEVICE_REPORT_REQ");
        assert_eq!(msg["deviceId"], "dev-1");
        assert_eq!(msg["sn"], "SN001");
        let metrics = msg["metrics"].as_array().unwrap();
        assert_eq!(metrics.len(), 2);
        // Lua fixture 按 code 排序：ifInOctets < uptime
        assert_eq!(metrics[0]["code"], "ifInOctets");
        assert_eq!(metrics[0]["value"], 4294967295u64);
        assert_eq!(metrics[0]["ts"], 1700000000000u64);
        assert_eq!(metrics[1]["code"], "uptime");
        assert_eq!(metrics[1]["value"], 12345678);

        let status = connector.status();
        assert_eq!(status["connector"], json!("snmp"));
        assert_eq!(status["version"], json!("v2c"));
        assert_eq!(status["servers"], json!(1));
        assert_eq!(status["messagesStored"], json!(1));
        assert!(status["roundsOk"].as_u64().unwrap() >= 1);
        assert_eq!(status["roundsFailed"], json!(0));

        connector.stop().await.unwrap();
        let rounds_after_stop = connector.status()["roundsOk"].as_u64().unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            connector.status()["roundsOk"].as_u64().unwrap(),
            rounds_after_stop,
            "stop 后不应继续轮询"
        );
        assert!(connector.lock_handles().is_empty());
    }

    /// 单点超时不中断整轮：黑洞 agent 连续失败仍继续下一轮，同轮另一个 agent 正常上行。
    #[tokio::test]
    async fn one_agent_timeout_does_not_stop_others_or_next_rounds() {
        let agent = spawn_fake_agent(sample_varbinds()).await;
        // 只绑定不应答的 socket = 确定性超时（不依赖 ICMP 行为）。
        let blackhole = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let blackhole_addr = blackhole.local_addr().unwrap();

        let good_server = server_config(agent, 2000, "1.3.6.1.2.1.1.3.0");
        let bad_server = server_config(blackhole_addr, 100, "1.3.6.1.2.1.1.3.0");
        let mock = Arc::new(MockSink::new());
        let sink: Arc<dyn DeviceSink> = mock.clone();

        let good_counters = Arc::new(Counters::default());
        let bad_counters = Arc::new(Counters::default());
        let good = Arc::new(Poller::new(
            &good_server,
            parse_oids(&good_server.protocol.params).unwrap(),
            test_protocol(&good_server),
            good_counters.clone(),
        ));
        let bad = Arc::new(Poller::new(
            &bad_server,
            parse_oids(&bad_server.protocol.params).unwrap(),
            test_protocol(&bad_server),
            bad_counters.clone(),
        ));

        let (tx, rx_good) = watch::channel(false);
        let rx_bad = tx.subscribe();
        // 测试用 30ms 周期（生产周期由 intervalMinute 决定，最小 1 分钟）。
        tokio::spawn(run_poll_loop(
            good,
            sink.clone(),
            Duration::from_millis(30),
            rx_good,
        ));
        tokio::spawn(run_poll_loop(
            bad,
            sink.clone(),
            Duration::from_millis(30),
            rx_bad,
        ));

        wait_until(
            "黑洞 agent 连续失败 ≥2 轮且正常 agent 已上行",
            || {
                bad_counters.rounds_failed.load(Ordering::Relaxed) >= 2
                    && bad_counters.rounds_ok.load(Ordering::Relaxed) == 0
                    && good_counters.rounds_ok.load(Ordering::Relaxed) >= 1
            },
        )
        .await;
        assert!(!mock.is_empty(), "正常 agent 的消息应已上行");

        let _ = tx.send(true);
        drop(blackhole);
    }

    /// 协议解码失败只打日志，不中断后续轮次（Java catch DecodeMessageException 等价）。
    #[tokio::test]
    async fn decode_failure_does_not_stop_polling() {
        let agent = spawn_fake_agent(sample_varbinds()).await;
        let server = server_config(agent, 2000, "1.3.6.1.2.1.1.3.0");
        let counters = Arc::new(Counters::default());
        let poller = Arc::new(Poller::new(
            &server,
            parse_oids(&server.protocol.params).unwrap(),
            failing_protocol(),
            counters.clone(),
        ));
        let sink: Arc<dyn DeviceSink> = Arc::new(MockSink::new());
        let (tx, rx) = watch::channel(false);
        tokio::spawn(run_poll_loop(poller, sink, Duration::from_millis(30), rx));

        // 解码错误在 poll_once 内部打日志并返回 Ok → rounds_ok 递增、无消息上行、循环继续
        wait_until("解码失败后仍继续轮询", || {
            counters.rounds_ok.load(Ordering::Relaxed) >= 2
        })
        .await;
        assert_eq!(counters.rounds_failed.load(Ordering::Relaxed), 0);
        assert_eq!(counters.messages.load(Ordering::Relaxed), 0);

        let _ = tx.send(true);
    }

    /// agent 返回 error-status → 本轮失败可见，错误信息带 RFC 名称。
    #[tokio::test]
    async fn agent_error_status_is_reported() {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buf = vec![0u8; MAX_DATAGRAM];
            loop {
                let Ok((n, peer)) = socket.recv_from(&mut buf).await else {
                    break;
                };
                let Ok(pdu) = parse_get_request(&buf[..n], "test-community") else {
                    continue;
                };
                let frame = build_response(pdu.request_id, "test-community", 2, 1, &[]);
                if socket.send_to(&frame, peer).await.is_err() {
                    break;
                }
            }
        });

        let server = server_config(addr, 2000, "1.3.6.1.2.1.1.3.0");
        let poller = Poller::new(
            &server,
            parse_oids(&server.protocol.params).unwrap(),
            test_protocol(&server),
            Arc::new(Counters::default()),
        );
        let sink = MockSink::new();
        let err = poller.poll_once(&sink).await.unwrap_err();
        assert!(err.to_string().contains("noSuchName"), "{err}");
        assert!(sink.is_empty());
    }

    /// community 不匹配的响应被丢弃（不拿别人的数据伪造上行）。
    #[tokio::test]
    async fn wrong_community_response_is_dropped() {
        let frame = build_message(1, "attacker", 0xA2, 1, 0, 0, &sample_varbinds());
        let agent = spawn_canned_agent(frame).await;
        let server = server_config(agent, 500, "1.3.6.1.2.1.1.3.0");
        let poller = Poller::new(
            &server,
            parse_oids(&server.protocol.params).unwrap(),
            test_protocol(&server),
            Arc::new(Counters::default()),
        );
        let sink = MockSink::new();
        let err = poller.poll_once(&sink).await.unwrap_err();
        assert!(err.to_string().contains("community"), "{err}");
        assert!(sink.is_empty());
    }

    /// 请求帧带上配置的 community 与全部 OID（修掉 Java 硬编码 "huawei123"）。
    #[tokio::test]
    async fn request_carries_configured_community_and_all_oids() {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
        tokio::spawn(async move {
            let mut buf = vec![0u8; MAX_DATAGRAM];
            if let Ok((n, _)) = socket.recv_from(&mut buf).await {
                let _ = tx.send(buf[..n].to_vec());
            }
        });

        let server = server_config(addr, 200, "1.3.6.1.2.1.1.1.0, 1.3.6.1.2.1.1.3.0");
        let poller = Poller::new(
            &server,
            parse_oids(&server.protocol.params).unwrap(),
            test_protocol(&server),
            Arc::new(Counters::default()),
        );
        let sink = MockSink::new();
        // 无响应 → 超时 Err，但请求帧已被捕获
        assert!(poller.poll_once(&sink).await.is_err());

        let frame = rx.recv().await.unwrap();
        assert!(parse_get_request(&frame, "wrong-community").is_err());
        let pdu = parse_get_request(&frame, "test-community").unwrap();
        assert_eq!(
            pdu.varbinds
                .iter()
                .map(|v| v.oid.clone())
                .collect::<Vec<_>>(),
            vec!["1.3.6.1.2.1.1.1.0", "1.3.6.1.2.1.1.3.0"]
        );
        assert_eq!(pdu.request_id, 1);
    }

    /// `type = "jar"` 协议 → start 报错（对齐 common 层失败可见约定）。
    #[tokio::test]
    async fn start_rejects_jar_protocol() {
        let agent = spawn_fake_agent(sample_varbinds()).await;
        let mut server = server_config(agent, 500, "1.3.6.1.2.1.1.3.0");
        server.protocol.protocol_type = "jar".into();
        let connector = SnmpConnector::new(
            SnmpConfig {
                configuration: vec![server],
            },
            None,
        )
        .unwrap();
        let sink: Arc<dyn DeviceSink> = Arc::new(MockSink::new());
        let err = connector.start(sink).await.unwrap_err();
        assert!(
            matches!(err, CommonError::UnsupportedProtocolType { .. }),
            "{err:?}"
        );
        connector.stop().await.unwrap();
    }

    /// script_dir 白名单：脚本真实存在但在目录外 → start 报错（失败可见）。
    #[tokio::test]
    async fn start_rejects_script_outside_dir() {
        let dir = std::env::temp_dir().join(format!("gw-scriptdir-{}-snmp", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let server = server_config("127.0.0.1:1".parse().unwrap(), 500, "1.3.6.1.2.1.1.3.0");
        let connector = SnmpConnector::new(
            SnmpConfig {
                configuration: vec![server],
            },
            Some(dir.to_str().unwrap()),
        )
        .unwrap();
        let sink: Arc<dyn DeviceSink> = Arc::new(MockSink::new());
        let err = connector.start(sink).await.unwrap_err();
        assert!(err.to_string().contains("拒绝加载"), "{err}");
        connector.stop().await.unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }

    /// v3 配置 → 构建即报错，不进入轮询。
    #[test]
    fn v3_config_fails_fast() {
        let value = json!({
            "configuration": [{
                "address": "10.0.0.1",
                "community": "public",
                "version": "v3",
                "protocol": {
                    "name": "p",
                    "path": "p.lua",
                    "params": {"OIDS": "1.3.6.1.2.1.1.1.0"},
                },
            }]
        });
        let err = SnmpConnector::from_value(&value, None).unwrap_err();
        assert!(err.to_string().contains("仅实现 v2c"), "{err}");
    }

    /// 缺 community → 构建即报错（Java 硬编码问题的显式化）。
    #[test]
    fn missing_community_fails_fast() {
        let value = json!({
            "configuration": [{
                "address": "10.0.0.1",
                "protocol": {
                    "name": "p",
                    "path": "p.lua",
                    "params": {"OIDS": "1.3.6.1.2.1.1.1.0"},
                },
            }]
        });
        let err = SnmpConnector::from_value(&value, None).unwrap_err();
        assert!(err.to_string().contains("community"), "{err}");
    }
}
