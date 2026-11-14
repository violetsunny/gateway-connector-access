//! opcua connector（票 10）：OPC UA client 按 intervalMinute 轮询（全新实现，Java 侧空壳）。
//!
//! 每个 server 配置一个 tokio 轮询任务：连接 endpointUrl（username 空则匿名）→
//! 周期 `read_values(protocol.params["NODES"])` → DataValue→[`Metric`] 映射（状态码非 Good
//! 单点跳过 + warn，不中断整轮）→ Lua `decode_multi` → `DeviceSink` 上行。
//!
//! 非目标（spec 决策）：订阅模式、地址空间自动浏览。
//!
//! ponytail: opcua 0.12 只有同步 client（异步 client 在其改名后继 async-opcua，未登记依赖
//! 白名单），故整轮「建连→读值→断开」放进 `spawn_blocking`；每轮重建会话即断线重连=下轮重试，
//! 不做会话保活。升级路径：白名单纳入 async-opcua 后改长会话 + async read_values。

mod config;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use connector_common::protocol::{Protocol, ProtocolRegistry};
use connector_common::sink::DeviceSink;
use connector_common::{CommonError, Connector, Metric, Result, StatusMap, async_trait};
use opcua::client::prelude::*;
use serde_json::{Value as JsonValue, json};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{info, warn};

pub use config::{OpcuaConfig, OpcuaServerConfig, parse_nodes};

/// 运行计数，供 `status()` 与测试断言。
#[derive(Debug, Default)]
struct Counters {
    rounds_ok: AtomicU64,
    rounds_failed: AtomicU64,
    messages: AtomicU64,
    nodes_skipped: AtomicU64,
}

/// 会话安全参数（票 12 C-2）：policy/mode + 客户端证书/私钥绝对路径（None 策略下为 None）。
#[derive(Clone)]
struct SecurityParams {
    policy: SecurityPolicy,
    mode: MessageSecurityMode,
    cert_path: Option<PathBuf>,
    key_path: Option<PathBuf>,
}

impl SecurityParams {
    /// 从已校验的 server 配置提取；policy != None 时解析 cert/key 绝对路径。
    fn from_config(server: &OpcuaServerConfig) -> Result<Self> {
        let (cert_path, key_path) = if server.security_policy == SecurityPolicy::None {
            (None, None)
        } else {
            let (cert, key) = server.cert_key_paths()?;
            (Some(cert), Some(key))
        };
        Ok(Self {
            policy: server.security_policy,
            mode: server.security_mode,
            cert_path,
            key_path,
        })
    }
}

/// 单个 OPC UA server 的轮询器：一轮 = 建会话 → read 全部 NodeId → Metric → decode_multi → sink。
struct Poller {
    label: String,
    endpoint_url: String,
    identity: IdentityToken,
    security: SecurityParams,
    nodes: Vec<NodeId>,
    proto: Arc<dyn Protocol>,
    params: JsonValue,
    counters: Arc<Counters>,
}

impl Poller {
    fn new(
        server: &OpcuaServerConfig,
        nodes: Vec<NodeId>,
        proto: Arc<dyn Protocol>,
        counters: Arc<Counters>,
    ) -> Result<Self> {
        Ok(Self {
            label: server.label(),
            endpoint_url: server.endpoint_url.trim().to_string(),
            identity: server.identity(),
            security: SecurityParams::from_config(server)?,
            nodes,
            proto,
            params: server.protocol.params.clone(),
            counters,
        })
    }

    /// 轮询一轮：读值（blocking）→ Metric 映射 → Lua 解码 → sink。
    /// 传输/会话失败返回 Err 由调用方打日志；协议解码失败就地打日志，不中断本轮。
    async fn poll_once(&self, sink: &dyn DeviceSink) -> Result<()> {
        let values = self.read_values().await?;
        let (metrics, skipped) = data_values_to_metrics(&self.nodes, &values, now_millis());
        self.counters
            .nodes_skipped
            .fetch_add(skipped as u64, Ordering::Relaxed);
        if metrics.is_empty() {
            warn!(
                target: "opcua",
                "{} 本轮 {} 个节点全部被跳过（状态码非 Good），不上行",
                self.label,
                self.nodes.len()
            );
            return Ok(());
        }
        let payload = serde_json::to_vec(&metrics)?;
        self.deliver(&payload, sink).await
    }

    /// 同步 client 调用整体挪进 spawn_blocking，不阻塞 tokio worker。
    async fn read_values(&self) -> Result<Vec<DataValue>> {
        let label = self.label.clone();
        let url = self.endpoint_url.clone();
        let identity = self.identity.clone();
        let security = self.security.clone();
        let nodes = self.nodes.clone();
        tokio::task::spawn_blocking(move || read_once(&label, &url, &identity, &security, &nodes))
            .await
            .map_err(|e| {
                CommonError::Internal(format!("opcua {}: 读值任务异常: {e}", self.label))
            })?
    }

    /// Metric JSON → Lua decode_multi → sink（上行唯一出口）。
    async fn deliver(&self, payload: &[u8], sink: &dyn DeviceSink) -> Result<()> {
        let messages = match self.proto.decode_multi(payload, &self.params) {
            Ok(messages) => messages,
            Err(e) => {
                // 解码失败只影响本轮本 server，打日志后正常返回（与 snmp 同一约定）。
                warn!(target: "opcua", "{} 协议解码失败，本轮丢弃: {e}", self.label);
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

/// 构造 endpoint description（票 12 #4 + C-2）。
///
/// user token policy 须与 identity 类型匹配：UserName identity → 声明 UserName policy，
/// 否则会话建立时 `find_policy` 找不到匹配项 → `BadSecurityPolicyRejected`，认证必失败。
/// security policy URI / mode 按配置（`to_uri` 对 None/Basic256Sha256 均安全，Unknown 已被配置层拦截）。
fn build_endpoint(
    url: &str,
    identity: &IdentityToken,
    security: &SecurityParams,
) -> EndpointDescription {
    let token_policy = match identity {
        IdentityToken::UserName(_, _) => UserTokenPolicy {
            policy_id: UAString::from("username"),
            token_type: UserTokenType::UserName,
            issued_token_type: UAString::null(),
            issuer_endpoint_url: UAString::null(),
            security_policy_uri: UAString::null(),
        },
        _ => UserTokenPolicy::anonymous(),
    };
    (url, security.policy.to_uri(), security.mode, token_policy).into()
}

/// 一轮读取：建 client → 连 endpoint（按 security policy/mode）→ read → 断开。
///
/// 失败一律 Err（失败可见）；每轮重建会话即「断线重连=下轮重试」。
fn read_once(
    label: &str,
    url: &str,
    identity: &IdentityToken,
    security: &SecurityParams,
    nodes: &[NodeId],
) -> Result<Vec<DataValue>> {
    // ponytail: PKI 目录（存信任/拒绝的服务端证书）放临时目录，避免污染工作目录；
    // 客户端自身证书/私钥走 security.cert_path/key_path（绝对路径，push 到 pki_dir 后仍取绝对值）。
    // 升级路径：需要固定信任链时把 pki_dir 提为配置项。
    let pki_dir = std::env::temp_dir().join("iot-gateway-opcua-pki");
    let mut builder = ClientBuilder::new()
        .application_name("iot-gateway opcua connector")
        .application_uri("urn:iot-gateway:opcua-client")
        .pki_dir(pki_dir.as_path())
        // ponytail: 信任未知服务端证书（不做 pinning）；升级路径：配置化证书校验。
        .trust_server_certs(true)
        .session_retry_limit(0);
    builder = match (&security.cert_path, &security.key_path) {
        // policy != None：用配置的证书/私钥，不自签采样密钥对（缺文件已在 validate 拦截）。
        (Some(cert), Some(key)) => builder
            .certificate_path(cert.clone())
            .private_key_path(key.clone())
            .create_sample_keypair(false),
        // policy == None：沿用采样密钥对（明文端点仅走形式）。
        _ => builder.create_sample_keypair(true),
    };
    let mut client = builder
        .client()
        .ok_or_else(|| CommonError::Internal(format!("opcua {label}: client 构建失败")))?;

    // #4：endpoint description 的 user token policy 必须与 identity 类型匹配，否则会话
    // 建立时 find_policy 返回 None → BadSecurityPolicyRejected，username 认证必失败。
    let endpoint = build_endpoint(url, identity, security);
    let session = client
        .connect_to_endpoint(endpoint, identity.clone())
        .map_err(|e| {
            CommonError::Internal(format!(
                "opcua {label}: 连接 {url} 失败（policy={}, mode={}）: {e}",
                security.policy, security.mode
            ))
        })?;

    let nodes_to_read: Vec<ReadValueId> = nodes.iter().cloned().map(Into::into).collect();
    let result = {
        let sess = session.read();
        sess.read(&nodes_to_read, TimestampsToReturn::Both, 0.0)
            .map_err(|e| CommonError::Internal(format!("opcua {label}: 读值失败: {e}")))
    };
    session.write().disconnect();
    result
}

/// DataValue 列表 → Metric 列表（Seam C）：按 index 与 nodes 配对。
///
/// - 状态码非 Good（含 Uncertain）→ 单点跳过 + warn，不中断整轮；status 缺失按 Good（OPC UA 语义）；
/// - `ts` 取 sourceTimestamp（毫秒），缺失用 `now_ms`；
/// - `code` 为 NodeId 字符串；`value` 经 [`variant_to_json`]。
///
/// 返回 (metrics, 跳过数)。
pub fn data_values_to_metrics(
    nodes: &[NodeId],
    values: &[DataValue],
    now_ms: i64,
) -> (Vec<Metric>, usize) {
    if values.len() != nodes.len() {
        warn!(
            target: "opcua",
            "read 返回 {} 个值但请求了 {} 个节点，按较短一侧配对",
            values.len(),
            nodes.len()
        );
    }
    let mut metrics = Vec::with_capacity(values.len().min(nodes.len()));
    let mut skipped = 0usize;
    for (node, dv) in nodes.iter().zip(values) {
        if let Some(status) = dv.status
            && !status.is_good()
        {
            skipped += 1;
            warn!(target: "opcua", "节点 {node} 状态码 {status}，跳过该点");
            continue;
        }
        let ts = dv
            .source_timestamp
            .as_ref()
            .map(|t| t.as_chrono().timestamp_millis())
            .unwrap_or(now_ms);
        let value = dv
            .value
            .as_ref()
            .map(variant_to_json)
            .unwrap_or(JsonValue::Null);
        metrics.push(Metric {
            ts,
            code: node.to_string(),
            value,
        });
    }
    (metrics, skipped)
}

/// Variant → serde_json::Value（Seam C）。穷举全部内建类型，不做通配兜底。
pub fn variant_to_json(variant: &Variant) -> JsonValue {
    match variant {
        Variant::Empty => JsonValue::Null,
        Variant::Boolean(v) => json!(v),
        Variant::SByte(v) => json!(v),
        Variant::Byte(v) => json!(v),
        Variant::Int16(v) => json!(v),
        Variant::UInt16(v) => json!(v),
        Variant::Int32(v) => json!(v),
        Variant::UInt32(v) => json!(v),
        Variant::Int64(v) => json!(v),
        Variant::UInt64(v) => json!(v),
        // ponytail: JSON 无 NaN/Inf，serde_json 会序列化成 null；升级路径：改字符串表示。
        Variant::Float(v) => json!(v),
        Variant::Double(v) => json!(v),
        Variant::String(v) => json!(v.to_string()),
        Variant::DateTime(v) => json!(v.as_chrono().timestamp_millis()),
        Variant::Guid(v) => json!(v.to_string()),
        Variant::StatusCode(v) => json!(v.to_string()),
        // 字节统一小写 hex（项目约定：字节双形态 hex+JSON，Metric.value 取 hex）。
        Variant::ByteString(v) => match &v.value {
            Some(bytes) => JsonValue::String(bytes.iter().map(|b| format!("{b:02x}")).collect()),
            None => JsonValue::Null,
        },
        Variant::XmlElement(v) => json!(v.to_string()),
        Variant::QualifiedName(v) => json!(format!("{}:{}", v.namespace_index, v.name)),
        Variant::LocalizedText(v) => json!(v.to_string()),
        Variant::NodeId(v) => json!(v.to_string()),
        Variant::ExpandedNodeId(v) => json!(v.to_string()),
        // ponytail: ExtensionObject/DiagnosticInfo 不做结构化解码，Debug 文本兜底；
        // 升级路径：按 encoding mask 解析 body。
        Variant::ExtensionObject(v) => json!(format!("{v:?}")),
        Variant::DiagnosticInfo(v) => json!(format!("{v:?}")),
        Variant::Variant(v) => variant_to_json(v),
        Variant::DataValue(v) => v
            .value
            .as_ref()
            .map(variant_to_json)
            .unwrap_or(JsonValue::Null),
        Variant::Array(items) => {
            JsonValue::Array(items.values.iter().map(variant_to_json).collect())
        }
    }
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 轮询主循环：首个 tick 立即执行，之后每 `interval` 一轮；单轮失败（含断线）只打日志、
/// 下轮重试；收到 shutdown 信号退出（与 snmp 同一骨架）。
async fn run_poll_loop(
    poller: Arc<Poller>,
    sink: Arc<dyn DeviceSink>,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(interval);
    // 轮次落后时不补偿堆积（server 长时间不可达后恢复时不应瞬间打满）。
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ticker.tick() => match poller.poll_once(sink.as_ref()).await {
                Ok(()) => {
                    poller.counters.rounds_ok.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    poller.counters.rounds_failed.fetch_add(1, Ordering::Relaxed);
                    warn!(target: "opcua", "轮询失败（下轮重试）: {e}");
                }
            },
            changed = shutdown.changed() => {
                if changed.is_err() {
                    warn!(target: "opcua", "shutdown 通道已关闭，退出轮询 {}", poller.label);
                }
                break;
            }
        }
    }
    info!(target: "opcua", "轮询任务退出 {}", poller.label);
}

/// OPC UA connector：每个配置的 server 一个轮询任务。
pub struct OpcuaConnector {
    servers: Vec<OpcuaServerConfig>,
    /// 脚本目录白名单（None = 不启用，向后兼容），start 构建注册表时透传。
    script_dir: Option<String>,
    counters: Arc<Counters>,
    shutdown: watch::Sender<bool>,
    handles: Mutex<Vec<JoinHandle<()>>>,
}

// `Arc<dyn Protocol>` / `JoinHandle` / `watch::Sender` 无 Debug，手写只暴露配置面。
impl std::fmt::Debug for OpcuaConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpcuaConnector")
            .field("servers", &self.servers)
            .finish_non_exhaustive()
    }
}

impl OpcuaConnector {
    /// 构建并校验配置（endpointUrl / intervalMinute / NODES），失败即启动报错。
    /// `script_dir` 为 Some 时 start 加载协议脚本做白名单校验。
    pub fn new(config: OpcuaConfig, script_dir: Option<&str>) -> Result<Self> {
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

    /// 从 `GatewayConfig.opcua`（一期为 JsonValue 兜底）构建。
    pub fn from_value(value: &JsonValue, script_dir: Option<&str>) -> Result<Self> {
        Self::new(OpcuaConfig::from_value(value)?, script_dir)
    }

    fn lock_handles(&self) -> std::sync::MutexGuard<'_, Vec<JoinHandle<()>>> {
        self.handles.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[async_trait]
impl Connector for OpcuaConnector {
    async fn start(&self, sink: Arc<dyn DeviceSink>) -> Result<()> {
        let mut handles = self.lock_handles();
        for server in &self.servers {
            // type != "script"（如 Java 的 jar）→ ProtocolRegistry 报错，启动失败可见。
            let registry = ProtocolRegistry::from_protocols_in(
                &[&server.protocol],
                self.script_dir.as_deref(),
            )?;
            let proto = registry.get(&server.protocol.name)?;
            let nodes = parse_nodes(&server.protocol.params)?;
            let interval = server.interval_duration();
            let poller = Arc::new(Poller::new(server, nodes, proto, self.counters.clone())?);
            info!(
                target: "opcua",
                "启动轮询 {}（{}）：每 {}s，节点 {} 个",
                server.label(),
                server.endpoint_url,
                interval.as_secs(),
                poller.nodes.len()
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
        info!(target: "opcua", "已停止");
        Ok(())
    }

    fn status(&self) -> StatusMap {
        let mut status = StatusMap::new();
        status.insert("connector".into(), json!("opcua"));
        status.insert("servers".into(), json!(self.servers.len()));
        status.insert(
            "endpoints".into(),
            json!(
                self.servers
                    .iter()
                    .map(|s| s.endpoint_url.clone())
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
            "nodesSkipped".into(),
            json!(self.counters.nodes_skipped.load(Ordering::Relaxed)),
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
    use connector_common::sink::MockSink;
    use std::str::FromStr;

    fn node(s: &str) -> NodeId {
        NodeId::from_str(s).unwrap()
    }

    fn lua_path() -> String {
        format!("{}/protocols/opcua_metric.lua", env!("CARGO_MANIFEST_DIR"))
    }

    /// 2023-11-14T22:13:20.123Z = epoch 1700000000123ms。
    fn dt_ms_1_700_000_000_123() -> DateTime {
        DateTime::parse_from_rfc3339("2023-11-14T22:13:20.123Z").unwrap()
    }

    /// 构造一份 server 配置（JSON 形态，走真实解析 + 校验路径）。
    fn server_config(endpoint_url: &str, nodes: &str) -> OpcuaServerConfig {
        let value = json!({
            "configuration": [{
                "endpointUrl": endpoint_url,
                "username": "u",
                "password": "p",
                "intervalMinute": 1,
                "protocol": {
                    "name": "opcua_metric",
                    "path": lua_path(),
                    "type": "script",
                    "params": {
                        "NODES": nodes,
                        "deviceId": "dev-1",
                        "sn": "SN001",
                    },
                },
            }]
        });
        let cfg = OpcuaConfig::from_value(&value).unwrap();
        let server = cfg.configuration.into_iter().next().unwrap();
        server.validate().unwrap();
        server
    }

    /// Good 值且不带 sourceTimestamp（触发 now_ms 兜底；`new_now` 会填当前时间不可断言）。
    fn good(value: Variant) -> DataValue {
        DataValue {
            value: Some(value),
            status: Some(StatusCode::Good),
            source_timestamp: None,
            ..Default::default()
        }
    }

    fn with_status(status: StatusCode) -> DataValue {
        DataValue {
            value: None,
            status: Some(status),
            ..Default::default()
        }
    }

    // ---- Variant → JSON 映射（Seam C）----

    /// 多值类型逐一断言：布尔/整数/浮点/字符串/时间/字节 hex/数组/NodeId。
    #[test]
    fn variant_maps_to_json() {
        assert_eq!(variant_to_json(&Variant::Empty), JsonValue::Null);
        assert_eq!(variant_to_json(&Variant::Boolean(true)), json!(true));
        assert_eq!(variant_to_json(&Variant::SByte(-8)), json!(-8));
        assert_eq!(variant_to_json(&Variant::Byte(255)), json!(255));
        assert_eq!(variant_to_json(&Variant::Int16(-16)), json!(-16));
        assert_eq!(variant_to_json(&Variant::UInt16(16)), json!(16));
        assert_eq!(variant_to_json(&Variant::Int32(-32)), json!(-32));
        assert_eq!(variant_to_json(&Variant::UInt32(32)), json!(32));
        assert_eq!(variant_to_json(&Variant::Int64(-64)), json!(-64));
        // u64 超 i64 上界也不丢精度
        assert_eq!(variant_to_json(&Variant::UInt64(u64::MAX)), json!(u64::MAX));
        assert_eq!(variant_to_json(&Variant::Float(1.5)), json!(1.5));
        assert_eq!(variant_to_json(&Variant::Double(2.25)), json!(2.25));
        assert_eq!(
            variant_to_json(&Variant::String("运行中".into())),
            json!("运行中")
        );
        assert_eq!(
            variant_to_json(&Variant::DateTime(Box::new(dt_ms_1_700_000_000_123()))),
            json!(1_700_000_000_123i64)
        );
        assert_eq!(
            variant_to_json(&Variant::ByteString(ByteString {
                value: Some(vec![0x0a, 0xff]),
            })),
            json!("0aff")
        );
        assert_eq!(
            variant_to_json(&Variant::ByteString(ByteString { value: None })),
            JsonValue::Null
        );
        assert_eq!(
            variant_to_json(&Variant::NodeId(Box::new(node("ns=2;s=Tag1")))),
            json!("ns=2;s=Tag1")
        );
        // 数组逐项递归
        let array = Variant::Array(Box::new(
            Array::new(
                VariantTypeId::Int32,
                vec![Variant::Int32(1), Variant::Int32(2)],
            )
            .unwrap(),
        ));
        assert_eq!(variant_to_json(&array), json!([1, 2]));
    }

    // ---- DataValue → Metric 映射（Seam C）----

    /// Good 值映射 ts/code/value；状态码非 Good 单点跳过不中断。
    #[test]
    fn data_values_map_to_metrics_and_skip_bad_status() {
        let nodes = vec![
            node("ns=2;s=Temp"),
            node("ns=2;s=Bad"),
            node("ns=2;s=Press"),
        ];
        let mut temp = good(Variant::Double(25.5));
        temp.source_timestamp = Some(dt_ms_1_700_000_000_123());
        let values = vec![
            temp,
            with_status(StatusCode::BadNodeIdUnknown),
            good(Variant::Int32(1013)),
        ];

        let (metrics, skipped) = data_values_to_metrics(&nodes, &values, 999);
        assert_eq!(skipped, 1);
        assert_eq!(metrics.len(), 2);
        assert_eq!(metrics[0].code, "ns=2;s=Temp");
        assert_eq!(metrics[0].ts, 1_700_000_000_123);
        assert_eq!(metrics[0].value, json!(25.5));
        assert_eq!(metrics[1].code, "ns=2;s=Press");
        assert_eq!(metrics[1].value, json!(1013));
        // sourceTimestamp 缺失 → now_ms 兜底
        assert_eq!(metrics[1].ts, 999);
    }

    /// Uncertain 也跳过；status 缺失按 Good 处理（OPC UA 语义）。
    #[test]
    fn uncertain_skipped_and_missing_status_treated_good() {
        let nodes = vec![node("ns=2;s=A"), node("ns=2;s=B")];
        let values = vec![
            with_status(StatusCode::UncertainInitialValue),
            DataValue {
                value: Some(Variant::Boolean(true)),
                status: None,
                ..Default::default()
            },
        ];
        let (metrics, skipped) = data_values_to_metrics(&nodes, &values, 1);
        assert_eq!(skipped, 1);
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].code, "ns=2;s=B");
        assert_eq!(metrics[0].value, json!(true));
    }

    /// 返回值与节点数不一致 → 按较短一侧配对，不 panic。
    #[test]
    fn length_mismatch_zips_shorter_side() {
        let nodes = vec![node("ns=2;s=A"), node("ns=2;s=B")];
        let (metrics, skipped) = data_values_to_metrics(&nodes, &[good(Variant::Int32(1))], 1);
        assert_eq!(metrics.len(), 1);
        assert_eq!(skipped, 0);
        let (metrics, _) = data_values_to_metrics(&nodes[..1], &[], 1);
        assert!(metrics.is_empty());
    }

    // ---- Metric → Lua decode_multi → 上行消息（无 server 的链路契约）----

    /// metrics JSON 数组 → opcua_metric.lua → DEVICE_REPORT_REQ。
    #[test]
    fn metrics_payload_decodes_via_lua() {
        let server = server_config("opc.tcp://127.0.0.1:4840", "ns=2;s=Temp");
        let registry = ProtocolRegistry::from_protocols(&[&server.protocol]).unwrap();
        let proto = registry.get(&server.protocol.name).unwrap();

        let nodes = parse_nodes(&server.protocol.params).unwrap();
        let values = vec![good(Variant::Double(25.5))];
        let (metrics, _) = data_values_to_metrics(&nodes, &values, 1_700_000_000_123);
        let payload = serde_json::to_vec(&metrics).unwrap();

        let messages = proto
            .decode_multi(&payload, &server.protocol.params)
            .unwrap();
        assert_eq!(messages.len(), 1);
        let msg = &messages[0];
        assert_eq!(msg["messageType"], "DEVICE_REPORT_REQ");
        assert_eq!(msg["deviceId"], "dev-1");
        assert_eq!(msg["sn"], "SN001");
        assert_eq!(msg["resume"], "N");
        let m = &msg["metrics"][0];
        assert_eq!(m["code"], "ns=2;s=Temp");
        assert_eq!(m["value"], 25.5);
        assert_eq!(m["ts"], 1_700_000_000_123i64);
    }

    // ---- connector 生命周期 ----

    /// `type = "jar"` 协议 → start 报错（对齐 common 层失败可见约定）。
    #[tokio::test]
    async fn start_rejects_jar_protocol() {
        let mut server = server_config("opc.tcp://127.0.0.1:4840", "ns=2;s=Temp");
        server.protocol.protocol_type = "jar".into();
        let connector = OpcuaConnector::new(
            OpcuaConfig {
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
        let dir = std::env::temp_dir().join(format!("gw-scriptdir-{}-opcua", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let server = server_config("opc.tcp://127.0.0.1:1", "ns=2;s=Temp");
        let connector = OpcuaConnector::new(
            OpcuaConfig {
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

    /// NODES 含非法 NodeId → 构建即报错，不进入轮询。
    #[test]
    fn invalid_nodes_fail_fast() {
        let value = json!({
            "configuration": [{
                "endpointUrl": "opc.tcp://127.0.0.1:4840",
                "protocol": {
                    "name": "p",
                    "path": "p.lua",
                    "params": {"NODES": "ns=2;s=Ok,bogus"},
                },
            }]
        });
        let err = OpcuaConnector::from_value(&value, None).unwrap_err();
        assert!(err.to_string().contains("非法 NodeId"), "{err}");
    }

    /// 断线重连从简：server 不可达时每轮失败只计数打日志，循环继续（下轮重试）。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unreachable_server_keeps_retrying_next_round() {
        // 127.0.0.1:1 连接立即被拒，确定性失败且快。
        let server = server_config("opc.tcp://127.0.0.1:1", "ns=2;s=Temp");
        let counters = Arc::new(Counters::default());
        let registry = ProtocolRegistry::from_protocols(&[&server.protocol]).unwrap();
        let poller = Arc::new(
            Poller::new(
                &server,
                parse_nodes(&server.protocol.params).unwrap(),
                registry.get(&server.protocol.name).unwrap(),
                counters.clone(),
            )
            .unwrap(),
        );
        let sink: Arc<dyn DeviceSink> = Arc::new(MockSink::new());
        let (tx, rx) = watch::channel(false);
        // 测试用 30ms 周期（生产周期由 intervalMinute 决定，最小 1 分钟）。
        tokio::spawn(run_poll_loop(poller, sink, Duration::from_millis(30), rx));

        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        while counters.rounds_failed.load(Ordering::Relaxed) < 2 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "等待超时：应连续失败 ≥2 轮（循环未继续）"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(counters.rounds_ok.load(Ordering::Relaxed), 0);
        let _ = tx.send(true);
    }

    // ---- 票 12 #4：endpoint user token policy 匹配 identity ----

    fn security_none() -> SecurityParams {
        SecurityParams {
            policy: SecurityPolicy::None,
            mode: MessageSecurityMode::None,
            cert_path: None,
            key_path: None,
        }
    }

    /// username identity → endpoint 须声明 UserName token policy（否则 find_policy 失配必拒认证）；
    /// 匿名 identity → 保留 Anonymous policy。
    #[test]
    fn endpoint_token_policy_matches_identity() {
        let sec = security_none();
        let user_ep = build_endpoint(
            "opc.tcp://h:4840",
            &IdentityToken::UserName("u".into(), "p".into()),
            &sec,
        );
        let user_policies = user_ep
            .user_identity_tokens
            .expect("username endpoint 应带 token policy");
        assert!(
            user_policies
                .iter()
                .any(|p| p.token_type == UserTokenType::UserName),
            "应声明 UserName policy: {user_policies:?}"
        );

        let anon_ep = build_endpoint("opc.tcp://h:4840", &IdentityToken::Anonymous, &sec);
        let anon_policies = anon_ep
            .user_identity_tokens
            .expect("匿名 endpoint 应带 token policy");
        assert!(
            anon_policies
                .iter()
                .any(|p| p.token_type == UserTokenType::Anonymous),
            "应声明 Anonymous policy: {anon_policies:?}"
        );
    }

    /// C-2：endpoint 的 security policy URI / mode 按配置携带。
    #[test]
    fn endpoint_carries_configured_policy_and_mode() {
        let sec = SecurityParams {
            policy: SecurityPolicy::Basic256Sha256,
            mode: MessageSecurityMode::SignAndEncrypt,
            cert_path: None,
            key_path: None,
        };
        let ep = build_endpoint("opc.tcp://h:4840", &IdentityToken::Anonymous, &sec);
        assert_eq!(
            ep.security_policy_uri,
            UAString::from(SecurityPolicy::Basic256Sha256.to_uri())
        );
        assert_eq!(ep.security_mode, MessageSecurityMode::SignAndEncrypt);
    }
}
