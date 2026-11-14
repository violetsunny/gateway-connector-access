//! gateway 主程序：读取 TOML 配置 → 初始化 tracing → 构建 Lua 协议注册表 →
//! 构建上行 sink（Kafka；bootstrapServers 为空或 producer 创建失败时降级 stdout）→
//! 装配 8 个 connector（失败隔离：单段构建失败仅记日志，不影响其余段）→
//! 逐个 start → 等待 Ctrl-C → 逆序 stop（优雅停机）。

use std::sync::Arc;

use anyhow::{Context, Result};
use connector_common::config::{GatewayConfig, KafkaConfig};
use connector_common::protocol::ProtocolRegistry;
use connector_common::sink::{DeviceSink, KafkaSink};
use connector_common::{Connector, Result as CommonResult, TomlDeviceDirectory, async_trait};
use serde_json::Value as JsonValue;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let path = config_path()?;
    let config = load_config(&path)?;
    let registry = Arc::new(
        ProtocolRegistry::from_protocols_in(
            &config.protocol_configs(),
            config.script_dir.as_deref(),
        )
        .with_context(|| format!("build protocol registry from {path}"))?,
    );
    print_summary(&path, &config, &registry);

    let sink = build_sink(&config.kafka);
    let assembled = build_connectors(&config, Arc::clone(&registry));
    if assembled.connectors.is_empty() {
        if !assembled.failed.is_empty() {
            anyhow::bail!("all connectors failed to build: {:?}", assembled.failed);
        }
        println!("  no connector sections configured -> idle exit");
        return Ok(());
    }

    let mut started: Vec<(&'static str, Arc<dyn Connector>)> = Vec::new();
    for (name, connector) in assembled.connectors {
        match connector.start(Arc::clone(&sink)).await {
            Ok(()) => {
                tracing::info!("{name} connector started");
                started.push((name, connector));
            }
            // 失败隔离：单 connector 启动失败不拖垮整个 gateway（失败可见，仅记日志）。
            Err(e) => tracing::error!("{name} connector start failed: {e}"),
        }
    }
    if started.is_empty() {
        anyhow::bail!("all connectors failed to start");
    }

    tokio::signal::ctrl_c().await.context("wait for ctrl-c")?;
    tracing::info!("ctrl-c received, stopping connectors");
    for (name, connector) in started.iter().rev() {
        if let Err(e) = connector.stop().await {
            tracing::error!("{name} connector stop failed: {e}");
        }
    }
    tracing::info!("gateway stopped");
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

/// 配置路径：优先第一个 CLI 参数，其次 `GATEWAY_CONFIG` 环境变量。
fn config_path() -> Result<String> {
    if let Some(p) = std::env::args().nth(1) {
        return Ok(p);
    }
    match std::env::var("GATEWAY_CONFIG") {
        Ok(p) if !p.is_empty() => Ok(p),
        _ => anyhow::bail!("no config path: pass as first CLI arg or set GATEWAY_CONFIG env"),
    }
}

fn load_config(path: &str) -> Result<GatewayConfig> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("read config from {path}"))?;
    let config: GatewayConfig =
        toml::from_str(&raw).with_context(|| format!("parse config from {path}"))?;
    Ok(config)
}

/// 降级 sink：Kafka 不可用时上行消息打到日志（失败可见，冒烟/本地无外部服务可跑）。
struct StdoutSink;

/// payload 按字符边界截断至 `max` 个字符（utf8 安全），超出追加标记。
fn truncate_chars(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max).collect();
    out.push_str("...truncated");
    out
}

#[async_trait]
impl DeviceSink for StdoutSink {
    async fn store_message(&self, msg: JsonValue) -> CommonResult<()> {
        // ponytail: 只打元数据 + payload 前 256 字符，防大报文刷爆日志；
        // 天花板：需要完整 payload 时修复 kafka 或接文件 sink。
        let payload = msg.to_string();
        let topic = msg.get("topic").and_then(JsonValue::as_str).unwrap_or("");
        let device_id = msg
            .get("deviceId")
            .and_then(JsonValue::as_str)
            .unwrap_or("");
        tracing::warn!(
            %topic,
            %device_id,
            payload_bytes = payload.len(),
            payload = %truncate_chars(&payload, 256),
            "kafka unavailable, upstream message dumped to log"
        );
        Ok(())
    }
}

/// Kafka 是否可用：bootstrapServers 非空（纯函数，便于单测）。
fn kafka_usable(kafka: &KafkaConfig) -> bool {
    !kafka.bootstrap_servers.trim().is_empty()
}

fn build_sink(kafka: &KafkaConfig) -> Arc<dyn DeviceSink> {
    if !kafka_usable(kafka) {
        tracing::warn!("kafka bootstrapServers empty -> falling back to stdout sink");
        return Arc::new(StdoutSink);
    }
    match KafkaSink::new(
        &kafka.bootstrap_servers,
        &kafka.metric_topic,
        &kafka.event_topic,
        &kafka.info_topic,
    ) {
        Ok(sink) => Arc::new(sink),
        Err(e) => {
            tracing::warn!("create kafka producer failed ({e}) -> falling back to stdout sink");
            Arc::new(StdoutSink)
        }
    }
}

/// 装配结果：成功构建的 connector + 构建失败的段名（失败隔离，失败可见）。
struct Assembled {
    connectors: Vec<(&'static str, Arc<dyn Connector>)>,
    failed: Vec<&'static str>,
}

/// 按配置段装配 8 个 connector。单段构建失败仅记 error 日志并计入 `failed`，
/// 不影响其余段（对齐"部分外部服务缺失时 gateway 仍可运行"的冒烟要求）。
fn build_connectors(config: &GatewayConfig, registry: Arc<ProtocolRegistry>) -> Assembled {
    let mut out = Assembled {
        connectors: Vec::new(),
        failed: Vec::new(),
    };
    let mut add = |name: &'static str, built: CommonResult<Arc<dyn Connector>>| match built {
        Ok(connector) => out.connectors.push((name, connector)),
        Err(e) => {
            tracing::error!("{name} connector build failed: {e}");
            out.failed.push(name);
        }
    };
    // 下行 Kafka 统一判据（mqtt/request 共用）：不可用/未配置时传 None，
    // mqtt 下行与 request 下行同样跳过，与上行 sink 降级 stdout 的行为一致。
    let kafka = kafka_usable(&config.kafka).then(|| config.kafka.clone());

    if let Some(tcp) = &config.tcp {
        let directory = Arc::new(TomlDeviceDirectory::new(config.devices.clone()));
        add(
            "tcp",
            Ok(Arc::new(connector_tcp::TcpConnector::new(
                tcp.clone(),
                Arc::clone(&registry),
                directory,
            ))),
        );
    }
    if let Some(udp) = &config.udp {
        add("udp", build_udp(udp, config.script_dir.as_deref()));
    }
    if let Some(mqtt) = &config.mqtt {
        add(
            "mqtt",
            connector_mqtt::MqttConnector::new(mqtt.clone(), Arc::clone(&registry), kafka.clone())
                .map(|c| Arc::new(c) as Arc<dyn Connector>),
        );
    }
    if let Some(modbus) = &config.modbus_tcp {
        add(
            "modbus-tcp",
            connector_modbus_tcp::ModbusTcpConnector::from_value(
                modbus,
                config.script_dir.as_deref(),
            )
            .map(|c| Arc::new(c) as Arc<dyn Connector>),
        );
    }
    if let Some(http) = &config.http {
        add("http", build_http(http, config.script_dir.as_deref()));
    }
    if let Some(snmp) = &config.snmp {
        add(
            "snmp",
            connector_snmp::SnmpConnector::from_value(snmp, config.script_dir.as_deref())
                .map(|c| Arc::new(c) as Arc<dyn Connector>),
        );
    }
    if let Some(request) = &config.request {
        add(
            "request",
            connector_request::RequestConnector::from_value(
                request,
                kafka,
                config.script_dir.as_deref(),
            )
            .map(|c| Arc::new(c) as Arc<dyn Connector>),
        );
    }
    if let Some(opcua) = &config.opcua {
        add(
            "opcua",
            connector_opcua::OpcuaConnector::from_value(opcua, config.script_dir.as_deref())
                .map(|c| Arc::new(c) as Arc<dyn Connector>),
        );
    }
    out
}

/// udp 段：GatewayConfig 里是 JsonValue 兜底 → 解析为 UdpConfig，
/// 并就其自带协议构建独立 registry（udp 协议不进 tcp/mqtt 共享注册表）。
fn build_udp(value: &JsonValue, script_dir: Option<&str>) -> CommonResult<Arc<dyn Connector>> {
    let config: connector_udp::UdpConfig = serde_json::from_value(value.clone())
        .map_err(|e| connector_common::CommonError::Config(format!("udp section: {e}")))?;
    let protocols: Vec<&connector_common::ProtocolConfig> =
        config.configuration.iter().map(|c| &c.protocol).collect();
    let registry = Arc::new(ProtocolRegistry::from_protocols_in(&protocols, script_dir)?);
    Ok(Arc::new(connector_udp::UdpConnector::new(config, registry)))
}

/// http 段：JsonValue 兜底 → HttpConfig（构造期读 Lua 脚本，错误立刻可见）。
fn build_http(value: &JsonValue, script_dir: Option<&str>) -> CommonResult<Arc<dyn Connector>> {
    let config: connector_http::HttpConfig = serde_json::from_value(value.clone())
        .map_err(|e| connector_common::CommonError::Config(format!("http section: {e}")))?;
    Ok(Arc::new(connector_http::HttpConnector::new(
        config, script_dir,
    )?))
}

fn print_summary(path: &str, config: &GatewayConfig, registry: &ProtocolRegistry) {
    let kafka = &config.kafka;
    println!("gateway config: {path}");
    println!("  kafka brokers: {}", kafka.bootstrap_servers);
    println!(
        "  kafka topics: metric={} event={} info={} command={} cloud={}",
        kafka.metric_topic,
        kafka.event_topic,
        kafka.info_topic,
        kafka.command_topic,
        kafka.cloud_topic
    );
    let protocols = registry.names();
    println!(
        "  protocols registered ({}): {:?}",
        protocols.len(),
        protocols
    );
    let connectors = config.configured_connectors();
    println!(
        "  connector sections ({}): {:?}",
        connectors.len(),
        connectors
    );
    println!("  local devices: {}", config.devices.len());
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn kafka(servers: &str) -> KafkaConfig {
        toml::from_str(&format!("bootstrapServers = \"{servers}\"")).unwrap()
    }

    #[test]
    fn kafka_usable_requires_non_blank_servers() {
        assert!(!kafka_usable(&kafka("")));
        assert!(!kafka_usable(&kafka("   ")));
        assert!(kafka_usable(&kafka("127.0.0.1:9092")));
    }

    fn parse(toml_str: &str) -> GatewayConfig {
        toml::from_str(toml_str).unwrap()
    }

    /// 失败隔离：合法 tcp + 非法 udp（configuration 类型错）+ 合法 snmp →
    /// connectors = [tcp, snmp]，failed = [udp]，非法段不拖垮合法段。
    #[test]
    fn build_connectors_isolates_bad_section() {
        let config = parse(
            r#"
[kafka]
bootstrapServers = ""
[[tcp.server.configuration]]
port = 0
[[snmp.configuration]]
address = "127.0.0.1"
port = 161
community = "public"
intervalMinute = 1
[snmp.configuration.protocol]
name = "p"
path = "nonexistent.lua"
[snmp.configuration.protocol.params]
OIDS = "1.3.6.1.2.1.1.1.0"
[udp]
configuration = "broken"
"#,
        );
        // udp.configuration 类型非法（应为数组）→ from_value 失败，段被隔离。
        let registry = Arc::new(ProtocolRegistry::new());
        let assembled = build_connectors(&config, registry);
        let names: Vec<&str> = assembled.connectors.iter().map(|(n, _)| *n).collect();
        assert_eq!(names, vec!["tcp", "snmp"]);
        assert_eq!(assembled.failed, vec!["udp"]);
    }

    /// 仅 kafka 段：无 connector 装配、无失败（gateway 走 idle exit 路径）。
    #[test]
    fn build_connectors_empty_config() {
        let config = parse(
            r#"
[kafka]
bootstrapServers = "127.0.0.1:9092"
"#,
        );
        let registry = Arc::new(ProtocolRegistry::new());
        let assembled = build_connectors(&config, registry);
        assert!(assembled.connectors.is_empty());
        assert!(assembled.failed.is_empty());
    }

    #[test]
    fn truncate_chars_is_utf8_safe_and_marks_overflow() {
        // 短于上限：原样返回
        assert_eq!(truncate_chars("abc", 256), "abc");
        // 恰等于上限：原样返回
        let exact = "x".repeat(256);
        assert_eq!(truncate_chars(&exact, 256), exact);
        // 超出：截断 + 标记
        let long = "x".repeat(300);
        let t = truncate_chars(&long, 256);
        assert!(t.ends_with("...truncated"));
        assert_eq!(t.chars().count(), 256 + "...truncated".chars().count());
        // 多字节字符不被劈开
        let cn = "汉".repeat(300);
        let t = truncate_chars(&cn, 256);
        assert!(t.starts_with("汉"));
        assert!(t.ends_with("...truncated"));
        assert_eq!(t.chars().take(256).collect::<String>(), "汉".repeat(256));
    }

    /// 大 payload 进 StdoutSink 不 panic、正常返回（截断在日志字段层生效）。
    #[tokio::test]
    async fn stdout_sink_handles_large_payload() {
        let sink = StdoutSink;
        let msg = json!({
            "topic": "enn/up",
            "deviceId": "dev-001",
            "messageType": "DEVICE_REPORT_REQ",
            "blob": "x".repeat(100_000),
        });
        sink.store_message(msg).await.unwrap();
    }
}
