//! modbus-tcp 段配置模型，字段名对齐 Java `ModbusServerConfig`（camelCase 经 serde rename）。
//!
//! Java 契约字段：clientId / pointDTOSPath / pointMapDTOSPath / host / port /
//! readDiff / maxRetries / retryDelay / connectTimeout，默认值 10/100/60/5000
//! 来自 `ModbusConnectorStater.init`(:67-70)。
//!
//! Rust 版新增 `protocol` 字段：Java 硬编码 ModbusDeviceProtocol，Rust 版协议一律
//! Lua 脚本（AGENTS.md 约定），点表解析逻辑不变、消息封装移入脚本。
//!
//! 入口 [`ModbusTcpConfig::from_value`]：`GatewayConfig.modbus_tcp` 一期是
//! `serde_json::Value` 兜底，本模块负责解析成强类型配置并校验
//! （沿用 connector-opcua 的既定模式）。

use std::time::Duration;

use connector_common::{CommonError, ProtocolConfig, Result};
use serde::Deserialize;
use serde_json::Value as JsonValue;

pub(crate) const DEFAULT_PORT: u16 = 502;
pub(crate) const DEFAULT_READ_DIFF: u64 = 10;
pub(crate) const DEFAULT_MAX_RETRIES: u32 = 100;
pub(crate) const DEFAULT_RETRY_DELAY: u64 = 60;
pub(crate) const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 5000;

fn default_port() -> u16 {
    DEFAULT_PORT
}
fn default_read_diff() -> u64 {
    DEFAULT_READ_DIFF
}
fn default_max_retries() -> u32 {
    DEFAULT_MAX_RETRIES
}
fn default_retry_delay() -> u64 {
    DEFAULT_RETRY_DELAY
}
fn default_connect_timeout_ms() -> u64 {
    DEFAULT_CONNECT_TIMEOUT_MS
}

/// `[modbus-tcp]` 段（多 server，各自独立连接与轮询）。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModbusTcpConfig {
    #[serde(default)]
    pub configuration: Vec<ModbusServerConfig>,
}

/// 单个 Modbus TCP server（PLC/网关从站侧），对齐 Java `ModbusServerConfig`。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModbusServerConfig {
    pub client_id: String,
    /// 点表 URL。Java 字段名 pointDTOSPath（rename_all=camelCase 会产出
    /// pointDtosPath，需显式 rename）。
    #[serde(rename = "pointDTOSPath")]
    pub point_dtos_path: String,
    /// 点位-设备映射 URL，同上需显式 rename。
    #[serde(rename = "pointMapDTOSPath")]
    pub point_map_dtos_path: String,
    pub host: String,
    /// Modbus TCP 标准端口 502 兜底（Java 无默认，null 会在 Netty 连接时 NPE）。
    #[serde(default = "default_port")]
    pub port: u16,
    /// 轮询间隔秒。Java `scheduleWithFixedDelay(readTask, 0, readDiff, SECONDS)`
    /// 的 delay——上一轮结束后再等 readDiff，非固定频率。
    #[serde(default = "default_read_diff")]
    pub read_diff: u64,
    /// 连续连接失败上限，达到后放弃该 server（Java ModBusTcpPoll 同语义）。
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// 重连间隔秒。
    #[serde(default = "default_retry_delay")]
    pub retry_delay: u64,
    /// 连接超时毫秒。Java 字段名 connectTimeout。
    #[serde(rename = "connectTimeout", default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// 上行协议（Lua），Rust 版新增字段，见模块头。
    pub protocol: ProtocolConfig,
}

impl ModbusTcpConfig {
    /// 从 `GatewayConfig.modbus_tcp`（JsonValue）解析。
    ///
    /// 逐 server 字段校验在 [`ModbusServerConfig::validate`]，由
    /// `ModbusTcpConnector::new` 调用。
    pub fn from_value(value: &JsonValue) -> Result<Self> {
        serde_json::from_value(value.clone())
            .map_err(|e| CommonError::Config(format!("modbus-tcp 段解析失败: {e}")))
    }
}

impl ModbusServerConfig {
    /// 日志/status 用标识（Java 以 clientId 为 client 管理键）。
    pub fn label(&self) -> &str {
        &self.client_id
    }

    pub fn read_diff_duration(&self) -> Duration {
        Duration::from_secs(self.read_diff)
    }

    pub fn retry_delay_duration(&self) -> Duration {
        Duration::from_secs(self.retry_delay)
    }

    pub fn connect_timeout(&self) -> Duration {
        Duration::from_millis(self.connect_timeout_ms)
    }

    /// 启动前校验：失败即报错，不带病运行。
    /// URL 合法性不在此查——reqwest 在 start() 拉点表时会带 URL 报错（失败可见）。
    pub fn validate(&self) -> Result<()> {
        let label = self.label();
        if label.trim().is_empty() {
            return Err(CommonError::Config(
                "modbus-tcp: clientId 不能为空".to_string(),
            ));
        }
        if self.host.trim().is_empty() {
            return Err(CommonError::Config(format!(
                "modbus-tcp {label}: host 不能为空"
            )));
        }
        if self.point_dtos_path.trim().is_empty() {
            return Err(CommonError::Config(format!(
                "modbus-tcp {label}: pointDTOSPath 不能为空"
            )));
        }
        if self.point_map_dtos_path.trim().is_empty() {
            return Err(CommonError::Config(format!(
                "modbus-tcp {label}: pointMapDTOSPath 不能为空"
            )));
        }
        // readDiff=0 在 Java 是 IllegalArgumentException；Rust 等价拒绝（否则 busy loop）。
        if self.read_diff == 0 {
            return Err(CommonError::Config(format!(
                "modbus-tcp {label}: readDiff 必须 > 0"
            )));
        }
        if self.connect_timeout_ms == 0 {
            return Err(CommonError::Config(format!(
                "modbus-tcp {label}: connectTimeout 必须 > 0"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use connector_common::GatewayConfig;

    /// TOML fixture → GatewayConfig.modbus_tcp（JsonValue）→ ModbusTcpConfig，
    /// 走 gateway 的真实解析路径。
    fn parse_toml(toml_text: &str) -> Result<ModbusTcpConfig> {
        let cfg: GatewayConfig = toml::from_str(toml_text).expect("TOML fixture 本身应合法");
        let section = cfg
            .modbus_tcp
            .as_ref()
            .expect("fixture 必须包含 [modbus-tcp] 段")
            .clone();
        ModbusTcpConfig::from_value(&section)
    }

    const FULL: &str = r#"
[kafka]
bootstrapServers = "127.0.0.1:9092"

[modbus-tcp]
[[modbus-tcp.configuration]]
clientId = "plc-1"
pointDTOSPath = "https://gw.example.com/access/cloud-gateway/x/modbus/points"
pointMapDTOSPath = "https://gw.example.com/access/cloud-gateway/x/point/mapping"
host = "10.0.0.30"
port = 1502
readDiff = 5
maxRetries = 3
retryDelay = 30
connectTimeout = 2000
[modbus-tcp.configuration.protocol]
name = "modbus_metric"
path = "protocols/modbus_metric.lua"
type = "script"
[modbus-tcp.configuration.protocol.params]
foo = "bar"
"#;

    #[test]
    fn parses_toml_fixture() {
        let cfg = parse_toml(FULL).unwrap();
        assert_eq!(cfg.configuration.len(), 1);
        let s = &cfg.configuration[0];
        // 显式 rename 的三个 Java 缩写字段名
        assert_eq!(s.client_id, "plc-1");
        assert!(s.point_dtos_path.ends_with("/modbus/points"));
        assert!(s.point_map_dtos_path.ends_with("/point/mapping"));
        assert_eq!(s.host, "10.0.0.30");
        assert_eq!(s.port, 1502);
        assert_eq!(s.read_diff, 5);
        assert_eq!(s.max_retries, 3);
        assert_eq!(s.retry_delay, 30);
        assert_eq!(s.connect_timeout_ms, 2000);
        assert_eq!(s.protocol.name, "modbus_metric");
        assert_eq!(s.protocol.protocol_type, "script");
        assert_eq!(s.protocol.params["foo"], "bar");
        assert_eq!(s.label(), "plc-1");
        assert_eq!(s.read_diff_duration(), Duration::from_secs(5));
        assert_eq!(s.retry_delay_duration(), Duration::from_secs(30));
        assert_eq!(s.connect_timeout(), Duration::from_millis(2000));
        s.validate().unwrap();
    }

    /// 最小配置：port/readDiff/maxRetries/retryDelay/connectTimeout 全走 Java 默认值。
    #[test]
    fn applies_java_defaults() {
        let cfg = parse_toml(
            r#"
[kafka]
bootstrapServers = "x"
[modbus-tcp]
[[modbus-tcp.configuration]]
clientId = "c1"
pointDTOSPath = "http://h/p"
pointMapDTOSPath = "http://h/m"
host = "10.0.0.30"
[modbus-tcp.configuration.protocol]
name = "p"
path = "p.lua"
"#,
        )
        .unwrap();
        let s = &cfg.configuration[0];
        assert_eq!(s.port, DEFAULT_PORT);
        assert_eq!(s.read_diff, DEFAULT_READ_DIFF);
        assert_eq!(s.max_retries, DEFAULT_MAX_RETRIES);
        assert_eq!(s.retry_delay, DEFAULT_RETRY_DELAY);
        assert_eq!(s.connect_timeout_ms, DEFAULT_CONNECT_TIMEOUT_MS);
        s.validate().unwrap();
    }

    /// 必填缺失 → 反序列化失败；非法值 → validate 报错（失败可见）。
    #[test]
    fn rejects_bad_config() {
        // 缺 clientId
        assert!(
            ModbusTcpConfig::from_value(&serde_json::json!({
                "configuration": [{
                    "pointDTOSPath": "http://h/p",
                    "pointMapDTOSPath": "http://h/m",
                    "host": "h",
                    "protocol": {"name": "p", "path": "p.lua"},
                }]
            }))
            .is_err()
        );
        // 缺 protocol（Rust 版必填）
        assert!(
            ModbusTcpConfig::from_value(&serde_json::json!({
                "configuration": [{
                    "clientId": "c",
                    "pointDTOSPath": "http://h/p",
                    "pointMapDTOSPath": "http://h/m",
                    "host": "h",
                }]
            }))
            .is_err()
        );

        let server = |extra: JsonValue| {
            let mut s = serde_json::json!({
                "clientId": "c1",
                "pointDTOSPath": "http://h/p",
                "pointMapDTOSPath": "http://h/m",
                "host": "10.0.0.1",
                "protocol": {"name": "p", "path": "p.lua"},
            });
            s.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            ModbusTcpConfig::from_value(&serde_json::json!({"configuration": [s]})).unwrap()
        };

        let err = server(serde_json::json!({"host": "  "}))
            .configuration
            .into_iter()
            .next()
            .unwrap()
            .validate()
            .unwrap_err();
        assert!(err.to_string().contains("host 不能为空"), "{err}");

        let err = server(serde_json::json!({"readDiff": 0}))
            .configuration
            .into_iter()
            .next()
            .unwrap()
            .validate()
            .unwrap_err();
        assert!(err.to_string().contains("readDiff"), "{err}");

        let err = server(serde_json::json!({"connectTimeout": 0}))
            .configuration
            .into_iter()
            .next()
            .unwrap()
            .validate()
            .unwrap_err();
        assert!(err.to_string().contains("connectTimeout"), "{err}");

        let err = server(serde_json::json!({"pointDTOSPath": ""}))
            .configuration
            .into_iter()
            .next()
            .unwrap()
            .validate()
            .unwrap_err();
        assert!(err.to_string().contains("pointDTOSPath"), "{err}");
    }
}
