//! snmp 段配置模型，字段名对齐 Java `SnmpServerConfig`（camelCase 经 serde rename）。
//!
//! 与 Java 的两处刻意差异（见 spec「Further Notes」）：
//! 1. `community` 必须显式配置——Java `SnmpTimerTask.java:47` 硬编码 `"huawei123"`，
//!    Rust 版缺失或为空即启动报错；
//! 2. 仅支持 v2c——`version` 非 v2c 或出现 v3 专有配置项（securityName 等）即启动报错（失败可见）。
//!
//! 入口 [`SnmpConfig::from_value`]：`GatewayConfig.snmp` 一期是 `serde_json::Value` 兜底，
//! 本模块负责把它解析成强类型配置并校验。

use std::time::Duration;

use connector_common::{CommonError, ProtocolConfig, Result};
use serde::Deserialize;
use serde_json::Value as JsonValue;

pub(crate) const DEFAULT_PORT: u16 = 161;
pub(crate) const DEFAULT_TIMEOUT_MS: u64 = 3000;
pub(crate) const DEFAULT_RETRY: u32 = 1;
pub(crate) const DEFAULT_INTERVAL_MINUTE: u64 = 1;

/// v3 专有配置项：出现即报错（Rust 版不实现 v3/USM，spec Out of Scope）。
const V3_ONLY_KEYS: &[&str] = &[
    "securityName",
    "securityLevel",
    "authProtocol",
    "authPassword",
    "privProtocol",
    "privPassword",
    "engineId",
    "contextEngineId",
    "contextName",
    "usmUser",
];

fn default_port() -> u16 {
    DEFAULT_PORT
}
fn default_timeout_ms() -> u64 {
    DEFAULT_TIMEOUT_MS
}
fn default_retry() -> u32 {
    DEFAULT_RETRY
}
fn default_interval_minute() -> u64 {
    DEFAULT_INTERVAL_MINUTE
}

/// `version` 是否等价于 v2c（空值/缺省视为 v2c，对齐 Java 恒用 version2c 的行为）。
fn is_v2c(version: &str) -> bool {
    matches!(
        version.trim().to_ascii_lowercase().as_str(),
        "" | "2c" | "v2c"
    )
}

/// `[snmp]` 段，对齐 Java `SnmpConfig`（`ennwiot.connector-snmp.configuration`）。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnmpConfig {
    #[serde(default)]
    pub configuration: Vec<SnmpServerConfig>,
}

/// 单个 SNMP agent，对齐 Java `SnmpServerConfig` + Rust 版新增的 `community`。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnmpServerConfig {
    pub address: String,
    /// Java 侧声明为 String，这里数字/字符串都接受（迁移时只做格式转换）。
    #[serde(default = "default_port", deserialize_with = "de_port")]
    pub port: u16,
    /// Java 侧硬编码 `"huawei123"`，Rust 版必须配置。
    pub community: String,
    /// Java 字段名 `timeout`（毫秒）。
    #[serde(rename = "timeout", default = "default_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_retry")]
    pub retry: u32,
    #[serde(default = "default_interval_minute")]
    pub interval_minute: u64,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    pub protocol: ProtocolConfig,
}

/// port 兼容数字与字符串两种写法（Java 侧字段类型是 String）。
fn de_port<'de, D>(deserializer: D) -> std::result::Result<u16, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Port {
        Num(u16),
        Str(String),
    }
    match Port::deserialize(deserializer)? {
        Port::Num(p) => Ok(p),
        Port::Str(s) => s
            .trim()
            .parse::<u16>()
            .map_err(|e| serde::de::Error::custom(format!("port '{s}' 不是合法端口: {e}"))),
    }
}

impl SnmpConfig {
    /// 从 `GatewayConfig.snmp`（JsonValue）解析：先扫 v3 项（需要原始键名），再反序列化。
    ///
    /// 逐 server 的字段校验（community/intervalMinute/OIDS）在
    /// [`SnmpServerConfig::validate`]，由 `SnmpConnector::new` 调用。
    pub fn from_value(value: &JsonValue) -> Result<Self> {
        reject_v3(value)?;
        serde_json::from_value(value.clone())
            .map_err(|e| CommonError::Config(format!("snmp 段解析失败: {e}")))
    }
}

impl SnmpServerConfig {
    /// 日志/status 用的标识。
    pub fn label(&self) -> String {
        format!("{}:{}", self.address, self.port)
    }

    /// 单次 GET 的等待超时。
    ///
    /// ponytail: `timeout = 0`（Java 未配置时的 int 默认值）归一到 3000ms，
    /// 而不是当成"立即超时"；升级路径：需要"无限等待"语义时在此显式分支。
    pub fn timeout_duration(&self) -> Duration {
        let ms = if self.timeout_ms == 0 {
            DEFAULT_TIMEOUT_MS
        } else {
            self.timeout_ms
        };
        Duration::from_millis(ms)
    }

    /// 轮询周期，对齐 Java `intervalMinute * 1000 * 60`（`SnmpConnectorStater.java:62`）。
    pub fn interval_duration(&self) -> Duration {
        Duration::from_secs(self.interval_minute.saturating_mul(60))
    }

    /// 总尝试次数 = 首次 + retry（对齐 snmp4j `setRetries` 语义）。
    pub fn attempts(&self) -> u32 {
        self.retry.saturating_add(1)
    }

    /// 启动前校验：失败即报错，不带病运行。
    pub fn validate(&self) -> Result<()> {
        let label = self.label();
        if self.address.trim().is_empty() {
            return Err(CommonError::Config(format!(
                "snmp {label}: address 不能为空"
            )));
        }
        if self.community.is_empty() {
            return Err(CommonError::Config(format!(
                "snmp {label}: community 必须配置（Java 版硬编码 \"huawei123\"，Rust 版改为配置项）"
            )));
        }
        if let Some(version) = &self.version
            && !is_v2c(version)
        {
            return Err(unsupported_version(&label, version));
        }
        if self.interval_minute == 0 {
            return Err(CommonError::Config(format!(
                "snmp {label}: intervalMinute 必须 > 0"
            )));
        }
        parse_oids(&self.protocol.params)?;
        Ok(())
    }
}

fn unsupported_version(label: &str, version: &str) -> CommonError {
    CommonError::Config(format!(
        "snmp {label}: version '{version}' 不支持，Rust 版仅实现 v2c（v3/USM 见 spec Out of Scope）"
    ))
}

/// 扫描原始 JSON，拒绝 v3 版本与 v3 专有配置项（失败可见）。
fn reject_v3(value: &JsonValue) -> Result<()> {
    let servers = value
        .get("configuration")
        .and_then(JsonValue::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    for (i, server) in servers.iter().enumerate() {
        let label = server
            .get("address")
            .and_then(JsonValue::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| format!("#{i}"));
        if let Some(version) = server.get("version").and_then(JsonValue::as_str)
            && !is_v2c(version)
        {
            return Err(unsupported_version(&label, version));
        }
        if let Some(obj) = server.as_object() {
            for key in obj.keys() {
                if V3_ONLY_KEYS.contains(&key.as_str()) {
                    return Err(CommonError::Config(format!(
                        "snmp {label}: 出现 v3 专有配置项 '{key}'，Rust 版仅支持 v2c"
                    )));
                }
            }
        }
    }
    Ok(())
}

/// `protocol.params["OIDS"]`（逗号分隔，对齐 Java `SnmpTimerTask.java:61`）→ OID 列表。
///
/// 逐项 trim、丢弃空段、校验 OID 格式；缺失/非字符串/解析后为空 → 报错。
pub fn parse_oids(params: &JsonValue) -> Result<Vec<String>> {
    let raw = params.get("OIDS").ok_or_else(|| {
        CommonError::Config("snmp: protocol.params 缺少 'OIDS'（逗号分隔 OID 列表）".to_string())
    })?;
    let text = raw.as_str().ok_or_else(|| {
        CommonError::Config(format!(
            "snmp: protocol.params['OIDS'] 必须是逗号分隔字符串，实际为 {raw}"
        ))
    })?;
    let oids: Vec<String> = text
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    if oids.is_empty() {
        return Err(CommonError::Config(format!(
            "snmp: OIDS 解析后为空（原值 '{text}'）"
        )));
    }
    for oid in &oids {
        crate::ber::encode_oid(oid).map(|_| ())?;
    }
    Ok(oids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use connector_common::GatewayConfig;
    use serde_json::json;

    /// TOML fixture → GatewayConfig.snmp（JsonValue）→ SnmpConfig，走 gateway 的真实解析路径。
    fn parse_toml(toml_text: &str) -> Result<SnmpConfig> {
        let cfg: GatewayConfig = toml::from_str(toml_text).expect("TOML fixture 本身应合法");
        let snmp = cfg
            .snmp
            .as_ref()
            .expect("fixture 必须包含 [snmp] 段")
            .clone();
        SnmpConfig::from_value(&snmp)
    }

    const FULL: &str = r#"
[kafka]
bootstrapServers = "127.0.0.1:9092"

[snmp]
[[snmp.configuration]]
address = "10.0.0.9"
port = "161"
community = "enn-public"
timeout = 2000
retry = 2
intervalMinute = 5
version = "v2c"
description = "核心交换机"
[snmp.configuration.protocol]
name = "snmp_metric"
path = "protocols/snmp_metric.lua"
type = "script"
[snmp.configuration.protocol.params]
OIDS = "1.3.6.1.2.1.1.1.0,1.3.6.1.2.1.1.3.0"
deviceId = "dev-1"
"#;

    #[test]
    fn parses_toml_fixture() {
        let cfg = parse_toml(FULL).unwrap();
        assert_eq!(cfg.configuration.len(), 1);
        let s = &cfg.configuration[0];
        assert_eq!(s.address, "10.0.0.9");
        assert_eq!(s.port, 161, "Java 侧 port 是字符串，应被接受");
        assert_eq!(s.community, "enn-public");
        assert_eq!(s.timeout_ms, 2000);
        assert_eq!(s.retry, 2);
        assert_eq!(s.interval_minute, 5);
        assert_eq!(s.version.as_deref(), Some("v2c"));
        assert_eq!(s.description.as_deref(), Some("核心交换机"));
        assert_eq!(s.protocol.name, "snmp_metric");
        assert_eq!(s.protocol.protocol_type, "script");
        assert_eq!(s.label(), "10.0.0.9:161");
        assert_eq!(s.timeout_duration(), Duration::from_millis(2000));
        assert_eq!(s.interval_duration(), Duration::from_secs(300));
        assert_eq!(s.attempts(), 3);
        s.validate().unwrap();
        assert_eq!(
            parse_oids(&s.protocol.params).unwrap(),
            vec!["1.3.6.1.2.1.1.1.0", "1.3.6.1.2.1.1.3.0"]
        );
    }

    /// 最小配置：port/timeout/retry/intervalMinute/version 全部走默认值。
    #[test]
    fn applies_defaults() {
        let cfg = parse_toml(
            r#"
[kafka]
bootstrapServers = "x"
[snmp]
[[snmp.configuration]]
address = "10.0.0.1"
community = "public"
[snmp.configuration.protocol]
name = "p"
path = "p.lua"
[snmp.configuration.protocol.params]
OIDS = "1.3.6.1.2.1.1.1.0"
"#,
        )
        .unwrap();
        let s = &cfg.configuration[0];
        assert_eq!(s.port, DEFAULT_PORT);
        assert_eq!(s.timeout_ms, DEFAULT_TIMEOUT_MS);
        assert_eq!(s.retry, DEFAULT_RETRY);
        assert_eq!(s.interval_minute, DEFAULT_INTERVAL_MINUTE);
        assert_eq!(s.version, None);
        assert_eq!(s.interval_duration(), Duration::from_secs(60));
        assert_eq!(s.attempts(), 2);
        s.validate().unwrap();
    }

    /// 缺字段 / 错类型 → 解析失败（失败可见）。
    #[test]
    fn rejects_missing_or_bad_fields() {
        // 缺 community
        let err = parse_toml(
            r#"
[kafka]
bootstrapServers = "x"
[snmp]
[[snmp.configuration]]
address = "10.0.0.1"
[snmp.configuration.protocol]
name = "p"
path = "p.lua"
"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("community"), "{err}");

        // port 非数字字符串
        let err = parse_toml(
            r#"
[kafka]
bootstrapServers = "x"
[snmp]
[[snmp.configuration]]
address = "10.0.0.1"
port = "snmp"
community = "public"
[snmp.configuration.protocol]
name = "p"
path = "p.lua"
"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("port"), "{err}");

        // 缺 address
        assert!(
            parse_toml(
                r#"
[kafka]
bootstrapServers = "x"
[snmp]
[[snmp.configuration]]
community = "public"
[snmp.configuration.protocol]
name = "p"
path = "p.lua"
"#
            )
            .is_err()
        );

        // 缺 protocol.params.OIDS：解析成功但 validate 报错
        let cfg = parse_toml(
            r#"
[kafka]
bootstrapServers = "x"
[snmp]
[[snmp.configuration]]
address = "10.0.0.1"
community = "public"
[snmp.configuration.protocol]
name = "p"
path = "p.lua"
"#,
        )
        .unwrap();
        let err = cfg.configuration[0].validate().unwrap_err();
        assert!(err.to_string().contains("OIDS"), "{err}");
    }

    /// v3 版本号 → 启动报错。
    #[test]
    fn rejects_v3_version() {
        for version in ["v3", "3", "V3", "1", "v2"] {
            let value = json!({
                "configuration": [{
                    "address": "10.0.0.1",
                    "community": "public",
                    "version": version,
                    "protocol": {"name": "p", "path": "p.lua", "params": {"OIDS": "1.3.6.1.2.1.1.1.0"}},
                }]
            });
            let err = SnmpConfig::from_value(&value).unwrap_err();
            assert!(
                err.to_string().contains("仅实现 v2c"),
                "version='{version}' 应被拒绝: {err}"
            );
        }
    }

    /// v3 专有配置项（即使 version 写 v2c）→ 启动报错。
    #[test]
    fn rejects_v3_only_keys() {
        for key in ["securityName", "authPassword", "securityLevel", "usmUser"] {
            let value = json!({
                "configuration": [{
                    "address": "10.0.0.1",
                    "community": "public",
                    "version": "v2c",
                    key: "whatever",
                    "protocol": {"name": "p", "path": "p.lua", "params": {"OIDS": "1.3.6.1.2.1.1.1.0"}},
                }]
            });
            let err = SnmpConfig::from_value(&value).unwrap_err();
            assert!(
                err.to_string().contains("v3 专有配置项"),
                "'{key}' 应被拒绝: {err}"
            );
            assert!(err.to_string().contains(key), "{err}");
        }
    }

    /// community 为空 / intervalMinute=0 → 启动报错。
    #[test]
    fn rejects_empty_community_and_zero_interval() {
        let base = |extra: JsonValue| {
            let mut server = json!({
                "address": "10.0.0.1",
                "community": "public",
                "protocol": {"name": "p", "path": "p.lua", "params": {"OIDS": "1.3.6.1.2.1.1.1.0"}},
            });
            server
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            json!({ "configuration": [server] })
        };

        let cfg = SnmpConfig::from_value(&base(json!({"community": ""}))).unwrap();
        let err = cfg.configuration[0].validate().unwrap_err();
        assert!(err.to_string().contains("community 必须配置"), "{err}");

        let cfg = SnmpConfig::from_value(&base(json!({"intervalMinute": 0}))).unwrap();
        let err = cfg.configuration[0].validate().unwrap_err();
        assert!(err.to_string().contains("intervalMinute"), "{err}");

        let cfg = SnmpConfig::from_value(&base(json!({"address": "  "}))).unwrap();
        let err = cfg.configuration[0].validate().unwrap_err();
        assert!(err.to_string().contains("address"), "{err}");
    }

    /// OIDS 解析：逗号分隔 + trim + 丢空段。
    #[test]
    fn parse_oids_splits_and_trims() {
        let params = json!({"OIDS": " 1.3.6.1.2.1.1.1.0 ,1.3.6.1.2.1.1.3.0,, "});
        assert_eq!(
            parse_oids(&params).unwrap(),
            vec!["1.3.6.1.2.1.1.1.0", "1.3.6.1.2.1.1.3.0"]
        );
        // 单个 OID、无前导点
        assert_eq!(
            parse_oids(&json!({"OIDS": "1.3.6.1.2.1.2.2.1.10.1"})).unwrap(),
            vec!["1.3.6.1.2.1.2.2.1.10.1"]
        );
    }

    /// OIDS 缺失 / 非字符串 / 全空 / 非法 OID → 报错。
    #[test]
    fn parse_oids_rejects_bad_input() {
        let err = parse_oids(&json!({})).unwrap_err();
        assert!(err.to_string().contains("缺少 'OIDS'"), "{err}");

        let err = parse_oids(&json!({"OIDS": ["1.3.6.1.2.1.1.1.0"]})).unwrap_err();
        assert!(err.to_string().contains("逗号分隔字符串"), "{err}");

        let err = parse_oids(&json!({"OIDS": " , ,"})).unwrap_err();
        assert!(err.to_string().contains("解析后为空"), "{err}");

        let err = parse_oids(&json!({"OIDS": "1.3.6.1,not-an-oid"})).unwrap_err();
        assert!(err.to_string().contains("非法 OID"), "{err}");
    }
}
