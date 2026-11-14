//! opcua 段配置模型，字段名对齐 Java `OpcuaServerConfig`（camelCase 经 serde rename）。
//!
//! Java 侧 opcua connector 是空壳（无实现逻辑可迁移），本模块按配置契约全新建模：
//! endpointUrl / username / password / description / intervalMinute / protocol{name,path,params,type}。
//!
//! 入口 [`OpcuaConfig::from_value`]：`GatewayConfig.opcua` 一期是 `serde_json::Value` 兜底，
//! 本模块负责把它解析成强类型配置并校验（沿用 connector-snmp 的既定模式）。

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use connector_common::{CommonError, ProtocolConfig, Result};
use opcua::client::prelude::*;
use serde::{Deserialize, Deserializer, de};
use serde_json::Value as JsonValue;

pub(crate) const DEFAULT_INTERVAL_MINUTE: u64 = 1;

fn default_interval_minute() -> u64 {
    DEFAULT_INTERVAL_MINUTE
}

fn default_security_policy() -> SecurityPolicy {
    SecurityPolicy::None
}

fn default_security_mode() -> MessageSecurityMode {
    MessageSecurityMode::None
}

/// 白名单解析 securityPolicy：未知字符串 → 反序列化报错（失败可见，禁静默回落 None）。
///
/// ponytail: 仅支持 None | Basic256Sha256（票 12 C-2 最小集，其余策略无真实设备可验证）；
/// 升级路径：按 `SecurityPolicy::is_supported` 放开 Aes128Sha256RsaOaep/Aes256Sha256RsaPss 等。
fn deserialize_security_policy<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<SecurityPolicy, D::Error> {
    let raw = String::deserialize(deserializer)?;
    match raw.as_str() {
        "None" => Ok(SecurityPolicy::None),
        "Basic256Sha256" => Ok(SecurityPolicy::Basic256Sha256),
        other => Err(de::Error::custom(format!(
            "未知 securityPolicy '{other}'，仅支持 None | Basic256Sha256"
        ))),
    }
}

/// 白名单解析 securityMode：未知字符串 → 反序列化报错（失败可见，禁静默回落 None）。
fn deserialize_security_mode<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<MessageSecurityMode, D::Error> {
    let raw = String::deserialize(deserializer)?;
    match raw.as_str() {
        "None" => Ok(MessageSecurityMode::None),
        "Sign" => Ok(MessageSecurityMode::Sign),
        "SignAndEncrypt" => Ok(MessageSecurityMode::SignAndEncrypt),
        other => Err(de::Error::custom(format!(
            "未知 securityMode '{other}'，仅支持 None | Sign | SignAndEncrypt"
        ))),
    }
}

/// `[opcua]` 段（多 server，各自独立轮询）。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpcuaConfig {
    #[serde(default)]
    pub configuration: Vec<OpcuaServerConfig>,
}

/// 单个 OPC UA server，对齐 Java `OpcuaServerConfig` 字段契约。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpcuaServerConfig {
    pub endpoint_url: String,
    /// 空/缺失 → 匿名认证；非空 → username/password。
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default = "default_interval_minute")]
    pub interval_minute: u64,
    /// 安全策略（票 12 C-2）：缺省 None（向后兼容明文）。TOML 键 `securityPolicy`
    /// （struct 级 `rename_all = "camelCase"` 生效，与本段其余键一致）。
    #[serde(
        default = "default_security_policy",
        deserialize_with = "deserialize_security_policy"
    )]
    pub security_policy: SecurityPolicy,
    /// 消息安全模式：缺省 None。TOML 键 `securityMode`。
    #[serde(
        default = "default_security_mode",
        deserialize_with = "deserialize_security_mode"
    )]
    pub security_mode: MessageSecurityMode,
    /// 客户端证书路径（DER/PEM）：`securityPolicy != None` 时必填且文件须存在。TOML 键 `certPath`。
    #[serde(default)]
    pub cert_path: Option<String>,
    /// 客户端私钥路径：`securityPolicy != None` 时必填且文件须存在。TOML 键 `keyPath`。
    #[serde(default)]
    pub key_path: Option<String>,
    pub protocol: ProtocolConfig,
}

impl OpcuaConfig {
    /// 从 `GatewayConfig.opcua`（JsonValue）解析。
    ///
    /// 逐 server 的字段校验（endpointUrl/intervalMinute/NODES）在
    /// [`OpcuaServerConfig::validate`]，由 `OpcuaConnector::new` 调用。
    pub fn from_value(value: &JsonValue) -> Result<Self> {
        serde_json::from_value(value.clone())
            .map_err(|e| CommonError::Config(format!("opcua 段解析失败: {e}")))
    }
}

impl OpcuaServerConfig {
    /// 日志/status 用的标识：优先 description，缺省用 endpointUrl。
    pub fn label(&self) -> String {
        self.description
            .clone()
            .filter(|d| !d.trim().is_empty())
            .unwrap_or_else(|| self.endpoint_url.clone())
    }

    /// 轮询周期，对齐 intervalMinute * 60s（与 snmp 同一约定）。
    pub fn interval_duration(&self) -> Duration {
        Duration::from_secs(self.interval_minute.saturating_mul(60))
    }

    /// 认证方式：username 非空 → UserName（password 缺省空串），否则匿名。
    pub fn identity(&self) -> IdentityToken {
        match self
            .username
            .as_deref()
            .map(str::trim)
            .filter(|u| !u.is_empty())
        {
            Some(user) => {
                IdentityToken::UserName(user.to_string(), self.password.clone().unwrap_or_default())
            }
            None => IdentityToken::Anonymous,
        }
    }

    /// 启动前校验：失败即报错，不带病运行。
    pub fn validate(&self) -> Result<()> {
        let label = self.label();
        let url = self.endpoint_url.trim();
        if url.is_empty() {
            return Err(CommonError::Config(format!(
                "opcua {label}: endpointUrl 不能为空"
            )));
        }
        if !url.starts_with("opc.tcp://") {
            return Err(CommonError::Config(format!(
                "opcua {label}: endpointUrl '{url}' 必须以 opc.tcp:// 开头"
            )));
        }
        if self.interval_minute == 0 {
            return Err(CommonError::Config(format!(
                "opcua {label}: intervalMinute 必须 > 0"
            )));
        }
        self.validate_security()?;
        parse_nodes(&self.protocol.params)?;
        Ok(())
    }

    /// 安全字段校验（票 12 C-2）：配错即启动报错，禁静默降级明文。
    fn validate_security(&self) -> Result<()> {
        let label = self.label();
        if self.security_policy == SecurityPolicy::None {
            if self.security_mode != MessageSecurityMode::None {
                return Err(CommonError::Config(format!(
                    "opcua {label}: securityPolicy = None 时 securityMode 必须为 None，实际为 {}（禁明文降级配置歧义）",
                    self.security_mode
                )));
            }
            // policy = None 时 cert/key 无意义，忽略（向后兼容既有匿名明文配置）。
            return Ok(());
        }
        if self.security_mode == MessageSecurityMode::None {
            return Err(CommonError::Config(format!(
                "opcua {label}: securityPolicy = {} 时 securityMode 必须为 Sign 或 SignAndEncrypt",
                self.security_policy
            )));
        }
        for (name, value) in [("certPath", &self.cert_path), ("keyPath", &self.key_path)] {
            let path = value
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    CommonError::Config(format!(
                        "opcua {label}: securityPolicy = {} 时 {name} 必填（禁降级明文）",
                        self.security_policy
                    ))
                })?;
            if !Path::new(path).is_file() {
                return Err(CommonError::Config(format!(
                    "opcua {label}: {name} '{path}' 文件不存在"
                )));
            }
        }
        Ok(())
    }

    /// cert/key 绝对路径（仅在 `security_policy != None` 且 validate 通过后调用）。
    ///
    /// opcua crate 的 `certificate_path`/`private_key_path` 按 pki_dir 相对解析
    /// （`PathBuf::push` 语义，绝对路径会整体替换），故必须传绝对路径。
    pub fn cert_key_paths(&self) -> Result<(PathBuf, PathBuf)> {
        let cert = std::path::absolute(self.cert_path.as_deref().unwrap_or_default().trim())?;
        let key = std::path::absolute(self.key_path.as_deref().unwrap_or_default().trim())?;
        Ok((cert, key))
    }
}

/// `protocol.params["NODES"]`（逗号分隔 NodeId，如 `ns=2;s=Tag1,ns=1;i=42`）→ NodeId 列表。
///
/// 逐项 trim、丢弃空段；缺失/非字符串/解析后为空/含非法 NodeId → 报错（失败可见）。
pub fn parse_nodes(params: &JsonValue) -> Result<Vec<NodeId>> {
    let raw = params.get("NODES").ok_or_else(|| {
        CommonError::Config(
            "opcua: protocol.params 缺少 'NODES'（逗号分隔 NodeId 列表）".to_string(),
        )
    })?;
    let text = raw.as_str().ok_or_else(|| {
        CommonError::Config(format!(
            "opcua: protocol.params['NODES'] 必须是逗号分隔字符串，实际为 {raw}"
        ))
    })?;
    let tokens: Vec<&str> = text
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    if tokens.is_empty() {
        return Err(CommonError::Config(format!(
            "opcua: NODES 解析后为空（原值 '{text}'）"
        )));
    }
    tokens
        .iter()
        .map(|t| {
            NodeId::from_str(t)
                .map_err(|e| CommonError::Config(format!("opcua: 非法 NodeId '{t}': {e}")))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use connector_common::GatewayConfig;
    use serde_json::json;

    /// TOML fixture → GatewayConfig.opcua（JsonValue）→ OpcuaConfig，走 gateway 的真实解析路径。
    fn parse_toml(toml_text: &str) -> Result<OpcuaConfig> {
        let cfg: GatewayConfig = toml::from_str(toml_text).expect("TOML fixture 本身应合法");
        let opcua = cfg
            .opcua
            .as_ref()
            .expect("fixture 必须包含 [opcua] 段")
            .clone();
        OpcuaConfig::from_value(&opcua)
    }

    const FULL: &str = r#"
[kafka]
bootstrapServers = "127.0.0.1:9092"

[opcua]
[[opcua.configuration]]
endpointUrl = "opc.tcp://10.0.0.20:4840"
username = "opcuser"
password = "opcpass"
description = "产线 PLC"
intervalMinute = 5
[opcua.configuration.protocol]
name = "opcua_metric"
path = "protocols/opcua_metric.lua"
type = "script"
[opcua.configuration.protocol.params]
NODES = "ns=2;s=Channel1.Device1.Temp,ns=2;s=Channel1.Device1.Pressure"
deviceId = "dev-1"
"#;

    #[test]
    fn parses_toml_fixture() {
        let cfg = parse_toml(FULL).unwrap();
        assert_eq!(cfg.configuration.len(), 1);
        let s = &cfg.configuration[0];
        assert_eq!(s.endpoint_url, "opc.tcp://10.0.0.20:4840");
        assert_eq!(s.username.as_deref(), Some("opcuser"));
        assert_eq!(s.password.as_deref(), Some("opcpass"));
        assert_eq!(s.description.as_deref(), Some("产线 PLC"));
        assert_eq!(s.interval_minute, 5);
        assert_eq!(s.protocol.name, "opcua_metric");
        assert_eq!(s.protocol.protocol_type, "script");
        assert_eq!(s.label(), "产线 PLC");
        assert_eq!(s.interval_duration(), Duration::from_secs(300));
        s.validate().unwrap();

        let nodes = parse_nodes(&s.protocol.params).unwrap();
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].to_string(), "ns=2;s=Channel1.Device1.Temp");
        assert_eq!(nodes[1].to_string(), "ns=2;s=Channel1.Device1.Pressure");
    }

    /// 最小配置：username/password/description/intervalMinute 全走缺省（匿名 + 1 分钟）。
    #[test]
    fn applies_defaults() {
        let cfg = parse_toml(
            r#"
[kafka]
bootstrapServers = "x"
[opcua]
[[opcua.configuration]]
endpointUrl = "opc.tcp://127.0.0.1:4840"
[opcua.configuration.protocol]
name = "p"
path = "p.lua"
[opcua.configuration.protocol.params]
NODES = "ns=2;s=Tag1"
"#,
        )
        .unwrap();
        let s = &cfg.configuration[0];
        assert_eq!(s.username, None);
        assert_eq!(s.password, None);
        assert_eq!(s.description, None);
        assert_eq!(s.interval_minute, DEFAULT_INTERVAL_MINUTE);
        assert_eq!(s.interval_duration(), Duration::from_secs(60));
        assert_eq!(
            s.label(),
            "opc.tcp://127.0.0.1:4840",
            "无 description 用 url"
        );
        assert!(matches!(s.identity(), IdentityToken::Anonymous));
        s.validate().unwrap();
    }

    /// username 非空 → UserName token；password 缺省空串；username 空白 → 匿名。
    #[test]
    fn identity_follows_username() {
        let base = json!({
            "endpointUrl": "opc.tcp://h:4840",
            "protocol": {"name": "p", "path": "p.lua", "params": {"NODES": "ns=2;s=T"}},
        });
        let with_user = json!({
            "endpointUrl": "opc.tcp://h:4840",
            "username": "u1",
            "password": "p1",
            "protocol": {"name": "p", "path": "p.lua", "params": {"NODES": "ns=2;s=T"}},
        });
        let blank_user = {
            let mut v = base.clone();
            v["username"] = json!("  ");
            v
        };

        let cfg = OpcuaConfig::from_value(&json!({"configuration": [with_user]})).unwrap();
        match cfg.configuration[0].identity() {
            IdentityToken::UserName(u, p) => {
                assert_eq!(u, "u1");
                assert_eq!(p, "p1");
            }
            other => panic!("应为 UserName，实际 {other:?}"),
        }

        let cfg = OpcuaConfig::from_value(&json!({"configuration": [base]})).unwrap();
        assert!(matches!(
            cfg.configuration[0].identity(),
            IdentityToken::Anonymous
        ));

        let cfg = OpcuaConfig::from_value(&json!({"configuration": [blank_user]})).unwrap();
        assert!(matches!(
            cfg.configuration[0].identity(),
            IdentityToken::Anonymous
        ));
    }

    /// endpointUrl 缺失/为空/非 opc.tcp、intervalMinute=0 → 校验报错（失败可见）。
    #[test]
    fn rejects_bad_server_fields() {
        // 缺 endpointUrl → 反序列化失败
        assert!(
            parse_toml(
                r#"
[kafka]
bootstrapServers = "x"
[opcua]
[[opcua.configuration]]
[opcua.configuration.protocol]
name = "p"
path = "p.lua"
"#
            )
            .is_err()
        );

        let server = |extra: JsonValue| {
            let mut s = json!({
                "endpointUrl": "opc.tcp://h:4840",
                "protocol": {"name": "p", "path": "p.lua", "params": {"NODES": "ns=2;s=T"}},
            });
            s.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            OpcuaConfig::from_value(&json!({"configuration": [s]})).unwrap()
        };

        let cfg = server(json!({"endpointUrl": "  "}));
        let err = cfg.configuration[0].validate().unwrap_err();
        assert!(err.to_string().contains("endpointUrl 不能为空"), "{err}");

        let cfg = server(json!({"endpointUrl": "http://h:4840"}));
        let err = cfg.configuration[0].validate().unwrap_err();
        assert!(err.to_string().contains("opc.tcp://"), "{err}");

        let cfg = server(json!({"intervalMinute": 0}));
        let err = cfg.configuration[0].validate().unwrap_err();
        assert!(err.to_string().contains("intervalMinute"), "{err}");
    }

    /// NODES 解析：逗号分隔 + trim + 丢空段，支持 s/i/g/b 四种标识。
    #[test]
    fn parse_nodes_splits_and_trims() {
        let nodes = parse_nodes(&json!({"NODES": " ns=2;s=Tag1 ,ns=1;i=42,, "})).unwrap();
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].to_string(), "ns=2;s=Tag1");
        assert_eq!(nodes[1].to_string(), "ns=1;i=42");
        // 无 ns 前缀 = namespace 0
        let nodes = parse_nodes(&json!({"NODES": "i=2258"})).unwrap();
        assert_eq!(nodes[0].to_string(), "i=2258");
    }

    /// NODES 缺失 / 非字符串 / 全空 / 含非法 NodeId → 报错。
    #[test]
    fn parse_nodes_rejects_bad_input() {
        let err = parse_nodes(&json!({})).unwrap_err();
        assert!(err.to_string().contains("缺少 'NODES'"), "{err}");

        let err = parse_nodes(&json!({"NODES": ["ns=2;s=T"]})).unwrap_err();
        assert!(err.to_string().contains("逗号分隔字符串"), "{err}");

        let err = parse_nodes(&json!({"NODES": " , ,"})).unwrap_err();
        assert!(err.to_string().contains("解析后为空"), "{err}");

        let err = parse_nodes(&json!({"NODES": "ns=2;s=Tag1,not-a-node"})).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("非法 NodeId") && msg.contains("not-a-node"),
            "{msg}"
        );

        let err = parse_nodes(&json!({"NODES": "ns=x;s=Tag1"})).unwrap_err();
        assert!(err.to_string().contains("非法 NodeId"), "{err}");
    }

    // ---- 票 12 C-2：SecurityPolicy/MessageSecurityMode 配置化 ----

    /// 构造带 security 字段的最小 server TOML（security 键为 camelCase，与本段其余键一致）。
    fn security_toml(security_lines: &str) -> String {
        format!(
            r#"
[kafka]
bootstrapServers = "x"
[opcua]
[[opcua.configuration]]
endpointUrl = "opc.tcp://127.0.0.1:4840"
{security_lines}
[opcua.configuration.protocol]
name = "p"
path = "p.lua"
[opcua.configuration.protocol.params]
NODES = "ns=2;s=Tag1"
"#
        )
    }

    /// 缺省 security_policy/mode = None/None（向后兼容明文），validate 通过。
    #[test]
    fn security_defaults_to_none() {
        let cfg = parse_toml(&security_toml("")).unwrap();
        let s = &cfg.configuration[0];
        assert_eq!(s.security_policy, SecurityPolicy::None);
        assert_eq!(s.security_mode, MessageSecurityMode::None);
        assert!(s.cert_path.is_none());
        assert!(s.key_path.is_none());
        s.validate().unwrap();
    }

    /// 显式 Basic256Sha256 + SignAndEncrypt + cert/key（文件存在）→ 解析 + 校验成功，
    /// cert_key_paths 返回绝对路径。
    #[test]
    fn parses_explicit_security_with_existing_certs() {
        let dir = std::env::temp_dir().join(format!("opcua-cert-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cert = dir.join("client.der");
        let key = dir.join("client.key");
        std::fs::write(&cert, b"dummy-cert").unwrap();
        std::fs::write(&key, b"dummy-key").unwrap();

        let lines = format!(
            r#"securityPolicy = "Basic256Sha256"
securityMode = "SignAndEncrypt"
certPath = "{}"
keyPath = "{}""#,
            cert.display(),
            key.display()
        );
        let cfg = parse_toml(&security_toml(&lines)).unwrap();
        let s = &cfg.configuration[0];
        assert_eq!(s.security_policy, SecurityPolicy::Basic256Sha256);
        assert_eq!(s.security_mode, MessageSecurityMode::SignAndEncrypt);
        s.validate().unwrap();
        let (c, k) = s.cert_key_paths().unwrap();
        assert!(c.is_absolute() && k.is_absolute());

        std::fs::remove_dir_all(&dir).ok();
    }

    /// 未知 security_policy / security_mode 字符串 → 解析报错（失败可见，禁静默回落 None）。
    #[test]
    fn rejects_unknown_security_strings() {
        let err = parse_toml(&security_toml(r#"securityPolicy = "Basic512Sha512""#)).unwrap_err();
        assert!(err.to_string().contains("securityPolicy"), "{err}");

        let err = parse_toml(&security_toml(r#"securityMode = "Encrypt""#)).unwrap_err();
        assert!(err.to_string().contains("securityMode"), "{err}");
    }

    /// security_policy != None 但缺 cert/key 或文件不存在 → validate 报错（禁降级明文）。
    #[test]
    fn rejects_encrypted_policy_without_valid_certs() {
        let cfg = parse_toml(&security_toml(
            r#"securityPolicy = "Basic256Sha256"
securityMode = "SignAndEncrypt""#,
        ))
        .unwrap();
        let err = cfg.configuration[0].validate().unwrap_err();
        assert!(err.to_string().contains("certPath"), "{err}");

        let cfg = parse_toml(&security_toml(
            r#"securityPolicy = "Basic256Sha256"
securityMode = "SignAndEncrypt"
certPath = "/nonexistent/opcua-cert.der"
keyPath = "/nonexistent/opcua-cert.key""#,
        ))
        .unwrap();
        let err = cfg.configuration[0].validate().unwrap_err();
        assert!(err.to_string().contains("不存在"), "{err}");
    }

    /// policy/mode 组合非法 → validate 报错（避免歧义配置静默降级明文）。
    #[test]
    fn rejects_mismatched_policy_mode() {
        // policy None（缺省）+ mode Sign
        let cfg = parse_toml(&security_toml(r#"securityMode = "Sign""#)).unwrap();
        let err = cfg.configuration[0].validate().unwrap_err();
        assert!(err.to_string().contains("securityPolicy = None"), "{err}");

        // policy Basic256Sha256 + mode None（缺省）
        let cfg = parse_toml(&security_toml(r#"securityPolicy = "Basic256Sha256""#)).unwrap();
        let err = cfg.configuration[0].validate().unwrap_err();
        assert!(err.to_string().contains("Sign 或 SignAndEncrypt"), "{err}");
    }
}
