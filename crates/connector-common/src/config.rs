//! TOML 配置模型，字段名对齐 Java `ennwiot.connector-*` 契约（camelCase 经 serde rename）。
//!
//! tcp / mqtt / kafka 关键字段真实建模；其余 connector（udp/modbus-tcp/http/snmp/request/opcua）
//! 一期用 `serde_json::Value` 兜底，后续票（03-09）按 Java 契约逐字段建模。

use serde::Deserialize;
use serde_json::Value as JsonValue;

fn default_metric_topic() -> String {
    "enn_data_iot_metric".to_string()
}
fn default_event_topic() -> String {
    "data_iot_event".to_string()
}
fn default_info_topic() -> String {
    "data_iot_device_info".to_string()
}
fn default_command_topic() -> String {
    "device_command_topic".to_string()
}
fn default_cloud_topic() -> String {
    "iot_gateway_trdPlatform_data".to_string()
}
fn default_protocol_type() -> String {
    "script".to_string()
}
fn default_params() -> JsonValue {
    JsonValue::Object(Default::default())
}
fn default_address() -> String {
    "0.0.0.0".to_string()
}

/// 顶层 gateway 配置。每个 connector 一段，`[kafka]` 独立段，`[[devices]]` 为本地设备清单。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayConfig {
    pub kafka: KafkaConfig,
    /// 协议脚本白名单根目录（可选）。设置时，gateway 装配的脚本路径 canonicalize 后
    /// 必须位于该目录内，越界拒绝加载（失败可见）；未设置时不限制（向后兼容）。
    #[serde(default)]
    pub script_dir: Option<String>,
    #[serde(default)]
    pub tcp: Option<TcpConfig>,
    #[serde(default)]
    pub mqtt: Option<MqttConfig>,
    // ponytail: 以下 connector 一期用 Value 兜底，后续票按 Java 契约建模
    #[serde(default)]
    pub udp: Option<JsonValue>,
    #[serde(rename = "modbus-tcp", default)]
    pub modbus_tcp: Option<JsonValue>,
    #[serde(default)]
    pub http: Option<JsonValue>,
    #[serde(default)]
    pub snmp: Option<JsonValue>,
    #[serde(default)]
    pub request: Option<JsonValue>,
    #[serde(default)]
    pub opcua: Option<JsonValue>,
    /// 本地设备清单（DeviceDirectory 一期实现），顶层 `[[devices]]`。
    #[serde(default)]
    pub devices: Vec<DeviceEntry>,
}

impl GatewayConfig {
    /// 收集所有已建模 connector 段里的 protocol 配置，供构建协议注册表。
    ///
    /// ponytail: 仅覆盖 tcp/mqtt（一期已建模）；其余 connector 为 Value 兜底，
    /// 其协议在各自票（03-09）建模后纳入。
    pub fn protocol_configs(&self) -> Vec<&ProtocolConfig> {
        let mut out = Vec::new();
        if let Some(tcp) = &self.tcp {
            if let Some(server) = &tcp.server {
                for c in &server.configuration {
                    out.extend(c.protocol.as_ref());
                }
            }
            if let Some(client) = &tcp.client {
                for c in &client.configuration {
                    out.extend(c.protocol.as_ref());
                }
            }
        }
        if let Some(mqtt) = &self.mqtt {
            for c in &mqtt.configuration {
                out.extend(c.protocol.as_ref());
            }
        }
        out
    }

    /// 列出已配置的 connector 段名（供 gateway 打印摘要）。
    pub fn configured_connectors(&self) -> Vec<&'static str> {
        let mut v = Vec::new();
        if self.tcp.is_some() {
            v.push("tcp");
        }
        if self.udp.is_some() {
            v.push("udp");
        }
        if self.mqtt.is_some() {
            v.push("mqtt");
        }
        if self.modbus_tcp.is_some() {
            v.push("modbus-tcp");
        }
        if self.http.is_some() {
            v.push("http");
        }
        if self.snmp.is_some() {
            v.push("snmp");
        }
        if self.request.is_some() {
            v.push("request");
        }
        if self.opcua.is_some() {
            v.push("opcua");
        }
        v
    }
}

/// `[kafka]` 段。topic 默认值对齐 Java application.yml。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    #[serde(default = "default_metric_topic")]
    pub metric_topic: String,
    #[serde(default = "default_event_topic")]
    pub event_topic: String,
    #[serde(default = "default_info_topic")]
    pub info_topic: String,
    #[serde(default = "default_command_topic")]
    pub command_topic: String,
    #[serde(default = "default_cloud_topic")]
    pub cloud_topic: String,
}

/// 内嵌协议配置，对齐 Java `ProtocolConfig`（仅保留 Rust 版生效字段 name/path/params/type）。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolConfig {
    pub name: String,
    pub path: String,
    #[serde(default = "default_params")]
    pub params: JsonValue,
    /// Java 字段名 `type`（Rust 关键字，重命名）。Rust 版仅支持 "script"（Lua）。
    #[serde(rename = "type", default = "default_protocol_type")]
    pub protocol_type: String,
}

/// 本地设备清单条目（DeviceDirectory 一期实现）。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceEntry {
    pub project_id: String,
    pub sn: String,
    pub device_id: String,
    pub password: String,
}

/// tcp 段，对齐 Java `TcpConfig`（server/client 双模式）。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TcpConfig {
    #[serde(default)]
    pub server: Option<TcpServerConnectorConfig>,
    #[serde(default)]
    pub client: Option<TcpClientConnectorConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TcpServerConnectorConfig {
    #[serde(default)]
    pub configuration: Vec<TcpServerConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TcpClientConnectorConfig {
    #[serde(default)]
    pub configuration: Vec<TcpClientConfig>,
}

/// 对齐 Java `TcpServerConfig`。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TcpServerConfig {
    #[serde(default = "default_address")]
    pub address: String,
    pub port: u16,
    #[serde(default)]
    pub buffer_size: Option<i32>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub protocol: Option<ProtocolConfig>,
    #[serde(default)]
    pub boss_group_thread: Option<i32>,
    #[serde(default)]
    pub worker_group_thread: Option<i32>,
    #[serde(default)]
    pub parser_type: Option<String>,
    #[serde(default)]
    pub parser_params: Option<JsonValue>,
    #[serde(default)]
    pub heartbeat_interval: Option<i32>,
    #[serde(default)]
    pub log_level: Option<String>,
}

/// 对齐 Java `TcpClientConfig`。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TcpClientConfig {
    pub remote_address: String,
    pub remote_port: u16,
    #[serde(default)]
    pub parser_type: Option<String>,
    #[serde(default)]
    pub parser_params: Option<JsonValue>,
    #[serde(default)]
    pub reconnect_enabled: Option<bool>,
    #[serde(default)]
    pub reconnect_interval: Option<i64>,
    #[serde(default)]
    pub worker_group_thread: Option<i32>,
    #[serde(default)]
    pub log_level: Option<String>,
    #[serde(default)]
    pub protocol: Option<ProtocolConfig>,
    #[serde(default)]
    pub heartbeat_interval: Option<i32>,
}

/// mqtt 段，对齐 Java `MqttConfig`（多 broker，各自独立配置）。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MqttConfig {
    #[serde(default)]
    pub configuration: Vec<MqttConfigServer>,
}

/// 对齐 Java `MqttConfigServer`。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MqttConfigServer {
    pub client_id: String,
    pub host: String,
    pub port: u16,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub tls: Option<String>,
    #[serde(default)]
    pub tls_cert: Option<String>,
    #[serde(default)]
    pub tls_key: Option<String>,
    #[serde(default)]
    pub qos: Option<i32>,
    #[serde(default)]
    pub topics: Vec<String>,
    #[serde(default)]
    pub protocol: Option<ProtocolConfig>,
    #[serde(default)]
    pub reconnect_delay: Option<i64>,
    #[serde(default)]
    pub reconnect_enabled: Option<bool>,
    #[serde(default)]
    pub message_max_bytes: Option<i32>,
    #[serde(default)]
    pub run_mode: Option<String>,
    /// 遗嘱（对齐 Java `MqttClientConfig.lastWill`，可空；缺省不设遗嘱）。
    #[serde(default)]
    pub last_will: Option<MqttLastWill>,
}

/// 对齐 Java `MqttLastWill`（topic/message/retain/qos，Java 侧四项均必填非空）。
/// Rust 版建模为配置字段：topic/payload 必填，qos 缺省 0，retain 缺省 false。
/// Java 字段名 `message` 在配置里写作 `payload`（语义相同：遗嘱消息内容）。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MqttLastWill {
    pub topic: String,
    pub payload: String,
    #[serde(default)]
    pub qos: Option<i32>,
    #[serde(default)]
    pub retain: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 完整 fixture：kafka + tcp(server+client) + mqtt + devices，断言关键字段解析正确。
    const FULL: &str = r#"
[kafka]
bootstrapServers = "10.0.0.1:9092,10.0.0.2:9092"

[[devices]]
projectId = "p1"
sn = "SN001"
deviceId = "dev-001"
password = "pw1"

[tcp]
[tcp.server]
[[tcp.server.configuration]]
address = "0.0.0.0"
port = 9000
bufferSize = 4096
parserType = "LENGTH_FIELD"
heartbeatInterval = 120
[tcp.server.configuration.protocol]
name = "json_passthrough"
path = "protocols/json_passthrough.lua"
type = "script"
[tcp.server.configuration.protocol.params]
foo = "bar"

[tcp.client]
[[tcp.client.configuration]]
remoteAddress = "10.0.0.5"
remotePort = 9001
reconnectEnabled = true
reconnectInterval = 5
[tcp.client.configuration.protocol]
name = "hex_frame"
path = "protocols/hex_frame.lua"
type = "script"

[mqtt]
[[mqtt.configuration]]
clientId = "gw-001"
host = "broker.local"
port = 1883
username = "u"
password = "p"
qos = 1
topics = ["enn/+/up", "enn/+/event"]
reconnectEnabled = true
messageMaxBytes = 110000
runMode = "common"
[mqtt.configuration.protocol]
name = "json_passthrough"
path = "protocols/json_passthrough.lua"
type = "script"
[mqtt.configuration.lastWill]
topic = "enn/gw-001/status"
payload = "offline"
qos = 1
retain = true
"#;

    #[test]
    fn parses_full_config() {
        let cfg: GatewayConfig = toml::from_str(FULL).unwrap();
        assert_eq!(cfg.kafka.bootstrap_servers, "10.0.0.1:9092,10.0.0.2:9092");
        // topic 默认值生效
        assert_eq!(cfg.kafka.metric_topic, "enn_data_iot_metric");
        assert_eq!(cfg.kafka.event_topic, "data_iot_event");
        assert_eq!(cfg.kafka.info_topic, "data_iot_device_info");
        assert_eq!(cfg.kafka.command_topic, "device_command_topic");
        assert_eq!(cfg.kafka.cloud_topic, "iot_gateway_trdPlatform_data");

        assert_eq!(cfg.devices.len(), 1);
        assert_eq!(cfg.devices[0].device_id, "dev-001");

        let tcp = cfg.tcp.as_ref().unwrap();
        let srv = tcp.server.as_ref().unwrap();
        assert_eq!(srv.configuration.len(), 1);
        assert_eq!(srv.configuration[0].port, 9000);
        assert_eq!(srv.configuration[0].address, "0.0.0.0");
        assert_eq!(srv.configuration[0].buffer_size, Some(4096));
        assert_eq!(
            srv.configuration[0].parser_type.as_deref(),
            Some("LENGTH_FIELD")
        );
        let p = srv.configuration[0].protocol.as_ref().unwrap();
        assert_eq!(p.name, "json_passthrough");
        assert_eq!(p.protocol_type, "script");
        assert_eq!(p.params["foo"], "bar");

        let cli = tcp.client.as_ref().unwrap();
        assert_eq!(cli.configuration[0].remote_address, "10.0.0.5");
        assert_eq!(cli.configuration[0].remote_port, 9001);
        assert_eq!(cli.configuration[0].reconnect_enabled, Some(true));

        let mqtt = cfg.mqtt.as_ref().unwrap();
        assert_eq!(mqtt.configuration.len(), 1);
        assert_eq!(mqtt.configuration[0].client_id, "gw-001");
        assert_eq!(
            mqtt.configuration[0].topics,
            vec!["enn/+/up", "enn/+/event"]
        );
        assert_eq!(mqtt.configuration[0].message_max_bytes, Some(110000));
        let will = mqtt.configuration[0].last_will.as_ref().unwrap();
        assert_eq!(will.topic, "enn/gw-001/status");
        assert_eq!(will.payload, "offline");
        assert_eq!(will.qos, Some(1));
        assert_eq!(will.retain, Some(true));
    }

    /// 向后兼容：不配 lastWill → None（不设遗嘱），qos/retain 缺省同理。
    #[test]
    fn last_will_is_optional() {
        let cfg: GatewayConfig = toml::from_str(
            r#"
[kafka]
bootstrapServers = "x"
[mqtt]
[[mqtt.configuration]]
clientId = "gw-001"
host = "broker.local"
port = 1883
"#,
        )
        .unwrap();
        assert!(cfg.mqtt.unwrap().configuration[0].last_will.is_none());

        let cfg: GatewayConfig = toml::from_str(
            r#"
[kafka]
bootstrapServers = "x"
[mqtt]
[[mqtt.configuration]]
clientId = "gw-001"
host = "broker.local"
port = 1883
[mqtt.configuration.lastWill]
topic = "t"
payload = "p"
"#,
        )
        .unwrap();
        let mqtt = cfg.mqtt.unwrap();
        let will = mqtt.configuration[0].last_will.as_ref().unwrap();
        assert_eq!(will.qos, None);
        assert_eq!(will.retain, None);
    }

    #[test]
    fn collects_protocol_configs_from_tcp_and_mqtt() {
        let cfg: GatewayConfig = toml::from_str(FULL).unwrap();
        let protos = cfg.protocol_configs();
        // tcp.server(1) + tcp.client(1) + mqtt(1) = 3
        assert_eq!(protos.len(), 3);
        let names: Vec<&str> = protos.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["json_passthrough", "hex_frame", "json_passthrough"]
        );
    }

    #[test]
    fn lists_configured_connectors() {
        let cfg: GatewayConfig = toml::from_str(FULL).unwrap();
        let mut conns = cfg.configured_connectors();
        conns.sort_unstable();
        assert_eq!(conns, vec!["mqtt", "tcp"]);
    }

    /// 仅 [kafka]、无 connector 段：解析成功且 connector 列表为空（gateway 空转退出路径）。
    #[test]
    fn parses_kafka_only_config() {
        let cfg: GatewayConfig = toml::from_str(
            r#"[kafka]
bootstrapServers = "127.0.0.1:9092"
"#,
        )
        .unwrap();
        assert!(cfg.configured_connectors().is_empty());
        assert!(cfg.protocol_configs().is_empty());
    }

    /// scriptDir 可选：设置时解析为 Some，缺省 None（向后兼容）。
    #[test]
    fn parses_optional_script_dir() {
        // scriptDir 是顶层键，必须位于任何 [section] 之前
        let cfg: GatewayConfig = toml::from_str(
            r#"scriptDir = "protocols"
[kafka]
bootstrapServers = "x"
"#,
        )
        .unwrap();
        assert_eq!(cfg.script_dir.as_deref(), Some("protocols"));

        let cfg: GatewayConfig = toml::from_str(
            r#"[kafka]
bootstrapServers = "x"
"#,
        )
        .unwrap();
        assert_eq!(cfg.script_dir, None);
    }

    /// 缺字段 / 错类型 → 解析失败（失败可见）。
    #[test]
    fn rejects_missing_required_field() {
        // kafka 缺 bootstrapServers
        assert!(
            toml::from_str::<GatewayConfig>(
                r#"[kafka]
metricTopic = "x"
"#
            )
            .is_err()
        );
        // tcp.server.configuration.port 类型错误（字符串）
        assert!(
            toml::from_str::<GatewayConfig>(
                r#"
[kafka]
bootstrapServers = "x"
[[tcp.server.configuration]]
port = "not-a-number"
"#
            )
            .is_err()
        );
    }
}
