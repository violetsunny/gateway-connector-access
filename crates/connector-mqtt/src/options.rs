//! `MqttConfigServer` → rumqttc 连接参数（纯函数，便于单测）。
//!
//! Java 侧 `tls/tlsCert/tlsKey/qos/runMode` 是死配置（`MqttClientImpl` 从不读取：订阅固定
//! QoS0、下行固定 QoS1、无 TLS 分支）。Rust 版让这些字段真实生效：
//!
//! - `qos` → 订阅 QoS，缺省 0（等价 Java `client.on(topic, handler)` 的 AT_MOST_ONCE）
//! - `tls/tlsCert/tlsKey` → rumqttc rustls（默认特性），下行 publish 仍固定 AT_LEAST_ONCE
//!   （对齐 `DownMqttHandler.java:79`）
//! - `messageMaxBytes` → 收发包大小上限，缺省 110000（对齐 Java 字段默认值）
//! - `reconnectDelay` 单位为秒（对齐 `MqttClientImpl.java:177` 的 `TimeUnit.SECONDS`）
//! - `lastWill` → rumqttc `set_last_will`（对齐 Java `MqttLastWill`：topic/message/retain/qos；
//!   缺省不设遗嘱）

use std::time::Duration;

use connector_common::{CommonError, MqttConfigServer, Result};
use rumqttc::{LastWill, MqttOptions, QoS, Transport};

/// 默认重连延迟（秒），对齐 Java `MqttConfigServer.reconnectDelay = 10`。
const DEFAULT_RECONNECT_DELAY_SECS: i64 = 10;
/// 默认单包最大字节数，对齐 Java `MqttConfigServer.messageMaxBytes = 110000`。
const DEFAULT_MESSAGE_MAX_BYTES: i32 = 110_000;
/// 请求通道容量（publish/subscribe 请求排队上限，满则 client 调用等待）。
pub const REQUEST_CHANNEL_CAP: usize = 10;

/// broker 唯一键，对齐 Java `MqttConfigServer.uuid()`（clientId+host+port）。
/// 会话表与下行路由都以它标识 broker。
pub fn broker_key(cfg: &MqttConfigServer) -> String {
    format!("{}{}{}", cfg.client_id, cfg.host, cfg.port)
}

/// `qos` 配置 → rumqttc QoS。缺省 AtMostOnce（对齐 Java 订阅行为）；越界报错（失败可见）。
pub fn parse_qos(qos: Option<i32>) -> Result<QoS> {
    match qos {
        None | Some(0) => Ok(QoS::AtMostOnce),
        Some(1) => Ok(QoS::AtLeastOnce),
        Some(2) => Ok(QoS::ExactlyOnce),
        Some(other) => Err(CommonError::Config(format!(
            "qos 非法: {other}（仅支持 0/1/2）"
        ))),
    }
}

/// `tls` 字段 → 是否启用 TLS。无法识别的取值报错，不静默当作关闭（失败可见）。
pub fn tls_enabled(tls: Option<&str>) -> Result<bool> {
    let normalized = tls
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .unwrap_or_default();
    match normalized.as_str() {
        "" | "false" | "0" | "no" | "off" => Ok(false),
        "true" | "1" | "yes" | "on" => Ok(true),
        other => Err(CommonError::Config(format!(
            "tls 取值无法识别: '{other}'（期望 true/false）"
        ))),
    }
}

/// 重连开关，缺省 true（对齐 Java `reconnectEnabled = true`）。
pub fn reconnect_enabled(cfg: &MqttConfigServer) -> bool {
    cfg.reconnect_enabled.unwrap_or(true)
}

/// 重连延迟，缺省 10 秒；负值按 0 处理（立即重试）。
pub fn reconnect_delay(cfg: &MqttConfigServer) -> Duration {
    let secs = cfg.reconnect_delay.unwrap_or(DEFAULT_RECONNECT_DELAY_SECS);
    Duration::from_secs(secs.max(0) as u64)
}

/// 构造 rumqttc 连接参数。TLS 证书读取失败 / 取值非法 → `Err`（构造期可见，不拖到连接时）。
pub fn build_options(cfg: &MqttConfigServer) -> Result<MqttOptions> {
    let key = broker_key(cfg);
    let mut options = MqttOptions::new(cfg.client_id.clone(), cfg.host.clone(), cfg.port);

    match (&cfg.username, &cfg.password) {
        (Some(username), Some(password)) => {
            options.set_credentials(username.clone(), password.clone());
        }
        (None, None) => {}
        _ => tracing::warn!(broker = %key, "username/password 只配了一个，按无认证连接"),
    }

    let max_bytes = cfg
        .message_max_bytes
        .unwrap_or(DEFAULT_MESSAGE_MAX_BYTES)
        .max(0) as usize;
    options.set_max_packet_size(max_bytes, max_bytes);

    if tls_enabled(cfg.tls.as_deref())? {
        options.set_transport(build_transport(cfg)?);
    }

    if let Some(mode) = cfg.run_mode.as_deref().filter(|m| *m != "common") {
        tracing::warn!(broker = %key, "runMode='{mode}' 无对应实现，按 common 处理");
    }

    if let Some(will) = &cfg.last_will {
        if will.topic.trim().is_empty() {
            return Err(CommonError::Config(format!(
                "mqtt broker {key}: lastWill.topic 不能为空"
            )));
        }
        let qos = parse_qos(will.qos)
            .map_err(|e| CommonError::Config(format!("mqtt broker {key}: lastWill {e}")))?;
        options.set_last_will(LastWill::new(
            will.topic.clone(),
            will.payload.clone().into_bytes(),
            qos,
            will.retain.unwrap_or(false),
        ));
    }

    // clean_session 保持 rumqttc 默认 true：重连后由 ConnAck 分支重新订阅（对齐 Java 连接成功后 client.on）
    Ok(options)
}

/// TLS 传输：`tlsCert` 作为 CA（PEM），同时给 `tlsKey` 时启用双向认证；
/// 未给 `tlsCert` 时用 rumqttc 默认配置（系统根证书）。
fn build_transport(cfg: &MqttConfigServer) -> Result<Transport> {
    let Some(cert_path) = cfg
        .tls_cert
        .as_deref()
        .map(str::trim)
        .filter(|p| !p.is_empty())
    else {
        // ponytail: 走 rumqttc 默认 TLS（系统根证书）这条最省事路线；crate 内部对
        // "读不到平台证书" 用 expect 处理。需要自签 CA / 双向认证时配 tlsCert+tlsKey。
        return Ok(Transport::tls_with_default_config());
    };
    let ca = read_file(cert_path, "tlsCert")?;
    let client_auth = match cfg
        .tls_key
        .as_deref()
        .map(str::trim)
        .filter(|p| !p.is_empty())
    {
        Some(key_path) => Some((ca.clone(), read_file(key_path, "tlsKey")?)),
        None => None,
    };
    Ok(Transport::tls(ca, client_auth, None))
}

fn read_file(path: &str, field: &str) -> Result<Vec<u8>> {
    std::fs::read(path)
        .map_err(|e| CommonError::Config(format!("{field} 证书读取失败 '{path}': {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use connector_common::{MqttLastWill, ProtocolConfig};
    use serde_json::json;

    fn server() -> MqttConfigServer {
        MqttConfigServer {
            client_id: "gw-001".into(),
            host: "broker.local".into(),
            port: 1883,
            password: None,
            username: None,
            tls: None,
            tls_cert: None,
            tls_key: None,
            qos: None,
            topics: vec!["enn/+/up".into()],
            protocol: Some(ProtocolConfig {
                name: "json_passthrough".into(),
                path: "protocols/json_passthrough.lua".into(),
                params: json!({}),
                protocol_type: "script".into(),
            }),
            reconnect_delay: None,
            reconnect_enabled: None,
            message_max_bytes: None,
            run_mode: None,
            last_will: None,
        }
    }

    #[test]
    fn broker_key_aligns_with_java_uuid() {
        // Java: clientId + host + port 直接拼接
        assert_eq!(broker_key(&server()), "gw-001broker.local1883");
    }

    #[test]
    fn parse_qos_maps_config_values() {
        assert_eq!(parse_qos(None).unwrap(), QoS::AtMostOnce);
        assert_eq!(parse_qos(Some(0)).unwrap(), QoS::AtMostOnce);
        assert_eq!(parse_qos(Some(1)).unwrap(), QoS::AtLeastOnce);
        assert_eq!(parse_qos(Some(2)).unwrap(), QoS::ExactlyOnce);
    }

    #[test]
    fn parse_qos_rejects_out_of_range() {
        let err = parse_qos(Some(3)).unwrap_err();
        assert!(matches!(err, CommonError::Config(_)), "got {err:?}");
        assert!(err.to_string().contains("qos 非法: 3"));
    }

    #[test]
    fn tls_enabled_parses_boolean_words() {
        assert!(!tls_enabled(None).unwrap());
        assert!(!tls_enabled(Some("")).unwrap());
        assert!(!tls_enabled(Some(" FALSE ")).unwrap());
        assert!(tls_enabled(Some("true")).unwrap());
        assert!(tls_enabled(Some("1")).unwrap());
        assert!(tls_enabled(Some("ON")).unwrap());
    }

    #[test]
    fn tls_enabled_rejects_unknown_value() {
        let err = tls_enabled(Some("maybe")).unwrap_err();
        assert!(err.to_string().contains("tls 取值无法识别: 'maybe'"));
    }

    #[test]
    fn reconnect_defaults_align_with_java() {
        let cfg = server();
        assert!(reconnect_enabled(&cfg));
        assert_eq!(reconnect_delay(&cfg), Duration::from_secs(10));

        let mut cfg = server();
        cfg.reconnect_enabled = Some(false);
        cfg.reconnect_delay = Some(-5);
        assert!(!reconnect_enabled(&cfg));
        assert_eq!(reconnect_delay(&cfg), Duration::ZERO);
    }

    #[test]
    fn build_options_applies_identity_and_packet_size() {
        let mut cfg = server();
        cfg.username = Some("u".into());
        cfg.password = Some("p".into());
        cfg.message_max_bytes = Some(4096);

        let options = build_options(&cfg).unwrap();
        assert_eq!(options.client_id(), "gw-001");
        assert_eq!(options.broker_address(), ("broker.local".to_string(), 1883));
        assert_eq!(options.max_packet_size(), 4096);
        let login = options.credentials().expect("credentials should be set");
        assert_eq!(login.username, "u");
        assert_eq!(login.password, "p");
        assert!(matches!(options.transport(), Transport::Tcp));
    }

    #[test]
    fn build_options_defaults_packet_size_to_java_value() {
        let options = build_options(&server()).unwrap();
        assert_eq!(options.max_packet_size(), 110_000);
        assert!(options.credentials().is_none());
    }

    #[test]
    fn build_options_uses_tls_transport_when_enabled() {
        let mut cfg = server();
        cfg.tls = Some("true".into());
        let options = build_options(&cfg).unwrap();
        assert!(matches!(options.transport(), Transport::Tls(_)));
    }

    /// tlsCert 指向不存在的文件 → 构造期报错（失败可见，不拖到连接时）。
    #[test]
    fn build_options_reports_missing_cert_file() {
        let mut cfg = server();
        cfg.tls = Some("true".into());
        cfg.tls_cert = Some("/definitely/not/here.pem".into());
        let err = build_options(&cfg).expect_err("expected config error");
        assert!(err.to_string().contains("tlsCert 证书读取失败"), "{err}");
    }

    /// 缺省无遗嘱（向后兼容）。
    #[test]
    fn build_options_without_last_will() {
        assert!(build_options(&server()).unwrap().last_will().is_none());
    }

    /// lastWill 全字段 → rumqttc LastWill（对齐 Java MqttLastWill 语义）。
    #[test]
    fn build_options_applies_last_will() {
        let mut cfg = server();
        cfg.last_will = Some(MqttLastWill {
            topic: "enn/gw-001/status".into(),
            payload: "offline".into(),
            qos: Some(1),
            retain: Some(true),
        });
        let will = build_options(&cfg).unwrap().last_will().expect("will set");
        assert_eq!(will.topic, "enn/gw-001/status");
        assert_eq!(will.message.as_ref(), b"offline");
        assert_eq!(will.qos, QoS::AtLeastOnce);
        assert!(will.retain);
    }

    /// lastWill 缺省 qos=0 / retain=false；空 topic 与非法 qos 构造期报错。
    #[test]
    fn build_options_last_will_defaults_and_errors() {
        let mut cfg = server();
        cfg.last_will = Some(MqttLastWill {
            topic: "t".into(),
            payload: String::new(),
            qos: None,
            retain: None,
        });
        let will = build_options(&cfg).unwrap().last_will().expect("will set");
        assert_eq!(will.qos, QoS::AtMostOnce);
        assert!(!will.retain);
        assert!(will.message.is_empty());

        cfg.last_will = Some(MqttLastWill {
            topic: " ".into(),
            payload: "p".into(),
            qos: None,
            retain: None,
        });
        let err = build_options(&cfg).expect_err("expected config error");
        assert!(err.to_string().contains("lastWill.topic 不能为空"), "{err}");

        cfg.last_will = Some(MqttLastWill {
            topic: "t".into(),
            payload: "p".into(),
            qos: Some(3),
            retain: None,
        });
        let err = build_options(&cfg).expect_err("expected config error");
        assert!(err.to_string().contains("lastWill"), "{err}");
        assert!(err.to_string().contains("qos 非法: 3"), "{err}");
    }
}
