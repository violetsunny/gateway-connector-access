//! http connector 配置，字段名对齐 Java `ennwiot.connector-http`（camelCase 经 serde rename）。
//!
//! Java `HttpConfig` 只有 `configuration`（pkey→协议）；std 四路由在 Java 侧硬编码
//! `EnnStandardHttpProtocol`。Rust 版 std 报文格式由 Lua 脚本承载（spec 决策），
//! 因此新增 `stdProtocol` 字段指向 std 示例脚本——这是 Rust 侧新增，非 Java 契约字段。

use connector_common::ProtocolConfig;
use serde::Deserialize;

fn default_address() -> String {
    "0.0.0.0".to_string()
}
fn default_port() -> u16 {
    8080
}

/// `[http]` 段。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpConfig {
    #[serde(default = "default_address")]
    pub address: String,
    #[serde(default = "default_port")]
    pub port: u16,
    /// 通配路由 `POST /api/{pkey}/**` 的 pkey→协议映射，对齐 Java `HttpConfigProtocol`。
    #[serde(default)]
    pub configuration: Vec<HttpRouteConfig>,
    /// std 四路由共用的协议脚本（承载 `/access/std/{rtg,history,event,info}` 报文格式）。
    pub std_protocol: ProtocolConfig,
}

/// 单条通配路由配置，对齐 Java `HttpConfigProtocol{requestPath, protocol}`。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpRouteConfig {
    /// 即 pkey：`/api/{pkey}/**` 中的路径段，用于选协议。
    pub request_path: String,
    pub protocol: ProtocolConfig,
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &str = r#"
address = "127.0.0.1"
port = 9080
[[configuration]]
requestPath = "productA"
[configuration.protocol]
name = "echo"
path = "protocols/echo.lua"
type = "script"
[configuration.protocol.params]
deviceId = "d1"
[stdProtocol]
name = "enn-standard-http"
path = "protocols/std_http.lua"
type = "script"
"#;

    #[test]
    fn parses_http_config() {
        let cfg: HttpConfig = toml::from_str(FIXTURE).unwrap();
        assert_eq!(cfg.address, "127.0.0.1");
        assert_eq!(cfg.port, 9080);
        assert_eq!(cfg.configuration.len(), 1);
        assert_eq!(cfg.configuration[0].request_path, "productA");
        assert_eq!(cfg.configuration[0].protocol.name, "echo");
        assert_eq!(cfg.configuration[0].protocol.params["deviceId"], "d1");
        assert_eq!(cfg.std_protocol.name, "enn-standard-http");
        assert_eq!(cfg.std_protocol.protocol_type, "script");
    }

    #[test]
    fn applies_address_and_port_defaults() {
        let cfg: HttpConfig = toml::from_str(
            r#"
[stdProtocol]
name = "std"
path = "std.lua"
"#,
        )
        .unwrap();
        assert_eq!(cfg.address, "0.0.0.0");
        assert_eq!(cfg.port, 8080);
        assert!(cfg.configuration.is_empty());
    }

    /// 缺 stdProtocol → 解析失败（失败可见，std 路由是 connector 必备能力）。
    #[test]
    fn rejects_missing_std_protocol() {
        assert!(toml::from_str::<HttpConfig>("port = 1").is_err());
    }
}
