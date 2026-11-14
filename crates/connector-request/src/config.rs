//! request 段配置模型。字段名对齐 Java `HttpRequestConfig`/`HttpServerConfig`
//! （`ennwiot.connector-request`，camelCase 经 serde rename）。
//!
//! 与 Java 的刻意差异：Java 侧请求定义（URL/method/headers/body/limit/AUTH）存 DB，
//! 经 `TrdPlatformCloudServer.taskReqContext` 查库获得；Rust 一期无 DB，
//! 请求定义直接进 TOML 任务配置（对齐 `TrdPlatformBody`/`TrdPlatformReqTask` 字段语义）。
//!
//! 入口 [`RequestConfig::from_value`]：`GatewayConfig.request` 一期是 `serde_json::Value`
//! 兜底，本模块负责解析成强类型配置并校验（参考 connector-snmp config.rs 的做法）。

use std::collections::BTreeMap;

use connector_common::{CommonError, ProtocolConfig, Result};
use cron::Schedule;
use serde::Deserialize;
use serde_json::Value as JsonValue;

pub(crate) const DEFAULT_METHOD: &str = "GET";
pub(crate) const DEFAULT_TIMEOUT_MS: u64 = 30_000;
pub(crate) const DEFAULT_AUTH_HEADER: &str = "Authorization";
pub(crate) const DEFAULT_EXPIRES_SECONDS: u64 = 3600;

fn default_method() -> String {
    DEFAULT_METHOD.to_string()
}
fn default_timeout_ms() -> u64 {
    DEFAULT_TIMEOUT_MS
}
fn default_auth_header() -> String {
    DEFAULT_AUTH_HEADER.to_string()
}
fn default_expires_seconds() -> u64 {
    DEFAULT_EXPIRES_SECONDS
}

/// `[request]` 段，对齐 Java `HttpRequestConfig`（configuration 为任务列表）。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestConfig {
    #[serde(default)]
    pub configuration: Vec<RequestTaskConfig>,
}

/// 单个轮询/下控任务。Java 侧对应 `HttpServerConfig`（appid/server/productId/cron/protocol）
/// + DB 里的 `TrdPlatformBody`（url/method/header/body）+ `TrdPlatformReqTask.limit`。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestTaskConfig {
    /// 平台编码，对齐 Java `HttpServerConfig.appid`（即 pCode）。
    pub appid: String,
    /// 任务编码，对齐 Java `HttpServerConfig.server`（即 taskCode）。
    pub server: String,
    #[serde(default)]
    pub product_id: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    /// Quartz 风格 cron（6-7 段，秒 分 时 日 月 周 [年]），替代 xxl-job 调度。
    /// 缺省 → 仅下控任务（不参与上行轮询）。
    #[serde(default)]
    pub cron: Option<String>,
    /// 目标 URL。下控任务可含 `${name}` 占位符（见 crate::url）。
    pub url: String,
    #[serde(default = "default_method")]
    pub method: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// 请求体（JSON），对齐 `TrdPlatformBody.body`。缺省 → 无 body。
    #[serde(default)]
    pub body: Option<JsonValue>,
    /// 令牌桶速率（令牌/秒），对齐 `TrdPlatformReqTask.limit`。缺省或 0 → 不限速。
    #[serde(default)]
    pub limit: Option<f64>,
    /// 单次 HTTP 超时（毫秒）。
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    pub protocol: ProtocolConfig,
    /// AUTH 型任务的 token 获取配置，对齐 Java `apiType=AUTH` + `authRefreshToken`。
    #[serde(default)]
    pub auth: Option<AuthConfig>,
}

/// AUTH token 获取配置。Java 侧 token 存 Redis（TTL=expirationTime 秒），
/// Rust 一期进程内缓存（`crate::auth::TokenCache`）。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthConfig {
    pub url: String,
    #[serde(default = "default_method")]
    pub method: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub body: Option<JsonValue>,
    /// token 在响应 JSON 中的位置，JSON Pointer（如 "/data/token"）。
    /// 对齐 Java `bodyAnalysisCode` 的 JSONPath 语义（一期用 serde_json 原生 Pointer，不引 JSONPath 库）。
    pub token_pointer: String,
    /// 过期秒数在响应 JSON 中的位置（JSON Pointer），对齐 `TrdPlatformAuthToken.expirationTime`。
    #[serde(default)]
    pub expires_pointer: Option<String>,
    /// 响应未给出过期时间时的兜底 TTL（秒）。
    #[serde(default = "default_expires_seconds")]
    pub expires_seconds: u64,
    /// token 注入的请求头名。
    #[serde(default = "default_auth_header")]
    pub header: String,
    /// token 值前缀（如 "Bearer "）。
    #[serde(default)]
    pub prefix: String,
}

/// cron 表达式语法校验（config 与 Kafka frequency 共用）。
pub(crate) fn parse_cron(expr: &str) -> Result<Schedule> {
    expr.parse::<Schedule>()
        .map_err(|e| CommonError::Config(format!("cron 表达式 '{expr}' 非法: {e}")))
}

fn check_http_url(what: &str, url: &str) -> Result<()> {
    if !url.starts_with("http://") && !url.starts_with("https://") {
        return Err(CommonError::Config(format!(
            "{what}: url 必须以 http:// 或 https:// 开头，实际 '{url}'"
        )));
    }
    Ok(())
}

fn check_method(what: &str, method: &str) -> Result<()> {
    method
        .parse::<reqwest::Method>()
        .map_err(|e| CommonError::Config(format!("{what}: method '{method}' 非法: {e}")))?;
    Ok(())
}

fn check_pointer(what: &str, pointer: &str) -> Result<()> {
    if !pointer.starts_with('/') {
        return Err(CommonError::Config(format!(
            "{what}: JSON Pointer 必须以 '/' 开头，实际 '{pointer}'"
        )));
    }
    Ok(())
}

impl RequestConfig {
    /// 从 `GatewayConfig.request`（JsonValue）解析并逐任务校验，失败即启动报错。
    pub fn from_value(value: &JsonValue) -> Result<Self> {
        let cfg: Self = serde_json::from_value(value.clone())
            .map_err(|e| CommonError::Config(format!("request 段解析失败: {e}")))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// 逐任务校验 + 任务键查重（直接构造 RequestConfig 的路径也须调用）。
    pub fn validate(&self) -> Result<()> {
        let mut seen = std::collections::HashSet::new();
        for task in &self.configuration {
            task.validate()?;
            if !seen.insert(task.key()) {
                return Err(CommonError::Config(format!(
                    "request 任务重复: {}",
                    task.key()
                )));
            }
        }
        Ok(())
    }
}

impl RequestTaskConfig {
    /// 任务唯一键，对齐 Java jobKey `pCode-taskCode`（TrdPlatformCloudServerImpl:587）。
    pub fn key(&self) -> String {
        format!("{}-{}", self.appid, self.server)
    }

    /// 空白 cron 归一为 None（对齐 Java `StringUtils.isBlank(frequency)` 判断）。
    pub fn cron_expr(&self) -> Option<&str> {
        self.cron
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
    }

    pub fn validate(&self) -> Result<()> {
        let key = self.key();
        if self.appid.trim().is_empty() || self.server.trim().is_empty() {
            return Err(CommonError::Config(format!(
                "request 任务 {key}: appid/server 不能为空"
            )));
        }
        check_http_url(&format!("request 任务 {key}"), &self.url)?;
        check_method(&format!("request 任务 {key}"), &self.method)?;
        if let Some(expr) = self.cron_expr() {
            parse_cron(expr)?;
        }
        if let Some(limit) = self.limit
            && !(limit.is_finite() && limit >= 0.0)
        {
            return Err(CommonError::Config(format!(
                "request 任务 {key}: limit 必须 ≥ 0，实际 {limit}"
            )));
        }
        if self.timeout_ms == 0 {
            return Err(CommonError::Config(format!(
                "request 任务 {key}: timeoutMs 必须 > 0"
            )));
        }
        if let Some(auth) = &self.auth {
            auth.validate(&key)?;
        }
        Ok(())
    }
}

impl AuthConfig {
    fn validate(&self, task_key: &str) -> Result<()> {
        let what = format!("request 任务 {task_key} auth");
        check_http_url(&what, &self.url)?;
        check_method(&what, &self.method)?;
        check_pointer(&format!("{what}.tokenPointer"), &self.token_pointer)?;
        if let Some(p) = &self.expires_pointer {
            check_pointer(&format!("{what}.expiresPointer"), p)?;
        }
        if self.header.trim().is_empty() {
            return Err(CommonError::Config(format!("{what}: header 不能为空")));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use connector_common::GatewayConfig;
    use serde_json::json;

    /// TOML fixture → GatewayConfig.request（JsonValue）→ RequestConfig，走 gateway 真实解析路径。
    fn parse_toml(toml_text: &str) -> Result<RequestConfig> {
        let cfg: GatewayConfig = toml::from_str(toml_text).expect("TOML fixture 本身应合法");
        let request = cfg
            .request
            .as_ref()
            .expect("fixture 必须包含 [request] 段")
            .clone();
        RequestConfig::from_value(&request)
    }

    const FULL: &str = r#"
[kafka]
bootstrapServers = "127.0.0.1:9092"

[request]
[[request.configuration]]
appid = "cloud-a"
server = "pull-meter"
productId = "prod-1"
description = "电表数据拉取"
cron = "0 */5 * * * *"
url = "https://api.example.com/v1/data"
method = "POST"
limit = 2.0
timeoutMs = 5000
headers = { "X-App" = "gw" }
body = { "pageSize" = 100 }
[request.configuration.protocol]
name = "cloud_pull"
path = "protocols/cloud_pull.lua"
type = "script"
[request.configuration.protocol.params]
deviceId = "dev-1"
[request.configuration.auth]
url = "https://api.example.com/token"
method = "POST"
body = { "user" = "u", "pass" = "p" }
tokenPointer = "/data/token"
expiresPointer = "/data/expires"
expiresSeconds = 600
header = "X-Token"
prefix = "T "
"#;

    #[test]
    fn parses_toml_fixture() {
        let cfg = parse_toml(FULL).unwrap();
        assert_eq!(cfg.configuration.len(), 1);
        let t = &cfg.configuration[0];
        assert_eq!(t.key(), "cloud-a-pull-meter");
        assert_eq!(t.product_id.as_deref(), Some("prod-1"));
        assert_eq!(t.cron_expr(), Some("0 */5 * * * *"));
        assert_eq!(t.method, "POST");
        assert_eq!(t.limit, Some(2.0));
        assert_eq!(t.timeout_ms, 5000);
        assert_eq!(t.headers.get("X-App").unwrap(), "gw");
        assert_eq!(t.body.as_ref().unwrap()["pageSize"], 100);
        assert_eq!(t.protocol.name, "cloud_pull");
        let auth = t.auth.as_ref().unwrap();
        assert_eq!(auth.token_pointer, "/data/token");
        assert_eq!(auth.expires_pointer.as_deref(), Some("/data/expires"));
        assert_eq!(auth.expires_seconds, 600);
        assert_eq!(auth.header, "X-Token");
        assert_eq!(auth.prefix, "T ");
    }

    /// 最小配置：method/timeoutMs/auth 默认值；无 cron → 仅下控任务。
    #[test]
    fn applies_defaults() {
        let cfg = parse_toml(
            r#"
[kafka]
bootstrapServers = "x"
[request]
[[request.configuration]]
appid = "p"
server = "t"
url = "http://10.0.0.1/api"
[request.configuration.protocol]
name = "p"
path = "p.lua"
"#,
        )
        .unwrap();
        let t = &cfg.configuration[0];
        assert_eq!(t.method, DEFAULT_METHOD);
        assert_eq!(t.timeout_ms, DEFAULT_TIMEOUT_MS);
        assert_eq!(t.cron_expr(), None);
        assert!(t.auth.is_none());
        assert!(t.limit.is_none());
        assert!(t.headers.is_empty());

        // auth 默认值
        let cfg = RequestConfig::from_value(&json!({
            "configuration": [{
                "appid": "p", "server": "t", "url": "http://x/api",
                "protocol": {"name": "p", "path": "p.lua"},
                "auth": {"url": "http://x/token", "tokenPointer": "/token"},
            }]
        }))
        .unwrap();
        let auth = cfg.configuration[0].auth.as_ref().unwrap();
        assert_eq!(auth.method, DEFAULT_METHOD);
        assert_eq!(auth.header, DEFAULT_AUTH_HEADER);
        assert_eq!(auth.prefix, "");
        assert_eq!(auth.expires_seconds, DEFAULT_EXPIRES_SECONDS);
    }

    /// 缺字段 / 错类型 / 非法值 → 解析或校验失败（失败可见）。
    #[test]
    fn rejects_bad_config() {
        // 缺 url
        assert!(
            parse_toml(
                r#"
[kafka]
bootstrapServers = "x"
[request]
[[request.configuration]]
appid = "p"
server = "t"
[request.configuration.protocol]
name = "p"
path = "p.lua"
"#
            )
            .is_err()
        );

        let base = |extra: JsonValue| {
            let mut task = json!({
                "appid": "p", "server": "t", "url": "http://x/api",
                "protocol": {"name": "p", "path": "p.lua"},
            });
            task.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            json!({ "configuration": [task] })
        };

        // url 非 http(s)
        let err = RequestConfig::from_value(&base(json!({"url": "ftp://x/api"}))).unwrap_err();
        assert!(err.to_string().contains("http"), "{err}");

        // method 非法（含空格不是合法 HTTP method token；reqwest 允许 FETCH 等自定义方法名）
        let err = RequestConfig::from_value(&base(json!({"method": "GET POST"}))).unwrap_err();
        assert!(err.to_string().contains("method"), "{err}");

        // cron 非法
        let err = RequestConfig::from_value(&base(json!({"cron": "every 5 min"}))).unwrap_err();
        assert!(err.to_string().contains("cron"), "{err}");

        // limit 负数
        let err = RequestConfig::from_value(&base(json!({"limit": -1.0}))).unwrap_err();
        assert!(err.to_string().contains("limit"), "{err}");

        // timeoutMs = 0
        let err = RequestConfig::from_value(&base(json!({"timeoutMs": 0}))).unwrap_err();
        assert!(err.to_string().contains("timeoutMs"), "{err}");

        // appid 空白
        let err = RequestConfig::from_value(&base(json!({"appid": "  "}))).unwrap_err();
        assert!(err.to_string().contains("appid"), "{err}");

        // auth.tokenPointer 非 JSON Pointer
        let err = RequestConfig::from_value(&base(
            json!({"auth": {"url": "http://x/token", "tokenPointer": "data.token"}}),
        ))
        .unwrap_err();
        assert!(err.to_string().contains("tokenPointer"), "{err}");

        // auth.url 非法
        let err = RequestConfig::from_value(&base(
            json!({"auth": {"url": "x/token", "tokenPointer": "/t"}}),
        ))
        .unwrap_err();
        assert!(err.to_string().contains("auth"), "{err}");
    }

    /// 同 appid+server 重复 → 报错（任务键冲突会让增删语义失效）。
    #[test]
    fn rejects_duplicate_task_key() {
        let task = json!({
            "appid": "p", "server": "t", "url": "http://x/api",
            "protocol": {"name": "p", "path": "p.lua"},
        });
        let err =
            RequestConfig::from_value(&json!({"configuration": [task.clone(), task]})).unwrap_err();
        assert!(err.to_string().contains("重复"), "{err}");
    }

    /// cron 语法校验函数本身。
    #[test]
    fn parse_cron_accepts_quartz_style() {
        assert!(parse_cron("* * * * * *").is_ok());
        assert!(parse_cron("0 0 0 29 2 *").is_ok());
        assert!(parse_cron("0 */5 * * * * 2030").is_ok());
        assert!(parse_cron("not a cron").is_err());
    }
}
