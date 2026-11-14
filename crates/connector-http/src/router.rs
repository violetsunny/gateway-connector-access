//! axum 路由：`POST /api/{pkey}/**` 通配接入 + `/access/std/{rtg,history,event,info}` 标准报文。
//!
//! 对齐 Java `HttpConnectorStater`（通配）与 `HttpStdConnector`（std）：
//! - 通配：按 pkey 选协议 → `decode_multi(body, params)` → 逐条 `store_message`；pkey 未知拒绝（4xx）。
//! - params 注入：pkey / wildcardPath / headers / query（对齐 HttpConnectorStater.java:83 调用语义，
//!   query 为 Rust 侧补充），与协议配置 params 合并后传给 Lua。
//! - std：四路由共用 std 协议脚本，注入 `route`=请求 URI，报文格式由脚本承载（不移植 Java 源码）。
//!
//! 路由分发 / pkey 解析 / params 注入为非平凡逻辑，测试见本模块 `tests`（tower oneshot 内存请求）。

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::Json;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use connector_common::{DeviceSink, Protocol};
use serde_json::{Value as JsonValue, json};

/// 单条通配路由的运行时条目：已加载协议 + 该协议的配置 params。
pub(crate) struct RouteEntry {
    pub protocol: Arc<dyn Protocol>,
    pub params: JsonValue,
}

/// 路由共享状态。计数器用 `Arc<AtomicUsize>`，与 `HttpConnector::status` 共享同一实例。
pub(crate) struct HttpState {
    pub routes: Arc<HashMap<String, RouteEntry>>,
    pub std_protocol: Arc<dyn Protocol>,
    pub std_params: JsonValue,
    pub sink: Arc<dyn DeviceSink>,
    pub requests: Arc<AtomicUsize>,
    pub messages: Arc<AtomicUsize>,
}

/// 构建 axum Router。独立于配置/文件加载，便于测试注入内联协议（Seam）。
pub(crate) fn build_router(state: Arc<HttpState>) -> Router {
    Router::new()
        // ponytail: axum 默认 DefaultBodyLimit 2MB，超限返回 413。
        // 升级路径：大报文接入时用 .layer(DefaultBodyLimit::max(n)) 放宽。
        // 两条路由对齐 Java Spring `/api/{pkey}/**`：`{*rest}` 不匹配裸 `/api/{pkey}`（无子路径），
        // 故补一条精确路由；此时 wildcardPath 为空串（Java 回退为整段 uri，此处取更干净的空串）。
        .route("/api/{pkey}/{*rest}", post(wildcard_handler))
        .route("/api/{pkey}", post(wildcard_handler))
        .route("/access/std/rtg", post(std_handler))
        .route("/access/std/history", post(std_handler))
        .route("/access/std/event", post(std_handler))
        .route("/access/std/info", post(std_handler))
        .with_state(state)
}

/// 通配接入 `POST /api/{pkey}/**`，对齐 Java `HttpConnectorStater.request`。
async fn wildcard_handler(
    State(state): State<Arc<HttpState>>,
    Path(params): Path<HashMap<String, String>>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    state.requests.fetch_add(1, Ordering::Relaxed);

    let Some(pkey) = params.get("pkey") else {
        return err(StatusCode::BAD_REQUEST, "missing pkey");
    };
    // `{*rest}` 捕获 pkey 之后的剩余路径（无子路径时为空串），对齐 Java getWildcardPath。
    let wildcard_path = params.get("rest").cloned().unwrap_or_default();

    let Some(entry) = state.routes.get(pkey.as_str()) else {
        // 失败可见：未知 pkey 明确拒绝，不静默丢弃。
        tracing::warn!(pkey = %pkey, "http wildcard: 没有找到协议");
        return err(StatusCode::NOT_FOUND, format!("pkey '{pkey}' 没有找到协议"));
    };

    let merged = merged_params(
        &entry.params,
        vec![
            ("pkey", json!(pkey)),
            ("wildcardPath", json!(wildcard_path)),
            ("headers", headers_to_json(&headers)),
            ("query", json!(query)),
        ],
    );

    match run_decode_store(&state, entry.protocol.as_ref(), &body, &merged).await {
        Ok(()) => ok(json!("OK")),
        Err((status, detail)) => {
            // 详情只进日志，响应回通用消息（防内部错误回显给客户端）
            tracing::error!(pkey = %pkey, error = %detail, "http wildcard decode/store 失败");
            err(status, public_message(status))
        }
    }
}

/// 标准报文 `POST /access/std/{rtg,history,event,info}`，对齐 Java `HttpStdConnector`。
async fn std_handler(State(state): State<Arc<HttpState>>, uri: Uri, body: Bytes) -> Response {
    state.requests.fetch_add(1, Ordering::Relaxed);
    let route = uri.path().to_string();
    let merged = merged_params(&state.std_params, vec![("route", json!(route))]);

    match run_decode_store(&state, state.std_protocol.as_ref(), &body, &merged).await {
        // Java：info 返回 data=true，其余返回 data="OK"。
        Ok(()) if route == "/access/std/info" => ok(json!(true)),
        Ok(()) => ok(json!("OK")),
        Err((status, detail)) => {
            // 详情只进日志，响应回通用消息（防内部错误回显给客户端）
            tracing::error!(route = %route, error = %detail, "http std decode/store 失败");
            err(status, public_message(status))
        }
    }
}

/// 解码并逐条上行。decode 失败 → 400（客户端报文/脚本问题）；store 失败 → 500（服务端 Kafka 问题）。
/// Err 携带的 String 为内部错误详情，仅用于日志，不回显给客户端（见 [`public_message`]）。
async fn run_decode_store(
    state: &HttpState,
    protocol: &dyn Protocol,
    body: &[u8],
    params: &JsonValue,
) -> Result<(), (StatusCode, String)> {
    let messages = protocol
        .decode_multi(body, params)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    for msg in messages {
        state
            .sink
            .store_message(msg)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        state.messages.fetch_add(1, Ordering::Relaxed);
    }
    Ok(())
}

/// 合并配置 params（base）与运行时注入（inject 覆盖同名键）。
fn merged_params(base: &JsonValue, inject: Vec<(&str, JsonValue)>) -> JsonValue {
    let mut map = match base {
        JsonValue::Object(m) => m.clone(),
        _ => serde_json::Map::new(),
    };
    for (k, v) in inject {
        map.insert(k.to_string(), v);
    }
    JsonValue::Object(map)
}

/// HeaderMap → JSON 对象。每个 header 名取首个值（对齐 Java `request.getHeader` 语义）。
/// 凭据类敏感头不注入 Lua params，防止进入脚本与上行消息。
// ponytail: 固定黑名单；升级路径：可配置黑名单
const SENSITIVE_HEADERS: [&str; 3] = ["authorization", "cookie", "proxy-authorization"];

fn headers_to_json(headers: &HeaderMap) -> JsonValue {
    let mut map = serde_json::Map::new();
    for name in headers.keys() {
        if SENSITIVE_HEADERS
            .iter()
            .any(|s| s.eq_ignore_ascii_case(name.as_str()))
        {
            continue;
        }
        if let Some(v) = headers.get(name) {
            map.insert(
                name.as_str().to_string(),
                json!(v.to_str().unwrap_or_default()),
            );
        }
    }
    JsonValue::Object(map)
}

/// 对外响应的通用错误消息：内部详情只进日志，不回显（防泄露脚本/服务端内部信息）。
fn public_message(status: StatusCode) -> &'static str {
    if status == StatusCode::BAD_REQUEST {
        "请求报文解码失败"
    } else {
        "服务端内部错误"
    }
}

/// 成功响应，信封对齐 Java `SingleResponse.buildSuccess`（success/code/message/data）。
fn ok(data: JsonValue) -> Response {
    (
        StatusCode::OK,
        Json(json!({
            "success": true,
            "code": "SUCCESS",
            "message": null,
            "data": data,
        })),
    )
        .into_response()
}

/// 失败响应，信封对齐 Java `SingleResponse.buildFailure`。
fn err(status: StatusCode, message: impl Into<String>) -> Response {
    (
        status,
        Json(json!({
            "success": false,
            "code": "ERROR",
            "message": message.into(),
            "data": null,
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use connector_common::{LuaProtocol, MockSink};
    use tower::ServiceExt; // oneshot

    /// 回显协议：把收到的 params 原样塞进消息，供断言 params 注入。
    fn echo_protocol() -> Arc<dyn Protocol> {
        Arc::new(
            LuaProtocol::from_source(
                "echo",
                r#"
                function decode(input, params)
                    return { messageType = "DEVICE_REPORT_REQ", params = params }
                end
                function decode_multi(input, params) return { decode(input, params) } end
                function encode(msg, params) return "" end
                function login(req, params) return true end
                "#,
            )
            .unwrap(),
        )
    }

    /// 真实 std 示例脚本（交付物），端到端验证 std 四路由报文格式。
    fn std_protocol() -> Arc<dyn Protocol> {
        let path = format!("{}/protocols/std_http.lua", env!("CARGO_MANIFEST_DIR"));
        Arc::new(LuaProtocol::from_script("enn-standard-http", &path).unwrap())
    }

    fn test_state(
        sink: Arc<MockSink>,
        routes: HashMap<String, RouteEntry>,
        std_protocol: Arc<dyn Protocol>,
    ) -> Arc<HttpState> {
        Arc::new(HttpState {
            routes: Arc::new(routes),
            std_protocol,
            std_params: json!({}),
            sink: sink as Arc<dyn DeviceSink>,
            requests: Arc::new(AtomicUsize::new(0)),
            messages: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn post(uri: &str, body: &str) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(uri)
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    // ---- 通配路由 ----

    #[tokio::test]
    async fn wildcard_decodes_to_sink_and_injects_params() {
        let sink = Arc::new(MockSink::new());
        let mut routes = HashMap::new();
        routes.insert(
            "productA".to_string(),
            RouteEntry {
                protocol: echo_protocol(),
                params: json!({"deviceId": "d1"}),
            },
        );
        let state = test_state(sink.clone(), routes, echo_protocol());
        let counters = state.clone();
        let app = build_router(state);

        let req = Request::builder()
            .method("POST")
            .uri("/api/productA/alarm/code?x=1&y=2")
            .header("x-custom", "hv")
            .body(Body::from(r#"{"raw":"payload"}"#))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let msgs = sink.messages();
        assert_eq!(msgs.len(), 1);
        let p = &msgs[0]["params"];
        // params 注入断言：wildcardPath / headers / query 在 Lua 脚本内可见
        assert_eq!(p["wildcardPath"], "alarm/code");
        assert_eq!(p["headers"]["x-custom"], "hv");
        assert_eq!(p["query"]["x"], "1");
        assert_eq!(p["query"]["y"], "2");
        assert_eq!(p["pkey"], "productA");
        // 配置 params 保留
        assert_eq!(p["deviceId"], "d1");
        // 计数器（status 用）
        assert_eq!(counters.requests.load(Ordering::Relaxed), 1);
        assert_eq!(counters.messages.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn wildcard_strips_sensitive_headers_from_params() {
        let sink = Arc::new(MockSink::new());
        let mut routes = HashMap::new();
        routes.insert(
            "pk".to_string(),
            RouteEntry {
                protocol: echo_protocol(),
                params: json!({}),
            },
        );
        let app = build_router(test_state(sink.clone(), routes, echo_protocol()));

        let req = Request::builder()
            .method("POST")
            .uri("/api/pk/x")
            .header("Authorization", "Bearer secret-token")
            .header("Cookie", "session=abc")
            .header("Proxy-Authorization", "Basic xyz")
            .header("x-custom", "hv")
            .body(Body::from("{}"))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let msgs = sink.messages();
        assert_eq!(msgs.len(), 1);
        let headers = &msgs[0]["params"]["headers"];
        assert_eq!(headers["x-custom"], "hv");
        for banned in ["authorization", "cookie", "proxy-authorization"] {
            assert!(
                !headers
                    .as_object()
                    .unwrap()
                    .keys()
                    .any(|k| k.eq_ignore_ascii_case(banned)),
                "sensitive header {banned} leaked into params: {headers}"
            );
        }
        // 凭据值不出现在上行消息任何位置
        let raw = msgs[0].to_string();
        for secret in ["secret-token", "session=abc", "Basic xyz"] {
            assert!(!raw.contains(secret), "credential {secret} leaked: {raw}");
        }
    }

    #[tokio::test]
    async fn wildcard_bare_pkey_matches_empty_rest() {
        let sink = Arc::new(MockSink::new());
        let mut routes = HashMap::new();
        routes.insert(
            "pk".to_string(),
            RouteEntry {
                protocol: echo_protocol(),
                params: json!({}),
            },
        );
        let app = build_router(test_state(sink.clone(), routes, echo_protocol()));

        let resp = app.oneshot(post("/api/pk", "{}")).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let msgs = sink.messages();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0]["params"]["wildcardPath"], "");
    }

    #[tokio::test]
    async fn wildcard_unknown_pkey_returns_4xx_and_stores_nothing() {
        let sink = Arc::new(MockSink::new());
        let app = build_router(test_state(sink.clone(), HashMap::new(), echo_protocol()));

        let resp = app.oneshot(post("/api/unknown/x", "{}")).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        assert!(resp.status().is_client_error());
        assert!(sink.is_empty());
    }

    #[tokio::test]
    async fn wildcard_decode_error_returns_400() {
        // 脚本 decode_multi 报错 → 400，且不静默上行
        let bad = Arc::new(
            LuaProtocol::from_source(
                "bad",
                r#"
                function decode(input, params) error("boom") end
                function decode_multi(input, params) error("boom") end
                function encode(msg, params) return "" end
                function login(req, params) return true end
                "#,
            )
            .unwrap(),
        );
        let sink = Arc::new(MockSink::new());
        let mut routes = HashMap::new();
        routes.insert(
            "pk".to_string(),
            RouteEntry {
                protocol: bad,
                params: json!({}),
            },
        );
        let app = build_router(test_state(sink.clone(), routes, echo_protocol()));

        let resp = app.oneshot(post("/api/pk/x", "{}")).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert!(sink.is_empty());
        // 内部错误详情（Lua "boom"）不回显，响应只含通用消息
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: JsonValue = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["success"], false);
        assert_eq!(v["message"], "请求报文解码失败");
        assert!(!bytes.windows(4).any(|w| w == b"boom"));
    }

    // ---- std 四路由 ----

    #[tokio::test]
    async fn std_rtg_decodes_metric_report() {
        let sink = Arc::new(MockSink::new());
        let app = build_router(test_state(sink.clone(), HashMap::new(), std_protocol()));
        let body = r#"{"ver":"0.0.1","pKey":"pk","sn":"s1","devs":[
            {"dev":"d1","ts":1700000000,"resume":"N","d":[
                {"m":"temp","v":25.5},
                {"m":"hum","v":60,"ts":1700000001000}
            ]}]}"#;
        let resp = app.oneshot(post("/access/std/rtg", body)).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let msgs = sink.messages();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0]["messageType"], "DEVICE_REPORT_REQ");
        assert_eq!(msgs[0]["deviceId"], "d1");
        assert_eq!(msgs[0]["resume"], "N");
        // 10 位秒级 ts → 毫秒
        assert_eq!(msgs[0]["timeStamp"], 1700000000000_i64);
        let metrics = msgs[0]["metrics"].as_array().unwrap();
        assert_eq!(metrics.len(), 2);
        assert_eq!(metrics[0]["code"], "temp");
        assert_eq!(metrics[0]["value"], 25.5);
        assert_eq!(metrics[0]["ts"], 1700000000000_i64); // 缺省回退 dev ts
        assert_eq!(metrics[1]["code"], "hum");
        assert_eq!(metrics[1]["ts"], 1700000001000_i64);
    }

    #[tokio::test]
    async fn std_history_decodes_metric_report() {
        let sink = Arc::new(MockSink::new());
        let app = build_router(test_state(sink.clone(), HashMap::new(), std_protocol()));
        let body = r#"{"devs":[{"dev":"d1","ts":1700000000000,"d":[{"m":"a","v":1}]}]}"#;
        let resp = app
            .oneshot(post("/access/std/history", body))
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let msgs = sink.messages();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0]["messageType"], "DEVICE_REPORT_REQ");
        assert_eq!(msgs[0]["deviceId"], "d1");
    }

    #[tokio::test]
    async fn std_event_decodes_event_report() {
        let sink = Arc::new(MockSink::new());
        let app = build_router(test_state(sink.clone(), HashMap::new(), std_protocol()));
        let body = r#"{"version":"v1","pKey":"pk","sn":"s1","devs":[
            {"dev":"d1","identifier":"alarm1","ts":1700000000000,"eventType":"alarm","value":{"k1":"v1"}}]}"#;
        let resp = app.oneshot(post("/access/std/event", body)).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let msgs = sink.messages();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0]["messageType"], "DEVICE_EVENT_REQ");
        assert_eq!(msgs[0]["deviceId"], "d1");
        assert_eq!(msgs[0]["identifier"], "alarm1");
        assert_eq!(msgs[0]["type"], "alarm");
        assert_eq!(msgs[0]["version"], "v1");
        let metrics = msgs[0]["metrics"].as_array().unwrap();
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0]["code"], "k1");
        assert_eq!(metrics[0]["value"], "v1");
    }

    #[tokio::test]
    async fn std_info_decodes_info_report() {
        let sink = Arc::new(MockSink::new());
        let app = build_router(test_state(sink.clone(), HashMap::new(), std_protocol()));
        let body = r#"{"pKey":"pk","sn":"s1","extra":"x"}"#;
        let resp = app.oneshot(post("/access/std/info", body)).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let msgs = sink.messages();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0]["messageType"], "DEVICE_INFO_REQ");
        assert_eq!(msgs[0]["sn"], "s1");
        assert_eq!(msgs[0]["data"]["pKey"], "pk");
        assert_eq!(msgs[0]["data"]["extra"], "x");
    }

    #[tokio::test]
    async fn std_info_missing_pkey_returns_4xx() {
        let sink = Arc::new(MockSink::new());
        let app = build_router(test_state(sink.clone(), HashMap::new(), std_protocol()));
        let resp = app
            .oneshot(post("/access/std/info", r#"{"sn":"s1"}"#))
            .await
            .unwrap();
        assert!(resp.status().is_client_error());
        assert!(sink.is_empty());
    }

    #[tokio::test]
    async fn std_rtg_multiple_devs_store_multiple_messages() {
        let sink = Arc::new(MockSink::new());
        let app = build_router(test_state(sink.clone(), HashMap::new(), std_protocol()));
        let body = r#"{"devs":[
            {"dev":"d1","ts":1700000000000,"d":[{"m":"a","v":1}]},
            {"dev":"d2","ts":1700000000000,"d":[{"m":"b","v":2}]}]}"#;
        let resp = app.oneshot(post("/access/std/rtg", body)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(sink.len(), 2);
    }

    #[tokio::test]
    async fn std_malformed_body_returns_400() {
        let sink = Arc::new(MockSink::new());
        let app = build_router(test_state(sink.clone(), HashMap::new(), std_protocol()));
        let resp = app
            .oneshot(post("/access/std/rtg", "not json"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert!(sink.is_empty());
        // std 路径同样只回通用消息，不回显内部详情
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: JsonValue = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["message"], "请求报文解码失败");
    }

    /// 未知 std 子路径不匹配任何路由 → axum 默认 404（对齐 Java 仅注册四个 @PostMapping）。
    #[tokio::test]
    async fn std_unknown_subpath_returns_404() {
        let sink = Arc::new(MockSink::new());
        let app = build_router(test_state(sink.clone(), HashMap::new(), std_protocol()));
        let resp = app.oneshot(post("/access/std/nope", "{}")).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // ---- 响应信封 ----

    #[tokio::test]
    async fn success_response_envelope_aligns_java() {
        let sink = Arc::new(MockSink::new());
        let app = build_router(test_state(sink.clone(), HashMap::new(), std_protocol()));
        let body = r#"{"devs":[{"dev":"d1","ts":1700000000000,"d":[{"m":"a","v":1}]}]}"#;
        let resp = app.oneshot(post("/access/std/rtg", body)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: JsonValue = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["success"], true);
        assert_eq!(v["data"], "OK");
    }
}
