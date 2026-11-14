//! request connector（票 09）：第三方云平台 HTTP 拉取 + Kafka 下控。
//!
//! 行为基准：Java `RequestConnectorStater`（启动加载任务列表 :59-83，Kafka 云任务通道
//! `iot_gateway_trdPlatform_data` :99-113）+ `TrdPlatformCloudJob`（限速 :67-70、AUTH
//! 刷新 :73-75、拉取→decodeMulti→storeMessage :125-131）+ `DownRequestHandler`
//! （`device_command_topic` 下控 :60-143）。
//!
//! 与 Java 的刻意差异（详见各模块头注）：
//! - cron 表达式调度替代 xxl-job；
//! - 请求定义（URL/method/headers/body/limit/AUTH）进 TOML 配置（Java 存 DB）；
//! - Kafka ADD/UPDATE 消息须内嵌 `task` 完整定义（无 DB）；
//! - 缺失 URL 占位符 / 非法 frequency → Err（Java 静默）；
//! - token 进程内缓存（Java 存 Redis）；JSON Pointer 替代 JSONPath。
//!
//! 上行链路：HTTP 响应 JSON（合并响应头）→ `Protocol::decode_multi` → `DeviceSink`；
//! 下行链路：OperationRequest → `Protocol::encode` → URL 模板替换 → HTTP。

mod auth;
mod config;
mod kafka;
mod manager;
mod task;
mod url;

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use connector_common::sink::DeviceSink;
use connector_common::{Connector, KafkaConfig, Result, StatusMap, async_trait};
use serde_json::{Value as JsonValue, json};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::info;

pub use config::{AuthConfig, RequestConfig, RequestTaskConfig};

use crate::manager::TaskManager;
use crate::task::Counters;

/// request connector：TOML 任务 + Kafka 动态任务共用一个 [`TaskManager`]。
pub struct RequestConnector {
    config: RequestConfig,
    /// None = 不启 Kafka 通道（测试 / 仅按配置轮询的部署）。
    kafka: Option<KafkaConfig>,
    manager: Arc<TaskManager>,
    counters: Arc<Counters>,
    shutdown: watch::Sender<bool>,
    handles: Mutex<Vec<JoinHandle<()>>>,
}

// `TaskManager` / `JoinHandle` / `watch::Sender` 无 Debug，手写只暴露配置面。
impl std::fmt::Debug for RequestConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestConnector")
            .field("config", &self.config)
            .field("kafka", &self.kafka)
            .finish_non_exhaustive()
    }
}

impl RequestConnector {
    /// 构建并校验配置（URL/method/cron/limit/auth/任务键查重），失败即启动报错。
    /// `script_dir` 为 Some 时任务注册加载协议脚本做白名单校验。
    pub fn new(
        config: RequestConfig,
        kafka: Option<KafkaConfig>,
        script_dir: Option<&str>,
    ) -> Result<Self> {
        config.validate()?;
        let counters = Arc::new(Counters::default());
        let (shutdown, _) = watch::channel(false);
        Ok(Self {
            config,
            kafka,
            manager: Arc::new(TaskManager::new(
                counters.clone(),
                shutdown.clone(),
                script_dir.map(String::from),
            )),
            counters,
            shutdown,
            handles: Mutex::new(Vec::new()),
        })
    }

    /// 从 `GatewayConfig.request`（一期为 JsonValue 兜底）构建；kafka 传 `GatewayConfig.kafka`。
    pub fn from_value(
        value: &JsonValue,
        kafka: Option<KafkaConfig>,
        script_dir: Option<&str>,
    ) -> Result<Self> {
        Self::new(RequestConfig::from_value(value)?, kafka, script_dir)
    }

    fn lock_handles(&self) -> std::sync::MutexGuard<'_, Vec<JoinHandle<()>>> {
        self.handles.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[async_trait]
impl Connector for RequestConnector {
    async fn start(&self, sink: Arc<dyn DeviceSink>) -> Result<()> {
        // reqwest::Client 须在 runtime 上下文构建（start 为 async，由调用方保证）。
        let client = reqwest::Client::new();
        self.manager.set_runtime(sink, client);
        for task in &self.config.configuration {
            if let Err(e) = self.manager.register_config_task(task) {
                // 中途注册失败：停掉已 spawn 的调度循环再报错（状态干净、失败可见）。
                let _ = self.shutdown.send(true);
                self.manager.stop_all().await;
                return Err(e);
            }
        }
        if let Some(kafka) = &self.kafka {
            let mut handles = self.lock_handles();
            handles.push(tokio::spawn(kafka::run_cloud_consumer(
                kafka.clone(),
                self.manager.clone(),
                self.counters.clone(),
                self.shutdown.subscribe(),
            )));
            handles.push(tokio::spawn(kafka::run_command_consumer(
                kafka.clone(),
                self.manager.clone(),
                self.counters.clone(),
                self.shutdown.subscribe(),
            )));
            info!(
                target: "request",
                "Kafka 通道已启动: 云任务 '{}' / 下控 '{}'",
                kafka.cloud_topic,
                kafka.command_topic
            );
        }
        info!(
            target: "request",
            "已启动: 配置任务 {} 个，Kafka 通道 {}",
            self.manager.task_count(),
            if self.kafka.is_some() { "开" } else { "关" }
        );
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        // 无接收者（未 start）时 send 返回 Err，此处无需处理。
        let _ = self.shutdown.send(true);
        self.manager.stop_all().await;
        let handles = std::mem::take(&mut *self.lock_handles());
        for handle in handles {
            // 循环内部不 panic；JoinError 只可能是外部 abort，同样视为已停止。
            let _ = handle.await;
        }
        info!(target: "request", "已停止");
        Ok(())
    }

    fn status(&self) -> StatusMap {
        let c = &self.counters;
        let mut status = StatusMap::new();
        status.insert("connector".into(), json!("request"));
        status.insert("tasks".into(), json!(self.manager.task_count()));
        status.insert("taskKeys".into(), json!(self.manager.task_keys()));
        status.insert(
            "kafkaChannels".into(),
            json!(if self.kafka.is_some() { 2 } else { 0 }),
        );
        status.insert(
            "roundsOk".into(),
            json!(c.rounds_ok.load(Ordering::Relaxed)),
        );
        status.insert(
            "roundsFailed".into(),
            json!(c.rounds_failed.load(Ordering::Relaxed)),
        );
        status.insert(
            "messagesStored".into(),
            json!(c.messages.load(Ordering::Relaxed)),
        );
        status.insert("downOk".into(), json!(c.down_ok.load(Ordering::Relaxed)));
        status.insert(
            "downFailed".into(),
            json!(c.down_failed.load(Ordering::Relaxed)),
        );
        status.insert(
            "tokenRefreshed".into(),
            json!(c.token_ok.load(Ordering::Relaxed)),
        );
        status.insert(
            "tokenRefreshFailed".into(),
            json!(c.token_failed.load(Ordering::Relaxed)),
        );
        status.insert(
            "cloudAccepted".into(),
            json!(c.cloud_ok.load(Ordering::Relaxed)),
        );
        status.insert(
            "cloudRejected".into(),
            json!(c.cloud_rejected.load(Ordering::Relaxed)),
        );
        status
    }
}

/// 测试基建：本地 axum 假 HTTP server（Seam C：真实 reqwest 走 loopback，不用 mock 框架）。
#[cfg(test)]
mod testutil {
    use std::collections::{HashMap, VecDeque};
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use axum::Router;
    use axum::body::Body;
    use axum::extract::{Request, State};
    use axum::response::Response;

    /// 记录到的请求。
    #[derive(Debug, Clone)]
    pub struct RecordedReq {
        pub method: String,
        /// path + query（如 "/cmd?dev=dev+1"）。
        pub target: String,
        /// 头名经 http crate 归一为小写。
        pub headers: Vec<(String, String)>,
        pub body: String,
    }

    /// (method, path) → 响应队列；只剩最后一个时重复返回（便于断言"只刷新一次"）。
    type Routes = HashMap<(String, String), VecDeque<(u16, String)>>;

    #[derive(Clone)]
    struct FakeState {
        routes: Arc<Mutex<Routes>>,
        recorded: Arc<Mutex<Vec<RecordedReq>>>,
    }

    pub struct FakeHttp {
        pub addr: SocketAddr,
        state: FakeState,
    }

    impl FakeHttp {
        /// 为 (method, path) 追加一个响应；同键多次调用形成先后队列。
        pub fn route(&self, method: &str, path: &str, status: u16, body: &str) {
            self.state
                .routes
                .lock()
                .unwrap()
                .entry((method.to_string(), path.to_string()))
                .or_default()
                .push_back((status, body.to_string()));
        }

        pub fn requests(&self) -> Vec<RecordedReq> {
            self.state.recorded.lock().unwrap().clone()
        }

        pub fn url(&self, path: &str) -> String {
            format!("http://{}{}", self.addr, path)
        }
    }

    pub async fn spawn_fake_http() -> FakeHttp {
        let state = FakeState {
            routes: Arc::new(Mutex::new(HashMap::new())),
            recorded: Arc::new(Mutex::new(Vec::new())),
        };
        let app = Router::new().fallback(handle).with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        FakeHttp { addr, state }
    }

    async fn handle(State(state): State<FakeState>, req: Request) -> Response {
        let method = req.method().to_string();
        let path = req.uri().path().to_string();
        let target = match req.uri().query() {
            Some(q) => format!("{path}?{q}"),
            None => path.clone(),
        };
        let headers = req
            .headers()
            .iter()
            .map(|(k, v)| {
                (
                    k.as_str().to_string(),
                    v.to_str().unwrap_or_default().to_string(),
                )
            })
            .collect();
        let body = axum::body::to_bytes(req.into_body(), usize::MAX)
            .await
            .map(|b| String::from_utf8_lossy(&b).into_owned())
            .unwrap_or_default();
        state.recorded.lock().unwrap().push(RecordedReq {
            method: method.clone(),
            target,
            headers,
            body,
        });
        let next = state
            .routes
            .lock()
            .unwrap()
            .get_mut(&(method, path))
            .map(|q| {
                if q.len() > 1 {
                    q.pop_front().unwrap()
                } else {
                    q.front().unwrap().clone()
                }
            });
        match next {
            Some((status, body)) => Response::builder()
                .status(status)
                .header("content-type", "application/json")
                .header("x-fake", "yes")
                .body(Body::from(body))
                .unwrap(),
            None => Response::builder()
                .status(404)
                .body(Body::from(r#"{"error":"no route"}"#))
                .unwrap(),
        }
    }

    /// 有界等待，避免固定 sleep 造成 flaky（同 connector-snmp）。
    pub async fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !cond() {
            assert!(tokio::time::Instant::now() < deadline, "等待超时: {what}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    pub fn lua_path() -> String {
        format!("{}/protocols/cloud_pull.lua", env!("CARGO_MANIFEST_DIR"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RequestTaskConfig;
    use crate::kafka::{apply_cloud_message, handle_command};
    use crate::manager::{OP_ADD, OP_REMOVE, OP_UPDATE};
    use crate::task::{TaskRunner, response_to_json};
    use crate::testutil::{FakeHttp, lua_path, spawn_fake_http, wait_until};
    use connector_common::sink::MockSink;
    use std::net::SocketAddr;
    use std::time::Duration;

    const NOWHERE: &str = "127.0.0.1:1";

    /// 任务定义 JSON（走真实 serde 解析路径）；extra 浅合并覆盖顶层字段。
    fn task_json(addr: SocketAddr, server: &str, extra: JsonValue) -> JsonValue {
        let mut task = json!({
            "appid": "cloud-a",
            "server": server,
            "url": format!("http://{addr}/api"),
            "protocol": {
                "name": "cloud_pull",
                "path": lua_path(),
                "type": "script",
                "params": {"deviceId": "dev-1", "sn": "SN001"},
            },
        });
        task.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        task
    }

    fn task_def(addr: SocketAddr, server: &str, extra: JsonValue) -> RequestTaskConfig {
        serde_json::from_value(task_json(addr, server, extra)).unwrap()
    }

    /// 指向假 server 指定路径的 runner（无 cron，纯手动触发）。
    fn runner_for(http: &FakeHttp, path: &str, server: &str, extra: JsonValue) -> Arc<TaskRunner> {
        let mut extra = extra;
        extra["url"] = json!(http.url(path));
        let def = task_def(http.addr, server, extra);
        TaskRunner::new(
            "cloud-a",
            server,
            None,
            &def,
            reqwest::Client::new(),
            Arc::new(Counters::default()),
            None,
        )
        .unwrap()
    }

    fn auth_extra(token_url: &str) -> JsonValue {
        json!({
            "auth": {
                "url": token_url,
                "method": "POST",
                "body": {"user": "u"},
                "tokenPointer": "/data/token",
                "expiresPointer": "/data/expires",
                "prefix": "Bearer ",
            }
        })
    }

    /// 已注入 runtime 的 manager（可注册任务并真实调度）。
    fn started_manager() -> TaskManager {
        let manager = bare_manager();
        manager.set_runtime(Arc::new(MockSink::new()), reqwest::Client::new());
        manager
    }

    fn bare_manager() -> TaskManager {
        let (shutdown, _rx) = watch::channel(false);
        TaskManager::new(Arc::new(Counters::default()), shutdown, None)
    }

    // ---- 上行：HTTP 响应 → Message ----

    /// 一轮拉取：响应 data 数组 → decode_multi → 单条 DEVICE_REPORT_REQ 落 sink；
    /// POST + body + 自定义头按配置发出；空 data 不产消息但轮次成功。
    #[tokio::test]
    async fn poll_once_stores_decoded_messages() {
        let http = spawn_fake_http().await;
        http.route(
            "GET",
            "/api",
            200,
            r#"{"data":[{"code":"temp","value":21.5,"ts":1700000000000},{"code":"humi","value":60,"ts":1700000000000}]}"#,
        );
        let runner = runner_for(&http, "/api", "pull", json!({}));
        let sink = MockSink::new();
        runner.poll_once(&sink).await.unwrap();

        let messages = sink.messages();
        assert_eq!(messages.len(), 1);
        let msg = &messages[0];
        assert_eq!(msg["messageType"], "DEVICE_REPORT_REQ");
        assert_eq!(msg["deviceId"], "dev-1");
        assert_eq!(msg["sn"], "SN001");
        let metrics = msg["metrics"].as_array().unwrap();
        assert_eq!(metrics.len(), 2);
        assert_eq!(metrics[0]["code"], "temp");
        assert_eq!(metrics[0]["value"], 21.5);
        assert_eq!(metrics[0]["ts"], 1700000000000u64);
        assert_eq!(metrics[1]["code"], "humi");
        assert_eq!(runner.counters.messages.load(Ordering::Relaxed), 1);

        let reqs = http.requests();
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0].method, "GET");
        assert_eq!(reqs[0].target, "/api");

        // 空 data → 不产消息、轮次成功
        http.route("GET", "/empty", 200, r#"{"data":[]}"#);
        runner_for(&http, "/empty", "pull2", json!({}))
            .poll_once(&sink)
            .await
            .unwrap();
        assert_eq!(sink.messages().len(), 1, "空 data 不应产生消息");

        // POST + JSON body + 自定义头
        http.route("POST", "/api2", 200, r#"{"data":[]}"#);
        runner_for(
            &http,
            "/api2",
            "pull3",
            json!({"method": "POST", "body": {"pageSize": 100}, "headers": {"X-App": "gw"}}),
        )
        .poll_once(&MockSink::new())
        .await
        .unwrap();
        let posted = http
            .requests()
            .into_iter()
            .find(|r| r.target == "/api2")
            .unwrap();
        assert_eq!(posted.method, "POST");
        assert_eq!(
            serde_json::from_str::<JsonValue>(&posted.body).unwrap(),
            json!({"pageSize": 100})
        );
        assert!(
            posted
                .headers
                .iter()
                .any(|(k, v)| k == "x-app" && v == "gw")
        );
        assert!(
            posted
                .headers
                .iter()
                .any(|(k, v)| k == "content-type" && v.contains("application/json"))
        );
    }

    /// 响应头合并进 "headers" 字段（对齐 Java sendRequest:104-110）；
    /// 数组 / 非 JSON 响应 → Err（失败可见）。
    #[tokio::test]
    async fn response_headers_merged_into_json() {
        let http = spawn_fake_http().await;
        let client = reqwest::Client::new();

        http.route("GET", "/api", 200, r#"{"data":[]}"#);
        let resp = client.get(http.url("/api")).send().await.unwrap();
        let obj = response_to_json("k", resp).await.unwrap();
        assert_eq!(obj["data"], json!([]));
        assert_eq!(obj["headers"]["x-fake"], "yes");
        assert!(
            obj["headers"]["content-type"]
                .as_str()
                .unwrap()
                .contains("application/json")
        );

        http.route("GET", "/arr", 200, "[]");
        let resp = client.get(http.url("/arr")).send().await.unwrap();
        let err = response_to_json("k", resp).await.unwrap_err();
        assert!(err.to_string().contains("不是 JSON 对象"), "{err}");

        http.route("GET", "/txt", 200, "oops");
        let resp = client.get(http.url("/txt")).send().await.unwrap();
        let err = response_to_json("k", resp).await.unwrap_err();
        assert!(err.to_string().contains("JSON"), "{err}");
    }

    /// HTTP 非 2xx / 响应非 JSON → poll_once Err，不落 sink。
    #[tokio::test]
    async fn http_and_json_failures_are_visible() {
        let http = spawn_fake_http().await;
        let sink = MockSink::new();

        http.route("GET", "/err", 500, r#"{"error":"boom"}"#);
        let err = runner_for(&http, "/err", "t1", json!({}))
            .poll_once(&sink)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("500"), "{err}");

        http.route("GET", "/txt", 200, "not json");
        let err = runner_for(&http, "/txt", "t2", json!({}))
            .poll_once(&sink)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("JSON"), "{err}");

        assert!(sink.is_empty());
    }

    // ---- AUTH token 缓存与刷新 ----

    /// 缓存有效期内只打一次 AUTH 接口，数据请求带注入头。
    #[tokio::test]
    async fn auth_token_cached_and_injected() {
        let http = spawn_fake_http().await;
        http.route(
            "POST",
            "/token",
            200,
            r#"{"data":{"token":"t1","expires":600}}"#,
        );
        http.route("GET", "/api", 200, r#"{"data":[]}"#);
        let runner = runner_for(&http, "/api", "pull", auth_extra(&http.url("/token")));
        let sink = MockSink::new();
        runner.poll_once(&sink).await.unwrap();
        runner.poll_once(&sink).await.unwrap();

        let reqs = http.requests();
        assert_eq!(
            reqs.iter().filter(|r| r.target == "/token").count(),
            1,
            "缓存有效期内 AUTH 只应请求一次"
        );
        let api_reqs: Vec<_> = reqs.iter().filter(|r| r.target == "/api").collect();
        assert_eq!(api_reqs.len(), 2);
        for r in api_reqs {
            assert!(
                r.headers
                    .iter()
                    .any(|(k, v)| k == "authorization" && v == "Bearer t1"),
                "{:?}",
                r.headers
            );
        }
        assert_eq!(runner.counters.token_ok.load(Ordering::Relaxed), 1);
        assert_eq!(runner.counters.token_failed.load(Ordering::Relaxed), 0);
    }

    /// 过期后自动刷新（paused clock 确定性推进，expires=10s）。
    #[tokio::test(start_paused = true)]
    async fn auth_token_refreshed_after_expiry() {
        let http = spawn_fake_http().await;
        http.route(
            "POST",
            "/token",
            200,
            r#"{"data":{"token":"t1","expires":10}}"#,
        );
        http.route(
            "POST",
            "/token",
            200,
            r#"{"data":{"token":"t2","expires":10}}"#,
        );
        http.route("GET", "/api", 200, r#"{"data":[]}"#);
        let runner = runner_for(&http, "/api", "pull", auth_extra(&http.url("/token")));
        let sink = MockSink::new();

        runner.poll_once(&sink).await.unwrap();
        tokio::time::advance(Duration::from_secs(11)).await;
        runner.poll_once(&sink).await.unwrap();

        let auth_values: Vec<String> = http
            .requests()
            .into_iter()
            .filter(|r| r.target == "/api")
            .map(|r| {
                r.headers
                    .iter()
                    .find(|(k, _)| k == "authorization")
                    .map(|(_, v)| v.clone())
                    .unwrap_or_default()
            })
            .collect();
        assert_eq!(auth_values, vec!["Bearer t1", "Bearer t2"]);
        assert_eq!(runner.counters.token_ok.load(Ordering::Relaxed), 2);
    }

    /// 刷新失败（HTTP 500 / tokenPointer 无值）→ Err 可见，不发数据请求，不静默用旧 token。
    #[tokio::test]
    async fn auth_refresh_failure_is_visible() {
        let http = spawn_fake_http().await;
        http.route("POST", "/token-500", 500, r#"{"error":"denied"}"#);
        http.route("POST", "/token-empty", 200, r#"{"data":{}}"#);
        http.route("GET", "/api", 200, r#"{"data":[]}"#);
        let sink = MockSink::new();

        let runner = runner_for(&http, "/api", "t1", auth_extra(&http.url("/token-500")));
        let err = runner.poll_once(&sink).await.unwrap_err();
        assert!(err.to_string().contains("auth"), "{err}");
        assert_eq!(runner.counters.token_failed.load(Ordering::Relaxed), 1);

        let runner2 = runner_for(&http, "/api", "t2", auth_extra(&http.url("/token-empty")));
        let err = runner2.poll_once(&sink).await.unwrap_err();
        assert!(err.to_string().contains("无值"), "{err}");

        assert!(
            http.requests().iter().all(|r| r.target != "/api"),
            "token 获取失败时不应发数据请求"
        );
        assert!(sink.is_empty());
    }

    // ---- 下行：OperationRequest → encode → URL 替换 → HTTP ----

    /// 下控全链路：encode 包装 param → `${deviceId}` 替换（URL 编码）→ body=encoded.param。
    #[tokio::test]
    async fn downlink_full_chain() {
        let http = spawn_fake_http().await;
        http.route("POST", "/cmd", 200, r#"{"result":"ok"}"#);
        let runner = runner_for(
            &http,
            "/cmd?dev=${deviceId}",
            "pull",
            json!({"method": "POST"}),
        );
        let request = json!({
            "messageType": "CLOUD_OPERATION_REQ",
            "deviceId": "dev 1",
            "sn": "SN001",
            "function": "pull",
            "timeStamp": 1700000000000i64,
            "param": {"switch": "on"},
        });
        runner.send_command(&request).await.unwrap();

        let reqs = http.requests();
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0].method, "POST");
        assert_eq!(reqs[0].target, "/cmd?dev=dev+1");
        assert_eq!(
            serde_json::from_str::<JsonValue>(&reqs[0].body).unwrap(),
            json!({"device": "dev 1", "cmd": {"switch": "on"}})
        );
        assert_eq!(runner.counters.down_ok.load(Ordering::Relaxed), 1);
        assert_eq!(runner.counters.down_failed.load(Ordering::Relaxed), 0);
    }

    /// 下控失败路径：占位符缺值不发 HTTP、非 2xx → Err；down_failed 计数可见。
    #[tokio::test]
    async fn downlink_failure_paths_are_visible() {
        let http = spawn_fake_http().await;
        http.route("POST", "/cmd", 200, "{}");
        http.route("POST", "/fail", 500, "{}");
        let request = json!({
            "messageType": "CLOUD_OPERATION_REQ",
            "deviceId": "d1",
            "function": "pull",
            "timeStamp": 0,
            "param": {},
        });

        let runner = runner_for(&http, "/cmd?sn=${nope}", "t1", json!({"method": "POST"}));
        let err = runner.send_command(&request).await.unwrap_err();
        assert!(err.to_string().contains("${nope}"), "{err}");
        assert!(http.requests().is_empty(), "占位符缺值时不应发出 HTTP 请求");
        assert_eq!(runner.counters.down_failed.load(Ordering::Relaxed), 1);
        assert_eq!(runner.counters.down_ok.load(Ordering::Relaxed), 0);

        let runner2 = runner_for(&http, "/fail", "t2", json!({"method": "POST"}));
        let err = runner2.send_command(&request).await.unwrap_err();
        assert!(err.to_string().contains("500"), "{err}");
        assert_eq!(runner2.counters.down_failed.load(Ordering::Relaxed), 1);
    }

    /// handle_command 路由：function 大小写不敏感匹配任务；非 CLOUD_OPERATION_REQ 跳过；
    /// 缺 messageType/function、无匹配任务、非法 JSON → Err。
    #[tokio::test]
    async fn command_routing_and_validation() {
        let http = spawn_fake_http().await;
        http.route("POST", "/cmd", 200, "{}");
        let manager = started_manager();
        // 无 cron 的仅下控任务（配置注册路径）
        let def = task_def(
            http.addr,
            "pull",
            json!({"method": "POST", "url": http.url("/cmd")}),
        );
        manager.register_config_task(&def).unwrap();

        let request = json!({
            "messageType": "CLOUD_OPERATION_REQ",
            "deviceId": "d1",
            "function": "PULL",
            "timeStamp": 0,
            "param": {},
        });
        handle_command(&serde_json::to_vec(&request).unwrap(), &manager)
            .await
            .unwrap();
        assert_eq!(http.requests().len(), 1);

        // 非 CLOUD_OPERATION_REQ → 跳过（Ok，不触发 HTTP）
        handle_command(br#"{"messageType":"DEVICE_REPORT_REQ"}"#, &manager)
            .await
            .unwrap();
        // 缺 messageType → Err
        let err = handle_command(br#"{"deviceId":"d"}"#, &manager)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("messageType"), "{err}");
        // 缺 function → Err
        let err = handle_command(br#"{"messageType":"CLOUD_OPERATION_REQ"}"#, &manager)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("function"), "{err}");
        // function 无匹配任务 → Err
        let err = handle_command(
            br#"{"messageType":"CLOUD_OPERATION_REQ","function":"nope"}"#,
            &manager,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("没有找到"), "{err}");
        // 非法 JSON → Err
        assert!(handle_command(b"not json", &manager).await.is_err());

        assert_eq!(http.requests().len(), 1, "无效消息不应触发 HTTP");
        manager.stop_all().await;
    }

    // ---- 云任务消息：增删改状态机 ----

    /// ADD 幂等保留 / UPDATE 替换（frequency 覆盖 cron）/ REMOVE 删除且可重复。
    #[tokio::test]
    async fn task_lifecycle_add_update_remove() {
        let manager = started_manager();
        let addr: SocketAddr = NOWHERE.parse().unwrap();
        let msg = |operate: i32, task: Option<JsonValue>, frequency: Option<&str>| {
            let mut m = json!({"pcode": "cloud-a", "taskCode": "pull", "operate": operate});
            if let Some(t) = task {
                m["task"] = t;
            }
            if let Some(f) = frequency {
                m["frequency"] = json!(f);
            }
            serde_json::to_vec(&m).unwrap()
        };

        // ADD
        apply_cloud_message(
            &msg(
                OP_ADD,
                Some(task_json(addr, "pull", json!({"cron": "* * * * * *"}))),
                None,
            ),
            &manager,
        )
        .unwrap();
        assert_eq!(manager.task_keys(), vec!["cloud-a-pull".to_string()]);
        let r1 = manager.find_by_function("pull").unwrap();
        assert_eq!(r1.cron_expr.as_deref(), Some("* * * * * *"));

        // ADD 已存在 → 保留原 runner（对齐 Java operateTaskWork:590-596）
        apply_cloud_message(
            &msg(
                OP_ADD,
                Some(task_json(addr, "pull", json!({"cron": "0 0 0 * * *"}))),
                None,
            ),
            &manager,
        )
        .unwrap();
        let r2 = manager.find_by_function("pull").unwrap();
        assert!(Arc::ptr_eq(&r1, &r2), "ADD 已存在任务应保留原 runner");
        assert_eq!(manager.task_count(), 1);

        // UPDATE + frequency 覆盖任务 cron
        apply_cloud_message(
            &msg(
                OP_UPDATE,
                Some(task_json(addr, "pull", json!({"cron": "* * * * * *"}))),
                Some("0 */5 * * * *"),
            ),
            &manager,
        )
        .unwrap();
        let r3 = manager.find_by_function("pull").unwrap();
        assert!(!Arc::ptr_eq(&r1, &r3), "UPDATE 应替换 runner");
        assert_eq!(r3.cron_expr.as_deref(), Some("0 */5 * * * *"));
        assert_eq!(manager.task_count(), 1);

        // REMOVE，重复 REMOVE 幂等（warn 不报错）
        apply_cloud_message(&msg(OP_REMOVE, None, None), &manager).unwrap();
        assert_eq!(manager.task_count(), 0);
        apply_cloud_message(&msg(OP_REMOVE, None, None), &manager).unwrap();
        manager.stop_all().await;
    }

    /// 拒绝路径全部 Err 可见且不产生任务：未知 operate、ADD 缺 task、frequency 非法、
    /// frequency 与 cron 双空、缺 pcode、内嵌 task 非法。
    #[tokio::test]
    async fn cloud_message_rejections_are_visible() {
        let manager = bare_manager();
        let addr: SocketAddr = NOWHERE.parse().unwrap();
        let raw = |m: JsonValue| serde_json::to_vec(&m).unwrap();
        let valid_task = task_json(addr, "t", json!({"cron": "* * * * * *"}));

        let err = apply_cloud_message(
            &raw(json!({"pcode": "p", "taskCode": "t", "operate": 9})),
            &manager,
        )
        .unwrap_err();
        assert!(err.to_string().contains("未知 operate"), "{err}");

        let err = apply_cloud_message(
            &raw(json!({"pcode": "p", "taskCode": "t", "operate": 1, "frequency": "* * * * * *"})),
            &manager,
        )
        .unwrap_err();
        assert!(err.to_string().contains("缺少 task"), "{err}");

        let err = apply_cloud_message(
            &raw(json!({"pcode": "p", "taskCode": "t", "operate": 1, "frequency": "every 5 min", "task": valid_task})),
            &manager,
        )
        .unwrap_err();
        assert!(err.to_string().contains("cron"), "{err}");

        let err = apply_cloud_message(
            &raw(json!({"pcode": "p", "taskCode": "t", "operate": 1, "task": task_json(addr, "t", json!({}))})),
            &manager,
        )
        .unwrap_err();
        assert!(err.to_string().contains("均为空"), "{err}");

        let err = apply_cloud_message(&raw(json!({"taskCode": "t", "operate": 3})), &manager)
            .unwrap_err();
        assert!(err.to_string().contains("pcode"), "{err}");

        let err = apply_cloud_message(
            &raw(
                json!({"pcode": "p", "taskCode": "t", "operate": 1, "frequency": "* * * * * *",
                "task": task_json(addr, "t", json!({"url": "ftp://x/api"}))}),
            ),
            &manager,
        )
        .unwrap_err();
        assert!(err.to_string().contains("http"), "{err}");

        assert_eq!(manager.task_count(), 0, "被拒绝的消息不应产生任务");
    }

    /// start 前（无 runtime）注册任务 → Err（失败可见）。
    #[tokio::test]
    async fn register_before_start_fails() {
        let manager = bare_manager();
        let addr: SocketAddr = NOWHERE.parse().unwrap();
        let err = manager
            .apply_operate(
                OP_ADD,
                "p",
                "t",
                Some("* * * * * *"),
                Some(task_def(addr, "t", json!({}))),
            )
            .unwrap_err();
        assert!(err.to_string().contains("未启动"), "{err}");
    }

    // ---- connector 生命周期 ----

    /// 端到端：start → cron 每秒调度 → 上行落 sink → status 反映 → stop 后不再轮询。
    #[tokio::test]
    async fn connector_schedules_polls_and_stops() {
        let http = spawn_fake_http().await;
        http.route(
            "GET",
            "/api",
            200,
            r#"{"data":[{"code":"temp","value":1,"ts":0}]}"#,
        );
        let cfg = json!({
            "configuration": [task_json(http.addr, "pull", json!({"cron": "* * * * * *"}))]
        });
        let connector = RequestConnector::from_value(&cfg, None, None).unwrap();
        let sink = Arc::new(MockSink::new());
        connector.start(sink.clone()).await.unwrap();

        wait_until("cron 首轮上行", || !sink.is_empty()).await;
        let status = connector.status();
        assert_eq!(status["connector"], json!("request"));
        assert_eq!(status["tasks"], json!(1));
        assert_eq!(status["taskKeys"], json!(["cloud-a-pull"]));
        assert_eq!(status["kafkaChannels"], json!(0));
        assert!(status["roundsOk"].as_u64().unwrap() >= 1);
        assert!(status["messagesStored"].as_u64().unwrap() >= 1);
        assert_eq!(status["roundsFailed"], json!(0));

        connector.stop().await.unwrap();
        assert_eq!(connector.status()["tasks"], json!(0));
        let rounds = connector.status()["roundsOk"].as_u64().unwrap();
        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert_eq!(
            connector.status()["roundsOk"].as_u64().unwrap(),
            rounds,
            "stop 后不应继续轮询"
        );
        assert!(connector.lock_handles().is_empty());
    }

    /// `type = "jar"` 协议 → start 报错（对齐 common 层失败可见约定）。
    #[tokio::test]
    async fn start_rejects_jar_protocol() {
        let addr: SocketAddr = NOWHERE.parse().unwrap();
        let cfg = json!({
            "configuration": [task_json(addr, "pull", json!({
                "protocol": {"name": "p", "path": "p.lua", "type": "jar"}
            }))]
        });
        let connector = RequestConnector::from_value(&cfg, None, None).unwrap();
        let sink: Arc<dyn DeviceSink> = Arc::new(MockSink::new());
        let err = connector.start(sink).await.unwrap_err();
        assert!(
            matches!(
                err,
                connector_common::CommonError::UnsupportedProtocolType { .. }
            ),
            "{err:?}"
        );
        connector.stop().await.unwrap();
    }

    /// script_dir 白名单：任务脚本真实存在但在目录外 → start 注册任务报错（失败可见）。
    #[tokio::test]
    async fn start_rejects_script_outside_dir() {
        let dir = std::env::temp_dir().join(format!("gw-scriptdir-{}-request", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let addr: SocketAddr = NOWHERE.parse().unwrap();
        let cfg = json!({
            "configuration": [task_json(addr, "pull", json!({}))]
        });
        let connector =
            RequestConnector::from_value(&cfg, None, Some(dir.to_str().unwrap())).unwrap();
        let sink: Arc<dyn DeviceSink> = Arc::new(MockSink::new());
        let err = connector.start(sink).await.unwrap_err();
        assert!(err.to_string().contains("拒绝加载"), "{err}");
        connector.stop().await.unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }

    /// from_value 构建即校验（非法 url 直接报错）。
    #[test]
    fn from_value_rejects_bad_config() {
        let addr: SocketAddr = NOWHERE.parse().unwrap();
        let cfg = json!({
            "configuration": [task_json(addr, "pull", json!({"url": "ftp://x/api"}))]
        });
        let err = RequestConnector::from_value(&cfg, None, None).unwrap_err();
        assert!(err.to_string().contains("http"), "{err}");
    }
}
