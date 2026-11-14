//! 任务执行器：一轮上行 = 限速等待 → AUTH token 注入 → HTTP → 响应 JSON（合并响应头）
//! → Lua decode_multi → DeviceSink；下行 = Lua encode → URL 模板替换 → HTTP。
//!
//! 行为基准：TrdPlatformCloudJob.exeTask/dealCloudData（:56-133）与
//! DownRequestHandler.dealCloudData（:138-143）。
//! cron 调度替代 xxl-job（TrdPlatformCloudJob.java:38-39）。

use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use chrono::Utc;
use connector_common::protocol::Protocol;
use connector_common::sink::DeviceSink;
use connector_common::{CommonError, Result};
use cron::Schedule;
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use serde_json::{Map, Value as JsonValue};
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::auth::TokenCache;
use crate::config::RequestTaskConfig;
use crate::url::replace_url;

/// 运行计数，供 `status()` 与测试断言。
#[derive(Debug, Default)]
pub(crate) struct Counters {
    pub rounds_ok: AtomicU64,
    pub rounds_failed: AtomicU64,
    pub messages: AtomicU64,
    pub down_ok: AtomicU64,
    pub down_failed: AtomicU64,
    pub token_ok: AtomicU64,
    pub token_failed: AtomicU64,
    pub cloud_ok: AtomicU64,
    pub cloud_rejected: AtomicU64,
}

/// limit（令牌/秒）→ governor Quota。None/0 → 不限速（对齐 Java `limit==0 → bodies.size()`：
/// 单请求任务下等价于不节流）。小数速率用 with_period 表达。
pub(crate) fn build_quota(limit: Option<f64>) -> Option<Quota> {
    let limit = limit.filter(|l| l.is_finite() && *l > 0.0)?;
    if limit >= 1.0 {
        let n = NonZeroU32::new(limit.round() as u32).or(NonZeroU32::new(1))?;
        Some(Quota::per_second(n))
    } else {
        // 0 < limit < 1：每 1/limit 秒 1 个令牌
        let period = Duration::from_secs_f64(1.0 / limit);
        Some(Quota::with_period(period)?.allow_burst(NonZeroU32::new(1)?))
    }
}

/// 响应 JSON 整理：文本 → JSON 对象，并把响应头合并进 "headers" 字段
/// （对齐 Java `TrdPlatformCloudServerImpl.sendRequest:104-110`，Lua 脚本可读响应头）。
/// 非 JSON / 非对象（数组、标量）→ Err（Java bodyAsJson(JSONObject.class) 同样抛异常）。
pub(crate) async fn response_to_json(key: &str, resp: reqwest::Response) -> Result<JsonValue> {
    let headers = resp.headers().clone();
    let text = resp
        .text()
        .await
        .map_err(|e| CommonError::Internal(format!("request 任务 {key} 响应读取失败: {e}")))?;
    let mut obj: JsonValue = serde_json::from_str(&text)
        .map_err(|e| CommonError::Internal(format!("request 任务 {key} 响应非合法 JSON: {e}")))?;
    let map = obj
        .as_object_mut()
        .ok_or_else(|| CommonError::Internal(format!("request 任务 {key} 响应不是 JSON 对象")))?;
    let mut hs = Map::new();
    for (name, value) in headers.iter() {
        hs.insert(
            name.as_str().to_string(),
            JsonValue::String(String::from_utf8_lossy(value.as_bytes()).into_owned()),
        );
    }
    map.insert("headers".into(), JsonValue::Object(hs));
    Ok(obj)
}

/// 单个任务的执行器：持有请求定义 + 协议 + 限速器 + AUTH 缓存。
/// 上行轮询与下行下控共用（Java 里分别是 TrdPlatformCloudJob 与 DownRequestHandler）。
pub(crate) struct TaskRunner {
    /// pCode-taskCode，对齐 Java jobKey。
    pub key: String,
    pub task_code: String,
    /// 生效的 cron 表达式（None = 仅下控任务），供 status/测试断言。
    pub cron_expr: Option<String>,
    client: reqwest::Client,
    method: reqwest::Method,
    url: String,
    headers: Vec<(String, String)>,
    body: Option<JsonValue>,
    proto: Arc<dyn Protocol>,
    params: JsonValue,
    limiter: Option<DefaultDirectRateLimiter>,
    auth: Option<Arc<TokenCache>>,
    timeout: Duration,
    pub counters: Arc<Counters>,
}

impl TaskRunner {
    /// 构建执行器：加载 Lua 协议（type=jar → Err；`script_dir` 为 Some 时做白名单校验）、
    /// 解析 method、建限速器与 AUTH 缓存。
    pub(crate) fn new(
        pcode: &str,
        task_code: &str,
        cron_expr: Option<String>,
        def: &RequestTaskConfig,
        client: reqwest::Client,
        counters: Arc<Counters>,
        script_dir: Option<&str>,
    ) -> Result<Arc<Self>> {
        let registry = connector_common::protocol::ProtocolRegistry::from_protocols_in(
            &[&def.protocol],
            script_dir,
        )?;
        let proto = registry.get(&def.protocol.name)?;
        let method = def
            .method
            .parse::<reqwest::Method>()
            .map_err(|e| CommonError::Config(format!("method '{}' 非法: {e}", def.method)))?;
        // ponytail: 进程内持续令牌桶。Java 每次执行 new RateLimiter（只有首轮 burst 语义），
        // Rust 版跨轮持续限速更贴近第三方平台的配额约束；升级路径：需要 Java 语义时按轮重建。
        let limiter = build_quota(def.limit).map(RateLimiter::direct);
        let auth = def
            .auth
            .clone()
            .map(|cfg| Arc::new(TokenCache::new(cfg, client.clone(), counters.clone())));
        Ok(Arc::new(Self {
            key: format!("{pcode}-{task_code}"),
            task_code: task_code.to_string(),
            cron_expr,
            client,
            method,
            url: def.url.clone(),
            headers: def
                .headers
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            body: def.body.clone(),
            proto,
            params: def.protocol.params.clone(),
            limiter,
            auth,
            timeout: Duration::from_millis(def.timeout_ms),
            counters,
        }))
    }

    /// 上行一轮（对齐 TrdPlatformCloudJob.dealCloudData:88-133）：
    /// HTTP → 响应 JSON → decode_multi → sink。任一步失败 → Err（调用方计数打日志）。
    pub(crate) async fn poll_once(&self, sink: &dyn DeviceSink) -> Result<()> {
        if let Some(limiter) = &self.limiter {
            limiter.until_ready().await;
        }
        let resp = self.send(&self.url, self.body.as_ref()).await?;
        let obj = response_to_json(&self.key, resp).await?;
        let payload = serde_json::to_vec(&obj)?;
        let messages = self.proto.decode_multi(&payload, &self.params)?;
        for message in messages {
            sink.store_message(message).await?;
            self.counters.messages.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    /// 下行下控（对齐 DownRequestHandler:95-143）：
    /// encode → 重解析为 JSON → URL 模板替换 → body=encoded.param → HTTP。
    pub(crate) async fn send_command(&self, request: &JsonValue) -> Result<()> {
        let result = self.send_command_inner(request).await;
        match &result {
            Ok(()) => {
                self.counters.down_ok.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.counters.down_failed.fetch_add(1, Ordering::Relaxed);
            }
        }
        result
    }

    async fn send_command_inner(&self, request: &JsonValue) -> Result<()> {
        if let Some(limiter) = &self.limiter {
            limiter.until_ready().await;
        }
        let encoded_bytes = self.proto.encode(request, &self.params)?;
        // Java :100 把 encode 字节重解析为 OperationRequest；这里要求 encode 输出 JSON 对象
        let encoded: JsonValue = serde_json::from_slice(&encoded_bytes).map_err(|e| {
            CommonError::Internal(format!(
                "request 任务 {} encode 输出不是合法 JSON: {e}",
                self.key
            ))
        })?;
        let encoded_obj = encoded.as_object().ok_or_else(|| {
            CommonError::Internal(format!(
                "request 任务 {} encode 输出不是 JSON 对象",
                self.key
            ))
        })?;
        let url = replace_url(&self.url, &encoded)?;
        // Java :141 body = operationRequest.getParam()
        let body = encoded_obj.get("param").filter(|v| !v.is_null());
        let resp = self.send(&url, body).await?;
        let text = resp.text().await.map_err(|e| {
            CommonError::Internal(format!("request 任务 {} 下控响应读取失败: {e}", self.key))
        })?;
        debug!(target: "request", "{} 下控成功: {text}", self.key);
        Ok(())
    }

    /// 组装并发送 HTTP 请求：配置 headers + AUTH token 注入 + 可选 JSON body。
    /// 非 2xx → Err（失败可见）。
    async fn send(&self, url: &str, body: Option<&JsonValue>) -> Result<reqwest::Response> {
        let mut builder = self
            .client
            .request(self.method.clone(), url)
            .timeout(self.timeout);
        for (k, v) in &self.headers {
            builder = builder.header(k.as_str(), v.as_str());
        }
        if let Some(auth) = &self.auth {
            let (name, value) = auth.header().await?;
            builder = builder.header(name.as_str(), value.as_str());
        }
        if let Some(b) = body {
            if !self
                .headers
                .iter()
                .any(|(k, _)| k.eq_ignore_ascii_case("content-type"))
            {
                builder = builder.header(reqwest::header::CONTENT_TYPE, "application/json");
            }
            builder = builder.body(serde_json::to_vec(b)?);
        }
        let resp = builder.send().await.map_err(|e| {
            CommonError::Internal(format!(
                "request 任务 {} HTTP 请求 {url} 失败: {e}",
                self.key
            ))
        })?;
        if !resp.status().is_success() {
            return Err(CommonError::Internal(format!(
                "request 任务 {} HTTP {url} 返回 {}",
                self.key,
                resp.status()
            )));
        }
        Ok(resp)
    }
}

/// cron 调度循环（替代 xxl-job）：每轮睡到下一个触发点再执行；
/// 单轮失败只打日志计数、循环继续；收到 shutdown 信号退出。
pub(crate) async fn run_cron_loop(
    runner: Arc<TaskRunner>,
    sink: Arc<dyn DeviceSink>,
    schedule: Schedule,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        let now = Utc::now();
        let Some(next) = schedule.after(&now).next() else {
            warn!(target: "request", "任务 {} 的 cron 无后续触发点，调度退出", runner.key);
            break;
        };
        let wait = (next - now).to_std().unwrap_or(Duration::ZERO);
        tokio::select! {
            _ = tokio::time::sleep(wait) => {
                match runner.poll_once(sink.as_ref()).await {
                    Ok(()) => {
                        runner.counters.rounds_ok.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        runner.counters.rounds_failed.fetch_add(1, Ordering::Relaxed);
                        warn!(target: "request", "任务 {} 轮询失败（不中断下轮）: {e}", runner.key);
                    }
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() {
                    warn!(target: "request", "shutdown 通道已关闭，退出调度 {}", runner.key);
                }
                break;
            }
        }
    }
    info!(target: "request", "任务 {} 调度退出", runner.key);
}

#[cfg(test)]
mod tests {
    use super::*;
    use governor::clock::FakeRelativeClock;

    #[test]
    fn quota_from_limit() {
        assert!(build_quota(None).is_none());
        assert!(build_quota(Some(0.0)).is_none());
        assert!(build_quota(Some(-1.0)).is_none());
        assert!(build_quota(Some(5.0)).is_some());
        assert!(build_quota(Some(0.5)).is_some());
    }

    /// 限速器行为（FakeClock 确定性验证，对齐 Java RateLimiter.acquire 阻塞语义）：
    /// 2/s → 首轮 burst 2 个立即通过，第 3 个被拒；推进 500ms 补 1 个令牌。
    #[test]
    fn limiter_throttles_to_configured_rate() {
        let clock = FakeRelativeClock::default();
        let limiter =
            RateLimiter::direct_with_clock(build_quota(Some(2.0)).unwrap(), clock.clone());
        assert!(limiter.check().is_ok());
        assert!(limiter.check().is_ok());
        assert!(limiter.check().is_err(), "burst 用尽后应被限流");
        clock.advance(Duration::from_millis(499));
        assert!(limiter.check().is_err());
        clock.advance(Duration::from_millis(2));
        assert!(limiter.check().is_ok(), "500ms 后应补 1 个令牌");
        assert!(limiter.check().is_err());
    }

    /// 小数速率：0.5/s → 每 2 秒 1 个令牌。
    #[test]
    fn limiter_handles_fractional_rate() {
        let clock = FakeRelativeClock::default();
        let limiter =
            RateLimiter::direct_with_clock(build_quota(Some(0.5)).unwrap(), clock.clone());
        assert!(limiter.check().is_ok());
        assert!(limiter.check().is_err());
        clock.advance(Duration::from_secs(1));
        assert!(limiter.check().is_err());
        clock.advance(Duration::from_secs(1));
        assert!(limiter.check().is_ok());
    }
}
