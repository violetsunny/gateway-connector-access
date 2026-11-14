//! AUTH 型 token 缓存与过期刷新，对齐 Java `TrdPlatformCloudServerImpl.authRefreshToken`
//! （TrdPlatformCloudJob.java:73-75 调用）。
//!
//! Java：token 存 Redis，TTL = 响应里的 expirationTime 秒；Rust 一期进程内缓存
//! （ponytail: 单 gateway 实例足够；升级路径：多实例共享需外置存储）。
//! 刷新失败返回 Err 由调用方打日志计数（失败可见，不静默用旧 token）。

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use connector_common::{CommonError, Result};
use serde_json::Value as JsonValue;
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::config::AuthConfig;
use crate::task::Counters;

struct Cached {
    /// 完整请求头值（prefix + token）。
    header_value: String,
    expires_at: Instant,
}

/// 进程内 token 缓存：命中且未过期直接返回，否则请求 AUTH 接口刷新。
pub(crate) struct TokenCache {
    cfg: AuthConfig,
    client: reqwest::Client,
    state: Mutex<Option<Cached>>,
    counters: Arc<Counters>,
}

impl TokenCache {
    pub(crate) fn new(cfg: AuthConfig, client: reqwest::Client, counters: Arc<Counters>) -> Self {
        Self {
            cfg,
            client,
            state: Mutex::new(None),
            counters,
        }
    }

    /// 取注入用请求头 (名, 值)。刷新失败 → Err（token_ok/token_failed 计数）。
    pub(crate) async fn header(&self) -> Result<(String, String)> {
        // ponytail: 全程持锁刷新——并发请求串行等同一个 token，避免过期瞬间的刷新风暴；
        // 升级路径：AUTH 接口本身有并发压力时改 double-check + 单独刷新锁。
        let mut guard = self.state.lock().await;
        if let Some(cached) = guard.as_ref()
            && cached.expires_at > Instant::now()
        {
            return Ok((self.cfg.header.clone(), cached.header_value.clone()));
        }
        match self.fetch().await {
            Ok(cached) => {
                self.counters.token_ok.fetch_add(1, Ordering::Relaxed);
                let out = (self.cfg.header.clone(), cached.header_value.clone());
                *guard = Some(cached);
                Ok(out)
            }
            Err(e) => {
                self.counters.token_failed.fetch_add(1, Ordering::Relaxed);
                // 旧 token 已过期，丢弃（不静默续用）
                *guard = None;
                Err(e)
            }
        }
    }

    /// 请求 AUTH 接口并按 JSON Pointer 提取 token / 过期秒数。
    async fn fetch(&self) -> Result<Cached> {
        let method = self
            .cfg
            .method
            .parse::<reqwest::Method>()
            .map_err(|e| CommonError::Internal(format!("auth method 非法: {e}")))?;
        let mut builder = self.client.request(method, &self.cfg.url);
        for (k, v) in &self.cfg.headers {
            builder = builder.header(k.as_str(), v.as_str());
        }
        if let Some(body) = &self.cfg.body {
            if !self
                .cfg
                .headers
                .keys()
                .any(|k| k.eq_ignore_ascii_case("content-type"))
            {
                builder = builder.header(reqwest::header::CONTENT_TYPE, "application/json");
            }
            builder = builder.body(serde_json::to_vec(body)?);
        }
        let resp = builder
            .send()
            .await
            .map_err(|e| CommonError::Internal(format!("auth 请求 {} 失败: {e}", self.cfg.url)))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| CommonError::Internal(format!("auth 响应读取失败: {e}")))?;
        if !status.is_success() {
            return Err(CommonError::Internal(format!(
                "auth 请求 {} 返回 HTTP {status}: {text}",
                self.cfg.url
            )));
        }

        let obj: JsonValue = serde_json::from_str(&text)
            .map_err(|e| CommonError::Internal(format!("auth 响应非合法 JSON: {e}")))?;
        let token = obj
            .pointer(&self.cfg.token_pointer)
            .and_then(|v| match v {
                JsonValue::String(s) => Some(s.clone()),
                JsonValue::Null => None,
                other => Some(other.to_string()),
            })
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                CommonError::Internal(format!(
                    "auth 响应中 tokenPointer '{}' 无值",
                    self.cfg.token_pointer
                ))
            })?;

        // 过期秒数：响应 expiresPointer 优先（对齐 Java expirationTime 来自响应），
        // 缺失/非法回退配置 expiresSeconds；0 → 立即过期（每轮刷新，同 Java TTL=0 语义）。
        let seconds = self
            .cfg
            .expires_pointer
            .as_deref()
            .and_then(|p| obj.pointer(p))
            .and_then(|v| v.as_u64().or_else(|| v.as_f64().map(|f| f as u64)))
            .unwrap_or(self.cfg.expires_seconds);

        Ok(Cached {
            header_value: format!("{}{token}", self.cfg.prefix),
            expires_at: expires_after(seconds),
        })
    }
}

/// 过期时间点。seconds 巨大（如 AUTH 响应异常返回 u64 极值）时 `Instant + Duration`
/// 会溢出 panic：checked_add 失败则折半收敛到平台可表示的最大过期时间（等效"永不过期"），
/// 消除 panic 路径。
fn expires_after(seconds: u64) -> Instant {
    let now = Instant::now();
    let mut secs = seconds;
    loop {
        if let Some(at) = now.checked_add(Duration::from_secs(secs)) {
            return at;
        }
        secs /= 2;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn huge_ttl_clamps_instead_of_panicking() {
        // u64::MAX 秒必然溢出平台 Instant 表示：不得 panic，收敛到未来时间点
        assert!(expires_after(u64::MAX) > Instant::now());
        // 常规值语义不变：0 → 立即过期，60 → 约 60 秒后
        assert!(expires_after(0) <= Instant::now());
        let at = expires_after(60);
        let delta = at.duration_since(Instant::now());
        assert!(delta > Duration::from_secs(58) && delta <= Duration::from_secs(60));
    }
}
