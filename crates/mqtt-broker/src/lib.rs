//! mqtt-broker：最小完整 MQTT 3.1.1 broker（手写 codec，零新增依赖）。
//!
//! 支持：CONNECT 鉴权 / SUBSCRIBE/UNSUBSCRIBE 通配路由 / QoS 0/1 / retained /
//! will / 持久会话（离线队列 + inflight 重发）/ keepalive / 同 clientId 接管。
//! 未实现：QoS2、共享订阅、TLS、集群（刻意简化处见各模块 `ponytail:` 注释）。
//!
//! 独立 crate，不依赖 connector-common（broker 不是 connector，不引入 mlua/rdkafka）。
//! 参照物：iot-gateway mqtt-component 是 Vert.x 设备接入网关（无 topic 路由），
//! 本 crate 按用户要求做完整 pub/sub broker，非其复刻。

pub mod broker;
pub mod codec;
pub mod conn;
pub mod error;
pub mod topic;

use serde::Deserialize;

pub use broker::Broker;
pub use conn::serve;
pub use error::{Error, Result};

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default = "default_listen")]
    pub listen: String,
    /// 设置后 CONNECT 必须携带匹配的用户名/密码；缺省允许匿名。
    pub username: Option<String>,
    pub password: Option<String>,
}

fn default_listen() -> String {
    "0.0.0.0:1883".to_string()
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            username: None,
            password: None,
        }
    }
}

/// 读取 TOML 配置。
pub fn load_config(path: &str) -> Result<Config> {
    let text =
        std::fs::read_to_string(path).map_err(|e| Error::Config(format!("read {path}: {e}")))?;
    toml::from_str(&text).map_err(|e| Error::Config(format!("parse {path}: {e}")))
}
