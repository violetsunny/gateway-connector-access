//! 统一错误类型（库层用 thiserror）。应用层（gateway）用 anyhow 包裹。

use thiserror::Error;

use crate::message::MessageType;

/// connector-common 及所有 connector crate 共用的错误类型。
#[derive(Debug, Error)]
pub enum CommonError {
    /// Lua 协议脚本运行时错误或返回值类型不匹配，携带脚本名 + 错误信息（失败可见）。
    #[error("lua protocol '{script}' error at {stage}: {message}")]
    Lua {
        script: String,
        stage: &'static str,
        message: String,
    },

    /// 配置里出现 `type = "jar"`：Java 侧的 jar 热加载机制已废弃，Rust 版只支持 Lua 脚本。
    #[error(
        "protocol '{name}': type='{kind}' is not supported, only type='script' (Lua) is allowed"
    )]
    UnsupportedProtocolType { name: String, kind: String },

    /// 协议脚本文件读取失败。
    #[error("protocol '{name}': failed to load script from '{path}': {source}")]
    ScriptLoad {
        name: String,
        path: String,
        source: std::io::Error,
    },

    /// 按名字找不到协议。
    #[error("protocol '{0}' not found in registry")]
    ProtocolNotFound(String),

    /// Kafka producer 创建或发送失败（不静默吞掉）。
    #[error("kafka error: {0}")]
    Kafka(String),

    /// 上行消息类型没有对应的 Kafka topic 路由。
    #[error("no kafka topic route for messageType {0:?}")]
    NoTopicRoute(MessageType),

    /// 配置解析 / 校验错误。
    #[error("config error: {0}")]
    Config(String),

    /// 内部错误（如 Lua 状态锁中毒）。
    #[error("internal error: {0}")]
    Internal(String),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

/// 库层统一 Result 别名。
pub type Result<T> = std::result::Result<T, CommonError>;
