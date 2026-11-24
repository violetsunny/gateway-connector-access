//! mqtt-broker 统一错误（库层 thiserror，bin 层 anyhow 包裹）。
//! 独立于 connector-common：broker 不是 connector，不引入 mlua/rdkafka 依赖链。

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    /// 报文损坏 / 超资源上限（失败可见，连接层据此断开）。
    #[error("malformed mqtt packet: {0}")]
    Malformed(String),

    /// 配置解析 / 校验错误。
    #[error("config error: {0}")]
    Config(String),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
