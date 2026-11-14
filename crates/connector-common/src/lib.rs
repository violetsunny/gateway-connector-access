//! connector-common：所有 connector crate 与 gateway 共享的基建。
//!
//! 提供：核心 trait（[`Connector`] / [`DeviceSink`] / [`DeviceDirectory`] / [`Protocol`]）、
//! 消息模型（[`message`]）、Lua 协议引擎（[`protocol`]）、TOML 配置模型（[`config`]）、
//! 统一错误（[`CommonError`]）。

pub mod config;
pub mod connector;
pub mod directory;
pub mod error;
pub mod message;
pub mod protocol;
pub mod sink;

// 供 connector 骨架 crate 实现 Connector trait 时复用（无需各自依赖 async-trait）。
pub use async_trait::async_trait;

pub use config::{
    DeviceEntry, GatewayConfig, KafkaConfig, MqttConfig, MqttConfigServer, MqttLastWill,
    ProtocolConfig, TcpClientConfig, TcpConfig, TcpServerConfig,
};
pub use connector::{Connector, StatusMap};
pub use directory::{DeviceDirectory, TomlDeviceDirectory};
pub use error::{CommonError, Result};
pub use message::{
    EventReportRequest, InfoReportRequest, LoginRequest, LoginResponse, Message, MessageType,
    Metric, MetricReportRequest, OperationRequest, OperationResponse,
};
pub use protocol::{LuaProtocol, Protocol, ProtocolRegistry};
pub use sink::{DeviceSink, KafkaSink, MockSink};
