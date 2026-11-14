//! Connector trait：所有 connector crate 的统一启停接口。
//!
//! 对齐 Java `Connector`，但删除了 Java 侧全部为空实现的 `setupProtocol`。
//! 配置在构造 connector 时注入（各 connector 持有自己的强类型 config），
//! `start` 只接收共享的上行出口 `sink`。

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::error::Result;
use crate::sink::DeviceSink;

/// connector 状态快照，对齐 Java `status() -> Map<String,Object>`。
pub type StatusMap = HashMap<String, JsonValue>;

#[async_trait]
pub trait Connector: Send + Sync {
    /// 启动 connector，上行消息统一写入 `sink`。
    async fn start(&self, sink: Arc<dyn DeviceSink>) -> Result<()>;
    /// 停止 connector，释放连接/任务。
    async fn stop(&self) -> Result<()>;
    /// 返回运行状态快照（连接数、收发计数等），供 gateway status 查询。
    fn status(&self) -> StatusMap;
}
