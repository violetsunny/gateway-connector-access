//! DeviceSink：上行消息唯一出口。
//!
//! - 生产实现 [`KafkaSink`]：rdkafka async producer，按 messageType 路由 topic，
//!   创建/发送失败一律返回 Err（失败可见，不静默吞）。
//! - 测试实现 [`MockSink`]：Vec 收集，供 Seam A 断言。
//!
//! wire message 用 `serde_json::Value`（Lua 解码出的动态 table，等价 Java 多态 Message
//! 序列化后的扁平 JSON 对象）。

use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use serde_json::Value as JsonValue;

use crate::error::{CommonError, Result};
use crate::message::MessageType;

#[async_trait]
pub trait DeviceSink: Send + Sync {
    /// 存储一条上行消息（生产环境投递 Kafka）。
    async fn store_message(&self, msg: JsonValue) -> Result<()>;
}

/// 按 messageType 路由到 Kafka topic（纯函数，便于单测，不依赖真实 producer）。
///
/// 对齐 Java `UpDataTransfer`：metric→enn_data_iot_metric、event→data_iot_event、
/// info→data_iot_device_info。其余类型无上行路由（返回 Err，失败可见）。
fn route_topic<'a>(
    mt: MessageType,
    metric: &'a str,
    event: &'a str,
    info: &'a str,
) -> Result<&'a str> {
    match mt {
        MessageType::DeviceReportReq => Ok(metric),
        MessageType::DeviceEventReq => Ok(event),
        MessageType::DeviceInfoReq => Ok(info),
        other => Err(CommonError::NoTopicRoute(other)),
    }
}

/// 测试用 sink：把消息收集进 Vec。
#[derive(Default)]
pub struct MockSink {
    messages: Mutex<Vec<JsonValue>>,
}

impl MockSink {
    pub fn new() -> Self {
        Self::default()
    }

    /// 返回已收集消息的快照。
    pub fn messages(&self) -> Vec<JsonValue> {
        self.lock().clone()
    }

    pub fn len(&self) -> usize {
        self.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<JsonValue>> {
        // 测试场景下锁中毒不应 panic 整个测试，恢复内部数据继续。
        self.messages.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[async_trait]
impl DeviceSink for MockSink {
    async fn store_message(&self, msg: JsonValue) -> Result<()> {
        self.lock().push(msg);
        Ok(())
    }
}

/// 生产用 sink：rdkafka async producer。
///
/// 构造需在 tokio runtime 上下文内（rdkafka 默认 TokioRuntime）。
pub struct KafkaSink {
    producer: FutureProducer,
    metric_topic: String,
    event_topic: String,
    info_topic: String,
}

impl KafkaSink {
    /// 创建 producer。失败（如 broker 配置非法）返回 Err，不静默。
    pub fn new(
        bootstrap_servers: &str,
        metric_topic: &str,
        event_topic: &str,
        info_topic: &str,
    ) -> Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .create()
            .map_err(|e| {
                CommonError::Kafka(format!("create producer for {bootstrap_servers}: {e}"))
            })?;
        Ok(Self {
            producer,
            metric_topic: metric_topic.to_string(),
            event_topic: event_topic.to_string(),
            info_topic: info_topic.to_string(),
        })
    }

    fn topic_for(&self, mt: MessageType) -> Result<&str> {
        route_topic(mt, &self.metric_topic, &self.event_topic, &self.info_topic)
    }
}

#[async_trait]
impl DeviceSink for KafkaSink {
    async fn store_message(&self, msg: JsonValue) -> Result<()> {
        let mt: MessageType = serde_json::from_value(
            msg.get("messageType")
                .cloned()
                .ok_or_else(|| CommonError::Config("message missing messageType".into()))?,
        )
        .map_err(|e| CommonError::Config(format!("bad messageType: {e}")))?;

        let topic = self.topic_for(mt)?;
        let payload = serde_json::to_vec(&msg)?;
        let record: FutureRecord<'_, (), [u8]> = FutureRecord::to(topic).payload(&payload[..]);

        self.producer
            .send(record, Timeout::After(Duration::from_secs(5)))
            .await
            .map_err(|(e, _)| CommonError::Kafka(format!("send to {topic}: {e}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn mock_sink_collects_messages() {
        let sink = MockSink::new();
        assert!(sink.is_empty());
        sink.store_message(json!({"messageType": "HEARTBEAT", "sn": "s1"}))
            .await
            .unwrap();
        sink.store_message(json!({"messageType": "DEVICE_REPORT_REQ"}))
            .await
            .unwrap();
        assert_eq!(sink.len(), 2);
        let msgs = sink.messages();
        assert_eq!(msgs[0]["messageType"], "HEARTBEAT");
        assert_eq!(msgs[0]["sn"], "s1");
        assert_eq!(msgs[1]["messageType"], "DEVICE_REPORT_REQ");
    }

    #[test]
    fn route_topic_maps_message_type_to_kafka_topic() {
        assert_eq!(
            route_topic(MessageType::DeviceReportReq, "m", "e", "i").unwrap(),
            "m"
        );
        assert_eq!(
            route_topic(MessageType::DeviceEventReq, "m", "e", "i").unwrap(),
            "e"
        );
        assert_eq!(
            route_topic(MessageType::DeviceInfoReq, "m", "e", "i").unwrap(),
            "i"
        );
    }

    #[test]
    fn route_topic_rejects_unroutable_type() {
        let err = route_topic(MessageType::Heartbeat, "m", "e", "i").unwrap_err();
        assert!(matches!(
            err,
            CommonError::NoTopicRoute(MessageType::Heartbeat)
        ));
    }
}
