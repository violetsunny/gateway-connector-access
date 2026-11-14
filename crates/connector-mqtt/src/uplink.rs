//! 上行：rumqttc 事件 → 协议解码 → `DeviceSink`（对齐 Java `UpMqttHander.onMessage`，
//! UpMqttHander.java:37-74）。
//!
//! rumqttc 0.25 把 Java 侧对应的 `Notification` 命名为 [`Event`]；本模块的
//! [`UplinkCtx::handle_event`] 即 Notification→Message 映射入口（Seam C 测试直接构造
//! `Event::Incoming(Packet::Publish(..))` 断言 sink 收到的消息，无需真实 broker）。

use std::sync::Arc;
use std::sync::atomic::Ordering;

use connector_common::DeviceSink;
use rumqttc::{AsyncClient, ConnectReturnCode, Event, Packet, QoS, SubscribeFilter};
use serde_json::Value as JsonValue;

use crate::Counters;
use crate::chooser::{Choice, ProtocolChooser};
use crate::session::DeviceSessions;

/// 单个 broker 的上行上下文（eventloop task 与测试共用）。
pub struct UplinkCtx {
    /// broker key（`clientId+host+port`），日志与会话登记用。
    pub broker: String,
    pub chooser: Arc<ProtocolChooser>,
    pub sink: Arc<dyn DeviceSink>,
    pub sessions: Arc<DeviceSessions>,
    pub counters: Arc<Counters>,
}

impl UplinkCtx {
    /// eventloop 事件分发：ConnAck 成功 → 订阅 topics；Publish → 上行；其余仅 trace。
    pub async fn handle_event(
        &self,
        event: &Event,
        client: &AsyncClient,
        topics: &[String],
        qos: QoS,
    ) {
        match event {
            Event::Incoming(Packet::ConnAck(connack)) => {
                if connack.code != ConnectReturnCode::Success {
                    tracing::error!(broker = %self.broker, code = ?connack.code, "broker 拒绝连接");
                    return;
                }
                self.subscribe(client, topics, qos).await;
            }
            Event::Incoming(Packet::Publish(publish)) => {
                self.handle_publish(&publish.topic, &publish.payload).await;
            }
            // 其余入向报文（SubAck/PubAck/PingResp 等）为协议管道事件，无业务语义
            Event::Incoming(other) => {
                tracing::trace!(broker = %self.broker, packet = ?other, "忽略入向报文");
            }
            Event::Outgoing(outgoing) => {
                tracing::trace!(broker = %self.broker, ?outgoing, "忽略出向事件");
            }
        }
    }

    /// 连接（含重连）成功后订阅配置的 topics，对齐 Java `MqttConnectorStater.java:92-98`。
    async fn subscribe(&self, client: &AsyncClient, topics: &[String], qos: QoS) {
        if topics.is_empty() {
            tracing::warn!(broker = %self.broker, "topics 为空，无可订阅");
            return;
        }
        let filters: Vec<SubscribeFilter> = topics
            .iter()
            .map(|topic| SubscribeFilter::new(topic.clone(), qos))
            .collect();
        tracing::info!(broker = %self.broker, ?topics, ?qos, "已连接，订阅 topics");
        if let Err(e) = client.subscribe_many(filters).await {
            tracing::error!(broker = %self.broker, error = %e, "订阅失败");
        }
    }

    /// Publish 报文 → 选协议 → decode_multi → store_message。
    ///
    /// 无匹配协议 / 解码失败 / sink 失败一律计数 + 记日志后丢弃（失败可见，不 panic、不静默）。
    pub async fn handle_publish(&self, topic: &str, payload: &[u8]) {
        self.counters.received.fetch_add(1, Ordering::Relaxed);
        tracing::info!(
            broker = %self.broker,
            %topic,
            bytes = payload.len(),
            "收到 MQTT 上行报文"
        );

        match self.chooser.choose(topic, payload) {
            Choice::Decoded { protocol, messages } => {
                tracing::debug!(
                    broker = %self.broker,
                    %topic,
                    %protocol,
                    count = messages.len(),
                    "协议解码完成"
                );
                for message in messages {
                    self.register_session(&message, topic);
                    match self.sink.store_message(message).await {
                        Ok(()) => {
                            self.counters.forwarded.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => {
                            self.counters.dropped.fetch_add(1, Ordering::Relaxed);
                            tracing::error!(
                                broker = %self.broker,
                                %topic,
                                %protocol,
                                error = %e,
                                "store_message 失败，消息丢弃"
                            );
                        }
                    }
                }
            }
            Choice::NoTopicMatch => {
                self.counters.dropped.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    broker = %self.broker,
                    %topic,
                    bytes = payload.len(),
                    "无匹配协议（topic 未命中任何配置过滤规则），报文丢弃"
                );
            }
            Choice::DecodeFailed { protocols, error } => {
                self.counters.dropped.fetch_add(1, Ordering::Relaxed);
                tracing::error!(
                    broker = %self.broker,
                    %topic,
                    ?protocols,
                    error = %error,
                    "协议解码失败，报文丢弃"
                );
            }
        }
    }

    /// 登记 deviceId → broker，作为下行指令路由依据（对齐 Java `addSession`）。
    fn register_session(&self, message: &JsonValue, topic: &str) {
        match message.get("deviceId").and_then(JsonValue::as_str) {
            Some(device_id) if !device_id.is_empty() => {
                self.sessions.add(device_id, &self.broker);
            }
            _ => tracing::debug!(
                broker = %self.broker,
                %topic,
                "上行消息无 deviceId，不登记下行会话"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chooser::Rule;
    use crate::testutil::{JSON_ECHO, protocol};
    use connector_common::MockSink;
    use rumqttc::{ConnAck, Publish};
    use serde_json::json;

    /// 构造上行上下文：rules 为 (filter, 协议名, decode_multi 函数体)。
    fn ctx(rules: Vec<(&str, &str, &str)>) -> (UplinkCtx, Arc<MockSink>) {
        let sink = Arc::new(MockSink::new());
        let chooser = Arc::new(ProtocolChooser::new(
            rules
                .into_iter()
                .map(|(filter, name, body)| Rule {
                    filter: filter.to_string(),
                    protocol: protocol(name, body, "return \"\""),
                    params: json!({}),
                })
                .collect(),
        ));
        let ctx = UplinkCtx {
            broker: "gw-001broker.local1883".into(),
            chooser,
            sink: sink.clone(),
            sessions: Arc::new(DeviceSessions::new()),
            counters: Arc::new(Counters::default()),
        };
        (ctx, sink)
    }

    /// 未连接的 client（无 eventloop 驱动），仅用于验证 handle_event 不 panic。
    fn idle_client() -> AsyncClient {
        let (client, _eventloop) = AsyncClient::new(
            rumqttc::MqttOptions::new("test", "127.0.0.1", 1883),
            crate::options::REQUEST_CHANNEL_CAP,
        );
        client
    }

    fn publish_event(topic: &str, payload: &[u8]) -> Event {
        Event::Incoming(Packet::Publish(Publish::new(
            topic,
            QoS::AtMostOnce,
            payload.to_vec(),
        )))
    }

    /// Notification(Event)→Message 映射：Publish 报文经协议解码后写入 sink，并登记会话。
    #[tokio::test]
    async fn publish_event_maps_to_sink_message_and_session() {
        let (ctx, sink) = ctx(vec![("enn/+/up", "json_up", JSON_ECHO)]);
        let payload = br#"{"deviceId":"d1","sn":"s1","messageType":"DEVICE_REPORT_REQ"}"#;

        ctx.handle_event(
            &publish_event("enn/dev1/up", payload),
            &idle_client(),
            &["enn/+/up".to_string()],
            QoS::AtMostOnce,
        )
        .await;

        let messages = sink.messages();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0]["deviceId"], "d1");
        assert_eq!(messages[0]["sn"], "s1");
        assert_eq!(messages[0]["messageType"], "DEVICE_REPORT_REQ");
        assert_eq!(
            ctx.sessions.broker_of("d1").as_deref(),
            Some("gw-001broker.local1883")
        );
        assert_eq!(ctx.counters.received.load(Ordering::Relaxed), 1);
        assert_eq!(ctx.counters.forwarded.load(Ordering::Relaxed), 1);
        assert_eq!(ctx.counters.dropped.load(Ordering::Relaxed), 0);
    }

    /// 一条报文解出多条消息 → 全部上行（decode_multi 语义）。
    #[tokio::test]
    async fn decode_multi_forwards_every_message() {
        let (ctx, sink) = ctx(vec![(
            "enn/#",
            "multi",
            r#"return {
                { deviceId = "d1", messageType = "DEVICE_REPORT_REQ" },
                { deviceId = "d2", messageType = "DEVICE_EVENT_REQ" }
            }"#,
        )]);

        ctx.handle_publish("enn/dev1/up", b"ignored").await;

        let messages = sink.messages();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["messageType"], "DEVICE_REPORT_REQ");
        assert_eq!(messages[1]["messageType"], "DEVICE_EVENT_REQ");
        assert_eq!(ctx.counters.forwarded.load(Ordering::Relaxed), 2);
        // 两条消息各自登记会话
        assert!(ctx.sessions.broker_of("d1").is_some());
        assert!(ctx.sessions.broker_of("d2").is_some());
    }

    /// 无匹配协议：warn 丢弃，sink 不收到消息，dropped 计数 +1（失败可见）。
    #[tokio::test]
    async fn unmatched_topic_is_dropped_visibly() {
        let (ctx, sink) = ctx(vec![("enn/+/up", "json_up", JSON_ECHO)]);

        ctx.handle_publish("other/dev1/up", br#"{"deviceId":"d1"}"#)
            .await;

        assert!(sink.is_empty());
        assert_eq!(ctx.counters.received.load(Ordering::Relaxed), 1);
        assert_eq!(ctx.counters.dropped.load(Ordering::Relaxed), 1);
        assert_eq!(ctx.counters.forwarded.load(Ordering::Relaxed), 0);
    }

    /// 命中协议但解码失败：error 丢弃，不上行。
    #[tokio::test]
    async fn decode_failure_is_dropped_visibly() {
        let (ctx, sink) = ctx(vec![("enn/+/up", "bad", "error(\"boom\")")]);

        ctx.handle_publish("enn/dev1/up", b"not decodable").await;

        assert!(sink.is_empty());
        assert_eq!(ctx.counters.dropped.load(Ordering::Relaxed), 1);
    }

    /// sink 失败（如 Kafka 不可用）：计数 + error，不 panic、不中断后续消息。
    #[tokio::test]
    async fn sink_failure_is_counted_and_does_not_stop_loop() {
        struct FailingSink;
        #[connector_common::async_trait]
        impl DeviceSink for FailingSink {
            async fn store_message(&self, _msg: JsonValue) -> connector_common::Result<()> {
                Err(connector_common::CommonError::Kafka("broker down".into()))
            }
        }

        let chooser = Arc::new(ProtocolChooser::new(vec![Rule {
            filter: "enn/#".into(),
            protocol: protocol(
                "multi",
                "return { { deviceId = \"d1\" }, { deviceId = \"d2\" } }",
                "return \"\"",
            ),
            params: json!({}),
        }]));
        let ctx = UplinkCtx {
            broker: "b".into(),
            chooser,
            sink: Arc::new(FailingSink),
            sessions: Arc::new(DeviceSessions::new()),
            counters: Arc::new(Counters::default()),
        };

        ctx.handle_publish("enn/x", b"ignored").await;

        assert_eq!(ctx.counters.received.load(Ordering::Relaxed), 1);
        assert_eq!(ctx.counters.dropped.load(Ordering::Relaxed), 2);
        assert_eq!(ctx.counters.forwarded.load(Ordering::Relaxed), 0);
    }

    /// 无 deviceId 的消息仍上行，但不登记会话（下行无法路由，debug 记录）。
    #[tokio::test]
    async fn message_without_device_id_is_forwarded_without_session() {
        let (ctx, sink) = ctx(vec![("enn/#", "json_up", JSON_ECHO)]);

        ctx.handle_publish("enn/dev1/up", br#"{"sn":"s1"}"#).await;

        assert_eq!(sink.len(), 1);
        assert!(ctx.sessions.is_empty());
    }

    /// ConnAck 成功 → 走订阅分支（无 eventloop 时请求入队即返回），不产生上行消息、不 panic。
    #[tokio::test]
    async fn connack_success_subscribes_without_uplink() {
        let (ctx, sink) = ctx(vec![("enn/+/up", "json_up", JSON_ECHO)]);
        let event = Event::Incoming(Packet::ConnAck(ConnAck::new(
            rumqttc::ConnectReturnCode::Success,
            false,
        )));

        ctx.handle_event(
            &event,
            &idle_client(),
            &["enn/+/up".to_string()],
            QoS::AtLeastOnce,
        )
        .await;

        assert!(sink.is_empty());
        assert_eq!(ctx.counters.received.load(Ordering::Relaxed), 0);
    }

    /// ConnAck 被拒 / 非业务报文：不上行、不 panic。
    #[tokio::test]
    async fn rejected_connack_and_other_packets_are_ignored() {
        let (ctx, sink) = ctx(vec![("enn/+/up", "json_up", JSON_ECHO)]);
        let client = idle_client();
        let topics = vec!["enn/+/up".to_string()];

        let refused = Event::Incoming(Packet::ConnAck(ConnAck::new(
            rumqttc::ConnectReturnCode::NotAuthorized,
            false,
        )));
        ctx.handle_event(&refused, &client, &topics, QoS::AtMostOnce)
            .await;
        ctx.handle_event(
            &Event::Incoming(Packet::PingResp),
            &client,
            &topics,
            QoS::AtMostOnce,
        )
        .await;
        ctx.handle_event(
            &Event::Outgoing(rumqttc::Outgoing::PingReq),
            &client,
            &topics,
            QoS::AtMostOnce,
        )
        .await;

        assert!(sink.is_empty());
        assert_eq!(ctx.counters.received.load(Ordering::Relaxed), 0);
    }
}
