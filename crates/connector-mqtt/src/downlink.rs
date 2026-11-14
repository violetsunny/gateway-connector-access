//! 下行：Kafka `device_command_topic` → `OperationRequest` → Lua encode → publish QoS1
//! （对齐 Java `DownMqttHandler.listen`，DownMqttHandler.java:48-91）。
//!
//! 链路拆成两层：
//!
//! - [`DownlinkCtx::plan`]：Kafka 记录字节 → [`Command`]（publish 参数或明确的跳过原因）。
//!   纯函数，Seam C 测试直接断言 topic/qos/payload 与 encode 入参，无需真实 Kafka/broker。
//! - [`run_downlink`] / [`dispatch`]：rdkafka 消费循环 + rumqttc publish 的薄封装。
//!
//! 与 Java 的两处刻意差异（均为修正而非复刻）：
//!
//! 1. Java 用 `operationRequest.getTopic()` 直接 publish，topic 为 null 时 NPE；Rust 侧
//!    topic 或 param 缺失一律 [`Command::Skip`] 并记 warn（失败可见）。
//! 2. Java 把 Kafka 记录先反序列化成 `OperationRequest` POJO 再交给 encode（未知字段丢失）；
//!    Rust 侧把原始 JSON 交给 Lua encode（超集，脚本按需取字段），类型校验仍走
//!    `OperationRequest`。

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, MutexGuard};

use connector_common::{MessageType, OperationRequest, Protocol};
use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message as KafkaMessage;
use rumqttc::{AsyncClient, QoS};
use serde_json::Value as JsonValue;
use tokio::sync::watch;

use crate::Counters;
use crate::session::DeviceSessions;

/// Java 侧 `@KafkaListener(groupId = "deviceCommandListenerForMqtt")`（DownMqttHandler.java:48）。
const GROUP_ID: &str = "deviceCommandListenerForMqtt";

/// broker key → rumqttc 客户端。`start` 时装配，下行按会话路由取用。
/// `AsyncClient` 内部是请求 channel 的 sender，可克隆。
#[derive(Default)]
pub struct BrokerClients {
    map: Mutex<HashMap<String, AsyncClient>>,
}

impl BrokerClients {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, broker: &str, client: AsyncClient) {
        self.lock().insert(broker.to_string(), client);
    }

    pub fn get(&self, broker: &str) -> Option<AsyncClient> {
        self.lock().get(broker).cloned()
    }

    pub fn clear(&self) {
        self.lock().clear();
    }

    pub fn len(&self) -> usize {
        self.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, AsyncClient>> {
        self.map.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// 下行决策：publish 参数已构造完成，或明确跳过（reason 进日志，失败可见）。
#[derive(Debug, PartialEq)]
pub enum Command {
    Publish {
        broker: String,
        topic: String,
        qos: QoS,
        payload: Vec<u8>,
    },
    Skip {
        reason: String,
    },
}

/// 下行上下文：会话表（deviceId→broker）+ 每 broker 的协议。
pub struct DownlinkCtx {
    pub sessions: Arc<DeviceSessions>,
    /// broker key → (协议, params)，来自各 broker 的 `protocol` 配置
    /// （对齐 Java `protocolManager.get(mqttConfigServer.getProtocol().getName())`）。
    pub protocols: HashMap<String, (Arc<dyn Protocol>, JsonValue)>,
}

impl DownlinkCtx {
    /// Kafka 记录字节 → publish 参数。任何一步不满足都返回带原因的 [`Command::Skip`]，
    /// 不吞错、不 panic。
    pub fn plan(&self, raw: &[u8]) -> Command {
        let value: JsonValue = match serde_json::from_slice(raw) {
            Ok(value) => value,
            Err(e) => {
                return Command::Skip {
                    reason: format!("kafka 消息不是合法 JSON: {e}"),
                };
            }
        };

        let message_type: MessageType = match value.get("messageType") {
            Some(raw_type) => match serde_json::from_value(raw_type.clone()) {
                Ok(message_type) => message_type,
                Err(e) => {
                    return Command::Skip {
                        reason: format!("messageType 无法识别: {e}"),
                    };
                }
            },
            None => {
                return Command::Skip {
                    reason: "kafka 消息缺 messageType 字段".into(),
                };
            }
        };

        match message_type {
            MessageType::CloudOperationReq => {}
            // Java 侧这两个分支为空实现（DownMqttHandler.java:69/83）
            MessageType::CloudHistoryReq | MessageType::CloudNtpReq => {
                return Command::Skip {
                    reason: "CLOUD_HISTORY_REQ / CLOUD_NTP_REQ 在 Java 侧为空分支，不下发".into(),
                };
            }
            other => {
                return Command::Skip {
                    reason: format!("messageType {other:?} 不属于 mqtt 下行指令"),
                };
            }
        }

        let request: OperationRequest = match serde_json::from_value(value.clone()) {
            Ok(request) => request,
            Err(e) => {
                return Command::Skip {
                    reason: format!("OperationRequest 解析失败: {e}"),
                };
            }
        };

        let Some(device_id) = request.base.device_id.as_deref() else {
            return Command::Skip {
                reason: "指令缺 deviceId，无法路由 broker".into(),
            };
        };
        let Some(broker) = self.sessions.broker_of(device_id) else {
            return Command::Skip {
                reason: format!(
                    "deviceId {device_id} 无 mqtt 会话（未在任何 broker 上行过），指令丢弃"
                ),
            };
        };
        let Some((protocol, params)) = self.protocols.get(&broker) else {
            return Command::Skip {
                reason: format!("broker {broker} 未配置协议，无法 encode"),
            };
        };

        let encoded = match protocol.encode(&value, params) {
            Ok(encoded) => encoded,
            Err(e) => {
                return Command::Skip {
                    reason: format!("协议 {} encode 失败: {e}", protocol.name()),
                };
            }
        };

        // 对齐 DownMqttHandler.java:75-82：从 encode 输出取 topic + param。Java 侧虽重解析为
        // OperationRequest POJO，但只消费这两个字段（Jackson 容忍缺省 timeStamp）；Rust 侧直接
        // 按 JSON 取字段，避免 serde 对必填字段的严格性把合法指令吞掉。
        let out: JsonValue = match serde_json::from_slice(&encoded) {
            Ok(out) => out,
            Err(e) => {
                return Command::Skip {
                    reason: format!("协议 {} encode 输出不是合法 JSON: {e}", protocol.name()),
                };
            }
        };
        let (Some(topic), Some(param)) = (
            out.get("topic").and_then(JsonValue::as_str),
            out.get("param"),
        ) else {
            return Command::Skip {
                reason: format!(
                    "指令下发取消，协议解析后无下发参数信息: {}",
                    String::from_utf8_lossy(&encoded)
                ),
            };
        };
        let payload = match serde_json::to_vec(param) {
            Ok(payload) => payload,
            Err(e) => {
                return Command::Skip {
                    reason: format!("param 序列化失败: {e}"),
                };
            }
        };

        Command::Publish {
            broker,
            topic: topic.to_string(),
            // 对齐 Java `MqttQoS.AT_LEAST_ONCE`（DownMqttHandler.java:79）
            qos: QoS::AtLeastOnce,
            payload,
        }
    }
}

/// Kafka 下行消费任务。consumer 创建/订阅失败即 error 退出（失败可见，不静默空转）。
pub async fn run_downlink(
    ctx: Arc<DownlinkCtx>,
    clients: Arc<BrokerClients>,
    counters: Arc<Counters>,
    bootstrap_servers: String,
    command_topic: String,
    mut shutdown: watch::Receiver<bool>,
) {
    let consumer: StreamConsumer = match ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", GROUP_ID)
        // 新消费组从最新位点起：gateway 重启不重放历史指令
        .set("auto.offset.reset", "latest")
        .create()
    {
        Ok(consumer) => consumer,
        Err(e) => {
            tracing::error!(error = %e, "创建 kafka consumer 失败，mqtt 下行停用");
            return;
        }
    };
    if let Err(e) = consumer.subscribe(&[command_topic.as_str()]) {
        tracing::error!(topic = %command_topic, error = %e, "订阅 kafka topic 失败，mqtt 下行停用");
        return;
    }
    tracing::info!(topic = %command_topic, group = GROUP_ID, "mqtt 下行消费已启动");

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                // sender 已 drop（changed 返回 Err）同样退出，避免 select 空转
                if changed.is_err() || *shutdown.borrow_and_update() {
                    break;
                }
            }
            received = consumer.recv() => match received {
                Ok(msg) => {
                    dispatch(&ctx, &clients, &counters, msg.payload().unwrap_or_default()).await;
                }
                // librdkafka 的可恢复错误（如 broker 短暂不可达）：记日志后继续轮询
                Err(e) => tracing::error!(error = %e, "kafka 消费错误，继续轮询"),
            }
        }
    }
    tracing::info!("mqtt 下行消费已停止");
}

/// 单条指令下发：plan → 取 broker 客户端 → publish。
pub(crate) async fn dispatch(
    ctx: &DownlinkCtx,
    clients: &BrokerClients,
    counters: &Counters,
    raw: &[u8],
) {
    match ctx.plan(raw) {
        Command::Publish {
            broker,
            topic,
            qos,
            payload,
        } => {
            let Some(client) = clients.get(&broker) else {
                counters.skipped.fetch_add(1, Ordering::Relaxed);
                tracing::error!(%broker, %topic, "无该 broker 的 mqtt 客户端，指令丢弃");
                return;
            };
            match client.publish(topic.clone(), qos, false, payload).await {
                Ok(()) => {
                    counters.published.fetch_add(1, Ordering::Relaxed);
                    tracing::info!(%broker, %topic, ?qos, "指令已下发");
                }
                Err(e) => {
                    counters.skipped.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(%broker, %topic, error = %e, "指令下发失败");
                }
            }
        }
        Command::Skip { reason } => {
            counters.skipped.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(%reason, "指令未下发");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{RecordingProtocol, protocol};
    use serde_json::json;

    /// encode 输出：topic + param 齐全（可下发）。
    const ENCODE_OK: &str = r#"return json_encode({
        messageType = "CLOUD_OPERATION_REQ",
        topic = "enn/" .. msg.deviceId .. "/down",
        param = msg.param
    })"#;
    /// encode 输出缺 param（对齐 Java warn 分支）。
    const ENCODE_NO_PARAM: &str =
        r#"return json_encode({ messageType = "CLOUD_OPERATION_REQ", topic = "enn/x/down" })"#;

    fn command_json(device_id: &str) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "messageId": "mid-1",
            "deviceId": device_id,
            "messageType": "CLOUD_OPERATION_REQ",
            "function": "switch",
            "param": { "cmd": "open" },
            "timeStamp": 1700000000000_i64,
        }))
        .unwrap()
    }

    fn ctx_with(protocol_impl: Arc<dyn Protocol>, broker: &str) -> DownlinkCtx {
        let sessions = Arc::new(DeviceSessions::new());
        sessions.add("d1", broker);
        DownlinkCtx {
            sessions,
            protocols: HashMap::from([(broker.to_string(), (protocol_impl, json!({"k": "v"})))]),
        }
    }

    /// 正常路径：encode 被调用（入参为原始指令 JSON + 配置 params），
    /// publish 参数为 encode 输出的 topic + param 的 JSON 字节 + QoS1。
    #[test]
    fn plans_publish_with_encoded_topic_param_and_qos1() {
        let recording = RecordingProtocol::new(
            "rec",
            serde_json::to_vec(&json!({
                "messageType": "CLOUD_OPERATION_REQ",
                "topic": "enn/d1/down",
                "param": { "cmd": "open" },
            }))
            .unwrap(),
        );
        let ctx = ctx_with(recording.clone(), "brokerA");

        let command = ctx.plan(&command_json("d1"));

        assert_eq!(
            command,
            Command::Publish {
                broker: "brokerA".into(),
                topic: "enn/d1/down".into(),
                qos: QoS::AtLeastOnce,
                payload: serde_json::to_vec(&json!({"cmd": "open"})).unwrap(),
            }
        );
        // encode 调用断言：收到原始指令 JSON 与配置 params
        let calls = recording.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0["deviceId"], "d1");
        assert_eq!(calls[0].0["function"], "switch");
        assert_eq!(calls[0].1, json!({"k": "v"}));
    }

    /// 真实 Lua 协议路径：脚本用 deviceId 拼 topic、透传 param。
    #[test]
    fn plans_publish_via_lua_protocol() {
        let ctx = ctx_with(protocol("lua_down", "return {}", ENCODE_OK), "brokerA");

        match ctx.plan(&command_json("d1")) {
            Command::Publish {
                broker,
                topic,
                qos,
                payload,
            } => {
                assert_eq!(broker, "brokerA");
                assert_eq!(topic, "enn/d1/down");
                assert_eq!(qos, QoS::AtLeastOnce);
                assert_eq!(
                    payload,
                    serde_json::to_vec(&json!({"cmd": "open"})).unwrap()
                );
            }
            other => panic!("expected Publish, got {other:?}"),
        }
    }

    /// 无会话（deviceId 未在任何 broker 上行过）→ Skip，对齐 Java `getSession == null` 直接 return。
    #[test]
    fn skips_when_device_has_no_session() {
        let ctx = ctx_with(protocol("lua_down", "return {}", ENCODE_OK), "brokerA");

        match ctx.plan(&command_json("unknown-device")) {
            Command::Skip { reason } => {
                assert!(reason.contains("无 mqtt 会话"), "{reason}");
                assert!(reason.contains("unknown-device"), "{reason}");
            }
            other => panic!("expected Skip, got {other:?}"),
        }
    }

    /// encode 输出缺 param → Skip（对齐 Java "指令下发取消，协议解析后无下发参数信息"）。
    #[test]
    fn skips_when_encoded_output_lacks_param() {
        let ctx = ctx_with(
            protocol("lua_down", "return {}", ENCODE_NO_PARAM),
            "brokerA",
        );

        match ctx.plan(&command_json("d1")) {
            Command::Skip { reason } => {
                assert!(reason.contains("指令下发取消"), "{reason}");
            }
            other => panic!("expected Skip, got {other:?}"),
        }
    }

    /// encode 输出不是 JSON → Skip（Java 侧 jsonBytes2Object 返回 null 走同一 warn 分支）。
    #[test]
    fn skips_when_encoded_output_is_not_json() {
        let ctx = ctx_with(
            protocol("lua_down", "return {}", "return \"not json\""),
            "brokerA",
        );

        match ctx.plan(&command_json("d1")) {
            Command::Skip { reason } => {
                assert!(reason.contains("encode 输出不是合法 JSON"), "{reason}")
            }
            other => panic!("expected Skip, got {other:?}"),
        }
    }

    /// encode 抛错 → Skip 并带上脚本错误信息。
    #[test]
    fn skips_when_encode_fails() {
        let ctx = ctx_with(
            protocol("lua_down", "return {}", "error(\"encode boom\")"),
            "brokerA",
        );

        match ctx.plan(&command_json("d1")) {
            Command::Skip { reason } => {
                assert!(reason.contains("encode 失败"), "{reason}");
                assert!(reason.contains("encode boom"), "{reason}");
            }
            other => panic!("expected Skip, got {other:?}"),
        }
    }

    /// broker 有会话但未配协议 → Skip。
    #[test]
    fn skips_when_broker_has_no_protocol() {
        let sessions = Arc::new(DeviceSessions::new());
        sessions.add("d1", "brokerA");
        let ctx = DownlinkCtx {
            sessions,
            protocols: HashMap::new(),
        };

        match ctx.plan(&command_json("d1")) {
            Command::Skip { reason } => assert!(reason.contains("未配置协议"), "{reason}"),
            other => panic!("expected Skip, got {other:?}"),
        }
    }

    /// 非 CLOUD_OPERATION_REQ 类型不下发（Java 侧 CLOUD_HISTORY_REQ / CLOUD_NTP_REQ 为空分支）。
    #[test]
    fn skips_non_operation_message_types() {
        let ctx = ctx_with(protocol("lua_down", "return {}", ENCODE_OK), "brokerA");

        for (message_type, keyword) in [
            ("CLOUD_NTP_REQ", "空分支"),
            ("CLOUD_HISTORY_REQ", "空分支"),
            ("DEVICE_REPORT_REQ", "不属于 mqtt 下行指令"),
        ] {
            let raw = serde_json::to_vec(&json!({
                "deviceId": "d1",
                "messageType": message_type,
                "param": { "cmd": "open" },
                "timeStamp": 1,
            }))
            .unwrap();
            match ctx.plan(&raw) {
                Command::Skip { reason } => assert!(reason.contains(keyword), "{reason}"),
                other => panic!("{message_type}: expected Skip, got {other:?}"),
            }
        }
    }

    /// 非法 JSON / 缺 messageType / 缺 deviceId → Skip（失败可见，不 panic）。
    #[test]
    fn skips_malformed_commands() {
        let ctx = ctx_with(protocol("lua_down", "return {}", ENCODE_OK), "brokerA");

        match ctx.plan(b"not json") {
            Command::Skip { reason } => assert!(reason.contains("不是合法 JSON"), "{reason}"),
            other => panic!("expected Skip, got {other:?}"),
        }
        match ctx.plan(br#"{"deviceId":"d1"}"#) {
            Command::Skip { reason } => assert!(reason.contains("缺 messageType"), "{reason}"),
            other => panic!("expected Skip, got {other:?}"),
        }
        match ctx.plan(br#"{"messageType":"WHAT"}"#) {
            Command::Skip { reason } => {
                assert!(reason.contains("messageType 无法识别"), "{reason}")
            }
            other => panic!("expected Skip, got {other:?}"),
        }
        match ctx.plan(br#"{"messageType":"CLOUD_OPERATION_REQ","param":{"a":1},"timeStamp":1}"#) {
            Command::Skip { reason } => assert!(reason.contains("缺 deviceId"), "{reason}"),
            other => panic!("expected Skip, got {other:?}"),
        }
    }

    /// 会话路由到的 broker 无客户端（如已停机）→ 计入 skipped，不 panic。
    #[tokio::test]
    async fn dispatch_counts_skip_when_broker_client_missing() {
        let ctx = ctx_with(protocol("lua_down", "return {}", ENCODE_OK), "brokerA");
        let clients = BrokerClients::new();
        let counters = Arc::new(Counters::default());

        dispatch(&ctx, &clients, &counters, &command_json("d1")).await;

        assert_eq!(counters.skipped.load(Ordering::Relaxed), 1);
        assert_eq!(counters.published.load(Ordering::Relaxed), 0);
    }

    /// Skip 决策同样计入 skipped（下行丢弃必须可见）。
    #[tokio::test]
    async fn dispatch_counts_skip_decision() {
        let ctx = ctx_with(protocol("lua_down", "return {}", ENCODE_OK), "brokerA");
        let counters = Arc::new(Counters::default());

        dispatch(&ctx, &BrokerClients::new(), &counters, b"not json").await;

        assert_eq!(counters.skipped.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn broker_clients_stores_and_clears() {
        let clients = BrokerClients::new();
        assert!(clients.is_empty());
        let (client, _eventloop) = AsyncClient::new(
            rumqttc::MqttOptions::new("t", "127.0.0.1", 1883),
            crate::options::REQUEST_CHANNEL_CAP,
        );
        clients.insert("brokerA", client);
        assert_eq!(clients.len(), 1);
        assert!(clients.get("brokerA").is_some());
        assert!(clients.get("brokerB").is_none());
        clients.clear();
        assert!(clients.is_empty());
    }
}
