//! mqtt connector：rumqttc 多 broker 上行 + rdkafka `device_command_topic` 下行。
//!
//! 上行（对齐 Java `UpMqttHander` / `MqttConnectorStater`）：每 broker 一个独立 EventLoop
//! task，ConnAck 成功后订阅 topics；Publish 报文经 [`ProtocolChooser`]（topic 过滤匹配 +
//! payload 探测）`decode_multi` 解码后逐条写入 [`DeviceSink`]，并登记 deviceId→broker 会话；
//! 连接错误按 `reconnectEnabled` / `reconnectDelay`（秒）重试，不 panic。
//!
//! 下行（对齐 Java `DownMqttHandler`）：消费 Kafka `device_command_topic`，
//! `CLOUD_OPERATION_REQ` 按会话表路由 broker，Lua `encode` 产出 topic+param 后以 QoS1
//! publish；每一步失败都是带原因的 Skip（失败可见）。
//!
//! 遗嘱（lastWill）：common 层 `MqttConfigServer.lastWill`（票 11 补）经
//! [`crate::options::build_options`] 映射为 rumqttc `set_last_will`，缺省不设遗嘱。

pub mod chooser;
pub mod downlink;
pub mod options;
pub mod session;
pub mod uplink;

#[cfg(test)]
mod testutil;

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use connector_common::{
    CommonError, Connector, DeviceSink, KafkaConfig, MqttConfig, Protocol, ProtocolRegistry,
    Result, StatusMap, async_trait,
};
use rumqttc::{AsyncClient, EventLoop, MqttOptions, QoS};
use serde_json::Value as JsonValue;
use serde_json::json;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::chooser::{ProtocolChooser, Rule};
use crate::downlink::{BrokerClients, DownlinkCtx, run_downlink};
use crate::options::{
    REQUEST_CHANNEL_CAP, broker_key, build_options, parse_qos, reconnect_delay, reconnect_enabled,
};
use crate::session::DeviceSessions;
use crate::uplink::UplinkCtx;

/// 运行计数（status 暴露；上行/下行的每次丢弃都必须可见）。
#[derive(Debug, Default)]
pub struct Counters {
    /// 收到的 Publish 报文数。
    pub received: AtomicUsize,
    /// 解码成功且写入 sink 的消息数。
    pub forwarded: AtomicUsize,
    /// 上行丢弃数（topic 无匹配 / 解码失败 / sink 失败）。
    pub dropped: AtomicUsize,
    /// 下行 publish 成功数。
    pub published: AtomicUsize,
    /// 下行跳过数（路由 / 协议 / publish 失败）。
    pub skipped: AtomicUsize,
}

/// 单个 broker 的启动计划（构造期完成全部校验与参数装配，start 只克隆并 spawn）。
#[derive(Clone)]
pub struct BrokerPlan {
    /// broker 唯一键，对齐 Java `MqttConfigServer.uuid()`（clientId+host+port）。
    pub key: String,
    pub options: MqttOptions,
    pub topics: Vec<String>,
    pub qos: QoS,
    pub reconnect_enabled: bool,
    pub reconnect_delay: Duration,
}

#[derive(Default)]
struct Inner {
    shutdown: Option<watch::Sender<bool>>,
    handles: Vec<JoinHandle<()>>,
}

/// mqtt connector（对齐 Java `MqttConnectorStater` + `UpMqttHander` + `DownMqttHandler`）。
pub struct MqttConnector {
    plans: Vec<BrokerPlan>,
    chooser: Arc<ProtocolChooser>,
    downlink: Arc<DownlinkCtx>,
    clients: Arc<BrokerClients>,
    sessions: Arc<DeviceSessions>,
    counters: Arc<Counters>,
    /// 下行 Kafka 配置：None = 未配置/不可用（gateway 统一判据），下行消费跳过，上行不受影响。
    kafka: Option<KafkaConfig>,
    inner: Mutex<Inner>,
}

impl MqttConnector {
    /// 构造期装配：校验每个 broker 配置、从注册表取协议、生成 topic 过滤规则与下行协议表。
    /// 任一 broker 配置非法 → `Err`（构造期可见，不拖到连接时）。
    pub fn new(
        cfg: MqttConfig,
        registry: Arc<ProtocolRegistry>,
        kafka: Option<KafkaConfig>,
    ) -> Result<Self> {
        let mut plans = Vec::with_capacity(cfg.configuration.len());
        let mut rules = Vec::new();
        let mut protocols: HashMap<String, (Arc<dyn Protocol>, JsonValue)> = HashMap::new();

        for server in &cfg.configuration {
            let key = broker_key(server);
            if server.client_id.trim().is_empty() {
                return Err(CommonError::Config(format!(
                    "mqtt broker {key}: clientId 不能为空"
                )));
            }
            let options = build_options(server)
                .map_err(|e| CommonError::Config(format!("mqtt broker {key}: {e}")))?;
            let qos = parse_qos(server.qos)
                .map_err(|e| CommonError::Config(format!("mqtt broker {key}: {e}")))?;

            let Some(pc) = server.protocol.as_ref() else {
                return Err(CommonError::Config(format!(
                    "mqtt broker {key}: 未配置 protocol，无法解码上行/编码下行"
                )));
            };
            let protocol = registry
                .get(&pc.name)
                .map_err(|e| CommonError::Config(format!("mqtt broker {key}: {e}")))?;

            if server.topics.is_empty() {
                tracing::warn!(broker = %key, "topics 为空，该 broker 无上行订阅");
            }
            rules.extend(server.topics.iter().map(|topic| Rule {
                filter: topic.clone(),
                protocol: Arc::clone(&protocol),
                params: pc.params.clone(),
            }));
            protocols.insert(key.clone(), (protocol, pc.params.clone()));

            plans.push(BrokerPlan {
                key,
                options,
                topics: server.topics.clone(),
                qos,
                reconnect_enabled: reconnect_enabled(server),
                reconnect_delay: reconnect_delay(server),
            });
        }

        let chooser = ProtocolChooser::new(rules);
        if chooser.is_empty() {
            tracing::warn!("mqtt 未配置任何 topic 过滤规则，上行消息将全部丢弃");
        }
        let sessions = Arc::new(DeviceSessions::new());
        Ok(Self {
            plans,
            chooser: Arc::new(chooser),
            downlink: Arc::new(DownlinkCtx {
                sessions: Arc::clone(&sessions),
                protocols,
            }),
            clients: Arc::new(BrokerClients::new()),
            sessions,
            counters: Arc::new(Counters::default()),
            kafka,
            inner: Mutex::new(Inner::default()),
        })
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[async_trait]
impl Connector for MqttConnector {
    /// 每 broker spawn 一个 [`run_broker`] task；有 broker 时再 spawn Kafka 下行消费 task。
    async fn start(&self, sink: Arc<dyn DeviceSink>) -> Result<()> {
        let mut inner = self.lock();
        if inner.shutdown.is_some() {
            return Err(CommonError::Internal(
                "mqtt connector 已启动，请先 stop".into(),
            ));
        }
        let (tx, rx) = watch::channel(false);
        for plan in &self.plans {
            let (client, eventloop) = AsyncClient::new(plan.options.clone(), REQUEST_CHANNEL_CAP);
            self.clients.insert(&plan.key, client.clone());
            let uplink = Arc::new(UplinkCtx {
                broker: plan.key.clone(),
                chooser: Arc::clone(&self.chooser),
                sink: Arc::clone(&sink),
                sessions: Arc::clone(&self.sessions),
                counters: Arc::clone(&self.counters),
            });
            inner.handles.push(tokio::spawn(run_broker(
                plan.clone(),
                eventloop,
                client,
                uplink,
                rx.clone(),
            )));
        }
        if !self.clients.is_empty() {
            match &self.kafka {
                Some(kafka) => {
                    inner.handles.push(tokio::spawn(run_downlink(
                        Arc::clone(&self.downlink),
                        Arc::clone(&self.clients),
                        Arc::clone(&self.counters),
                        kafka.bootstrap_servers.clone(),
                        kafka.command_topic.clone(),
                        rx.clone(),
                    )));
                }
                // 与上行 sink 降级同判据：kafka 不可用时下行跳过（失败可见，仅记日志）
                None => tracing::warn!("kafka 未配置/不可用，mqtt 下行消费跳过（上行不受影响）"),
            }
        }
        inner.shutdown = Some(tx);
        tracing::info!(
            brokers = self.plans.len(),
            command_topic = self
                .kafka
                .as_ref()
                .map(|k| k.command_topic.as_str())
                .unwrap_or("<disabled>"),
            "mqtt connector 已启动"
        );
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        let mut inner = self.lock();
        if let Some(tx) = inner.shutdown.take() {
            let _ = tx.send(true);
        }
        // ponytail: abort 兜底会跳过 run_broker 的优雅 disconnect 与重连等待期的 shutdown
        // 响应（仅影响 broker 侧会话清理时机）；升级路径：先等 watch 广播自然退出再超时 abort
        for handle in inner.handles.drain(..) {
            handle.abort();
        }
        drop(inner);
        self.clients.clear();
        tracing::info!("mqtt connector 已停止");
        Ok(())
    }

    fn status(&self) -> StatusMap {
        let running = self.lock().shutdown.is_some();
        let c = &self.counters;
        let mut m = StatusMap::new();
        m.insert("running".into(), json!(running));
        m.insert("brokers".into(), json!(self.plans.len()));
        m.insert(
            "brokerKeys".into(),
            json!(self.plans.iter().map(|p| &p.key).collect::<Vec<_>>()),
        );
        m.insert("clients".into(), json!(self.clients.len()));
        m.insert("topicFilters".into(), json!(self.chooser.filters()));
        m.insert("sessions".into(), json!(self.sessions.len()));
        m.insert(
            "commandTopic".into(),
            json!(self.kafka.as_ref().map(|k| &k.command_topic)),
        );
        m.insert("received".into(), json!(c.received.load(Ordering::Relaxed)));
        m.insert(
            "forwarded".into(),
            json!(c.forwarded.load(Ordering::Relaxed)),
        );
        m.insert("dropped".into(), json!(c.dropped.load(Ordering::Relaxed)));
        m.insert(
            "published".into(),
            json!(c.published.load(Ordering::Relaxed)),
        );
        m.insert("skipped".into(), json!(c.skipped.load(Ordering::Relaxed)));
        m
    }
}

/// 单 broker EventLoop task：poll 事件 → 上行分发；错误 → 按配置重连或退出。
///
/// rumqttc 的重连由继续 poll 驱动（EventLoop 内部重建连接），`reconnect_delay` 用于失败后
/// 节流重试（对齐 Java `MqttClientImpl` 的 `TimeUnit.SECONDS` 语义）。
async fn run_broker(
    plan: BrokerPlan,
    mut eventloop: EventLoop,
    client: AsyncClient,
    uplink: Arc<UplinkCtx>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow_and_update() {
                    break;
                }
            }
            polled = eventloop.poll() => match polled {
                Ok(event) => {
                    uplink
                        .handle_event(&event, &client, &plan.topics, plan.qos)
                        .await;
                }
                Err(e) => {
                    tracing::error!(broker = %plan.key, error = %e, "mqtt eventloop 错误");
                    if !plan.reconnect_enabled {
                        tracing::error!(
                            broker = %plan.key,
                            "reconnectEnabled=false，该 broker eventloop 退出"
                        );
                        break;
                    }
                    tracing::info!(
                        broker = %plan.key,
                        delay_secs = plan.reconnect_delay.as_secs(),
                        "等待后重连"
                    );
                    tokio::time::sleep(plan.reconnect_delay).await;
                }
            }
        }
    }
    if let Err(e) = client.disconnect().await {
        tracing::debug!(broker = %plan.key, error = %e, "disconnect 请求失败（连接可能已断开）");
    }
    tracing::info!(broker = %plan.key, "mqtt eventloop task 退出");
}

#[cfg(test)]
mod tests {
    use super::*;
    use connector_common::{LuaProtocol, MockSink, ProtocolConfig};

    const FULL_TOML: &str = r#"
[[configuration]]
clientId = "gw-001"
host = "127.0.0.1"
port = 1883
username = "u"
password = "p"
qos = 1
topics = ["enn/+/up", "enn/+/event"]
reconnectEnabled = true
reconnectDelay = 3
messageMaxBytes = 4096
runMode = "common"
[configuration.protocol]
name = "json_up"
path = "protocols/json_passthrough.lua"
type = "script"
[configuration.protocol.params]
foo = "bar"
"#;

    fn full_cfg() -> MqttConfig {
        toml::from_str(FULL_TOML).unwrap()
    }

    fn kafka() -> Option<KafkaConfig> {
        Some(toml::from_str("bootstrapServers = \"127.0.0.1:9092\"").unwrap())
    }

    /// 四入口齐全的注册表（内联脚本，不读文件，避免测试依赖 CWD）。
    fn registry_with(names: &[&str]) -> Arc<ProtocolRegistry> {
        let mut reg = ProtocolRegistry::new();
        for name in names {
            reg.register(
                LuaProtocol::from_source(
                    name,
                    "function decode(input, params) return {} end\n\
                     function decode_multi(input, params) return {} end\n\
                     function encode(msg, params) return \"\" end\n\
                     function login(req, params) return true end",
                )
                .unwrap(),
            );
        }
        Arc::new(reg)
    }

    #[test]
    fn parses_full_mqtt_config() {
        let cfg = full_cfg();
        assert_eq!(cfg.configuration.len(), 1);
        let s = &cfg.configuration[0];
        assert_eq!(s.client_id, "gw-001");
        assert_eq!(s.host, "127.0.0.1");
        assert_eq!(s.port, 1883);
        assert_eq!(s.qos, Some(1));
        assert_eq!(s.topics, vec!["enn/+/up", "enn/+/event"]);
        assert_eq!(s.reconnect_delay, Some(3));
        assert_eq!(s.message_max_bytes, Some(4096));
        let pc = s.protocol.as_ref().unwrap();
        assert_eq!(pc.name, "json_up");
        assert_eq!(pc.protocol_type, "script");
        assert_eq!(pc.params["foo"], "bar");
    }

    /// 缺必填字段（clientId）→ 解析失败（失败可见）。
    #[test]
    fn rejects_config_missing_required_field() {
        assert!(
            toml::from_str::<MqttConfig>(
                r#"[[configuration]]
host = "h"
port = 1"#
            )
            .is_err()
        );
    }

    #[test]
    fn new_rejects_invalid_broker_config() {
        // clientId 为空
        let mut cfg = full_cfg();
        cfg.configuration[0].client_id = "  ".into();
        let err = MqttConnector::new(cfg, registry_with(&["json_up"]), kafka())
            .err()
            .expect("expected config error");
        assert!(err.to_string().contains("clientId 不能为空"), "{err}");

        // 未配置 protocol
        let mut cfg = full_cfg();
        cfg.configuration[0].protocol = None;
        let err = MqttConnector::new(cfg, registry_with(&["json_up"]), kafka())
            .err()
            .expect("expected config error");
        assert!(err.to_string().contains("未配置 protocol"), "{err}");

        // 协议未在注册表：错误带协议名与 broker key 上下文
        let err = MqttConnector::new(full_cfg(), registry_with(&[]), kafka())
            .err()
            .expect("expected config error");
        let msg = err.to_string();
        assert!(msg.contains("json_up"), "{msg}");
        assert!(msg.contains("gw-001127.0.0.11883"), "{msg}");

        // qos 越界
        let mut cfg = full_cfg();
        cfg.configuration[0].qos = Some(9);
        let err = MqttConnector::new(cfg, registry_with(&["json_up"]), kafka())
            .err()
            .expect("expected config error");
        assert!(err.to_string().contains("qos 非法: 9"), "{err}");
    }

    /// 多 broker：规则按配置顺序汇总，下行协议表每 broker 一项。
    #[test]
    fn new_assembles_multi_broker_rules_and_protocols() {
        let mut cfg = full_cfg();
        let mut second = cfg.configuration[0].clone();
        second.client_id = "gw-002".into();
        second.topics = vec!["enn/#".into()];
        second.protocol = Some(ProtocolConfig {
            name: "hex_down".into(),
            path: "protocols/hex_frame.lua".into(),
            params: json!({}),
            protocol_type: "script".into(),
        });
        cfg.configuration.push(second);

        let conn =
            MqttConnector::new(cfg, registry_with(&["json_up", "hex_down"]), kafka()).unwrap();

        assert_eq!(conn.plans.len(), 2);
        assert_eq!(
            conn.chooser.filters(),
            vec!["enn/+/up", "enn/+/event", "enn/#"]
        );
        assert_eq!(conn.plans[0].qos, QoS::AtLeastOnce);
        assert_eq!(conn.plans[0].reconnect_delay, Duration::from_secs(3));
        assert!(conn.plans[0].reconnect_enabled);
        assert_eq!(conn.plans[1].key, "gw-002127.0.0.11883");
        assert_eq!(conn.downlink.protocols.len(), 2);
        assert_eq!(
            conn.downlink.protocols["gw-001127.0.0.11883"].0.name(),
            "json_up"
        );
        assert_eq!(
            conn.downlink.protocols["gw-002127.0.0.11883"].0.name(),
            "hex_down"
        );
    }

    /// 空 configuration：构造成功（warn），无 broker、无规则。
    #[test]
    fn empty_configuration_constructs_with_warning() {
        let cfg: MqttConfig = toml::from_str("").unwrap();
        let conn = MqttConnector::new(cfg, registry_with(&[]), kafka()).unwrap();
        assert!(conn.plans.is_empty());
        assert!(conn.chooser.is_empty());
        assert_eq!(conn.status()["brokers"], json!(0));
    }

    #[test]
    fn status_snapshot_before_start() {
        let conn = MqttConnector::new(full_cfg(), registry_with(&["json_up"]), kafka()).unwrap();
        let s = conn.status();
        assert_eq!(s["running"], json!(false));
        assert_eq!(s["brokers"], json!(1));
        assert_eq!(s["brokerKeys"], json!(["gw-001127.0.0.11883"]));
        assert_eq!(s["clients"], json!(0));
        assert_eq!(s["topicFilters"], json!(["enn/+/up", "enn/+/event"]));
        assert_eq!(s["sessions"], json!(0));
        assert_eq!(s["commandTopic"], json!("device_command_topic"));
        assert_eq!(s["received"], json!(0));
        assert_eq!(s["skipped"], json!(0));
    }

    /// kafka=None（gateway 统一判据：不可用/未配置）：构造成功，commandTopic 为 null，
    /// start 不 spawn 下行消费（上行不受影响）。
    #[tokio::test]
    async fn none_kafka_disables_downlink() {
        let conn = MqttConnector::new(full_cfg(), registry_with(&["json_up"]), None).unwrap();
        assert_eq!(conn.status()["commandTopic"], json!(null));

        conn.start(Arc::new(MockSink::new())).await.unwrap();
        assert_eq!(conn.status()["running"], json!(true));
        conn.stop().await.unwrap();
    }

    /// 假 broker（127.0.0.1:1 无监听）+ reconnectEnabled=false：start/stop 不 panic，
    /// 状态正确翻转，重复 start 报错（失败可见），stop 幂等。
    #[tokio::test]
    async fn start_stop_with_unreachable_broker() {
        let mut cfg = full_cfg();
        cfg.configuration[0].port = 1;
        cfg.configuration[0].reconnect_enabled = Some(false);
        let conn = MqttConnector::new(cfg, registry_with(&["json_up"]), kafka()).unwrap();

        conn.start(Arc::new(MockSink::new())).await.unwrap();
        let s = conn.status();
        assert_eq!(s["running"], json!(true));
        assert_eq!(s["clients"], json!(1));

        assert!(conn.start(Arc::new(MockSink::new())).await.is_err());

        // 让 eventloop 跑几轮：连接被拒 → reconnectEnabled=false → task 自行退出，不 panic
        tokio::time::sleep(Duration::from_millis(200)).await;

        conn.stop().await.unwrap();
        let s = conn.status();
        assert_eq!(s["running"], json!(false));
        assert_eq!(s["clients"], json!(0));
        conn.stop().await.unwrap();
    }
}
