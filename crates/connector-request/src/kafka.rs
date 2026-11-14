//! Kafka 通道：
//! - 云任务通道 `iot_gateway_trdPlatform_data`（对齐 RequestConnectorStater.listen:99-113，
//!   group `TrdPlatformCloudKafkaConsumer`）：消息 → [`apply_cloud_message`] → 任务增删改；
//! - 下控通道 `device_command_topic`（对齐 DownRequestHandler.listen:60，
//!   group `deviceCommandListenerForRequest`）：OperationRequest → [`handle_command`] → HTTP。
//!
//! 消息 wire 结构对齐 Java `TrdPlatformTaskMessage`（Jackson 命名：id/pcode/taskCode/
//! taskName/apiId/frequency/productId/status/operate，operate 1=ADD 2=UPDATE 3=REMOVE）。
//! Rust 一期扩展：ADD/UPDATE 须内嵌 `task` 字段携带完整任务定义（无 DB，见 manager.rs 头注）。
//!
//! 消费语义：处理完（含拒绝）手动提交 offset，at-least-once（对齐 Spring Kafka BATCH）。
//! 消费循环本身不进单测（spec：真实 Kafka 联调列人工验证清单）；
//! 消息解析与分发逻辑（apply_cloud_message / handle_command）为纯函数层，单测覆盖。

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use connector_common::{CommonError, KafkaConfig, Result};
use rdkafka::ClientConfig;
use rdkafka::Message as KafkaMessage;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use crate::config::RequestTaskConfig;
use crate::manager::TaskManager;
use crate::task::Counters;

/// 云任务消息，对齐 Java `TrdPlatformTaskMessage`（仅保留 Rust 侧消费的字段，
/// 其余 wire 字段如 id/apiId/productId 解析时忽略——Java operateTaskWork 同样不消费）。
/// `task` 为 Rust 一期扩展字段（内嵌任务定义，同 TOML `[[request.configuration]]` 结构）。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CloudTaskMessage {
    /// Jackson 对 `getPCode()` 推断为 "pcode"；alias 兼容 fastjson 侧可能的 "PCode"。
    #[serde(default, alias = "PCode", alias = "pCode")]
    pub pcode: Option<String>,
    #[serde(default)]
    pub task_code: Option<String>,
    #[serde(default)]
    pub task_name: Option<String>,
    /// cron 表达式（Java 用 frequency 注册 xxl-job 调度）。
    #[serde(default)]
    pub frequency: Option<String>,
    /// TaskStatusEnum（1=未启动 2=启动 3=暂停）；Java operateTaskWork 不消费该字段，仅日志。
    #[serde(default)]
    pub status: Option<i32>,
    pub operate: i32,
    #[serde(default)]
    pub task: Option<RequestTaskConfig>,
}

/// 解析并分发一条云任务消息。解析失败 / 缺关键字段 / 非法 operate → Err（失败可见）。
pub(crate) fn apply_cloud_message(payload: &[u8], manager: &TaskManager) -> Result<()> {
    let msg: CloudTaskMessage = serde_json::from_slice(payload)
        .map_err(|e| CommonError::Internal(format!("云任务消息非合法 JSON: {e}")))?;
    let pcode = non_empty(msg.pcode.as_deref(), "pcode")?;
    let task_code = non_empty(msg.task_code.as_deref(), "taskCode")?;
    if let Some(def) = &msg.task {
        def.validate()?;
    }
    info!(
        target: "request",
        "云任务消息: operate={} {pcode}-{task_code} status={:?} taskName={:?}",
        msg.operate,
        msg.status,
        msg.task_name
    );
    manager.apply_operate(
        msg.operate,
        pcode,
        task_code,
        msg.frequency.as_deref(),
        msg.task,
    )
}

fn non_empty<'a>(v: Option<&'a str>, field: &str) -> Result<&'a str> {
    v.map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| CommonError::Internal(format!("云任务消息缺少 {field}")))
}

/// 下控消息目标类型的 wire 名（与 `MessageType::CloudOperationReq` 序列化一致，测试锚定）。
const CLOUD_OPERATION_REQ: &str = "CLOUD_OPERATION_REQ";

/// 处理一条下控消息（对齐 DownRequestHandler.listen:61-136）：
/// 仅 CLOUD_OPERATION_REQ 生效（topic 与 mqtt 等 connector 共享，其余类型跳过）；
/// function → 任务 → encode → URL 替换 → HTTP。
pub(crate) async fn handle_command(payload: &[u8], manager: &TaskManager) -> Result<()> {
    let request: JsonValue = serde_json::from_slice(payload)
        .map_err(|e| CommonError::Internal(format!("下控消息非合法 JSON: {e}")))?;
    let message_type = request
        .get("messageType")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| CommonError::Internal("下控消息缺少 messageType".to_string()))?;
    if message_type != CLOUD_OPERATION_REQ {
        debug!(target: "request", "跳过 messageType={message_type} 的下控消息（非 request connector 目标）");
        return Ok(());
    }
    let function = request
        .get("function")
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            CommonError::Internal(
                "CLOUD_OPERATION_REQ 缺少 function（无法定位下控任务）".to_string(),
            )
        })?;
    let runner = manager.find_by_function(function).ok_or_else(|| {
        CommonError::Internal(format!("没有找到 function '{function}' 对应的下控任务"))
    })?;
    runner.send_command(&request).await
}

fn create_consumer(bootstrap: &str, group: &str, topic: &str) -> Result<StreamConsumer> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "latest")
        .create()
        .map_err(|e| CommonError::Kafka(format!("创建消费者 {group}: {e}")))?;
    consumer
        .subscribe(&[topic])
        .map_err(|e| CommonError::Kafka(format!("{group} 订阅 {topic}: {e}")))?;
    Ok(consumer)
}

/// 消费循环骨架：recv → handle（payload 拷贝，避免消息借用跨 await）→ 手动提交；
/// broker 不可达时 5s 退避重试（失败可见）。
async fn run_consumer_loop<F, Fut>(
    group: &str,
    consumer: StreamConsumer,
    mut shutdown: watch::Receiver<bool>,
    mut handle: F,
) where
    F: FnMut(Vec<u8>) -> Fut + Send,
    Fut: Future<Output = ()> + Send,
{
    loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() {
                    warn!(target: "request", "shutdown 通道已关闭，退出消费 {group}");
                }
                break;
            }
            msg = consumer.recv() => match msg {
                Ok(m) => {
                    let payload = m.payload().unwrap_or_default().to_vec();
                    handle(payload).await;
                    // 处理完（含拒绝）即提交：坏消息重放也不会变好，日志+计数已可见
                    if let Err(e) = consumer.commit_message(&m, CommitMode::Async) {
                        warn!(target: "request", "{group} 提交 offset 失败: {e}");
                    }
                }
                Err(e) => {
                    warn!(target: "request", "{group} 消费失败（5s 后退避重试）: {e}");
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                        changed = shutdown.changed() => { let _ = changed; break; }
                    }
                }
            }
        }
    }
    info!(target: "request", "消费循环退出 {group}");
}

/// 云任务通道消费循环。
pub(crate) async fn run_cloud_consumer(
    kafka: KafkaConfig,
    manager: Arc<TaskManager>,
    counters: Arc<Counters>,
    shutdown: watch::Receiver<bool>,
) {
    let consumer = match create_consumer(
        &kafka.bootstrap_servers,
        "TrdPlatformCloudKafkaConsumer",
        &kafka.cloud_topic,
    ) {
        Ok(c) => c,
        Err(e) => {
            // 启动即失败必须可见：error 日志后退出该通道（不拖垮上行调度）
            error!(target: "request", "云任务通道启动失败: {e}");
            return;
        }
    };
    run_consumer_loop(
        "TrdPlatformCloudKafkaConsumer",
        consumer,
        shutdown,
        move |payload| {
            let manager = manager.clone();
            let counters = counters.clone();
            async move {
                match apply_cloud_message(&payload, &manager) {
                    Ok(()) => {
                        counters.cloud_ok.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        counters.cloud_rejected.fetch_add(1, Ordering::Relaxed);
                        warn!(target: "request", "云任务消息被拒绝: {e}");
                    }
                }
            }
        },
    )
    .await;
}

/// 下控通道消费循环。
pub(crate) async fn run_command_consumer(
    kafka: KafkaConfig,
    manager: Arc<TaskManager>,
    counters: Arc<Counters>,
    shutdown: watch::Receiver<bool>,
) {
    let consumer = match create_consumer(
        &kafka.bootstrap_servers,
        "deviceCommandListenerForRequest",
        &kafka.command_topic,
    ) {
        Ok(c) => c,
        Err(e) => {
            error!(target: "request", "下控通道启动失败: {e}");
            return;
        }
    };
    run_consumer_loop(
        "deviceCommandListenerForRequest",
        consumer,
        shutdown,
        move |payload| {
            let manager = manager.clone();
            let counters = counters.clone();
            async move {
                if let Err(e) = handle_command(&payload, &manager).await {
                    counters.down_failed.fetch_add(1, Ordering::Relaxed);
                    warn!(target: "request", "下控消息处理失败: {e}");
                }
            }
        },
    )
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use connector_common::message::MessageType;

    /// CLOUD_OPERATION_REQ 常量与 MessageType 序列化保持同步。
    #[test]
    fn cloud_operation_req_constant_matches_message_type() {
        assert_eq!(
            serde_json::to_value(MessageType::CloudOperationReq).unwrap(),
            JsonValue::String(CLOUD_OPERATION_REQ.to_string())
        );
    }

    /// Java `TrdPlatformTaskMessage` 序列化形态（Jackson wire 命名）能被解析，
    /// Rust 侧不消费的字段（id/apiId/productId）安全忽略。
    #[test]
    fn parses_java_wire_format() {
        let payload = r#"{
            "id": 42,
            "pcode": "cloud-a",
            "taskCode": "pull-meter",
            "taskName": "电表拉取",
            "apiId": 7,
            "frequency": "0 */5 * * * *",
            "productId": "prod-1",
            "status": 2,
            "operate": 1
        }"#;
        let msg: CloudTaskMessage = serde_json::from_slice(payload.as_bytes()).unwrap();
        assert_eq!(msg.pcode.as_deref(), Some("cloud-a"));
        assert_eq!(msg.task_code.as_deref(), Some("pull-meter"));
        assert_eq!(msg.task_name.as_deref(), Some("电表拉取"));
        assert_eq!(msg.frequency.as_deref(), Some("0 */5 * * * *"));
        assert_eq!(msg.status, Some(2));
        assert_eq!(msg.operate, 1);
        assert!(msg.task.is_none());

        // fastjson 侧可能的大写形态也接受
        let upper = br#"{"PCode":"p","taskCode":"t","operate":3}"#;
        let msg: CloudTaskMessage = serde_json::from_slice(upper).unwrap();
        assert_eq!(msg.pcode.as_deref(), Some("p"));
    }

    /// 缺 operate / 非法 JSON → 解析失败（失败可见）。
    #[test]
    fn rejects_bad_message() {
        assert!(serde_json::from_slice::<CloudTaskMessage>(br#"{"pcode":"p"}"#).is_err());
        assert!(serde_json::from_slice::<CloudTaskMessage>(b"not json").is_err());
    }
}
