//! 消息模型，serde 字段名与 Java `cn.enncloud.iot.gateway.message.*` 逐一对齐。
//!
//! Java 用继承（子类 extends Message），Rust 用 `#[serde(flatten)]` 把基类字段平铺进子类，
//! 序列化出的 JSON 字段名与 Java（Lombok @Data + Jackson）一致（camelCase）。
//!
//! 注意：Lua 协议引擎与 DeviceSink 之间流动的「wire message」是 `serde_json::Value`
//! （Lua 返回的动态 table，等价于 Java 多态 Message 序列化后的扁平 JSON 对象）。
//! 本模块的强类型结构体用于：(1) serde 字段名对齐断言；(2) Rust 侧构造下行消息（encode）。

use serde::{Deserialize, Serialize};

/// 消息类型，序列化为 Java 同名大写下划线字符串（如 `DEVICE_REPORT_REQ`）。
///
/// 变体用 CamelCase，经 `SCREAMING_SNAKE_CASE` 重命名后与 Java 枚举名逐字一致。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MessageType {
    Heartbeat,
    DeviceLoginReq,
    DeviceLoginRsp,
    DeviceReportReq,
    DeviceReportRsp,
    DeviceStatusReq,
    DeviceInfoReq,
    DeviceEventReq,
    DeviceNtpReq,
    DeviceNtpRsp,
    CloudOperationReq,
    CloudOperationRsp,
    CloudHistoryReq,
    CloudNtpReq,
    CloudNtpRsp,
}

/// 消息基类，对齐 Java `Message`。子类通过 `#[serde(flatten)]` 内嵌本结构。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sn: Option<String>,
    pub message_type: MessageType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response: Option<String>,
}

impl Message {
    /// 构造仅含 messageType 的基类消息（其余字段为空），对齐 Java 子类构造器设置 messageType 的行为。
    pub fn new(message_type: MessageType) -> Self {
        Self {
            message_id: None,
            device_id: None,
            sn: None,
            message_type,
            response: None,
        }
    }
}

/// 单个指标点，对齐 Java `Metric`（`long ts; String code; Object value`）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Metric {
    pub ts: i64,
    pub code: String,
    pub value: serde_json::Value,
}

fn default_resume() -> String {
    "N".to_string()
}

/// 指标上报，对齐 Java `MetricReportRequest extends Message`（messageType=DEVICE_REPORT_REQ）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricReportRequest {
    #[serde(flatten)]
    pub base: Message,
    pub metrics: Vec<Metric>,
    #[serde(default = "default_resume")]
    pub resume: String,
    pub time_stamp: i64,
    pub ingestion_time: i64,
    #[serde(default)]
    pub change_rpt: bool,
}

/// 事件上报，对齐 Java `EventReportRequest extends Message`（messageType=DEVICE_EVENT_REQ）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventReportRequest {
    #[serde(flatten)]
    pub base: Message,
    pub metrics: Vec<Metric>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_stamp: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier: Option<String>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub event_type: Option<String>,
}

/// 信息上报，对齐 Java `InfoReportRequest extends Message`（messageType=DEVICE_INFO_REQ）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InfoReportRequest {
    #[serde(flatten)]
    pub base: Message,
    pub data: serde_json::Value,
    pub time_stamp: i64,
}

/// 登录请求，对齐 Java `LoginRequest extends Message`（messageType=DEVICE_LOGIN_REQ）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoginRequest {
    #[serde(flatten)]
    pub base: Message,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
}

/// 登录响应，对齐 Java `LoginResponse extends Message`（messageType=DEVICE_LOGIN_RSP）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoginResponse {
    #[serde(flatten)]
    pub base: Message,
    pub login: bool,
    pub time_stamp: i64,
}

/// 云端操作请求，对齐 Java `OperationRequest extends Message`（messageType=CLOUD_OPERATION_REQ）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationRequest {
    #[serde(flatten)]
    pub base: Message,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub param: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function: Option<String>,
    pub time_stamp: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
}

/// 云端操作响应，对齐 Java `OperationResponse extends Message`（messageType=CLOUD_OPERATION_RSP）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationResponse {
    #[serde(flatten)]
    pub base: Message,
    pub result: bool,
    pub time_stamp: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// 断言 MetricReportRequest 序列化出的 JSON 字段名与 Java 一致（含基类平铺字段）。
    #[test]
    fn metric_report_request_field_names_align_with_java() {
        let m = MetricReportRequest {
            base: Message {
                message_id: Some("mid-1".into()),
                device_id: Some("dev-1".into()),
                sn: Some("sn-1".into()),
                message_type: MessageType::DeviceReportReq,
                response: None,
            },
            metrics: vec![Metric {
                ts: 100,
                code: "temp".into(),
                value: json!(25.5),
            }],
            resume: "N".into(),
            time_stamp: 1000,
            ingestion_time: 2000,
            change_rpt: false,
        };
        let v = serde_json::to_value(&m).unwrap();
        assert_eq!(v["messageId"], "mid-1");
        assert_eq!(v["deviceId"], "dev-1");
        assert_eq!(v["sn"], "sn-1");
        assert_eq!(v["messageType"], "DEVICE_REPORT_REQ");
        assert_eq!(v["resume"], "N");
        assert_eq!(v["timeStamp"], 1000);
        assert_eq!(v["ingestionTime"], 2000);
        assert_eq!(v["changeRpt"], false);
        assert_eq!(v["metrics"][0]["ts"], 100);
        assert_eq!(v["metrics"][0]["code"], "temp");
        assert_eq!(v["metrics"][0]["value"], 25.5);
        // response 为 None 时不出现（skip_serializing_if）
        assert!(v.get("response").is_none());
    }

    /// 断言 MessageType 15 个枚举值序列化为 Java 同名大写下划线字符串。
    #[test]
    fn message_type_serializes_to_java_names() {
        let cases = [
            (MessageType::Heartbeat, "HEARTBEAT"),
            (MessageType::DeviceLoginReq, "DEVICE_LOGIN_REQ"),
            (MessageType::DeviceLoginRsp, "DEVICE_LOGIN_RSP"),
            (MessageType::DeviceReportReq, "DEVICE_REPORT_REQ"),
            (MessageType::DeviceReportRsp, "DEVICE_REPORT_RSP"),
            (MessageType::DeviceStatusReq, "DEVICE_STATUS_REQ"),
            (MessageType::DeviceInfoReq, "DEVICE_INFO_REQ"),
            (MessageType::DeviceEventReq, "DEVICE_EVENT_REQ"),
            (MessageType::DeviceNtpReq, "DEVICE_NTP_REQ"),
            (MessageType::DeviceNtpRsp, "DEVICE_NTP_RSP"),
            (MessageType::CloudOperationReq, "CLOUD_OPERATION_REQ"),
            (MessageType::CloudOperationRsp, "CLOUD_OPERATION_RSP"),
            (MessageType::CloudHistoryReq, "CLOUD_HISTORY_REQ"),
            (MessageType::CloudNtpReq, "CLOUD_NTP_REQ"),
            (MessageType::CloudNtpRsp, "CLOUD_NTP_RSP"),
        ];
        for (mt, expected) in cases {
            assert_eq!(serde_json::to_value(mt).unwrap(), json!(expected));
            // 反向往返一致
            let back: MessageType = serde_json::from_value(json!(expected)).unwrap();
            assert_eq!(back, mt);
        }
    }

    /// EventReportRequest 的 `type` 字段（Rust 关键字，重命名）序列化为 Java 的 "type"。
    #[test]
    fn event_report_request_type_field_aligns() {
        let e = EventReportRequest {
            base: Message::new(MessageType::DeviceEventReq),
            metrics: vec![],
            time_stamp: Some(5),
            version: Some("v1".into()),
            identifier: Some("id-1".into()),
            event_type: Some("alarm".into()),
        };
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["messageType"], "DEVICE_EVENT_REQ");
        assert_eq!(v["timeStamp"], 5);
        assert_eq!(v["identifier"], "id-1");
        assert_eq!(v["type"], "alarm");
    }

    /// LoginResponse / OperationResponse 的布尔与时间戳字段名对齐。
    #[test]
    fn response_messages_align() {
        let lr = LoginResponse {
            base: Message::new(MessageType::DeviceLoginRsp),
            login: true,
            time_stamp: 7,
        };
        let v = serde_json::to_value(&lr).unwrap();
        assert_eq!(v["messageType"], "DEVICE_LOGIN_RSP");
        assert_eq!(v["login"], true);
        assert_eq!(v["timeStamp"], 7);

        let or = OperationResponse {
            base: Message::new(MessageType::CloudOperationRsp),
            result: false,
            time_stamp: 9,
        };
        let v = serde_json::to_value(&or).unwrap();
        assert_eq!(v["messageType"], "CLOUD_OPERATION_RSP");
        assert_eq!(v["result"], false);
        assert_eq!(v["timeStamp"], 9);
    }
}
