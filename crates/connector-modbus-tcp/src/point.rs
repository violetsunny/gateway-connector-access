//! 点表 / 点位-设备映射 DTO 与拉取——Java `ModbusConnectorStater.getModbusPoints /
//! getPointMap / buildPointMap`(:134-188) 的等价实现。
//!
//! 与 Java 的差异（失败可见方向）：
//! - Java 拉取/解析失败 log.warn + 跳过该 server；Rust 一律返回 Err，connector 拒绝启动。
//! - Java 仅 cloud-gateway URL 下 slaveAddr null→1；Rust 一律 serde 默认 1
//!   （非 cloud-gateway 点表缺 slaveAddr 属脏数据，默认 1 与 Java 实际部署行为一致）。
//! - `DevicePointMap` 只建模读路径用到的 pointId/deviceId/metric；rw/productCode/productId
//!   仅用于 Java 死代码写路径（encodeMulti），不建模（serde 默认忽略未知字段）。
//! - deviceId/metric 为必填：Java 容忍 null 并上报 null deviceId 的垃圾消息，
//!   Rust 在启动解析时即报错。

use std::collections::HashMap;
use std::str::FromStr;

use connector_common::{CommonError, Result};
use serde::Deserialize;
use serde_json::Value as JsonValue;

use crate::datatype::{ByteOrder, DataType};

/// 读功能码，对齐 Java `ModbusFCode`（写码 0x05/0x06/0x0F/0x10 属死代码下行，不建模）。
pub const READ_COIL_STATUS: u8 = 0x01;
pub const READ_INPUT_STATUS: u8 = 0x02;
pub const READ_HOLDING_REGISTER: u8 = 0x03;
pub const READ_INPUT_REGISTER: u8 = 0x04;

fn default_slave_addr() -> u8 {
    1
}

/// 点表条目，对齐 Java `ModbusPointDTO`（Long/Integer → i64/u8/u16，越界即解析报错）。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModbusPoint {
    pub point_id: i64,
    #[serde(default = "default_slave_addr")]
    pub slave_addr: u8,
    pub function_code: u8,
    pub register_address: u16,
    pub data_type: String,
    pub byte_order: String,
}

/// 点位-设备映射条目，对齐 Java `DevicePointMapDTO`（仅读路径字段，见模块头）。
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DevicePointMap {
    pub point_id: i64,
    pub device_id: String,
    pub metric: String,
}

/// 校验完成的轮询点：dataType/byteOrder/功能码在启动时解析定案
/// （Java `DataTypeEnum/ByteOrderEnum.getInstance` 抛异常等价的 fail-fast）。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PollPoint {
    pub point_id: i64,
    pub slave_addr: u8,
    pub function_code: u8,
    pub register_address: u16,
    pub data_type: DataType,
    pub byte_order: ByteOrder,
}

/// cloud-gateway 包装解壳：URL 含 "cloud-gateway" 时响应为 `{"data": ...}` 包装，
/// data 可为 JSON 数组或字符串化 JSON 数组（Java hutool `getStr("data")` + `toList`
/// 对两种形态同样处理）；否则响应体本身就是 JSON 数组。
fn extract_list(url: &str, body: &str) -> Result<JsonValue> {
    let parse = |s: &str| {
        serde_json::from_str::<JsonValue>(s)
            .map_err(|e| CommonError::Config(format!("点表响应不是合法 JSON: {e}")))
    };
    if !url.contains("cloud-gateway") {
        return parse(body);
    }
    let wrapper = parse(body)?;
    let data = wrapper
        .get("data")
        .ok_or_else(|| CommonError::Config(format!("cloud-gateway 响应缺 data 字段: {url}")))?;
    match data {
        JsonValue::String(s) => parse(s),
        other => Ok(other.clone()),
    }
}

/// 响应体 → 校验完成的轮询点列表。
///
/// 未知 dataType/byteOrder、非读功能码（Java respHandler 只认 1/2/3/4）→ Err（fail-fast）。
/// 与 Java 的差异：Java `POINT_CACHE` 以 registerAddress 为键，同地址点相互覆盖
/// （同寄存器多个 bit 点只剩一个，属缺陷）；Rust 按列表全量轮询，不去重。
pub fn parse_points(url: &str, body: &str) -> Result<Vec<PollPoint>> {
    let list = extract_list(url, body)?;
    let raw: Vec<ModbusPoint> = serde_json::from_value(list)
        .map_err(|e| CommonError::Config(format!("modbus 点表解析失败: {e}")))?;
    raw.into_iter()
        .map(|p| {
            if !matches!(
                p.function_code,
                READ_COIL_STATUS | READ_INPUT_STATUS | READ_HOLDING_REGISTER | READ_INPUT_REGISTER
            ) {
                return Err(CommonError::Config(format!(
                    "点位 {}: 不支持的功能码 {}（仅支持读码 1/2/3/4）",
                    p.point_id, p.function_code
                )));
            }
            Ok(PollPoint {
                point_id: p.point_id,
                slave_addr: p.slave_addr,
                function_code: p.function_code,
                register_address: p.register_address,
                data_type: DataType::from_str(&p.data_type)?,
                byte_order: ByteOrder::from_str(&p.byte_order)?,
            })
        })
        .collect()
}

/// 响应体 → pointId → 设备映射表（Java `buildPointMap`：HashMap.put，后项覆盖前项）。
pub fn parse_point_map(url: &str, body: &str) -> Result<HashMap<i64, DevicePointMap>> {
    let list = extract_list(url, body)?;
    let raw: Vec<DevicePointMap> = serde_json::from_value(list)
        .map_err(|e| CommonError::Config(format!("点位设备映射解析失败: {e}")))?;
    Ok(raw.into_iter().map(|m| (m.point_id, m)).collect())
}

/// GET 点表 URL，返回响应体。网络错误 / 非 2xx → Err
/// （Java log.warn+跳过 → Rust 失败可见，start() 拒绝启动）。
pub async fn fetch_body(client: &reqwest::Client, url: &str) -> Result<String> {
    let resp = client
        .get(url)
        .send()
        .await
        .map_err(|e| CommonError::Internal(format!("点表拉取 {url} 失败: {e}")))?;
    let status = resp.status();
    let body = resp
        .text()
        .await
        .map_err(|e| CommonError::Internal(format!("点表 {url} 响应读取失败: {e}")))?;
    if !status.is_success() {
        return Err(CommonError::Internal(format!("点表 {url} 返回 {status}")));
    }
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const PLAIN_URL: &str = "http://10.0.0.1:8080/modbus/points";
    const CLOUD_URL: &str = "https://gw.example.com/access/cloud-gateway/x/modbus/points";

    fn point_json(point_id: i64, code: u8, addr: u16, dt: &str, bo: &str) -> JsonValue {
        json!({
            "pointId": point_id,
            "slaveAddr": 2,
            "functionCode": code,
            "registerAddress": addr,
            "dataType": dt,
            "byteOrder": bo,
        })
    }

    // ---- 点表解析（Seam C）----

    /// 普通 URL：响应体即数组；字段逐一映射，dataType/byteOrder 解析定案。
    #[test]
    fn parses_plain_array_body() {
        let body = json!([
            point_json(101, READ_HOLDING_REGISTER, 100, "int32", "1234"),
            point_json(102, READ_INPUT_REGISTER, 103, "float64", "3412"),
        ])
        .to_string();
        let points = parse_points(PLAIN_URL, &body).unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(
            points[0],
            PollPoint {
                point_id: 101,
                slave_addr: 2,
                function_code: READ_HOLDING_REGISTER,
                register_address: 100,
                data_type: DataType::Int32,
                byte_order: ByteOrder {
                    byte_swap: false,
                    word_swap: false
                },
            }
        );
        assert_eq!(points[1].data_type, DataType::Float64);
        assert_eq!(points[1].register_address, 103);
    }

    /// slaveAddr 缺失 → 默认 1（Java cloud-gateway 路径的 null→1 行为推广到全部 URL）。
    #[test]
    fn defaults_slave_addr_to_one() {
        let body = json!([{
            "pointId": 1,
            "functionCode": 3,
            "registerAddress": 0,
            "dataType": "uint16",
            "byteOrder": "1234",
        }])
        .to_string();
        let points = parse_points(PLAIN_URL, &body).unwrap();
        assert_eq!(points[0].slave_addr, 1);
    }

    /// cloud-gateway URL：data 为 JSON 数组或字符串化数组均可（hutool getStr 等价）。
    #[test]
    fn unwraps_cloud_gateway_data_field() {
        let inner = json!([point_json(7, READ_COIL_STATUS, 0, "bit0", "1234")]);
        // data = 数组
        let body = json!({"code": 0, "data": inner}).to_string();
        let points = parse_points(CLOUD_URL, &body).unwrap();
        assert_eq!(points[0].point_id, 7);
        assert_eq!(points[0].data_type, DataType::Bit(0));
        // data = 字符串化数组
        let body = json!({"code": 0, "data": inner.to_string()}).to_string();
        let points = parse_points(CLOUD_URL, &body).unwrap();
        assert_eq!(points[0].point_id, 7);
    }

    /// cloud-gateway 响应缺 data / 非法 JSON → 报错。
    #[test]
    fn rejects_bad_cloud_response() {
        let err = parse_points(CLOUD_URL, r#"{"code":500}"#).unwrap_err();
        assert!(err.to_string().contains("缺 data 字段"), "{err}");

        let err = parse_points(PLAIN_URL, "not json").unwrap_err();
        assert!(err.to_string().contains("不是合法 JSON"), "{err}");
    }

    /// 未知 dataType（含 Java 死类型 int24）/ 未知 byteOrder / 非读功能码 → fail-fast。
    #[test]
    fn rejects_bad_point_fields() {
        let body = json!([point_json(1, 3, 0, "int24", "1234")]).to_string();
        let err = parse_points(PLAIN_URL, &body).unwrap_err();
        assert!(err.to_string().contains("未知的数据类型"), "{err}");

        let body = json!([point_json(1, 3, 0, "int32", "9999")]).to_string();
        let err = parse_points(PLAIN_URL, &body).unwrap_err();
        assert!(err.to_string().contains("未知的字节序"), "{err}");

        // Java ModbusFCode.WRITE_SINGLE_REGISTER=0x06（写码属死代码下行，读路径拒绝）
        let body = json!([point_json(1, 0x06, 0, "int32", "1234")]).to_string();
        let err = parse_points(PLAIN_URL, &body).unwrap_err();
        assert!(err.to_string().contains("不支持的功能码"), "{err}");
    }

    // ---- 点位-设备映射解析（Seam C）----

    /// 映射表解析：未知字段（rw/productCode/productId）忽略；重复 pointId 后项覆盖前项。
    #[test]
    fn parses_point_map_and_last_wins() {
        let body = json!([
            {"pointId": 1, "deviceId": "dev-a", "metric": "power",
             "rw": "r", "productCode": "pc", "productId": "pid"},
            {"pointId": 1, "deviceId": "dev-b", "metric": "energy"},
            {"pointId": 2, "deviceId": "dev-c", "metric": "temp"},
        ])
        .to_string();
        let map = parse_point_map(PLAIN_URL, &body).unwrap();
        assert_eq!(map.len(), 2);
        assert_eq!(
            map[&1],
            DevicePointMap {
                point_id: 1,
                device_id: "dev-b".into(),
                metric: "energy".into(),
            }
        );
        assert_eq!(map[&2].device_id, "dev-c");
    }

    /// deviceId/metric 缺失或 null → 解析报错（Java 容忍 null 上报垃圾消息，Rust fail-fast）。
    #[test]
    fn rejects_incomplete_point_map() {
        let body = json!([{"pointId": 1, "metric": "power"}]).to_string();
        assert!(parse_point_map(PLAIN_URL, &body).is_err());
        let body = json!([{"pointId": 1, "deviceId": null, "metric": "p"}]).to_string();
        assert!(parse_point_map(PLAIN_URL, &body).is_err());
    }

    // ---- HTTP 拉取（axum 假服务）----

    /// 200 → 响应体；500 → Err（失败可见）。
    #[tokio::test]
    async fn fetch_body_checks_status() {
        let app = axum::Router::new()
            .route("/ok", axum::routing::get(|| async { "[1,2]" }))
            .route(
                "/bad",
                axum::routing::get(|| async {
                    (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "boom")
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let client = reqwest::Client::new();
        let body = fetch_body(&client, &format!("http://{addr}/ok"))
            .await
            .unwrap();
        assert_eq!(body, "[1,2]");

        let err = fetch_body(&client, &format!("http://{addr}/bad"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("500"), "{err}");

        // 连接拒绝 → Err
        let err = fetch_body(&client, "http://127.0.0.1:1/x")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("失败"), "{err}");
    }
}
