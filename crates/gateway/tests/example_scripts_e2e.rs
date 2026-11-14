//! 票 11 集成测试：交付的示例 Lua 脚本经真实 connector 链路可被调用并上行。
//!
//! 链路选择：http 通配路由 `POST /api/{pkey}/**`（无登录要求，body 即协议输入）。
//! - `hex_frame.lua`：POST 3 字节二进制帧 [0x01,0x00,0x2a] → DEVICE_REPORT_REQ，value=42。
//! - `json_passthrough.lua`：POST JSON 报文 → 原样透传上行。
//!
//! json_passthrough 经 tcp 链路（含登录）由 connector-tcp 的 server_e2e/client_e2e 覆盖；
//! hex_frame 无法经 tcp 链路上行（tcp 要求先登录，而该脚本不产生登录帧），故此处走 http。

use std::sync::Arc;

use connector_common::{Connector, DeviceSink, MockSink, ProtocolConfig};
use connector_http::{HttpConfig, HttpConnector, HttpRouteConfig};
use serde_json::{Value as JsonValue, json};

fn protocol(name: &str, relative_path: &str, params: JsonValue) -> ProtocolConfig {
    ProtocolConfig {
        name: name.into(),
        // 示例脚本是 workspace 内交付物，路径相对 gateway crate 定位到源树。
        path: format!("{}/{relative_path}", env!("CARGO_MANIFEST_DIR")),
        params,
        protocol_type: "script".into(),
    }
}

/// 启动 http connector（port=0），返回 connector、sink 与真实监听地址。
async fn start_http() -> (HttpConnector, Arc<MockSink>, String) {
    let config = HttpConfig {
        address: "127.0.0.1".into(),
        port: 0,
        configuration: vec![
            HttpRouteConfig {
                request_path: "productHex".into(),
                protocol: protocol(
                    "hex_frame",
                    "../connector-common/protocols/hex_frame.lua",
                    json!({"deviceId": "dev-001", "sn": "SN001"}),
                ),
            },
            HttpRouteConfig {
                request_path: "productJson".into(),
                protocol: protocol(
                    "json_passthrough",
                    "../connector-common/protocols/json_passthrough.lua",
                    json!({}),
                ),
            },
        ],
        std_protocol: protocol(
            "enn-standard-http",
            "../connector-http/protocols/std_http.lua",
            json!({}),
        ),
    };
    let connector = HttpConnector::new(config, None).expect("build http connector");
    let sink = Arc::new(MockSink::new());
    connector
        .start(sink.clone() as Arc<dyn DeviceSink>)
        .await
        .expect("start http connector");
    let listening = connector.status()["listening"]
        .as_str()
        .expect("listening addr")
        .to_string();
    (connector, sink, listening)
}

/// hex_frame.lua：3 字节帧 [0x01,0x00,0x2a] → big-endian uint16 = 42，
/// deviceId/sn 来自协议 params，messageType 可路由上行。
#[tokio::test]
async fn hex_frame_script_decodes_binary_post_to_metric() {
    let (connector, sink, listening) = start_http().await;

    let resp = reqwest::Client::new()
        .post(format!("http://{listening}/api/productHex/data"))
        .body(vec![0x01u8, 0x00, 0x2a])
        .send()
        .await
        .expect("post hex frame");
    assert_eq!(resp.status(), 200);
    // reqwest 未开 json feature：手动解析响应信封。
    let envelope: JsonValue =
        serde_json::from_str(&resp.text().await.expect("response text")).expect("json envelope");
    assert_eq!(envelope["success"], true);

    let msgs = sink.messages();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0]["messageType"], "DEVICE_REPORT_REQ");
    assert_eq!(msgs[0]["deviceId"], "dev-001");
    assert_eq!(msgs[0]["sn"], "SN001");
    assert_eq!(msgs[0]["metrics"][0]["value"], 42);

    connector.stop().await.expect("stop http connector");
}

/// json_passthrough.lua：JSON 报文原样透传上行（input.json 字节形态还原字符串）。
#[tokio::test]
async fn json_passthrough_script_forwards_json_post() {
    let (connector, sink, listening) = start_http().await;

    let report = json!({
        "messageType": "DEVICE_REPORT_REQ",
        "deviceId": "dev-002",
        "sn": "SN002",
        "metrics": [{"ts": 1700000000000_i64, "code": "temp", "value": 25.5}],
    });
    let resp = reqwest::Client::new()
        .post(format!("http://{listening}/api/productJson"))
        .header("content-type", "application/json")
        .body(serde_json::to_vec(&report).expect("serialize report"))
        .send()
        .await
        .expect("post json report");
    assert_eq!(resp.status(), 200);

    let msgs = sink.messages();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0], report);

    connector.stop().await.expect("stop http connector");
}
