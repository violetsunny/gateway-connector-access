//! tcp server loopback 端到端测试（Seam A：MockSink 收集上行）。
//!
//! 链路：真实 TcpStream → LINE 拆包 → json_passthrough Lua 协议 → 登录/上行路由 → MockSink。

use std::sync::Arc;
use std::time::Duration;

use connector_common::{
    Connector, MockSink, ProtocolConfig, ProtocolRegistry, TcpConfig, TomlDeviceDirectory,
};
use connector_tcp::TcpConnector;
use serde_json::Value as JsonValue;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const DEVICES: &str = r#"
[[devices]]
projectId = "p1"
sn = "SN001"
deviceId = "dev-001"
password = "pw1"
"#;

const LOGIN_OK: &str = r#"{"messageType":"DEVICE_LOGIN_REQ","sn":"SN001","password":"pw1"}"#;
const LOGIN_BAD_PW: &str = r#"{"messageType":"DEVICE_LOGIN_REQ","sn":"SN001","password":"wrong"}"#;
const LOGIN_UNKNOWN_SN: &str = r#"{"messageType":"DEVICE_LOGIN_REQ","sn":"NOPE","password":"pw1"}"#;
const REPORT: &str = r#"{"messageType":"DEVICE_REPORT_REQ","deviceId":"dev-001","sn":"SN001","metrics":[{"ts":1,"code":"temp","value":25.5}]}"#;
const REPORT_EMPTY_METRICS: &str =
    r#"{"messageType":"DEVICE_REPORT_REQ","deviceId":"dev-001","sn":"SN001","metrics":[]}"#;

fn script_path() -> String {
    format!(
        "{}/../connector-common/protocols/json_passthrough.lua",
        env!("CARGO_MANIFEST_DIR")
    )
}

fn registry() -> Arc<ProtocolRegistry> {
    let pc = ProtocolConfig {
        name: "json_passthrough".into(),
        path: script_path(),
        params: serde_json::json!({"projectId": "p1"}),
        protocol_type: "script".into(),
    };
    Arc::new(ProtocolRegistry::from_protocols(&[&pc]).unwrap())
}

/// heartbeatInterval 可变的 LINE 拆包 server 配置（port=0 由 OS 分配）。
fn tcp_config(heartbeat_interval: i32) -> TcpConfig {
    toml::from_str(&format!(
        r#"
[server]
[[server.configuration]]
address = "127.0.0.1"
port = 0
parserType = "LINE"
heartbeatInterval = {heartbeat_interval}
[server.configuration.parserParams]
maxLength = 4096
[server.configuration.protocol]
name = "json_passthrough"
path = "{path}"
type = "script"
[server.configuration.protocol.params]
projectId = "p1"
"#,
        path = script_path()
    ))
    .unwrap()
}

async fn start_server(heartbeat_interval: i32) -> (TcpConnector, Arc<MockSink>) {
    let sink = Arc::new(MockSink::new());
    let connector = TcpConnector::new(
        tcp_config(heartbeat_interval),
        registry(),
        Arc::new(TomlDeviceDirectory::from_toml_str(DEVICES).unwrap()),
    );
    connector.start(sink.clone()).await.unwrap();
    (connector, sink)
}

async fn connect(connector: &TcpConnector) -> TcpStream {
    TcpStream::connect(connector.local_addrs()[0])
        .await
        .unwrap()
}

async fn send_line(stream: &mut TcpStream, line: &str) {
    stream.write_all(line.as_bytes()).await.unwrap();
    stream.write_all(b"\n").await.unwrap();
}

/// 轮询等待 sink 收集到 n 条消息（上限 5s）。
async fn wait_for_messages(sink: &MockSink, n: usize) -> Vec<JsonValue> {
    for _ in 0..250 {
        if sink.len() >= n {
            return sink.messages();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timeout waiting for {n} messages, got {}", sink.len());
}

/// 等待一小段时间并断言 sink 保持为空（丢弃路径）。
async fn expect_no_messages(sink: &MockSink) {
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(sink.len(), 0, "unexpected messages: {:?}", sink.messages());
}

/// 等待对端关闭（EOF）。
async fn expect_eof(stream: &mut TcpStream) {
    let mut b = [0u8; 16];
    let n = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut b))
        .await
        .expect("timeout waiting for server close")
        .unwrap();
    assert_eq!(n, 0, "expected EOF, got {n} bytes");
}

#[tokio::test]
async fn login_then_report_reaches_sink() {
    let (connector, sink) = start_server(0).await;
    let mut c = connect(&connector).await;
    send_line(&mut c, LOGIN_OK).await;
    send_line(&mut c, REPORT).await;

    let msgs = wait_for_messages(&sink, 1).await;
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0]["messageType"], "DEVICE_REPORT_REQ");
    assert_eq!(msgs[0]["deviceId"], "dev-001");
    assert_eq!(msgs[0]["metrics"][0]["code"], "temp");
    assert_eq!(msgs[0]["metrics"][0]["value"], 25.5);

    // 会话绑定 + status 快照
    assert_eq!(connector.sessions().len(), 1);
    let s = connector.sessions().get("dev-001").unwrap();
    assert_eq!(s.sn, "SN001");
    let status = connector.status();
    assert_eq!(status["servers"], 1);
    assert_eq!(status["connections"], 1);

    connector.stop().await.unwrap();
    // stop 后连接任务退出，会话清理
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(connector.sessions().len(), 0);
}

#[tokio::test]
async fn login_auth_failure_closes_connection() {
    let (connector, sink) = start_server(0).await;
    // 密码错误
    let mut c = connect(&connector).await;
    send_line(&mut c, LOGIN_BAD_PW).await;
    expect_eof(&mut c).await;
    // sn 不存在
    let mut c2 = connect(&connector).await;
    send_line(&mut c2, LOGIN_UNKNOWN_SN).await;
    expect_eof(&mut c2).await;

    assert_eq!(sink.len(), 0);
    assert_eq!(connector.sessions().len(), 0);
    connector.stop().await.unwrap();
}

#[tokio::test]
async fn data_before_login_dropped_then_login_still_works() {
    let (connector, sink) = start_server(0).await;
    let mut c = connect(&connector).await;
    // 未登录直接上报 → 丢弃（连接保持，偏差修正见 server.rs 模块注释）
    send_line(&mut c, REPORT).await;
    expect_no_messages(&sink).await;
    // 同连接登录后上报 → 上行
    send_line(&mut c, LOGIN_OK).await;
    send_line(&mut c, REPORT).await;
    let msgs = wait_for_messages(&sink, 1).await;
    assert_eq!(msgs[0]["deviceId"], "dev-001");
    connector.stop().await.unwrap();
}

#[tokio::test]
async fn empty_metrics_dropped() {
    let (connector, sink) = start_server(0).await;
    let mut c = connect(&connector).await;
    send_line(&mut c, LOGIN_OK).await;
    send_line(&mut c, REPORT_EMPTY_METRICS).await;
    expect_no_messages(&sink).await;
    connector.stop().await.unwrap();
}

#[tokio::test]
async fn read_idle_timeout_closes_connection() {
    // heartbeatInterval=1s：静默连接应被服务端断开
    let (connector, sink) = start_server(1).await;
    let mut c = connect(&connector).await;
    expect_eof(&mut c).await;
    assert_eq!(sink.len(), 0);
    connector.stop().await.unwrap();
}

#[tokio::test]
async fn protocol_decode_error_closes_connection() {
    let (connector, sink) = start_server(0).await;
    let mut c = connect(&connector).await;
    // json_passthrough 对非法 JSON 抛错 → 对齐 Java exceptionCaught 关闭连接
    send_line(&mut c, "not-json{{").await;
    expect_eof(&mut c).await;
    assert_eq!(sink.len(), 0);
    connector.stop().await.unwrap();
}

#[tokio::test]
async fn response_field_does_not_block_uplink() {
    // 下行不实现：带 response 字段的消息仅 warn，上行照常
    let (connector, sink) = start_server(0).await;
    let mut c = connect(&connector).await;
    send_line(&mut c, LOGIN_OK).await;
    let report = r#"{"messageType":"DEVICE_REPORT_REQ","deviceId":"dev-001","response":"deadbeef","metrics":[{"ts":1,"code":"t","value":1}]}"#;
    send_line(&mut c, report).await;
    let msgs = wait_for_messages(&sink, 1).await;
    assert_eq!(msgs[0]["response"], "deadbeef");
    connector.stop().await.unwrap();
}

#[tokio::test]
async fn start_fails_on_bad_parser_or_protocol_config() {
    let sink = Arc::new(MockSink::new());
    let directory: Arc<TomlDeviceDirectory> =
        Arc::new(TomlDeviceDirectory::from_toml_str(DEVICES).unwrap());

    // 未知 parserType → 启动报错（失败可见）
    let cfg: TcpConfig = toml::from_str(&format!(
        r#"
[server]
[[server.configuration]]
address = "127.0.0.1"
port = 0
parserType = "NOPE"
[server.configuration.protocol]
name = "json_passthrough"
path = "{path}"
type = "script"
"#,
        path = script_path()
    ))
    .unwrap();
    let connector = TcpConnector::new(cfg, registry(), directory.clone());
    assert!(connector.start(sink.clone()).await.is_err());

    // protocol 名不在注册表 → 启动报错
    let cfg2: TcpConfig = toml::from_str(
        r#"
[server]
[[server.configuration]]
address = "127.0.0.1"
port = 0
parserType = "DIRECT"
[server.configuration.protocol]
name = "missing_proto"
path = "/definitely/not/here.lua"
type = "script"
"#,
    )
    .unwrap();
    let connector2 = TcpConnector::new(cfg2, registry(), directory);
    assert!(connector2.start(sink).await.is_err());
}

#[tokio::test]
async fn double_start_rejected() {
    let (connector, _sink) = start_server(0).await;
    let sink2 = Arc::new(MockSink::new());
    assert!(connector.start(sink2).await.is_err());
    connector.stop().await.unwrap();
}
