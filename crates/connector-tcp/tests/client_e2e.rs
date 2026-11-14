//! tcp client loopback 端到端测试（票 04，Seam A：MockSink 收集上行）。
//!
//! 链路：测试内最小远端 server（TcpListener）→ client 外连 → LINE 拆包 →
//! json_passthrough Lua 协议 → 登录/上行路由（复用票 03 链路）→ MockSink。
//! 覆盖：外连收数上行、断开自动重连恢复上行、写空闲心跳发送。

use std::sync::Arc;
use std::time::Duration;

use connector_common::{
    Connector, MockSink, ProtocolConfig, ProtocolRegistry, TcpConfig, TomlDeviceDirectory,
};
use connector_tcp::TcpConnector;
use serde_json::Value as JsonValue;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const DEVICES: &str = r#"
[[devices]]
projectId = "p1"
sn = "SN001"
deviceId = "dev-001"
password = "pw1"
"#;

const LOGIN_OK: &str = r#"{"messageType":"DEVICE_LOGIN_REQ","sn":"SN001","password":"pw1"}"#;
const REPORT: &str = r#"{"messageType":"DEVICE_REPORT_REQ","deviceId":"dev-001","sn":"SN001","metrics":[{"ts":1,"code":"temp","value":25.5}]}"#;

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

/// client 配置：外连 127.0.0.1:remote_port，LINE 拆包；心跳/重连间隔可变（0 关闭）。
fn client_config(remote_port: u16, heartbeat_interval: i32, reconnect_interval: i64) -> TcpConfig {
    toml::from_str(&format!(
        r#"
[client]
[[client.configuration]]
remoteAddress = "127.0.0.1"
remotePort = {remote_port}
parserType = "LINE"
heartbeatInterval = {heartbeat_interval}
reconnectInterval = {reconnect_interval}
[client.configuration.parserParams]
maxLength = 4096
[client.configuration.protocol]
name = "json_passthrough"
path = "{path}"
type = "script"
[client.configuration.protocol.params]
projectId = "p1"
"#,
        path = script_path()
    ))
    .unwrap()
}

async fn start_client(port: u16, heartbeat: i32, reconnect: i64) -> (TcpConnector, Arc<MockSink>) {
    let sink = Arc::new(MockSink::new());
    let connector = TcpConnector::new(
        client_config(port, heartbeat, reconnect),
        registry(),
        Arc::new(TomlDeviceDirectory::from_toml_str(DEVICES).unwrap()),
    );
    connector.start(sink.clone()).await.unwrap();
    (connector, sink)
}

/// 轮询等待 sink 收集到 n 条消息（上限 8s，重连路径含 1s 重连等待）。
async fn wait_for_messages(sink: &MockSink, n: usize) -> Vec<JsonValue> {
    for _ in 0..400 {
        if sink.len() >= n {
            return sink.messages();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timeout waiting for {n} messages, got {}", sink.len());
}

/// 按序发送帧（LINE 分隔）。
async fn send_frames(sock: &mut TcpStream, frames: &[&str]) {
    for f in frames {
        sock.write_all(f.as_bytes()).await.unwrap();
        sock.write_all(b"\n").await.unwrap();
    }
}

#[tokio::test]
async fn client_connects_and_uplinks_reports() {
    // 远端：接受连接后发登录+上报，随后挂起保持连接
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        send_frames(&mut sock, &[LOGIN_OK, REPORT]).await;
        std::future::pending::<()>().await;
    });

    let (connector, sink) = start_client(port, 0, 0).await;
    let msgs = wait_for_messages(&sink, 1).await;
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0]["messageType"], "DEVICE_REPORT_REQ");
    assert_eq!(msgs[0]["deviceId"], "dev-001");
    assert_eq!(msgs[0]["metrics"][0]["code"], "temp");
    assert_eq!(msgs[0]["metrics"][0]["value"], 25.5);

    // 登录链路复用：会话绑定 + status 快照
    assert_eq!(connector.sessions().len(), 1);
    let s = connector.sessions().get("dev-001").unwrap();
    assert_eq!(s.sn, "SN001");
    let status = connector.status();
    assert_eq!(status["clients"], 1);
    assert_eq!(status["connections"], 1);
    connector.stop().await.unwrap();
}

#[tokio::test]
async fn client_reconnects_after_remote_close_and_resumes_uplink() {
    // 远端：第一条连接立即断开（触发断线重连），第二条发登录+上报后挂起
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (sock1, _) = listener.accept().await.unwrap();
        drop(sock1);
        let (mut sock2, _) = listener.accept().await.unwrap();
        send_frames(&mut sock2, &[LOGIN_OK, REPORT]).await;
        std::future::pending::<()>().await;
    });

    // reconnectInterval=1s：断开后自动重连并恢复上行
    let (connector, sink) = start_client(port, 0, 1).await;
    let msgs = wait_for_messages(&sink, 1).await;
    assert_eq!(msgs[0]["messageType"], "DEVICE_REPORT_REQ");
    assert_eq!(msgs[0]["deviceId"], "dev-001");
    // 重连后新会话重新登录绑定
    assert_eq!(connector.sessions().len(), 1);
    connector.stop().await.unwrap();
}

#[tokio::test]
async fn write_idle_heartbeat_sent_to_remote() {
    // 远端：收集收到的字节（client 无下行，唯一写操作就是心跳）
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        loop {
            match sock.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => {
                    let _ = tx.send(buf[..n].to_vec());
                }
            }
        }
    });

    // heartbeatInterval=1s：写空闲约 1s 后应收到心跳字节（内容 = protocol.encode 输出）
    let (connector, _sink) = start_client(port, 1, 0).await;
    let got = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timeout waiting for heartbeat")
        .unwrap();
    let text = String::from_utf8_lossy(&got);
    assert!(
        text.contains(r#"{"messageType":"HEARTBEAT"}"#),
        "unexpected heartbeat bytes: {text}"
    );
    connector.stop().await.unwrap();
}
