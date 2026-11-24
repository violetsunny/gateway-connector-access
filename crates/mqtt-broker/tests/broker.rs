//! 集成测试：真实 TCP + rumqttc 客户端（白名单内 dev-dependency）验证 broker 行为。
//! 每个测试独立绑定 127.0.0.1:0 临时端口，互不干扰。

use std::sync::Arc;
use std::time::Duration;

use mqtt_broker::codec::{self, Connect, Packet, Will};
use mqtt_broker::{Broker, Config, serve};
use rumqttc::{
    AsyncClient, ConnectReturnCode, ConnectionError, Event, LastWill, MqttOptions,
    Packet as RPacket, Publish, QoS,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

/// 启动 broker，返回端口与 shutdown sender（测试须持有 `_stop` 防止 serve 提前退出）。
async fn spawn_broker(config: Config) -> (u16, watch::Sender<bool>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (stop_tx, stop_rx) = watch::channel(false);
    let broker = Arc::new(Broker::new(config));
    tokio::spawn(serve(listener, broker, stop_rx));
    (port, stop_tx)
}

struct TestClient {
    client: AsyncClient,
    incoming: mpsc::Receiver<Publish>,
    session_present: bool,
    poll: JoinHandle<()>,
}

impl TestClient {
    /// 异常断开：abort poll 任务 + 丢弃 client，不发 DISCONNECT（触发 will）。
    fn abrupt_close(self) {
        self.poll.abort();
    }
}

async fn spawn_client(id: &str, port: u16, tweak: impl FnOnce(&mut MqttOptions)) -> TestClient {
    let mut opts = MqttOptions::new(id, "127.0.0.1", port);
    opts.set_keep_alive(Duration::from_secs(30));
    tweak(&mut opts);
    let (client, mut eventloop) = AsyncClient::new(opts, 64);
    let (pub_tx, incoming) = mpsc::channel(64);
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let poll = tokio::spawn(async move {
        let mut ready = Some(ready_tx);
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(RPacket::ConnAck(ack))) => {
                    if let Some(tx) = ready.take() {
                        let _ = tx.send(ack.session_present);
                    }
                }
                Ok(Event::Incoming(RPacket::Publish(p))) => {
                    let _ = pub_tx.send(p).await;
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });
    let session_present = timeout(Duration::from_secs(5), ready_rx)
        .await
        .expect("connack timeout")
        .expect("connack channel (connection refused?)");
    TestClient {
        client,
        incoming,
        session_present,
        poll,
    }
}

async fn recv(rx: &mut mpsc::Receiver<Publish>) -> Publish {
    timeout(Duration::from_secs(3), rx.recv())
        .await
        .expect("message timeout")
        .expect("channel closed")
}

async fn assert_no_message(rx: &mut mpsc::Receiver<Publish>, within: Duration) {
    match timeout(within, rx.recv()).await {
        Err(_) | Ok(None) => {}
        Ok(Some(m)) => panic!("unexpected message: {m:?}"),
    }
}

fn raw_connect(id: &str) -> Connect {
    Connect {
        protocol_level: 4,
        client_id: id.to_string(),
        clean_session: true,
        keep_alive: 0,
        username: None,
        password: None,
        will: None,
    }
}

/// 裸 TCP 发 CONNECT 读 CONNACK 返回码（测协议层拒绝路径）。
async fn raw_connack(port: u16, connect: Connect) -> u8 {
    let mut sock = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut buf = Vec::new();
    codec::encode(&Packet::Connect(connect), &mut buf);
    sock.write_all(&buf).await.unwrap();
    let mut rd = [0u8; 64];
    let n = timeout(Duration::from_secs(3), sock.read(&mut rd))
        .await
        .expect("connack read timeout")
        .unwrap();
    let mut cb = rd[..n].to_vec();
    match codec::decode(&mut cb).unwrap() {
        Some(Packet::Connack { code, .. }) => code,
        other => panic!("expected connack, got {other:?}"),
    }
}

#[tokio::test]
async fn qos0_pub_sub_wildcard_routing() {
    let (port, _stop) = spawn_broker(Config::default()).await;
    let mut sub = spawn_client("sub", port, |_| {}).await;
    sub.client
        .subscribe("sensor/+/temp", QoS::AtMostOnce)
        .await
        .unwrap();
    let pubc = spawn_client("pub", port, |_| {}).await;
    sleep(Duration::from_millis(100)).await;

    pubc.client
        .publish("sensor/a/temp", QoS::AtMostOnce, false, b"25.5".to_vec())
        .await
        .unwrap();
    let m = recv(&mut sub.incoming).await;
    assert_eq!(m.topic, "sensor/a/temp");
    assert_eq!(&m.payload[..], b"25.5");

    // 不匹配的 topic 不投递
    pubc.client
        .publish("sensor/a/humidity", QoS::AtMostOnce, false, b"60".to_vec())
        .await
        .unwrap();
    assert_no_message(&mut sub.incoming, Duration::from_millis(300)).await;
}

#[tokio::test]
async fn qos1_delivery() {
    let (port, _stop) = spawn_broker(Config::default()).await;
    let mut sub = spawn_client("sub", port, |_| {}).await;
    sub.client.subscribe("q1", QoS::AtLeastOnce).await.unwrap();
    let pubc = spawn_client("pub", port, |_| {}).await;
    sleep(Duration::from_millis(100)).await;

    pubc.client
        .publish("q1", QoS::AtLeastOnce, false, b"payload".to_vec())
        .await
        .unwrap();
    let m = recv(&mut sub.incoming).await;
    assert_eq!(&m.payload[..], b"payload");
    assert_eq!(m.qos, QoS::AtLeastOnce);
}

#[tokio::test]
async fn retained_message_on_late_subscribe() {
    let (port, _stop) = spawn_broker(Config::default()).await;
    let pubc = spawn_client("pub", port, |_| {}).await;
    pubc.client
        .publish("cfg/1", QoS::AtLeastOnce, true, b"v1".to_vec())
        .await
        .unwrap();
    sleep(Duration::from_millis(150)).await;

    let mut sub = spawn_client("sub", port, |_| {}).await;
    sub.client
        .subscribe("cfg/#", QoS::AtMostOnce)
        .await
        .unwrap();
    let m = recv(&mut sub.incoming).await;
    assert_eq!(m.topic, "cfg/1");
    assert_eq!(&m.payload[..], b"v1");
    assert!(m.retain, "retained replay must set retain flag");
}

#[tokio::test]
async fn retained_empty_payload_deletes() {
    let (port, _stop) = spawn_broker(Config::default()).await;
    let pubc = spawn_client("pub", port, |_| {}).await;
    pubc.client
        .publish("cfg/2", QoS::AtLeastOnce, true, b"v1".to_vec())
        .await
        .unwrap();
    sleep(Duration::from_millis(100)).await;
    pubc.client
        .publish("cfg/2", QoS::AtLeastOnce, true, Vec::new())
        .await
        .unwrap();
    sleep(Duration::from_millis(100)).await;

    let mut sub = spawn_client("sub", port, |_| {}).await;
    sub.client
        .subscribe("cfg/2", QoS::AtMostOnce)
        .await
        .unwrap();
    assert_no_message(&mut sub.incoming, Duration::from_millis(300)).await;
}

#[tokio::test]
async fn unsubscribe_stops_delivery() {
    let (port, _stop) = spawn_broker(Config::default()).await;
    let mut sub = spawn_client("sub", port, |_| {}).await;
    sub.client.subscribe("u", QoS::AtMostOnce).await.unwrap();
    let pubc = spawn_client("pub", port, |_| {}).await;
    sleep(Duration::from_millis(100)).await;

    pubc.client
        .publish("u", QoS::AtMostOnce, false, b"first".to_vec())
        .await
        .unwrap();
    assert_eq!(&recv(&mut sub.incoming).await.payload[..], b"first");

    sub.client.unsubscribe("u").await.unwrap();
    sleep(Duration::from_millis(100)).await;
    pubc.client
        .publish("u", QoS::AtMostOnce, false, b"second".to_vec())
        .await
        .unwrap();
    assert_no_message(&mut sub.incoming, Duration::from_millis(300)).await;
}

#[tokio::test]
async fn will_on_abrupt_disconnect() {
    let (port, _stop) = spawn_broker(Config::default()).await;
    let mut watcher = spawn_client("watcher", port, |_| {}).await;
    watcher
        .client
        .subscribe("status/#", QoS::AtMostOnce)
        .await
        .unwrap();
    let a = spawn_client("a", port, |o| {
        o.set_last_will(LastWill::new(
            "status/a",
            b"offline".to_vec(),
            QoS::AtMostOnce,
            false,
        ));
    })
    .await;
    sleep(Duration::from_millis(100)).await;

    a.abrupt_close();
    let m = recv(&mut watcher.incoming).await;
    assert_eq!(m.topic, "status/a");
    assert_eq!(&m.payload[..], b"offline");
}

#[tokio::test]
async fn keepalive_timeout_publishes_will() {
    let (port, _stop) = spawn_broker(Config::default()).await;
    let mut watcher = spawn_client("watcher", port, |_| {}).await;
    watcher
        .client
        .subscribe("status/#", QoS::AtMostOnce)
        .await
        .unwrap();
    sleep(Duration::from_millis(100)).await;

    // 裸客户端：keep_alive=1s + will，CONNECT 后静默，broker 应在 1.5s 后断开并发 will
    let mut sock = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut connect = raw_connect("silent");
    connect.keep_alive = 1;
    connect.will = Some(Will {
        topic: "status/silent".to_string(),
        payload: b"gone".to_vec(),
        qos: 0,
        retain: false,
    });
    let mut buf = Vec::new();
    codec::encode(&Packet::Connect(connect), &mut buf);
    sock.write_all(&buf).await.unwrap();
    let mut rd = [0u8; 64];
    let n = sock.read(&mut rd).await.unwrap();
    let mut cb = rd[..n].to_vec();
    assert!(matches!(
        codec::decode(&mut cb).unwrap(),
        Some(Packet::Connack { code: 0, .. })
    ));

    let m = timeout(Duration::from_secs(4), watcher.incoming.recv())
        .await
        .expect("will timeout (keepalive not enforced?)")
        .unwrap();
    assert_eq!(m.topic, "status/silent");
    assert_eq!(&m.payload[..], b"gone");
}

#[tokio::test]
async fn persistent_session_offline_queue() {
    let (port, _stop) = spawn_broker(Config::default()).await;
    let sub = spawn_client("psub", port, |o| {
        o.set_clean_session(false);
    })
    .await;
    sub.client.subscribe("pq", QoS::AtLeastOnce).await.unwrap();
    sleep(Duration::from_millis(100)).await;
    sub.client.disconnect().await.unwrap();
    sleep(Duration::from_millis(100)).await;
    sub.poll.abort();
    drop(sub.client);
    drop(sub.incoming);

    let pubc = spawn_client("pub", port, |_| {}).await;
    pubc.client
        .publish("pq", QoS::AtLeastOnce, false, b"m1".to_vec())
        .await
        .unwrap();
    sleep(Duration::from_millis(150)).await;

    let mut sub2 = spawn_client("psub", port, |o| {
        o.set_clean_session(false);
    })
    .await;
    assert!(sub2.session_present, "persistent session must resume");
    let m = recv(&mut sub2.incoming).await;
    assert_eq!(&m.payload[..], b"m1");
}

#[tokio::test]
async fn same_client_id_takeover_kicks_old() {
    let (port, _stop) = spawn_broker(Config::default()).await;
    let mut c1 = spawn_client("same", port, |_| {}).await;
    c1.client.subscribe("t", QoS::AtMostOnce).await.unwrap();
    sleep(Duration::from_millis(100)).await;
    let mut c2 = spawn_client("same", port, |_| {}).await;
    c2.client.subscribe("t", QoS::AtMostOnce).await.unwrap();
    sleep(Duration::from_millis(150)).await;
    assert!(c1.poll.is_finished(), "old connection must be kicked");

    let pubc = spawn_client("pub", port, |_| {}).await;
    pubc.client
        .publish("t", QoS::AtMostOnce, false, b"x".to_vec())
        .await
        .unwrap();
    assert_eq!(&recv(&mut c2.incoming).await.payload[..], b"x");
    assert_no_message(&mut c1.incoming, Duration::from_millis(300)).await;
}

#[tokio::test]
async fn auth_rejects_bad_credentials() {
    let config = Config {
        username: Some("admin".to_string()),
        password: Some("secret".to_string()),
        ..Config::default()
    };
    let (port, _stop) = spawn_broker(config).await;
    let mut opts = MqttOptions::new("bad", "127.0.0.1", port);
    opts.set_credentials("admin", "wrong");
    let (_client, mut eventloop) = AsyncClient::new(opts, 16);
    match timeout(Duration::from_secs(5), eventloop.poll())
        .await
        .unwrap()
    {
        Err(ConnectionError::ConnectionRefused(code)) => {
            assert_eq!(code, ConnectReturnCode::BadUserNamePassword);
        }
        other => panic!("expected ConnectionRefused, got {other:?}"),
    }
}

#[tokio::test]
async fn auth_accepts_correct_credentials() {
    let config = Config {
        username: Some("admin".to_string()),
        password: Some("secret".to_string()),
        ..Config::default()
    };
    let (port, _stop) = spawn_broker(config).await;
    let mut sub = spawn_client("sub", port, |o| {
        o.set_credentials("admin", "secret");
    })
    .await;
    sub.client.subscribe("auth", QoS::AtMostOnce).await.unwrap();
    let pubc = spawn_client("pub", port, |o| {
        o.set_credentials("admin", "secret");
    })
    .await;
    sleep(Duration::from_millis(100)).await;
    pubc.client
        .publish("auth", QoS::AtMostOnce, false, b"ok".to_vec())
        .await
        .unwrap();
    assert_eq!(&recv(&mut sub.incoming).await.payload[..], b"ok");
}

#[tokio::test]
async fn bad_protocol_level_refused() {
    let (port, _stop) = spawn_broker(Config::default()).await;
    let mut connect = raw_connect("old");
    connect.protocol_level = 3;
    assert_eq!(raw_connack(port, connect).await, 0x01);
}

#[tokio::test]
async fn empty_client_id_persistent_refused() {
    let (port, _stop) = spawn_broker(Config::default()).await;
    let mut connect = raw_connect("");
    connect.clean_session = false;
    assert_eq!(raw_connack(port, connect).await, 0x02);
}
