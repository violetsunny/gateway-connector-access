//! broker 共享状态：会话 / 订阅 / retained / topic 路由 / QoS1 inflight。
//!
//! ponytail: 单全局 Mutex，demo 级吞吐可接受；升级路径：topic 分片或无锁订阅树。
//! 资源上限（防恶意客户端耗尽内存）：每会话订阅数 [`MAX_SUBSCRIPTIONS`]、
//! 离线队列 [`MAX_OFFLINE_QUEUE`]、inflight [`MAX_INFLIGHT`]、retained 总数 [`MAX_RETAINED`]。

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

use tokio::sync::{mpsc, watch};

use crate::Config;
use crate::codec::{Connect, Packet, Publish, Will};
use crate::topic;

const MAX_SUBSCRIPTIONS: usize = 1024;
const MAX_OFFLINE_QUEUE: usize = 1000;
const MAX_INFLIGHT: usize = 1024;
const MAX_RETAINED: usize = 10_000;

/// 单连接出站通道容量；写满即断开慢消费者（失败可见，不静默丢）。
pub const OUTBOUND_CAP: usize = 1024;

pub struct Broker {
    inner: Mutex<Inner>,
    username: Option<String>,
    password: Option<String>,
    conn_seq: AtomicU64,
    auto_seq: AtomicU64,
}

struct Inner {
    sessions: HashMap<String, Session>,
    retained: HashMap<String, Publish>,
}

struct Session {
    clean_session: bool,
    subs: HashMap<String, u8>,
    will: Option<Will>,
    online: Option<Online>,
    /// 持久会话离线期间的 QoS1 消息（重连后下发）。
    offline: VecDeque<Publish>,
    /// 已发未 PUBACK 的 QoS1（重连后 dup 重发）。BTreeMap 保证重发按 packet id 有序。
    inflight: BTreeMap<u16, Publish>,
    next_id: u16,
}

struct Online {
    conn_id: u64,
    tx: mpsc::Sender<Packet>,
    /// broker 侧丢弃即通知 reader 关闭连接（接管/慢消费者），sender 丢弃 = changed() 返回 Err。
    #[expect(dead_code, reason = "RAII 信号：仅靠 Drop 通知 reader，无需读取")]
    kick: watch::Sender<bool>,
}

impl Session {
    fn new(clean_session: bool) -> Self {
        Self {
            clean_session,
            subs: HashMap::new(),
            will: None,
            online: None,
            offline: VecDeque::new(),
            inflight: BTreeMap::new(),
            next_id: 0,
        }
    }

    fn next_packet_id(&mut self) -> u16 {
        loop {
            self.next_id = if self.next_id == u16::MAX {
                1
            } else {
                self.next_id + 1
            };
            if !self.inflight.contains_key(&self.next_id) {
                return self.next_id;
            }
        }
    }

    fn queue_offline(&mut self, client_id: &str, msg: &Publish) {
        if self.offline.len() >= MAX_OFFLINE_QUEUE {
            self.offline.pop_front();
            tracing::warn!(
                client_id,
                "offline queue full (cap {MAX_OFFLINE_QUEUE}), oldest message dropped"
            );
        }
        let mut m = msg.clone();
        m.qos = 1;
        m.dup = false;
        m.retain = false;
        m.packet_id = None;
        self.offline.push_back(m);
    }

    /// 向本会话投递一条路由消息，qos = min(订阅 qos, 消息 qos)。
    /// 慢消费者被断开时返回其 will（调用方释放锁后发布，避免重入死锁）。
    fn deliver(&mut self, client_id: &str, msg: &Publish, qos: u8) -> Option<Publish> {
        let Some(tx) = self.online.as_ref().map(|o| o.tx.clone()) else {
            if !self.clean_session && qos == 1 {
                self.queue_offline(client_id, msg);
            }
            return None;
        };
        let mut out = msg.clone();
        out.dup = false;
        out.retain = false;
        out.qos = qos;
        out.packet_id = None;
        let mut inflight_id = None;
        if qos == 1 {
            if self.inflight.len() >= MAX_INFLIGHT {
                tracing::warn!(
                    client_id,
                    "inflight full (cap {MAX_INFLIGHT}), QoS1 message dropped"
                );
                return None;
            }
            let id = self.next_packet_id();
            out.packet_id = Some(id);
            self.inflight.insert(id, out.clone());
            inflight_id = Some(id);
        }
        if let Err(e) = tx.try_send(Packet::Publish(out)) {
            tracing::warn!(
                client_id,
                "outbound channel unavailable ({e}), disconnecting slow/dead consumer"
            );
            if let Some(id) = inflight_id {
                self.inflight.remove(&id);
            }
            // 丢弃 Online（含 kick sender）→ reader 关闭连接；持久会话转离线队列
            self.online = None;
            if !self.clean_session && qos == 1 {
                self.queue_offline(client_id, msg);
            }
            return self.will.take().map(will_to_publish);
        }
        None
    }
}

fn will_to_publish(w: Will) -> Publish {
    Publish {
        dup: false,
        qos: w.qos,
        retain: w.retain,
        topic: w.topic,
        packet_id: None,
        payload: w.payload,
    }
}

/// CONNECT 处理结果。
pub enum ConnectOutcome {
    Accepted {
        client_id: String,
        session_present: bool,
        keep_alive: u16,
        /// CONNACK 之后下发：inflight 重发（dup=true）+ 离线队列。
        pending: Vec<Publish>,
    },
    /// CONNACK 返回码（0x01 协议版本 / 0x02 clientId / 0x04 用户名密码）。
    Rejected(u8),
}

/// SUBSCRIBE 处理结果：SUBACK 返回码 + 随 SUBACK 之后下发的 retained 消息。
pub struct SubOutcome {
    pub codes: Vec<u8>,
    pub retained: Vec<Publish>,
}

impl Broker {
    pub fn new(config: Config) -> Self {
        Self {
            inner: Mutex::new(Inner {
                sessions: HashMap::new(),
                retained: HashMap::new(),
            }),
            username: config.username,
            password: config.password,
            conn_seq: AtomicU64::new(0),
            auto_seq: AtomicU64::new(0),
        }
    }

    pub fn next_conn_id(&self) -> u64 {
        self.conn_seq.fetch_add(1, Ordering::Relaxed)
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::error!("broker state lock poisoned, recovering");
            poisoned.into_inner()
        })
    }

    /// CONNECT：鉴权 → clientId 分配/校验 → 会话接管或新建 → 收集重发/离线消息。
    pub fn connect(
        &self,
        req: Connect,
        conn_id: u64,
        tx: mpsc::Sender<Packet>,
        kick: watch::Sender<bool>,
    ) -> ConnectOutcome {
        if req.protocol_level != 4 {
            return ConnectOutcome::Rejected(0x01);
        }
        if let Some(expect_user) = self.username.as_deref() {
            let expect_pass = self.password.as_deref().unwrap_or("");
            let user_ok = req.username.as_deref() == Some(expect_user);
            let pass_ok = req.password.as_deref() == Some(expect_pass.as_bytes());
            if !user_ok || !pass_ok {
                return ConnectOutcome::Rejected(0x04);
            }
        }
        let mut inner = self.lock();
        let mut client_id = req.client_id.clone();
        if client_id.is_empty() {
            if !req.clean_session {
                return ConnectOutcome::Rejected(0x02);
            }
            client_id = format!("auto-{}", self.auto_seq.fetch_add(1, Ordering::Relaxed));
        }
        let old = inner.sessions.remove(&client_id);
        let session_present = old.is_some() && !req.clean_session;
        let mut session = match old {
            // 持久会话恢复：订阅/inflight/离线队列保留；旧连接 sender 被丢弃后自行关闭
            Some(s) if !req.clean_session => s,
            _ => Session::new(req.clean_session),
        };
        session.clean_session = req.clean_session;
        session.will = req.will;
        session.online = Some(Online { conn_id, tx, kick });
        let mut pending = Vec::new();
        if session_present {
            for msg in session.inflight.values() {
                let mut m = msg.clone();
                m.dup = true;
                pending.push(m);
            }
            while session.inflight.len() < MAX_INFLIGHT {
                let Some(mut m) = session.offline.pop_front() else {
                    break;
                };
                let id = session.next_packet_id();
                m.packet_id = Some(id);
                m.qos = 1;
                session.inflight.insert(id, m.clone());
                pending.push(m);
            }
            if !session.offline.is_empty() {
                tracing::warn!(client_id, "inflight full, offline queue partially drained");
            }
        }
        let keep_alive = req.keep_alive;
        inner.sessions.insert(client_id.clone(), session);
        ConnectOutcome::Accepted {
            client_id,
            session_present,
            keep_alive,
            pending,
        }
    }

    /// 连接关闭清理。`send_will=true`（异常断开）时返回 will 由调用方发布。
    /// conn_id 不匹配 = 已被接管的旧连接清理，忽略（防误删新会话/误发 will）。
    pub fn disconnect(&self, client_id: &str, conn_id: u64, send_will: bool) -> Option<Publish> {
        let mut inner = self.lock();
        let session = inner.sessions.get_mut(client_id)?;
        // online 为 Some 且 conn_id 不符 = 已被接管的旧连接清理，忽略；
        // online 为 None（慢消费者已被 broker 断开）仍需清理会话本体。
        if let Some(o) = &session.online
            && o.conn_id != conn_id
        {
            return None;
        }
        session.online = None;
        let will = session.will.take();
        if session.clean_session {
            inner.sessions.remove(client_id);
        }
        if send_will {
            will.map(will_to_publish)
        } else {
            None
        }
    }

    /// PUBLISH 路由：retained 存储 + 向所有匹配订阅的会话投递（含发布者自身，3.1.1 无 no-local）。
    pub fn publish(&self, msg: Publish) {
        let mut inner = self.lock();
        if msg.retain {
            if msg.payload.is_empty() {
                inner.retained.remove(&msg.topic);
            } else if inner.retained.contains_key(&msg.topic) || inner.retained.len() < MAX_RETAINED
            {
                inner.retained.insert(msg.topic.clone(), msg.clone());
            } else {
                tracing::warn!(topic = %msg.topic, "retained store full (cap {MAX_RETAINED}), retained message dropped");
            }
        }
        let Inner {
            sessions,
            retained: _,
        } = &mut *inner;
        let mut wills = Vec::new();
        for (id, session) in sessions.iter_mut() {
            let qos = session
                .subs
                .iter()
                .filter(|(f, _)| topic::matches(f, &msg.topic))
                .map(|(_, q)| *q)
                .max();
            if let Some(q) = qos
                && let Some(will) = session.deliver(id, &msg, q.min(msg.qos))
            {
                wills.push(will);
            }
        }
        drop(inner);
        // 慢消费者断开触发的 will：释放锁后再路由（避免重入死锁）
        for will in wills {
            self.publish(will);
        }
    }

    /// SUBSCRIBE：校验过滤器 → 授权 qos（未实现 QoS2，最高授 1，MQTT-3.8.4-6 允许降级）→
    /// 收集匹配 retained（retain=true 回放）。
    pub fn subscribe(&self, client_id: &str, filters: &[(String, u8)]) -> SubOutcome {
        let mut inner = self.lock();
        let Inner { sessions, retained } = &mut *inner;
        let Some(session) = sessions.get_mut(client_id) else {
            return SubOutcome {
                codes: vec![0x80; filters.len()],
                retained: Vec::new(),
            };
        };
        let mut codes = Vec::with_capacity(filters.len());
        let mut replay = Vec::new();
        for (filter, req_qos) in filters {
            let over_cap =
                session.subs.len() >= MAX_SUBSCRIPTIONS && !session.subs.contains_key(filter);
            if !topic::valid_filter(filter) || over_cap {
                codes.push(0x80);
                continue;
            }
            let granted = (*req_qos).min(1);
            session.subs.insert(filter.clone(), granted);
            codes.push(granted);
            for (t, msg) in retained.iter() {
                if !topic::matches(filter, t) {
                    continue;
                }
                let qos = granted.min(msg.qos);
                let mut out = msg.clone();
                out.dup = false;
                out.retain = true;
                out.qos = qos;
                out.packet_id = None;
                if qos == 1 {
                    if session.inflight.len() >= MAX_INFLIGHT {
                        tracing::warn!(client_id, "inflight full, retained replay dropped");
                        continue;
                    }
                    let id = session.next_packet_id();
                    out.packet_id = Some(id);
                    session.inflight.insert(id, out.clone());
                }
                replay.push(out);
            }
        }
        SubOutcome {
            codes,
            retained: replay,
        }
    }

    pub fn unsubscribe(&self, client_id: &str, filters: &[String]) {
        let mut inner = self.lock();
        if let Some(s) = inner.sessions.get_mut(client_id) {
            for f in filters {
                s.subs.remove(f);
            }
        }
    }

    pub fn puback(&self, client_id: &str, packet_id: u16) {
        let mut inner = self.lock();
        if let Some(s) = inner.sessions.get_mut(client_id)
            && s.inflight.remove(&packet_id).is_none()
        {
            tracing::debug!(
                client_id,
                packet_id,
                "PUBACK for unknown inflight (stale or duplicate)"
            );
        }
    }
}
