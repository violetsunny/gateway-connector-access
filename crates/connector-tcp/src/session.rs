//! 会话管理：登录成功后 deviceId/sn 与连接的绑定（对齐 Java TcpSessionManger/LocalSession）。
//!
//! 独立模块，票 04（tcp client）可复用。Java 侧的 Redis 分布式会话缓存为注释掉的死代码，
//! 此处仅本地 Map（与 spec 一期简化一致）。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

/// 一个已登录设备的会话绑定。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    pub device_id: String,
    pub sn: String,
    /// 对端地址（展示/排障用）。
    pub peer: String,
    /// 连接唯一 id（同一进程内），断开时按此清理。
    pub conn_id: u64,
}

/// deviceId → Session。同 deviceId 重复登录以最新连接为准（对齐 Java Map.put 覆盖）。
#[derive(Debug, Default)]
pub struct SessionManager {
    inner: Mutex<HashMap<String, Session>>,
    conn_seq: AtomicU64,
}

impl SessionManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// 分配进程内唯一的连接 id。
    pub fn next_conn_id(&self) -> u64 {
        self.conn_seq.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn add(&self, session: Session) {
        self.lock().insert(session.device_id.clone(), session);
    }

    pub fn get(&self, device_id: &str) -> Option<Session> {
        self.lock().get(device_id).cloned()
    }

    /// 连接断开时清理该连接绑定的所有会话。
    pub fn remove_by_conn(&self, conn_id: u64) {
        self.lock().retain(|_, s| s.conn_id != conn_id);
    }

    pub fn len(&self) -> usize {
        self.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, Session>> {
        // 对齐 common crate 惯例：锁中毒不 panic，恢复内部数据继续
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(device_id: &str, conn_id: u64) -> Session {
        Session {
            device_id: device_id.to_string(),
            sn: format!("sn-{device_id}"),
            peer: "127.0.0.1:1".to_string(),
            conn_id,
        }
    }

    #[test]
    fn add_get_and_len() {
        let m = SessionManager::new();
        assert!(m.is_empty());
        m.add(session("d1", 1));
        assert_eq!(m.len(), 1);
        let s = m.get("d1").unwrap();
        assert_eq!(s.sn, "sn-d1");
        assert!(m.get("nope").is_none());
    }

    #[test]
    fn relogin_replaces_session() {
        let m = SessionManager::new();
        m.add(session("d1", 1));
        m.add(session("d1", 2));
        assert_eq!(m.len(), 1);
        assert_eq!(m.get("d1").unwrap().conn_id, 2);
    }

    #[test]
    fn disconnect_removes_only_own_sessions() {
        let m = SessionManager::new();
        m.add(session("d1", 1));
        m.add(session("d2", 2));
        m.remove_by_conn(1);
        assert_eq!(m.len(), 1);
        assert!(m.get("d1").is_none());
        assert!(m.get("d2").is_some());
    }

    #[test]
    fn conn_ids_are_unique() {
        let m = SessionManager::new();
        let a = m.next_conn_id();
        let b = m.next_conn_id();
        assert_ne!(a, b);
    }
}
