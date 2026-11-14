//! deviceId → broker 会话表（对齐 Java `MqttDeviceSessionManager`）。
//!
//! 上行解码出的 deviceId 登记到收到它的 broker；下行按 deviceId 反查 broker 路由指令
//! （对齐 `DownMqttHandler.java:60` 的 `getSession(deviceId)`）。
//!
//! Java 侧存 `MqttConfigServer` 对象，Rust 侧存 broker key（`clientId+host+port`，对齐
//! `MqttConfigServer.uuid()`）。
//!
//! ponytail: 进程内 HashMap，无过期、无持久化，gateway 重启后需等设备再次上行才能下发指令
//! （与 Java 侧行为一致，Java 也是内存 Map）。升级路径：接 Redis 设备目录（Java
//! `DeviceContext` 的生产实现），跨实例共享 deviceId→broker 映射。

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

#[derive(Default)]
pub struct DeviceSessions {
    map: Mutex<HashMap<String, String>>,
}

impl DeviceSessions {
    pub fn new() -> Self {
        Self::default()
    }

    /// 登记（或覆盖）deviceId 所属 broker。
    pub fn add(&self, device_id: &str, broker: &str) {
        self.lock()
            .insert(device_id.to_string(), broker.to_string());
    }

    /// 反查 deviceId 所属 broker；无会话返回 None（下行据此跳过指令）。
    pub fn broker_of(&self, device_id: &str) -> Option<String> {
        self.lock().get(device_id).cloned()
    }

    pub fn len(&self) -> usize {
        self.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, String>> {
        // 锁中毒不 panic 整个 connector，恢复内部数据继续（对齐 common 层 MockSink 处理）
        self.map.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registers_and_looks_up_device_broker() {
        let sessions = DeviceSessions::new();
        assert!(sessions.is_empty());
        assert_eq!(sessions.broker_of("d1"), None);

        sessions.add("d1", "brokerA");
        sessions.add("d2", "brokerB");
        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions.broker_of("d1").as_deref(), Some("brokerA"));
        assert_eq!(sessions.broker_of("d2").as_deref(), Some("brokerB"));
        assert_eq!(sessions.broker_of("unknown"), None);
    }

    /// 设备换 broker 上行后，路由指向最新 broker（对齐 Java `Map.put` 覆盖语义）。
    #[test]
    fn later_registration_overwrites_broker() {
        let sessions = DeviceSessions::new();
        sessions.add("d1", "brokerA");
        sessions.add("d1", "brokerB");
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions.broker_of("d1").as_deref(), Some("brokerB"));
    }
}
