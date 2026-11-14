//! DeviceDirectory：sn→deviceId 映射与设备鉴权（仅 tcp 登录链路用）。
//!
//! 对齐 Java `DeviceContext` 的 `getDeviceIdBySn` / `authDevice` 两个方法，
//! 其余设备管理方法（TSL/modelRef 等）不在范围内（见 spec Out of Scope）。

use crate::config::DeviceEntry;
use crate::error::{CommonError, Result};

pub trait DeviceDirectory: Send + Sync {
    /// 按 projectId + sn 查 deviceId，找不到返回 None。
    fn get_device_id_by_sn(&self, project_id: &str, sn: &str) -> Option<String>;
    /// 校验 deviceId + password 是否匹配。
    fn auth_device(&self, device_id: &str, password: &str) -> bool;
}

/// 一期本地 TOML 设备清单实现。
///
/// ponytail: 一期本地清单，生产需接 Redis/DB（spec DeviceDirectory 决策）。
/// 线性扫描 O(n)，设备量大时升级为 HashMap 索引（sn→entry、deviceId→entry）。
#[derive(Debug)]
pub struct TomlDeviceDirectory {
    devices: Vec<DeviceEntry>,
}

impl TomlDeviceDirectory {
    pub fn new(devices: Vec<DeviceEntry>) -> Self {
        Self { devices }
    }

    /// 从独立 TOML 文本解析（顶层 `[[devices]]` 数组）。
    pub fn from_toml_str(s: &str) -> Result<Self> {
        #[derive(serde::Deserialize)]
        struct File {
            #[serde(default)]
            devices: Vec<DeviceEntry>,
        }
        let f: File = toml::from_str(s)
            .map_err(|e| CommonError::Config(format!("parse device list: {e}")))?;
        Ok(Self::new(f.devices))
    }
}

impl DeviceDirectory for TomlDeviceDirectory {
    fn get_device_id_by_sn(&self, project_id: &str, sn: &str) -> Option<String> {
        self.devices
            .iter()
            .find(|d| d.project_id == project_id && d.sn == sn)
            .map(|d| d.device_id.clone())
    }

    fn auth_device(&self, device_id: &str, password: &str) -> bool {
        self.devices
            .iter()
            .any(|d| d.device_id == device_id && d.password == password)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &str = r#"
[[devices]]
projectId = "p1"
sn = "SN001"
deviceId = "dev-001"
password = "pw1"

[[devices]]
projectId = "p1"
sn = "SN002"
deviceId = "dev-002"
password = "pw2"
"#;

    #[test]
    fn parses_toml_device_list() {
        let dir = TomlDeviceDirectory::from_toml_str(FIXTURE).unwrap();
        assert_eq!(dir.devices.len(), 2);
    }

    #[test]
    fn maps_sn_to_device_id() {
        let dir = TomlDeviceDirectory::from_toml_str(FIXTURE).unwrap();
        assert_eq!(
            dir.get_device_id_by_sn("p1", "SN001").as_deref(),
            Some("dev-001")
        );
        assert_eq!(
            dir.get_device_id_by_sn("p1", "SN002").as_deref(),
            Some("dev-002")
        );
        // projectId 不匹配 / sn 不存在 → None
        assert_eq!(dir.get_device_id_by_sn("p2", "SN001"), None);
        assert_eq!(dir.get_device_id_by_sn("p1", "NOPE"), None);
    }

    #[test]
    fn auths_device_by_password() {
        let dir = TomlDeviceDirectory::from_toml_str(FIXTURE).unwrap();
        assert!(dir.auth_device("dev-001", "pw1"));
        assert!(!dir.auth_device("dev-001", "wrong"));
        assert!(!dir.auth_device("nope", "pw1"));
    }

    #[test]
    fn rejects_invalid_toml() {
        let err = TomlDeviceDirectory::from_toml_str("not = = toml").unwrap_err();
        assert!(matches!(err, CommonError::Config(_)));
    }
}
