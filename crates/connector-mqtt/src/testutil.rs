//! 测试共用工具：内联 Lua 协议脚本与记录型假协议（无真实 broker / Kafka）。

#![cfg(test)]

use std::sync::{Arc, Mutex, MutexGuard};

use connector_common::{CommonError, LuaProtocol, Protocol, Result};
use serde_json::Value as JsonValue;

/// 把 payload 当 JSON 对象解析并作为单条消息返回（上行透传）。
pub const JSON_ECHO: &str = "return { json_decode(string.char(table.unpack(input.json))) }";

/// 构造四入口齐全的 Lua 协议：`decode_multi_body` / `encode_body` 为函数体语句。
pub fn protocol(name: &str, decode_multi_body: &str, encode_body: &str) -> Arc<dyn Protocol> {
    let src = format!(
        "function decode(input, params) return {{}} end\n\
         function decode_multi(input, params) {decode_multi_body} end\n\
         function encode(msg, params) {encode_body} end\n\
         function login(req, params) return true end\n"
    );
    Arc::new(
        LuaProtocol::from_source(name, &src)
            .unwrap_or_else(|e| panic!("测试脚本 {name} 加载失败: {e}")),
    )
}

/// 记录 `encode` 调用的假协议：断言下行 encode 入参与调用次数，不依赖 Lua。
pub struct RecordingProtocol {
    name: String,
    out: Vec<u8>,
    calls: Mutex<Vec<(JsonValue, JsonValue)>>,
}

impl RecordingProtocol {
    /// `out` 为 encode 返回的字节（测试里通常是 OperationRequest 的 JSON）。
    pub fn new(name: &str, out: Vec<u8>) -> Arc<Self> {
        Arc::new(Self {
            name: name.to_string(),
            out,
            calls: Mutex::new(Vec::new()),
        })
    }

    /// 已记录的 (msg, params) 调用序列。
    pub fn calls(&self) -> Vec<(JsonValue, JsonValue)> {
        self.lock().clone()
    }

    fn lock(&self) -> MutexGuard<'_, Vec<(JsonValue, JsonValue)>> {
        self.calls.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl Protocol for RecordingProtocol {
    fn name(&self) -> &str {
        &self.name
    }

    fn decode(&self, _bytes: &[u8], _params: &JsonValue) -> Result<JsonValue> {
        Err(CommonError::Internal(
            "RecordingProtocol 不支持 decode".into(),
        ))
    }

    fn decode_multi(&self, _bytes: &[u8], _params: &JsonValue) -> Result<Vec<JsonValue>> {
        Err(CommonError::Internal(
            "RecordingProtocol 不支持 decode_multi".into(),
        ))
    }

    fn encode(&self, msg: &JsonValue, params: &JsonValue) -> Result<Vec<u8>> {
        self.lock().push((msg.clone(), params.clone()));
        Ok(self.out.clone())
    }

    fn login(&self, _req: &JsonValue, _params: &JsonValue) -> Result<bool> {
        Err(CommonError::Internal(
            "RecordingProtocol 不支持 login".into(),
        ))
    }
}
