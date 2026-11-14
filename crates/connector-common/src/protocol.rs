//! Lua 协议引擎（mlua，Lua 5.4，vendored）。
//!
//! 脚本入口四函数：`decode(input, params)` / `decode_multi(input, params)` /
//! `encode(msg, params)` / `login(req, params)`。
//!
//! - `input` 为 table，字节双形态：`input.hex`（小写 hex 字符串）+ `input.json`（字节整数数组）。
//! - `params` 为配置 `protocol.params` 注入的 table。
//! - 引擎注入全局 `json_decode(str)->table` / `json_encode(table)->str`（serde_json 实现，
//!   因 vendored Lua 不带 JSON 库）。
//! - 脚本返回 table，引擎经 serde 反序列化为 `serde_json::Value`（wire message）。
//! - `encode` 返回 Lua string，引擎转为字节。
//! - 脚本运行时错误 / 返回值类型不匹配 / 语法错误 / 缺入口函数，一律包装为
//!   [`CommonError::Lua`]（携带脚本名 + 阶段 + 错误信息，失败可见）。
//! - 沙箱：受限标准库（base/table/string/math，无 io/os/package/debug，`loadfile`/`dofile`
//!   移除）+ 64MB 内存上限 + instruction hook 指令数上限（防死循环）。
//! - 可选 `scriptDir` 白名单：脚本路径 canonicalize 后必须位于目录内，越界拒绝加载。
//!
//! 对齐 Java `type=script`；`type=jar` 不支持，配置中出现即启动报错。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use mlua::{
    Function, HookTriggers, Lua, LuaOptions, LuaString, StdLib, Table, Value as LuaValue, VmState,
};
use serde_json::Value as JsonValue;

use crate::config::ProtocolConfig;
use crate::error::{CommonError, Result};

/// 协议契约，由 Lua 引擎统一实现。connector 通过协议名从 [`ProtocolRegistry`] 取。
pub trait Protocol: Send + Sync {
    fn name(&self) -> &str;
    /// 解码单条消息：字节 → wire message（JSON 对象）。
    fn decode(&self, bytes: &[u8], params: &JsonValue) -> Result<JsonValue>;
    /// 解码多条消息：字节 → wire message 数组。
    fn decode_multi(&self, bytes: &[u8], params: &JsonValue) -> Result<Vec<JsonValue>>;
    /// 编码下行消息：wire message → 字节。
    fn encode(&self, msg: &JsonValue, params: &JsonValue) -> Result<Vec<u8>>;
    /// 登录鉴权：请求 → 是否通过。
    fn login(&self, req: &JsonValue, params: &JsonValue) -> Result<bool>;
}

fn lua_error(script: &str, stage: &'static str, e: mlua::Error) -> CommonError {
    CommonError::Lua {
        script: script.to_string(),
        stage,
        message: e.to_string(),
    }
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// JsonValue → Lua 值，手工遍历。
/// 不走 mlua `LuaSerdeExt`：workspace feature 统一后 serde_json 可能带上
/// `arbitrary_precision`（如 opcua crate 传递启用），Number 的 serde 表示变为
/// 包装 struct，经 serde 通道进 Lua 会变成 table，脚本内算术直接失败。
/// `as_i64`/`as_f64` 访问器不受该 feature 影响。
fn json_to_lua(lua: &Lua, v: &JsonValue) -> mlua::Result<LuaValue> {
    Ok(match v {
        JsonValue::Null => LuaValue::Nil,
        JsonValue::Bool(b) => LuaValue::Boolean(*b),
        JsonValue::Number(n) => match n.as_i64() {
            Some(i) => LuaValue::Integer(i),
            None => LuaValue::Number(n.as_f64().unwrap_or(f64::NAN)),
        },
        JsonValue::String(s) => LuaValue::String(lua.create_string(s)?),
        JsonValue::Array(arr) => {
            let t = lua.create_table()?;
            for (i, item) in arr.iter().enumerate() {
                t.set(i + 1, json_to_lua(lua, item)?)?;
            }
            LuaValue::Table(t)
        }
        JsonValue::Object(map) => {
            let t = lua.create_table()?;
            for (k, item) in map {
                t.set(k.as_str(), json_to_lua(lua, item)?)?;
            }
            LuaValue::Table(t)
        }
    })
}

/// Lua 值 → JsonValue。table 键为 1..n 连续整数视为数组，否则视为对象；
/// 非字符串/整数键跳过（JSON 无对应键类型）。NaN/Inf 报错（失败可见）。
fn lua_to_json(v: LuaValue) -> mlua::Result<JsonValue> {
    Ok(match v {
        LuaValue::Nil => JsonValue::Null,
        LuaValue::Boolean(b) => JsonValue::Bool(b),
        LuaValue::Integer(i) => JsonValue::from(i),
        LuaValue::Number(n) => serde_json::Number::from_f64(n)
            .map(JsonValue::Number)
            .ok_or_else(|| mlua::Error::external(format!("float {n} 无法表示为 JSON 数字")))?,
        LuaValue::String(s) => JsonValue::String(s.to_string_lossy()),
        LuaValue::Table(t) => {
            let len = t.raw_len();
            // raw_len 只统计数组部分：len=0 时空表按数组（与旧 serde 行为一致），
            // 非空（纯 hash 键）必须按对象，否则 params/脚本返回值被吞成空数组。
            let is_array = if len > 0 {
                let mut ok = true;
                for i in 1..=len {
                    if t.raw_get::<LuaValue>(i)?.is_nil() {
                        ok = false;
                        break;
                    }
                }
                ok
            } else {
                t.clone().pairs::<LuaValue, LuaValue>().next().is_none()
            };
            if is_array {
                let mut arr = Vec::with_capacity(len);
                for i in 1..=len {
                    arr.push(lua_to_json(t.raw_get(i)?)?);
                }
                JsonValue::Array(arr)
            } else {
                let mut map = serde_json::Map::new();
                for pair in t.pairs::<LuaValue, LuaValue>() {
                    let (k, val) = pair?;
                    let key = match k {
                        LuaValue::String(s) => s.to_string_lossy(),
                        LuaValue::Integer(i) => i.to_string(),
                        _ => continue,
                    };
                    map.insert(key, lua_to_json(val)?);
                }
                JsonValue::Object(map)
            }
        }
        other => {
            return Err(mlua::Error::external(format!(
                "Lua 值 {other:?} 无 JSON 对应形式"
            )));
        }
    })
}

/// 构造脚本入参 `input` table：hex 字符串 + json 字节数组双形态。
fn build_input(lua: &Lua, bytes: &[u8]) -> mlua::Result<Table> {
    let t = lua.create_table()?;
    t.set("hex", to_hex(bytes))?;
    let json = json_to_lua(
        lua,
        &JsonValue::Array(bytes.iter().map(|b| (*b).into()).collect()),
    )?;
    t.set("json", json)?;
    Ok(t)
}

/// 注入 json_decode / json_encode 全局函数（serde_json 实现）。
fn inject_json_helpers(lua: &Lua) -> mlua::Result<()> {
    let globals = lua.globals();
    let decode = lua.create_function(|lua, s: std::string::String| {
        let v: JsonValue = serde_json::from_str(&s).map_err(mlua::Error::external)?;
        json_to_lua(lua, &v)
    })?;
    globals.set("json_decode", decode)?;
    let encode = lua.create_function(|_lua, v: LuaValue| {
        let j = lua_to_json(v)?;
        serde_json::to_string(&j).map_err(mlua::Error::external)
    })?;
    globals.set("json_encode", encode)?;
    Ok(())
}

/// Lua 内存上限（字节）：单次分配越过即 MemoryError（失败可见，不 OOM 宿主进程）。
const LUA_MEMORY_LIMIT: usize = 64 * 1024 * 1024;
/// instruction hook 触发间隔（每 N 条指令回调一次）。
const LUA_HOOK_INTERVAL: u32 = 1000;
/// 单次入口调用的累计指令上限，超限 hook 返回错误终止脚本（防死循环）。
const LUA_INSTRUCTION_LIMIT: u64 = 100_000_000;

/// 构造受限 Lua 实例：
///
/// - 标准库白名单：base/table/string/math（现有协议脚本的实际用量，io/os/package/debug/
///   coroutine/utf8 一律不加载）；base 自带的 `loadfile`/`dofile` 可读任意文件，显式移除。
/// - 内存上限 [`LUA_MEMORY_LIMIT`] 字节。
/// - 全局 instruction hook：每 [`LUA_HOOK_INTERVAL`] 条指令累计一次，超过
///   [`LUA_INSTRUCTION_LIMIT`] 返回 RuntimeError 终止脚本。计数器由调用方在每次入口
///   调用前清零（Lua 状态被 Mutex 串行化，无并发竞争）。
fn new_sandboxed_lua(instructions: Arc<AtomicU64>) -> mlua::Result<Lua> {
    // ponytail: 白名单按现有脚本用量收敛（base 库 mlua 恒加载，无独立开关，
    // 其中的 loadfile/dofile 在下方显式移除）；升级路径：脚本需要 os.time 等再按需加入并评审。
    let lua = Lua::new_with(
        StdLib::TABLE | StdLib::STRING | StdLib::MATH,
        LuaOptions::default(),
    )?;
    let globals = lua.globals();
    globals.set("loadfile", LuaValue::Nil)?;
    globals.set("dofile", LuaValue::Nil)?;
    lua.set_memory_limit(LUA_MEMORY_LIMIT)?;
    lua.set_global_hook(
        HookTriggers::new().every_nth_instruction(LUA_HOOK_INTERVAL),
        move |_lua, _dbg| {
            let used = instructions.fetch_add(u64::from(LUA_HOOK_INTERVAL), Ordering::Relaxed)
                + u64::from(LUA_HOOK_INTERVAL);
            if used > LUA_INSTRUCTION_LIMIT {
                return Err(mlua::Error::RuntimeError(format!(
                    "脚本超过指令数上限 {LUA_INSTRUCTION_LIMIT}（疑似死循环），已强制终止"
                )));
            }
            Ok(VmState::Continue)
        },
    )?;
    Ok(lua)
}

/// Lua 脚本协议实现。
pub struct LuaProtocol {
    name: String,
    // ponytail: Mutex 串行化 Lua 访问。mlua `send` feature 下 Lua 本身 Send+Sync，
    // 但 Lua 状态非可重入，加锁保证多线程并发调用安全。
    // 升级路径：每线程一个 Lua 实例（thread_local）或对象池以提升并发吞吐。
    lua: Mutex<Lua>,
    /// instruction hook 的累计计数器（每次入口调用前清零）。
    instructions: Arc<AtomicU64>,
}

// mlua `Lua` 未实现 Debug，手写仅暴露 name（Lua 状态机内部不可打印）。
impl std::fmt::Debug for LuaProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LuaProtocol")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl LuaProtocol {
    /// 从脚本文件加载。读取失败 → [`CommonError::ScriptLoad`]。
    pub fn from_script(name: &str, path: &str) -> Result<Self> {
        Self::from_script_in(name, path, None)
    }

    /// 同 [`from_script`](Self::from_script)，但 `script_dir` 为 Some 时先做白名单校验：
    /// 脚本路径 canonicalize 后必须位于 script_dir（canonicalize 后）之内，
    /// 越界或路径不可达 → [`CommonError::Config`]（失败可见，禁静默回落）。
    pub fn from_script_in(name: &str, path: &str, script_dir: Option<&str>) -> Result<Self> {
        if let Some(dir) = script_dir {
            check_within_script_dir(name, path, dir)?;
        }
        let src = std::fs::read_to_string(path).map_err(|e| CommonError::ScriptLoad {
            name: name.to_string(),
            path: path.to_string(),
            source: e,
        })?;
        Self::from_source(name, &src)
    }

    /// 从脚本源码加载（测试与内联脚本用）。校验四入口函数存在，缺失即报错。
    pub fn from_source(name: &str, src: &str) -> Result<Self> {
        let instructions = Arc::new(AtomicU64::new(0));
        let lua =
            new_sandboxed_lua(Arc::clone(&instructions)).map_err(|e| lua_error(name, "load", e))?;
        inject_json_helpers(&lua).map_err(|e| lua_error(name, "load", e))?;
        lua.load(src)
            .set_name(name)
            .exec()
            .map_err(|e| lua_error(name, "load", e))?;
        for entry in ["decode", "decode_multi", "encode", "login"] {
            let v: LuaValue = lua
                .globals()
                .get(entry)
                .map_err(|e| lua_error(name, "load", e))?;
            if !matches!(v, LuaValue::Function(_)) {
                return Err(CommonError::Lua {
                    script: name.to_string(),
                    stage: "load",
                    message: format!("missing entry function '{entry}'"),
                });
            }
        }
        Ok(Self {
            name: name.to_string(),
            lua: Mutex::new(lua),
            instructions,
        })
    }

    fn lock(&self) -> MutexGuard<'_, Lua> {
        self.instructions.store(0, Ordering::Relaxed);
        self.lua.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// script_dir 白名单校验：双方 canonicalize 后做前缀比较（消解 `..`/符号链接逃逸）。
fn check_within_script_dir(name: &str, path: &str, dir: &str) -> Result<()> {
    let canon_dir = std::fs::canonicalize(dir).map_err(|e| {
        CommonError::Config(format!(
            "protocol '{name}': scriptDir '{dir}' 不可访问: {e}"
        ))
    })?;
    let canon_path = std::fs::canonicalize(path).map_err(|e| {
        CommonError::Config(format!(
            "protocol '{name}': 脚本路径 '{path}' 不可访问: {e}"
        ))
    })?;
    if !canon_path.starts_with(&canon_dir) {
        return Err(CommonError::Config(format!(
            "protocol '{name}': 脚本路径 '{}' 超出 scriptDir '{}'，拒绝加载",
            canon_path.display(),
            canon_dir.display()
        )));
    }
    Ok(())
}

impl Protocol for LuaProtocol {
    fn name(&self) -> &str {
        &self.name
    }

    fn decode(&self, bytes: &[u8], params: &JsonValue) -> Result<JsonValue> {
        let lua = self.lock();
        let input = build_input(&lua, bytes).map_err(|e| lua_error(&self.name, "decode", e))?;
        let params_lua =
            json_to_lua(&lua, params).map_err(|e| lua_error(&self.name, "decode", e))?;
        let f: Function = lua
            .globals()
            .get("decode")
            .map_err(|e| lua_error(&self.name, "decode", e))?;
        let out: LuaValue = f
            .call((input, params_lua))
            .map_err(|e| lua_error(&self.name, "decode", e))?;
        lua_to_json(out).map_err(|e| lua_error(&self.name, "decode", e))
    }

    fn decode_multi(&self, bytes: &[u8], params: &JsonValue) -> Result<Vec<JsonValue>> {
        let lua = self.lock();
        let input =
            build_input(&lua, bytes).map_err(|e| lua_error(&self.name, "decode_multi", e))?;
        let params_lua =
            json_to_lua(&lua, params).map_err(|e| lua_error(&self.name, "decode_multi", e))?;
        let f: Function = lua
            .globals()
            .get("decode_multi")
            .map_err(|e| lua_error(&self.name, "decode_multi", e))?;
        let out: LuaValue = f
            .call((input, params_lua))
            .map_err(|e| lua_error(&self.name, "decode_multi", e))?;
        let j = lua_to_json(out).map_err(|e| lua_error(&self.name, "decode_multi", e))?;
        match j {
            JsonValue::Array(arr) => Ok(arr),
            other => Err(CommonError::Lua {
                script: self.name.clone(),
                stage: "decode_multi",
                message: format!("脚本须返回消息数组，实际返回 {other}"),
            }),
        }
    }

    fn encode(&self, msg: &JsonValue, params: &JsonValue) -> Result<Vec<u8>> {
        let lua = self.lock();
        let msg_lua = json_to_lua(&lua, msg).map_err(|e| lua_error(&self.name, "encode", e))?;
        let params_lua =
            json_to_lua(&lua, params).map_err(|e| lua_error(&self.name, "encode", e))?;
        let f: Function = lua
            .globals()
            .get("encode")
            .map_err(|e| lua_error(&self.name, "encode", e))?;
        let out: LuaString = f
            .call((msg_lua, params_lua))
            .map_err(|e| lua_error(&self.name, "encode", e))?;
        Ok(out.as_bytes().to_vec())
    }

    fn login(&self, req: &JsonValue, params: &JsonValue) -> Result<bool> {
        let lua = self.lock();
        let req_lua = json_to_lua(&lua, req).map_err(|e| lua_error(&self.name, "login", e))?;
        let params_lua =
            json_to_lua(&lua, params).map_err(|e| lua_error(&self.name, "login", e))?;
        let f: Function = lua
            .globals()
            .get("login")
            .map_err(|e| lua_error(&self.name, "login", e))?;
        f.call((req_lua, params_lua))
            .map_err(|e| lua_error(&self.name, "login", e))
    }
}

/// 协议注册表：按名字索引已加载的 Lua 协议，connector 共享。
#[derive(Debug, Default)]
pub struct ProtocolRegistry {
    protocols: HashMap<String, Arc<LuaProtocol>>,
}

impl ProtocolRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, proto: LuaProtocol) {
        self.protocols.insert(proto.name.clone(), Arc::new(proto));
    }

    /// 按名取协议；找不到 → [`CommonError::ProtocolNotFound`]。
    pub fn get(&self, name: &str) -> Result<Arc<dyn Protocol>> {
        let p = self
            .protocols
            .get(name)
            .ok_or_else(|| CommonError::ProtocolNotFound(name.to_string()))?;
        Ok(p.clone() as Arc<dyn Protocol>)
    }

    /// 已注册协议名（排序，供 gateway 摘要打印）。
    pub fn names(&self) -> Vec<String> {
        let mut n: Vec<String> = self.protocols.keys().cloned().collect();
        n.sort();
        n
    }

    /// 按配置构建注册表。`type != "script"`（如 Java 的 jar）→ 启动报错（失败可见）。
    pub fn from_protocols(configs: &[&ProtocolConfig]) -> Result<Self> {
        Self::from_protocols_in(configs, None)
    }

    /// 同 [`from_protocols`](Self::from_protocols)，但 `script_dir` 为 Some 时
    /// 对每个脚本路径做白名单校验（见 [`LuaProtocol::from_script_in`]）。
    pub fn from_protocols_in(
        configs: &[&ProtocolConfig],
        script_dir: Option<&str>,
    ) -> Result<Self> {
        let mut reg = Self::new();
        for pc in configs {
            if pc.protocol_type != "script" {
                return Err(CommonError::UnsupportedProtocolType {
                    name: pc.name.clone(),
                    kind: pc.protocol_type.clone(),
                });
            }
            reg.register(LuaProtocol::from_script_in(&pc.name, &pc.path, script_dir)?);
        }
        Ok(reg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn script_path(name: &str) -> String {
        format!("{}/protocols/{}", env!("CARGO_MANIFEST_DIR"), name)
    }

    // ---- 成功路径：示例脚本 ----

    #[test]
    fn json_passthrough_decodes_via_json_form() {
        let proto =
            LuaProtocol::from_script("json_passthrough", &script_path("json_passthrough.lua"))
                .unwrap();
        let payload = br#"{"deviceId":"d1","sn":"s1","messageType":"HEARTBEAT"}"#;
        let msg = proto.decode(payload, &json!({})).unwrap();
        assert_eq!(msg["deviceId"], "d1");
        assert_eq!(msg["sn"], "s1");
        assert_eq!(msg["messageType"], "HEARTBEAT");
    }

    #[test]
    fn hex_frame_decodes_via_hex_form() {
        let proto = LuaProtocol::from_script("hex_frame", &script_path("hex_frame.lua")).unwrap();
        let bytes = [0x01u8, 0x00, 0x2a]; // value = 42
        let params = json!({"deviceId": "d1", "sn": "s1", "ts": 100});
        let msg = proto.decode(&bytes, &params).unwrap();
        assert_eq!(msg["deviceId"], "d1");
        assert_eq!(msg["sn"], "s1");
        assert_eq!(msg["messageType"], "DEVICE_REPORT_REQ");
        assert_eq!(msg["metrics"][0]["ts"], 100);
        assert_eq!(msg["metrics"][0]["code"], "v");
        assert_eq!(msg["metrics"][0]["value"], 42);
    }

    #[test]
    fn decode_multi_returns_vec() {
        let proto =
            LuaProtocol::from_script("json_passthrough", &script_path("json_passthrough.lua"))
                .unwrap();
        let msgs = proto
            .decode_multi(br#"{"sn":"s1","messageType":"HEARTBEAT"}"#, &json!({}))
            .unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0]["sn"], "s1");
    }

    #[test]
    fn encode_produces_bytes() {
        let proto = LuaProtocol::from_script("hex_frame", &script_path("hex_frame.lua")).unwrap();
        let msg = json!({"metrics": [{"value": 42}]});
        let bytes = proto.encode(&msg, &json!({})).unwrap();
        assert_eq!(bytes, vec![1, 0, 42]);
    }

    #[test]
    fn json_lua_roundtrip_keeps_numbers_scalar() {
        // 回归锚定：serde_json 带 arbitrary_precision（opcua 传递启用，workspace
        // feature 统一波及本 crate）时，mlua serde 通道会把数字变 table，
        // 脚本算术失败。手工转换必须保持数字为 Lua 标量。
        let lua = Lua::new();
        // 不含 null 字段：Lua table 不存 nil（set nil 即删除键），round-trip 必丢，
        // 属 Lua 固有语义（旧 mlua serde 通道行为一致），非转换缺陷。
        let v = json!({
            "i": 42, "f": 1.5, "s": "x", "b": true,
            "arr": [1, 2, {"k": "v"}], "obj": {"deep": [true]}
        });
        let lv = json_to_lua(&lua, &v).unwrap();
        assert_eq!(lua_to_json(lv.clone()).unwrap(), v);
        let t = match lv {
            LuaValue::Table(t) => t,
            other => panic!("期望 table，实际 {other:?}"),
        };
        assert!(matches!(
            t.get::<LuaValue>("i").unwrap(),
            LuaValue::Integer(42)
        ));
        assert!(matches!(
            t.get::<LuaValue>("f").unwrap(),
            LuaValue::Number(_)
        ));
    }

    #[test]
    fn login_returns_bool() {
        let proto =
            LuaProtocol::from_script("json_passthrough", &script_path("json_passthrough.lua"))
                .unwrap();
        let params = json!({"username": "u", "password": "p"});
        assert!(
            proto
                .login(&json!({"username":"u","password":"p"}), &params)
                .unwrap()
        );
        assert!(
            !proto
                .login(&json!({"username":"u","password":"x"}), &params)
                .unwrap()
        );
    }

    // ---- 字节双形态契约 ----

    #[test]
    fn input_provides_hex_and_json_forms() {
        let proto = LuaProtocol::from_source(
            "echo",
            r#"
            function decode(input, params) return { hex = input.hex, json = input.json } end
            function decode_multi(input, params) return { decode(input, params) } end
            function encode(msg, params) return "" end
            function login(req, params) return true end
            "#,
        )
        .unwrap();
        let bytes = [0xdeu8, 0xad, 0xbe, 0xef];
        let out = proto.decode(&bytes, &json!({})).unwrap();
        assert_eq!(out["hex"], "deadbeef");
        assert_eq!(out["json"], json!([222, 173, 190, 239]));
    }

    // ---- 失败路径 ----

    #[test]
    fn runtime_error_script_reports_failure() {
        let proto = LuaProtocol::from_source(
            "bad",
            r#"
            function decode(input, params) error("boom") end
            function decode_multi(input, params) return {} end
            function encode(msg, params) return "" end
            function login(req, params) return true end
            "#,
        )
        .unwrap();
        let err = proto.decode(b"x", &json!({})).unwrap_err();
        match err {
            CommonError::Lua {
                script,
                stage,
                message,
            } => {
                assert_eq!(script, "bad");
                assert_eq!(stage, "decode");
                assert!(message.contains("boom"), "message was: {message}");
            }
            other => panic!("expected Lua error, got {other:?}"),
        }
    }

    #[test]
    fn type_mismatch_reports_failure() {
        // decode_multi 返回字符串而非数组 → from_value::<Vec> 失败
        let proto = LuaProtocol::from_source(
            "mismatch",
            r#"
            function decode(input, params) return {} end
            function decode_multi(input, params) return "not an array" end
            function encode(msg, params) return "" end
            function login(req, params) return true end
            "#,
        )
        .unwrap();
        let err = proto.decode_multi(b"x", &json!({})).unwrap_err();
        assert!(
            matches!(
                err,
                CommonError::Lua {
                    stage: "decode_multi",
                    ..
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn syntax_error_fails_load() {
        let err = LuaProtocol::from_source("bad_syntax", "function decode( !!! ").unwrap_err();
        assert!(
            matches!(err, CommonError::Lua { stage: "load", .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn missing_entry_function_fails_load() {
        let err =
            LuaProtocol::from_source("partial", "function decode(i, p) return {} end").unwrap_err();
        match err {
            CommonError::Lua { stage, message, .. } => {
                assert_eq!(stage, "load");
                assert!(message.contains("decode_multi"), "message was: {message}");
            }
            other => panic!("expected Lua load error, got {other:?}"),
        }
    }

    #[test]
    fn missing_script_file_reports_failure() {
        let err = LuaProtocol::from_script("nope", "/definitely/not/here.lua").unwrap_err();
        assert!(matches!(err, CommonError::ScriptLoad { .. }), "got {err:?}");
    }

    // ---- 沙箱 ----

    /// 构造只含 decode 有效逻辑的内联脚本（其余三入口为空实现）。
    fn decode_only(name: &str, decode_body: &str) -> LuaProtocol {
        LuaProtocol::from_source(
            name,
            &format!(
                "function decode(input, params) {decode_body} end\n\
                 function decode_multi(input, params) return {{}} end\n\
                 function encode(msg, params) return \"\" end\n\
                 function login(req, params) return true end"
            ),
        )
        .unwrap()
    }

    #[test]
    fn sandbox_denies_fs_process_and_module_access() {
        // 危险全局均为 nil：io/os/package/debug 未加载，loadfile/dofile/require 被移除
        let proto = decode_only(
            "escape",
            "return { \
               io = io == nil, os = os == nil, package = package == nil, \
               debug = debug == nil, loadfile = loadfile == nil, \
               dofile = dofile == nil, require = require == nil }",
        );
        let out = proto.decode(b"x", &json!({})).unwrap();
        for key in [
            "io", "os", "package", "debug", "loadfile", "dofile", "require",
        ] {
            assert_eq!(out[key], json!(true), "{key} 应不可用");
        }
    }

    #[test]
    fn sandbox_kills_infinite_loop() {
        let proto = decode_only("loop", "while true do end");
        let err = proto.decode(b"x", &json!({})).unwrap_err();
        match err {
            CommonError::Lua { message, .. } => {
                assert!(message.contains("指令数上限"), "message was: {message}")
            }
            other => panic!("expected Lua error, got {other:?}"),
        }
    }

    #[test]
    fn sandbox_enforces_memory_limit() {
        // 单次分配 128MB > 64MB 上限 → MemoryError（失败可见，不拖垮宿主进程）
        let proto = decode_only("mem", "return string.rep('x', 128 * 1024 * 1024)");
        let err = proto.decode(b"x", &json!({})).unwrap_err();
        assert!(
            matches!(err, CommonError::Lua { .. }),
            "expected Lua error, got {err:?}"
        );
    }

    #[test]
    fn sandbox_keeps_whitelisted_stdlib() {
        let proto = decode_only(
            "stdlib",
            "local t = {3, 1, 2} table.sort(t) \
             return { sorted = t[1] == 1 and t[3] == 3, \
                      sub = string.sub('abc', 1, 2), \
                      floor = math.floor(1.7), \
                      char = string.char(65) }",
        );
        let out = proto.decode(b"x", &json!({})).unwrap();
        assert_eq!(out["sorted"], json!(true));
        assert_eq!(out["sub"], "ab");
        assert_eq!(out["floor"], 1);
        assert_eq!(out["char"], "A");
    }

    // ---- scriptDir 白名单 ----

    const MINIMAL_SCRIPT: &str = "\
function decode(input, params) return {} end\n\
function decode_multi(input, params) return {} end\n\
function encode(msg, params) return \"\" end\n\
function login(req, params) return true end";

    #[test]
    fn script_dir_rejects_outside_and_escaping_paths() {
        let base = std::env::temp_dir().join(format!(
            "gw-scriptdir-{}-{}",
            std::process::id(),
            "script_dir_rejects_outside_and_escaping_paths"
        ));
        let dir = base.join("scripts");
        std::fs::create_dir_all(&dir).unwrap();
        let inside = dir.join("inside.lua");
        let outside = base.join("outside.lua");
        std::fs::write(&inside, MINIMAL_SCRIPT).unwrap();
        std::fs::write(&outside, MINIMAL_SCRIPT).unwrap();
        let dir_s = dir.to_str().unwrap();

        // 目录内 → 放行
        assert!(LuaProtocol::from_script_in("p", inside.to_str().unwrap(), Some(dir_s)).is_ok());
        // 目录外 → 拒绝（失败可见）
        let err = LuaProtocol::from_script_in("p", outside.to_str().unwrap(), Some(dir_s))
            .expect_err("expected rejection");
        assert!(err.to_string().contains("拒绝加载"), "{err}");
        // `..` 逃逸 → canonicalize 后同样拒绝
        let escaping = dir.join("../outside.lua");
        assert!(LuaProtocol::from_script_in("p", escaping.to_str().unwrap(), Some(dir_s)).is_err());
        // scriptDir 不存在 → 拒绝（不静默放行）
        assert!(
            LuaProtocol::from_script_in("p", inside.to_str().unwrap(), Some("/no/such/dir"))
                .is_err()
        );
        // 未设置 scriptDir → 向后兼容，目录外脚本照常加载
        assert!(LuaProtocol::from_script_in("p", outside.to_str().unwrap(), None).is_ok());

        std::fs::remove_dir_all(&base).ok();
    }

    #[test]
    fn registry_from_protocols_in_applies_script_dir() {
        let base = std::env::temp_dir().join(format!(
            "gw-scriptdir-{}-{}",
            std::process::id(),
            "registry_from_protocols_in_applies_script_dir"
        ));
        std::fs::create_dir_all(&base).unwrap();
        let outside = base.join("outside.lua");
        std::fs::write(&outside, MINIMAL_SCRIPT).unwrap();
        let pc = ProtocolConfig {
            name: "p".into(),
            path: outside.to_str().unwrap().to_string(),
            params: json!({}),
            protocol_type: "script".into(),
        };
        let err = ProtocolRegistry::from_protocols_in(
            &[&pc],
            Some(base.join("scripts").to_str().unwrap()),
        )
        .expect_err("expected rejection");
        assert!(err.to_string().contains("不可访问"), "{err}");
        std::fs::remove_dir_all(&base).ok();
    }

    // ---- 注册表 ----

    #[test]
    fn jar_protocol_type_rejected() {
        let pc = ProtocolConfig {
            name: "legacy".into(),
            path: "x.lua".into(),
            params: json!({}),
            protocol_type: "jar".into(),
        };
        let err = ProtocolRegistry::from_protocols(&[&pc]).unwrap_err();
        assert!(
            matches!(err, CommonError::UnsupportedProtocolType { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn registry_builds_and_serves_protocols() {
        let dir = env!("CARGO_MANIFEST_DIR");
        let pc = ProtocolConfig {
            name: "json_passthrough".into(),
            path: format!("{dir}/protocols/json_passthrough.lua"),
            params: json!({}),
            protocol_type: "script".into(),
        };
        let reg = ProtocolRegistry::from_protocols(&[&pc]).unwrap();
        assert_eq!(reg.names(), vec!["json_passthrough".to_string()]);
        let proto = reg.get("json_passthrough").unwrap();
        assert_eq!(proto.name(), "json_passthrough");
        let msg = proto
            .decode(br#"{"sn":"s9","messageType":"HEARTBEAT"}"#, &json!({}))
            .unwrap();
        assert_eq!(msg["sn"], "s9");
    }

    #[test]
    fn registry_get_missing_errors() {
        let reg = ProtocolRegistry::new();
        // get() 返回 Arc<dyn Protocol>，trait object 无 Debug，用 .err() 丢弃 Ok 值再取错误。
        let err = reg.get("nope").err().expect("expected ProtocolNotFound");
        assert!(matches!(err, CommonError::ProtocolNotFound(_)));
    }
}
