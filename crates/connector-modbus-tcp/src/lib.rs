//! modbus-tcp connector（票 07）：tokio-modbus master 按 readDiff 轮询从站点位。
//!
//! 每个 server 配置一个 tokio 轮询任务（对齐 Java `ModbusConnectorStater.init` 每
//! ModbusServerConfig 一个 ModbusTcpClient）：
//! 1. 启动时 reqwest 拉点表与点位-设备映射（cloud-gateway 包装解壳见 [`point`]），
//!    任一失败 → start() 报错、connector 拒绝启动（Java warn+跳过 → Rust 失败可见）；
//! 2. 轮询节奏对齐 Java `scheduleWithFixedDelay(readTask, 0, readDiff, SECONDS)`：
//!    上一轮结束后再等 readDiff（非固定频率 interval）；
//! 3. 单点读取：`set_slave`（对齐 Java 每请求把 slaveAddr 写进 MBAP unit-id、
//!    单连接复用）→ 按功能码读 → 响应转 wire 字节（寄存器大端拼接 / 线圈 LSB-first
//!    打包，对齐 Java MessageCodec 解码出的 data 段）→ [`datatype::DataType::parse_value`]
//!    （票 07 验收核心，Java IotByteUtils 等价，逐类型对拍测试在 datatype 模块）；
//! 4. 单点值 + 设备映射 → 单点 JSON → Lua `decode_multi` → `DeviceSink::store_message`
//!    （对齐 Java UpModbusHandler：每点独立一条 DEVICE_REPORT_REQ，deviceId 来自映射）。
//!
//! 连接管理对齐 Java ModBusTcpPoll：连接失败等 retryDelay 重试，连续 maxRetries 次
//! 失败放弃该 server（error 日志可见）；读传输错误 → 丢弃连接、下轮重连。
//!
//! 不实现（Java 死代码）：下行写（encodeMulti/send 为 TODO 或硬编码假值）、
//! ModbusDeviceSessionManager（仅服务下行）。
//!
//! ponytail: 单点顺序 await 读（Java 并发发出全部请求 + 事务号回调匹配；tokio-modbus
//! client 内部处理事务匹配，顺序读牺牲轮询吞吐）。升级路径：点数 × 单点延迟 > readDiff
//! 时改 futures::join_all 并发读。

mod config;
mod datatype;
mod point;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use connector_common::protocol::{Protocol, ProtocolRegistry};
use connector_common::sink::DeviceSink;
use connector_common::{CommonError, Connector, Result, StatusMap, async_trait};
use serde_json::{Value as JsonValue, json};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_modbus::client::{Context, Reader, tcp::connect_slave};
use tokio_modbus::prelude::{ExceptionCode, Slave, SlaveContext};
use tracing::{error, info, warn};

pub use config::{ModbusServerConfig, ModbusTcpConfig};
use point::{DevicePointMap, PollPoint};

/// 运行计数，供 `status()` 与测试断言。
#[derive(Debug, Default)]
struct Counters {
    rounds_ok: AtomicU64,
    rounds_failed: AtomicU64,
    messages: AtomicU64,
    points_skipped: AtomicU64,
}

/// 单点读取结果：wire 字节，或从站异常码（跳过该点，对齐 Java respHandler 异常分支）。
enum ReadOutcome {
    Wire(Vec<u8>),
    Exception(ExceptionCode),
}

/// 寄存器字 → wire 字节：大端拼接，对齐 Java MessageCodec 寄存器响应 data 段字节序。
pub fn registers_to_wire(words: &[u16]) -> Vec<u8> {
    words.iter().flat_map(|w| w.to_be_bytes()).collect()
}

/// 线圈/离散输入位序列 → wire 字节：每字节 LSB-first 打包，对齐 Modbus 标准
/// 响应编码（Java MessageCodec 读到的同一格式）。
pub fn coils_to_wire(coils: &[bool]) -> Vec<u8> {
    let mut out = vec![0u8; coils.len().div_ceil(8)];
    for (i, on) in coils.iter().enumerate() {
        if *on {
            out[i / 8] |= 1 << (i % 8);
        }
    }
    out
}

/// tokio-modbus 双层 Result → 读取结果：数据 → wire 字节，从站异常码 → Exception，
/// 传输/协议层错误上抛（调用方丢连接重连）。
fn to_outcome<T>(
    r: tokio_modbus::Result<Vec<T>>,
    wire: fn(&[T]) -> Vec<u8>,
) -> std::result::Result<ReadOutcome, tokio_modbus::Error> {
    match r {
        Ok(Ok(data)) => Ok(ReadOutcome::Wire(wire(&data))),
        Ok(Err(code)) => Ok(ReadOutcome::Exception(code)),
        Err(e) => Err(e),
    }
}

/// 读单点：set_slave → 按功能码发读请求 → 响应转 wire 字节。
///
/// Err 仅为传输/协议层错误（连接已不可信，调用方重连）；从站异常码走
/// [`ReadOutcome::Exception`]（单点跳过，不伤连接）。
async fn read_point(ctx: &mut Context, p: &PollPoint) -> Result<ReadOutcome> {
    ctx.set_slave(Slave(p.slave_addr));
    let addr = p.register_address;
    let qty = p.data_type.request_quantity();
    let outcome = match p.function_code {
        point::READ_COIL_STATUS => to_outcome(ctx.read_coils(addr, qty).await, coils_to_wire),
        point::READ_INPUT_STATUS => {
            to_outcome(ctx.read_discrete_inputs(addr, qty).await, coils_to_wire)
        }
        point::READ_HOLDING_REGISTER => to_outcome(
            ctx.read_holding_registers(addr, qty).await,
            registers_to_wire,
        ),
        point::READ_INPUT_REGISTER => {
            to_outcome(ctx.read_input_registers(addr, qty).await, registers_to_wire)
        }
        // parse_points 启动时已拒绝非 1/2/3/4 功能码（fail-fast）
        other => unreachable!("功能码 {other} 已在启动校验中拒绝"),
    };
    outcome.map_err(|e| {
        CommonError::Internal(format!(
            "modbus 读失败（slave {} 地址 {} 功能码 {}）: {e}",
            p.slave_addr, p.register_address, p.function_code
        ))
    })
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 单个 Modbus TCP server 的轮询器：持有点表与设备映射；连接生命周期在
/// [`run_poll_loop`]（Context 非 Clone，由循环独占持有）。
struct Poller {
    label: String,
    host: String,
    port: u16,
    connect_timeout: Duration,
    points: Vec<PollPoint>,
    point_map: HashMap<i64, DevicePointMap>,
    proto: Arc<dyn Protocol>,
    params: JsonValue,
    counters: Arc<Counters>,
}

impl Poller {
    fn new(
        server: &ModbusServerConfig,
        points: Vec<PollPoint>,
        point_map: HashMap<i64, DevicePointMap>,
        proto: Arc<dyn Protocol>,
        counters: Arc<Counters>,
    ) -> Self {
        Self {
            label: server.client_id.clone(),
            host: server.host.trim().to_string(),
            port: server.port,
            connect_timeout: server.connect_timeout(),
            points,
            point_map,
            proto,
            params: server.protocol.params.clone(),
            counters,
        }
    }

    /// 建立 Modbus TCP 连接。host 可为主机名（lookup_host 解析取首地址，Java Netty
    /// 同样按主机名解析）；connectTimeout 对齐 Java Netty 连接超时。
    async fn connect(&self) -> Result<Context> {
        let addrs = tokio::net::lookup_host((self.host.as_str(), self.port))
            .await
            .map_err(|e| {
                CommonError::Internal(format!(
                    "modbus {}: 解析 {}:{} 失败: {e}",
                    self.label, self.host, self.port
                ))
            })?;
        let addr = addrs.into_iter().next().ok_or_else(|| {
            CommonError::Internal(format!(
                "modbus {}: {}:{} 未解析到地址",
                self.label, self.host, self.port
            ))
        })?;
        // 初始 unit-id 用 TCP 设备广播地址，每点读取前 set_slave 覆盖（Java 同语义）
        let ctx = tokio::time::timeout(
            self.connect_timeout,
            connect_slave(addr, Slave::tcp_device()),
        )
        .await
        .map_err(|_| {
            CommonError::Internal(format!(
                "modbus {}: 连接 {addr} 超时（{}ms）",
                self.label,
                self.connect_timeout.as_millis()
            ))
        })?
        .map_err(|e| {
            CommonError::Internal(format!("modbus {}: 连接 {addr} 失败: {e}", self.label))
        })?;
        info!(target: "modbus", "modbus {}: 已连接 {addr}", self.label);
        Ok(ctx)
    }

    /// 轮询一轮：顺序读全部点 → 解析 → 上行。
    ///
    /// 单点异常/解析失败/映射缺失只跳过并打日志；传输级错误返回 Err
    /// （调用方丢连接、下轮重连）。
    async fn poll_once(&self, ctx: &mut Context, sink: &dyn DeviceSink) -> Result<()> {
        for p in &self.points {
            match read_point(ctx, p).await? {
                ReadOutcome::Wire(wire) => self.handle_point(p, &wire, sink).await,
                ReadOutcome::Exception(code) => {
                    self.skip(p, format!("从站异常码 {code:?}"));
                }
            }
        }
        Ok(())
    }

    /// 单点后处理：字节解析 → 设备映射 → Lua 封装 → sink（上行唯一出口）。
    ///
    /// 任一单点失败只 warn 跳过（对齐 Java UpModbusHandler 捕获 DecodeMessageException、
    /// ModbusDeviceProtocol.decodeMulti 映射缺失 warn+return null），不影响本轮其余点。
    async fn handle_point(&self, p: &PollPoint, wire: &[u8], sink: &dyn DeviceSink) {
        let value = match p.data_type.parse_value(wire, p.byte_order) {
            Ok(value) => value,
            Err(e) => {
                self.skip(p, format!("字节解析失败: {e}"));
                return;
            }
        };
        // Java ModbusDeviceProtocol.decodeMulti(:54-58)：映射缺失 → warn + 丢弃
        let Some(map) = self.point_map.get(&p.point_id) else {
            self.skip(p, "设备测点物模型映射配置缺失".to_string());
            return;
        };
        let payload = json!({
            "deviceId": map.device_id,
            "code": map.metric,
            "value": value,
            "ts": now_millis(),
        });
        // json! 构造的值（字符串/数值/bool）序列化不会失败
        let payload = serde_json::to_vec(&payload).expect("JSON 值序列化不可失败");
        let messages = match self.proto.decode_multi(&payload, &self.params) {
            Ok(messages) => messages,
            Err(e) => {
                self.skip(p, format!("协议解码失败: {e}"));
                return;
            }
        };
        for message in messages {
            match sink.store_message(message).await {
                Ok(()) => {
                    self.counters.messages.fetch_add(1, Ordering::Relaxed);
                }
                // 上行写失败（如 Kafka 不可用）打 error 可见，不中断本轮
                // （Java DeviceContext.storeMessage 异常同样只记日志）。
                Err(e) => {
                    error!(target: "modbus", "{} 点 {} 上行写入失败: {e}", self.label, p.point_id)
                }
            }
        }
    }

    /// 单点跳过：计数 + warn（失败可见，禁止静默吞错）。
    fn skip(&self, p: &PollPoint, reason: String) {
        self.counters.points_skipped.fetch_add(1, Ordering::Relaxed);
        warn!(
            target: "modbus",
            "{} 点 {}（slave {} 地址 {}）跳过: {reason}",
            self.label, p.point_id, p.slave_addr, p.register_address
        );
    }
}

/// 轮询主循环，对齐 Java `scheduleWithFixedDelay(readTask, 0, readDiff, SECONDS)`：
/// 上一轮结束后再等 readDiff 才开始下一轮（非固定频率）；首轮立即执行（initialDelay=0）。
///
/// 连接管理对齐 Java ModBusTcpPoll：连接失败等 retryDelay 重试，连续 maxRetries 次
/// 失败放弃该 server（error 日志）；连接成功清零计数；读传输错误 → 丢连接下轮重连。
async fn run_poll_loop(
    poller: Arc<Poller>,
    sink: Arc<dyn DeviceSink>,
    read_diff: Duration,
    retry_delay: Duration,
    max_retries: u32,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut conn: Option<Context> = None;
    let mut retries = 0u32;
    loop {
        // 1. 确保连接（Java ModBusTcpPoll RETRY 状态：等 retryDelay，maxRetries 放弃）
        if conn.is_none() {
            match poller.connect().await {
                Ok(ctx) => {
                    retries = 0;
                    conn = Some(ctx);
                }
                Err(e) => {
                    retries += 1;
                    poller
                        .counters
                        .rounds_failed
                        .fetch_add(1, Ordering::Relaxed);
                    if retries >= max_retries {
                        error!(
                            target: "modbus",
                            "modbus {}: 连续 {retries} 次连接失败，放弃该 server（最后错误: {e}）",
                            poller.label
                        );
                        break;
                    }
                    warn!(
                        target: "modbus",
                        "modbus {}: 连接失败（{retries}/{max_retries}，{}s 后重试）: {e}",
                        poller.label,
                        retry_delay.as_secs()
                    );
                    tokio::select! {
                        _ = tokio::time::sleep(retry_delay) => continue,
                        changed = shutdown.changed() => {
                            if changed.is_err() {
                                warn!(target: "modbus", "shutdown 通道已关闭，退出轮询 {}", poller.label);
                            }
                            break;
                        }
                    }
                }
            }
        }
        // 2. 轮询一轮（传输级失败 → 丢连接，下轮重连）
        let ctx = conn.as_mut().expect("上一段已确保连接为 Some");
        match poller.poll_once(ctx, sink.as_ref()).await {
            Ok(()) => {
                poller.counters.rounds_ok.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                poller
                    .counters
                    .rounds_failed
                    .fetch_add(1, Ordering::Relaxed);
                warn!(target: "modbus", "modbus {}: 轮询失败，下轮重连: {e}", poller.label);
                conn = None;
            }
        }
        // 3. scheduleWithFixedDelay：本轮结束后再等 readDiff
        tokio::select! {
            _ = tokio::time::sleep(read_diff) => {}
            changed = shutdown.changed() => {
                if changed.is_err() {
                    warn!(target: "modbus", "shutdown 通道已关闭，退出轮询 {}", poller.label);
                }
                break;
            }
        }
    }
    info!(target: "modbus", "轮询任务退出 {}", poller.label);
}

/// Modbus TCP connector：每个配置的 server 一个轮询任务。
pub struct ModbusTcpConnector {
    servers: Vec<ModbusServerConfig>,
    /// 脚本目录白名单（None = 不启用，向后兼容），start 构建注册表时透传。
    script_dir: Option<String>,
    counters: Arc<Counters>,
    shutdown: watch::Sender<bool>,
    handles: Mutex<Vec<JoinHandle<()>>>,
}

// `Arc<dyn Protocol>` / `JoinHandle` / `watch::Sender` 无 Debug，手写只暴露配置面。
impl std::fmt::Debug for ModbusTcpConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModbusTcpConnector")
            .field("servers", &self.servers)
            .finish_non_exhaustive()
    }
}

impl ModbusTcpConnector {
    /// 构建并校验配置（clientId/host/点表 URL/readDiff/connectTimeout），失败即启动报错。
    /// `script_dir` 为 Some 时 start 加载协议脚本做白名单校验。
    pub fn new(config: ModbusTcpConfig, script_dir: Option<&str>) -> Result<Self> {
        for server in &config.configuration {
            server.validate()?;
        }
        let (shutdown, _) = watch::channel(false);
        Ok(Self {
            servers: config.configuration,
            script_dir: script_dir.map(String::from),
            counters: Arc::new(Counters::default()),
            shutdown,
            handles: Mutex::new(Vec::new()),
        })
    }

    /// 从 `GatewayConfig.modbus_tcp`（一期为 JsonValue 兜底）构建。
    pub fn from_value(value: &JsonValue, script_dir: Option<&str>) -> Result<Self> {
        Self::new(ModbusTcpConfig::from_value(value)?, script_dir)
    }

    fn lock_handles(&self) -> std::sync::MutexGuard<'_, Vec<JoinHandle<()>>> {
        self.handles.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[async_trait]
impl Connector for ModbusTcpConnector {
    async fn start(&self, sink: Arc<dyn DeviceSink>) -> Result<()> {
        // 阶段一：构建 poller（含 HTTP 拉点表；std Mutex 锁不能跨 await，先收集再统一 spawn）。
        // 任一 server 失败 → 整个 connector 拒绝启动（失败可见，不半启动）。
        let http = reqwest::Client::new();
        let mut pollers = Vec::new();
        for server in &self.servers {
            // type != "script"（如 Java 的 jar）→ ProtocolRegistry 报错，启动失败可见。
            let registry = ProtocolRegistry::from_protocols_in(
                &[&server.protocol],
                self.script_dir.as_deref(),
            )?;
            let proto = registry.get(&server.protocol.name)?;

            let points_body = point::fetch_body(&http, &server.point_dtos_path).await?;
            let points = point::parse_points(&server.point_dtos_path, &points_body)?;
            // Java 空点表 warn+跳过该 server；Rust 报错拒绝启动（空点表必是配置/接口异常）
            if points.is_empty() {
                return Err(CommonError::Config(format!(
                    "modbus-tcp {}: 点表为空（{}），拒绝启动",
                    server.label(),
                    server.point_dtos_path
                )));
            }
            let map_body = point::fetch_body(&http, &server.point_map_dtos_path).await?;
            let point_map = point::parse_point_map(&server.point_map_dtos_path, &map_body)?;

            let poller = Arc::new(Poller::new(
                server,
                points,
                point_map,
                proto,
                self.counters.clone(),
            ));
            pollers.push((server, poller));
        }
        // 阶段二：spawn 轮询任务
        let mut handles = self.lock_handles();
        for (server, poller) in pollers {
            info!(
                target: "modbus",
                "启动轮询 {}（{}:{}）：{} 个点，每 {}s",
                server.label(),
                server.host,
                server.port,
                poller.points.len(),
                server.read_diff
            );
            handles.push(tokio::spawn(run_poll_loop(
                poller,
                sink.clone(),
                server.read_diff_duration(),
                server.retry_delay_duration(),
                server.max_retries,
                self.shutdown.subscribe(),
            )));
        }
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        // 无接收者（未 start）时 send 返回 Err，此处无需处理。
        let _ = self.shutdown.send(true);
        let handles = std::mem::take(&mut *self.lock_handles());
        for handle in handles {
            // 轮询任务内部不 panic；JoinError 只可能是外部 abort，同样视为已停止。
            let _ = handle.await;
        }
        info!(target: "modbus", "已停止");
        Ok(())
    }

    fn status(&self) -> StatusMap {
        let mut status = StatusMap::new();
        status.insert("connector".into(), json!("modbus-tcp"));
        status.insert("servers".into(), json!(self.servers.len()));
        status.insert(
            "clients".into(),
            json!(
                self.servers
                    .iter()
                    .map(|s| s.client_id.clone())
                    .collect::<Vec<_>>()
            ),
        );
        status.insert(
            "roundsOk".into(),
            json!(self.counters.rounds_ok.load(Ordering::Relaxed)),
        );
        status.insert(
            "roundsFailed".into(),
            json!(self.counters.rounds_failed.load(Ordering::Relaxed)),
        );
        status.insert(
            "pointsSkipped".into(),
            json!(self.counters.points_skipped.load(Ordering::Relaxed)),
        );
        status.insert(
            "messagesStored".into(),
            json!(self.counters.messages.load(Ordering::Relaxed)),
        );
        status
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use connector_common::LuaProtocol;
    use connector_common::sink::MockSink;
    use datatype::{ByteOrder, DataType};

    fn lua_path() -> String {
        format!("{}/protocols/modbus_metric.lua", env!("CARGO_MANIFEST_DIR"))
    }

    fn order(s: &str) -> ByteOrder {
        s.parse().unwrap()
    }

    fn poll_point(point_id: i64, code: u8, addr: u16, dt: DataType) -> PollPoint {
        PollPoint {
            point_id,
            slave_addr: 1,
            function_code: code,
            register_address: addr,
            data_type: dt,
            byte_order: order("1234"),
        }
    }

    /// 构造一份 server 配置（JSON 形态，走真实解析 + 校验路径）。
    fn server_config(points_url: &str, map_url: &str, host: &str, port: u16) -> ModbusServerConfig {
        let value = json!({
            "configuration": [{
                "clientId": "test",
                "pointDTOSPath": points_url,
                "pointMapDTOSPath": map_url,
                "host": host,
                "port": port,
                "readDiff": 1,
                "maxRetries": 100,
                "retryDelay": 1,
                "connectTimeout": 500,
                "protocol": {
                    "name": "modbus_metric",
                    "path": lua_path(),
                    "type": "script",
                },
            }]
        });
        let cfg = ModbusTcpConfig::from_value(&value).unwrap();
        let server = cfg.configuration.into_iter().next().unwrap();
        server.validate().unwrap();
        server
    }

    // ---- wire 字节转换（Seam C）----

    #[test]
    fn registers_and_coils_convert_to_wire_bytes() {
        // 寄存器：大端拼接（Java MessageCodec data 段字节序）
        assert_eq!(
            registers_to_wire(&[0x1234, 0xABCD]),
            vec![0x12, 0x34, 0xAB, 0xCD]
        );
        assert_eq!(registers_to_wire(&[]), Vec::<u8>::new());
        // 线圈：每字节 LSB-first 打包（Modbus 标准响应编码）
        assert_eq!(coils_to_wire(&[true, false, true]), vec![0b0000_0101]);
        // 第 9 个线圈落到第二字节 bit0
        let mut nine = vec![false; 9];
        nine[8] = true;
        assert_eq!(coils_to_wire(&nine), vec![0x00, 0x01]);
        assert_eq!(coils_to_wire(&[]), Vec::<u8>::new());
    }

    // ---- 单点 payload → Lua → 上行消息（链路契约）----

    /// 单点 JSON → modbus_metric.lua → DEVICE_REPORT_REQ（对齐 Java
    /// ModbusDeviceProtocol.decodeMulti 的消息形态：无 sn/messageId，metrics 单元素）。
    #[test]
    fn per_point_payload_decodes_via_lua() {
        let proto = LuaProtocol::from_script("modbus_metric", &lua_path()).unwrap();
        let payload = json!({
            "deviceId": "dev-1",
            "code": "power",
            "value": 42,
            "ts": 1_700_000_000_123i64,
        });
        let messages = proto
            .decode_multi(&serde_json::to_vec(&payload).unwrap(), &json!({}))
            .unwrap();
        assert_eq!(messages.len(), 1);
        let msg = &messages[0];
        assert_eq!(msg["messageType"], "DEVICE_REPORT_REQ");
        assert_eq!(msg["deviceId"], "dev-1");
        assert_eq!(msg["resume"], "N");
        assert_eq!(msg["timeStamp"], json!(1_700_000_000_123i64));
        assert_eq!(msg["ingestionTime"], json!(1_700_000_000_123i64));
        assert_eq!(msg["changeRpt"], json!(false));
        // Java 不设置 sn/messageId，脚本同样不输出
        assert!(msg.get("sn").is_none());
        assert!(msg.get("messageId").is_none());
        let m = &msg["metrics"][0];
        assert_eq!(m["ts"], json!(1_700_000_000_123i64));
        assert_eq!(m["code"], "power");
        assert_eq!(m["value"], json!(42));
    }

    /// 浮点值经 Lua 往返不失真（f64 round-trip）。
    #[test]
    fn float_value_survives_lua_roundtrip() {
        let proto = LuaProtocol::from_script("modbus_metric", &lua_path()).unwrap();
        let payload = json!({"deviceId": "d", "code": "temp", "value": 25.5, "ts": 1});
        let messages = proto
            .decode_multi(&serde_json::to_vec(&payload).unwrap(), &json!({}))
            .unwrap();
        assert_eq!(messages[0]["metrics"][0]["value"], json!(25.5));
    }

    // ---- connector 生命周期 ----

    /// `type = "jar"` 协议 → start 报错（注册表先于点表拉取检查，URL 可为假值）。
    #[tokio::test]
    async fn start_rejects_jar_protocol() {
        let mut server = server_config("http://127.0.0.1:1/p", "http://127.0.0.1:1/m", "h", 502);
        server.protocol.protocol_type = "jar".into();
        let connector = ModbusTcpConnector::new(
            ModbusTcpConfig {
                configuration: vec![server],
            },
            None,
        )
        .unwrap();
        let sink: Arc<dyn DeviceSink> = Arc::new(MockSink::new());
        let err = connector.start(sink).await.unwrap_err();
        assert!(
            matches!(err, CommonError::UnsupportedProtocolType { .. }),
            "{err:?}"
        );
        connector.stop().await.unwrap();
    }

    /// script_dir 白名单：脚本真实存在但在目录外 → start 报错（注册表先于点表拉取）。
    #[tokio::test]
    async fn start_rejects_script_outside_dir() {
        let dir = std::env::temp_dir().join(format!("gw-scriptdir-{}-modbus", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let server = server_config("http://127.0.0.1:1/p", "http://127.0.0.1:1/m", "h", 502);
        let connector = ModbusTcpConnector::new(
            ModbusTcpConfig {
                configuration: vec![server],
            },
            Some(dir.to_str().unwrap()),
        )
        .unwrap();
        let sink: Arc<dyn DeviceSink> = Arc::new(MockSink::new());
        let err = connector.start(sink).await.unwrap_err();
        assert!(err.to_string().contains("拒绝加载"), "{err}");
        connector.stop().await.unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }

    /// 点表拉取失败 → start 报错（Java warn+跳过 → Rust 失败可见）。
    #[tokio::test]
    async fn start_fails_when_point_fetch_fails() {
        // 127.0.0.1:1 连接立即被拒，确定性失败且快
        let server = server_config(
            "http://127.0.0.1:1/points",
            "http://127.0.0.1:1/map",
            "127.0.0.1",
            502,
        );
        let connector = ModbusTcpConnector::new(
            ModbusTcpConfig {
                configuration: vec![server],
            },
            None,
        )
        .unwrap();
        let sink: Arc<dyn DeviceSink> = Arc::new(MockSink::new());
        let err = connector.start(sink).await.unwrap_err();
        assert!(err.to_string().contains("点表拉取"), "{err}");
        connector.stop().await.unwrap();
    }

    /// 从站不可达：连接失败按 retryDelay 重试，连续 maxRetries 次后放弃退出
    /// （对齐 Java ModBusTcpPoll 的 RETRY→放弃语义）。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unreachable_slave_retries_then_gives_up() {
        // 127.0.0.1:1 连接立即被拒
        let server = server_config(
            "http://127.0.0.1:1/p",
            "http://127.0.0.1:1/m",
            "127.0.0.1",
            1,
        );
        let counters = Arc::new(Counters::default());
        let registry = ProtocolRegistry::from_protocols(&[&server.protocol]).unwrap();
        let proto = registry.get(&server.protocol.name).unwrap();
        let points = vec![poll_point(
            1,
            point::READ_HOLDING_REGISTER,
            0,
            DataType::Int16,
        )];
        let poller = Arc::new(Poller::new(
            &server,
            points,
            HashMap::new(),
            proto,
            counters.clone(),
        ));
        let sink: Arc<dyn DeviceSink> = Arc::new(MockSink::new());
        let (_tx, rx) = watch::channel(false);
        let handle = tokio::spawn(run_poll_loop(
            poller,
            sink,
            Duration::from_millis(10), // readDiff
            Duration::from_millis(10), // retryDelay
            2,                         // maxRetries：2 次失败即放弃
            rx,
        ));
        tokio::time::timeout(Duration::from_secs(60), handle)
            .await
            .expect("等待超时：应在 maxRetries 次失败后放弃退出")
            .unwrap();
        assert_eq!(counters.rounds_failed.load(Ordering::Relaxed), 2);
        assert_eq!(counters.rounds_ok.load(Ordering::Relaxed), 0);
    }

    // ---- 端到端（假 HTTP 点表 + 假 Modbus 从站）----

    /// 假从站：保持寄存器 100..101 = [0x0001, 0x86A0]（int32 大端 = 100000），
    /// 其余请求一律异常码 IllegalDataAddress。
    struct FakeSlave;

    impl tokio_modbus::server::Service for FakeSlave {
        type Request = tokio_modbus::Request<'static>;
        type Response = tokio_modbus::Response;
        type Exception = ExceptionCode;
        type Future = std::future::Ready<std::result::Result<Self::Response, Self::Exception>>;

        fn call(&self, req: Self::Request) -> Self::Future {
            use tokio_modbus::{Request, Response};
            std::future::ready(match req {
                Request::ReadHoldingRegisters(100, _) => {
                    Ok(Response::ReadHoldingRegisters(vec![0x0001, 0x86A0]))
                }
                _ => Err(ExceptionCode::IllegalDataAddress),
            })
        }
    }

    /// 端到端：connector start（拉点表）→ 连接假从站 → 首轮立即轮询 →
    /// MockSink 收到 DEVICE_REPORT_REQ。覆盖 连接/set_slave/读/wire 转换/解析/
    /// 映射/Lua 封装/上行 全链路。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn end_to_end_polls_fake_slave_into_sink() {
        // 假 HTTP 点表（axum，与 connector-request 测试同一模式）
        let points = json!([{
            "pointId": 11,
            "slaveAddr": 1,
            "functionCode": 3,
            "registerAddress": 100,
            "dataType": "int32",
            "byteOrder": "1234",
        }])
        .to_string();
        let point_map =
            json!([{"pointId": 11, "deviceId": "dev-9", "metric": "power"}]).to_string();
        let app = axum::Router::new()
            .route(
                "/points",
                axum::routing::get(move || {
                    let body = points.clone();
                    async move { body }
                }),
            )
            .route(
                "/map",
                axum::routing::get(move || {
                    let body = point_map.clone();
                    async move { body }
                }),
            );
        let http_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(http_listener, app).await.unwrap() });

        // 假 Modbus 从站（tokio-modbus tcp-server，仅测试启用该 feature）
        let modbus_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let modbus_addr = modbus_listener.local_addr().unwrap();
        tokio::spawn(async move {
            use tokio_modbus::server::tcp::{Server, accept_tcp_connection};
            let svc = |_socket_addr| Ok(Some(FakeSlave));
            let on_connected = |stream, socket_addr| async move {
                accept_tcp_connection(stream, socket_addr, svc)
            };
            Server::new(modbus_listener)
                .serve(&on_connected, |e| eprintln!("假从站处理错误: {e}"))
                .await
                .unwrap();
        });

        let server = server_config(
            &format!("http://{http_addr}/points"),
            &format!("http://{http_addr}/map"),
            &modbus_addr.ip().to_string(),
            modbus_addr.port(),
        );
        let connector = ModbusTcpConnector::new(
            ModbusTcpConfig {
                configuration: vec![server],
            },
            None,
        )
        .unwrap();
        let sink = Arc::new(MockSink::new());
        connector
            .start(sink.clone() as Arc<dyn DeviceSink>)
            .await
            .unwrap();

        // 首轮立即执行（scheduleWithFixedDelay initialDelay=0）→ 等首条上行消息
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        while sink.is_empty() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "等待超时：应收到上行消息"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let messages = sink.messages();
        let msg = &messages[0];
        assert_eq!(msg["messageType"], "DEVICE_REPORT_REQ");
        assert_eq!(msg["deviceId"], "dev-9");
        assert_eq!(msg["metrics"][0]["code"], "power");
        // 寄存器 [0x0001, 0x86A0] → 大端字节 00 01 86 A0 → int32 = 100000
        assert_eq!(msg["metrics"][0]["value"], json!(100000));

        connector.stop().await.unwrap();
    }
}
