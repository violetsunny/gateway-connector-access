# 01: workspace 基建 + connector-common

**What to build:** `cargo run -p gateway -- config.toml` 能读取 TOML 配置、构建 Lua 协议注册表并启动（无 connector 段时空转退出）；示例 Lua 脚本对同一输入字节分别以 hex 与 JSON 形态解码出 Message 并经 MockSink 断言。

**Blocked by:** None (can start immediately)

**Status:** done（commit 90de9c9，主 agent 验收：fmt/clippy/build/test 全绿，33 tests passed）

- [ ] rust-demo 改造为 cargo workspace：`crates/connector-common`、`crates/gateway`、8 个 connector crate 骨架（tcp/udp/mqtt/modbus-tcp/http/snmp/request/opcua）全部预建并加入 workspace members（后续票只填自己 crate，不再改根 Cargo.toml）
- [ ] connector-common：`Connector` trait（async start/stop/status）、`DeviceSink` trait + MockSink + KafkaSink（rdkafka，topic 按 messageType 路由：metric→enn_data_iot_metric、event→data_iot_event、info→data_iot_device_info）
- [ ] `DeviceDirectory` trait（get_device_id_by_sn/auth_device）+ TOML 本地设备清单实现（`ponytail:` 注释标注生产需接 Redis/DB）
- [ ] Message 模型 serde 对齐 Java：Message/MetricReportRequest/Metric/EventReportRequest/InfoReportRequest/LoginRequest/LoginResponse/OperationRequest/OperationResponse，MessageType 15 枚举值序列化为 Java 同名大写下划线字符串，字段名与 Java JSON 一致
- [ ] Lua 协议引擎（mlua 5.4 vendored）：decode/decode_multi/encode/login 四入口，字节双形态（hex 字符串 + JSON 数组），params table 注入，脚本文件加载，协议注册表按配置构建；type=jar 配置启动报错（失败可见）；脚本错误/panic 有明确错误信息
- [ ] TOML 配置模型：每 connector 一段、字段对齐 Java XxxConfig camelCase（serde rename），内嵌 protocol{name,path,params,type}；`[kafka]` 段
- [ ] gateway 主程序：装配启停全部 connector 段 + tracing 初始化
- [ ] 根 AGENTS.md 修订：解除 rand-only 限制，登记 workspace 命令与依赖白名单（见 spec）
- [ ] 现有 src/main.rs demo 保留不动
- [ ] `cargo fmt --check` / `cargo clippy -- -D warnings` / `cargo build` / `cargo test` 全绿；Seam B 测试覆盖成功 + 错误脚本 + 类型不匹配路径
