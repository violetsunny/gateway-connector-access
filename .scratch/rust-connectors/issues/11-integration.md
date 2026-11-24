# 11: 集成冒烟 + 交付收尾

**What to build:** 一份完整示例 TOML（8 connector 全装配）能让 gateway 启动冒烟（无真实外设时各 connector 报连接失败但进程不退、日志可见）；2 个示例 Lua 协议脚本定稿（JSON 透传、hex 帧解析）；人工验证清单产出交用户。

**Blocked by:** 02,03,04,05,06,07,08,09,10

**Status:** done

- [x] 示例配置 `config.example.toml` 覆盖 8 connector + kafka + 协议注册
- [x] 启动冒烟：gateway 加载全配置不 panic，单 connector 失败不影响其余（隔离性）
- [x] 示例 Lua 脚本 ×2 可被 http/tcp 链路实际调用（集成测试）
- [x] 人工验证清单：真实 Kafka 收发、MQTT broker 上下行、Modbus slave 轮询、OPC UA server 读值、第三方云 API 轮询（每项含步骤+预期+判定标准）
- [x] 全 workspace fmt/clippy/build/test 终验全绿
- [x] 收尾登记项：Cargo.lock 统一提交；依赖上收 [workspace.dependencies]；common MqttConfigServer 补 lastWill 字段；chrono 登记白名单；CommonError 是否加 Http/Protocol 变体裁决；GatewayConfig 各 connector 段强类型上收或确立 from_value 模式；spec 中 Connector::start(cfg,sink) 文字与实际 start(sink)+构造期注入对齐；http std/info 上行行为差异列入人工核对

## Comments

- **交付物**：`crates/gateway/src/main.rs`（装配 8 connector + 失败隔离 + 降级 sink + Ctrl-C 逆序优雅停机）；`crates/gateway/tests/example_scripts_e2e.rs`（hex_frame + json_passthrough 经 http 通配链路真实上行）；`config.example.toml`（workspace 根，无外部服务可冒烟）；`.scratch/rust-connectors/manual-verification.md`（7 项人工验证）。
- **冒烟结果**（无外部服务机器）：8 段全装配；kafka bootstrapServers 空 → stdout sink 降级；tcp server 监听 9000、client 连 9001 失败自动重连；udp 9100 收 hex 帧上行 value=42；http 8080 curl 上行成功；modbus 点表拉取失败仅记 ERROR；mqtt/opcua/snmp 连接失败仅记日志重连；SIGINT → 逆序 stop，exit=0。
- **决策 1（CommonError）**：不加 Http/Protocol 变体。现有 `Internal(String)` / `Config(String)` 已覆盖各 connector 报错场景，加变体只增匹配负担不增信息。
- **决策 2（GatewayConfig 段建模）**：维持 JsonValue 兜底 + 各 crate `from_value` 模式，不上收强类型。理由：强类型上收会让 common 依赖 8 个 connector crate（循环依赖），或把各 crate 的 config 结构搬进 common（违背"各 connector 持有自己的强类型 config"边界）。gateway 侧 `serde_json::from_value` 一次转换即得强类型，错误信息完整。
- **决策 3（lastWill）**：common `MqttConfigServer` 补 `last_will: Option<MqttLastWill>`（topic/payload 必填，qos 缺省 0、retain 缺省 false），mqtt connector 经 rumqttc `LastWill::new` + `set_last_will` 消费；Java 字段名 `message` 在配置里写作 `payload`。
- **偏差说明**：hex_frame 示例未经 tcp 链路验证——tcp 链路要求先登录（LOGIN_REQUEST），hex_frame 不产生登录帧，无法走通；改经 http 通配链路集成测试覆盖（body 即协议输入）。json_passthrough 经 tcp 链路由 connector-tcp 既有 server_e2e/client_e2e 覆盖。
- **依赖上收**：全部第三方依赖 + 8 个 connector path 依赖统一进根 `[workspace.dependencies]`，子 crate `workspace = true`；Cargo.lock 随本票入库；白名单补 chrono。

## Comments

- 主 agent 验收（2026-11-02）：280 tests 全绿；亲测冒烟通过——udp 9100 hex 帧→DEVICE_REPORT_REQ value=42 上行、http 8080 /api/productA→SUCCESS+value=1 上行、modbus 点表失败隔离不拖垮进程、SIGINT 优雅停机 exit=0、kafka 缺失降级 stdout sink（WARN 可见）。
