# AGENTS.md — rust-connector-demo

Rust cargo workspace（edition 2024，需 rustc ≥ 1.85）。根为纯 workspace manifest（无根 package），成员：`learn-demo`（rand 学习 demo）、`crates/connector-common`（Connector/DeviceSink trait、Message、Lua 协议引擎、TOML 配置、统一错误）、`crates/gateway`（主程序）、8 个 connector：`crates/connector-{tcp,udp,mqtt,modbus-tcp,http,snmp,request,opcua}`。

工程背景：iot-gateway Java（Spring Boot 2.7 + Netty）connectors 的 Rust 重写，票 01-12 已全部 done。真源在 `.scratch/rust-connectors/`（本地未跟踪目录，勿提交）：`spec.md`（需求）、`issues/*.md`（每票一文件，状态看文件头 `Status:` 行）、`review-findings.md`（评审台账）、`manual-verification.md`（需真实外部服务的人工验证清单）。结构与 agent skills 细节见根 `AGENTS.md` 与 `docs/agents/`。

## 命令

- 全 workspace：`cargo build --workspace` / `cargo test --workspace`
- 单 crate（并行开发期只用这个，减少 target 锁竞争）：`cargo build -p connector-<name>` / `cargo test -p connector-<name>`
- 改动后验证链：`cargo fmt -p <crate> -- --check` → `cargo clippy -p <crate> --all-targets -- -D warnings` → `cargo test -p <crate>`
- 运行 gateway：`cargo run -p gateway -- config.example.toml`（配置路径 = 首个 CLI 参数，缺省读 `GATEWAY_CONFIG` 环境变量；`RUST_LOG=debug` 调日志）
- 冒烟回归：`scripts/smoke.sh`（无外部服务，断言 udp/http 上行、kafka 空配置降级 stdout、SIGINT 优雅停机；退出码 0=全过）
- 质量闸门（一次性）：`cp scripts/pre-commit .git/hooks/pre-commit && chmod +x .git/hooks/pre-commit`（hook 只跑 fmt 检查；clippy/test 分钟级，走上面验证链）

## 依赖纪律

- 白名单登记在根 `AGENTS.md`；依赖统一声明在根 `Cargo.toml` 的 `[workspace.dependencies]`，子 crate 用 `workspace = true` 引用；新增依赖先登记白名单
- snmp 无可用 crate，v2c BER 为手写实现（票 08），勿引入 snmp 依赖
- opcua 传递启用 serde_json `arbitrary_precision`（Number 表示会变），common 层已适配并留回归锚定测试（connector-common/src/protocol.rs）
- `learn-demo` 用 rand 0.10.2 API：`rand::rng()` + `rng.random_range(..)`（trait `rand::RngExt`）；`thread_rng()`/`gen_range()` 编译不过
- rdkafka/mlua/opcua 首次编译慢，命令 timeout 设大

## 约定

- 上行唯一出口 `DeviceSink::store_message`（connector-common/src/sink.rs）；协议一律 Lua 脚本（mlua，脚本在各 crate 的 `protocols/*.lua`），字节以 hex+JSON 双形态暴露
- 解析外部输入必须带资源上限：Lua 沙箱 64MB 内存+指令数 hook、ZLIB 解压输出、TCP 拆包缓冲、SNMP 报文均设上限，超限报错可见（评审 3 个 Critical 同族教训）
- gateway 装配失败隔离：单 connector 构建/启动失败仅记 error 不拖垮进程；`config.example.toml` 零外部服务可跑（kafka 留空 → stdout sink）
- 刻意简化处留 `ponytail:` 注释；失败必须可见（禁静默吞错）
- 提交纪律：每票独立 commit、只 stage 本票文件、禁 `git add -A`、禁 `git push`；信息格式参照 f537968：`FEAT: <说明> JIRA: <单号> DIFF: <变更摘要>`
