# AGENTS.md

Rust cargo workspace（edition 2024），由单包 demo 改造为 iot-gateway 重写项目。
根为纯 workspace manifest（无根 package）+ `crates/*` 与 `learn-demo`（rand 学习 demo）全部成员。

## workspace 结构

- `crates/connector-common`：共享基建——核心 trait（Connector / DeviceSink / DeviceDirectory / Protocol）、消息模型、Lua 协议引擎（mlua）、TOML 配置模型、统一错误（thiserror）。
- `crates/gateway`：主程序，读 TOML 配置 → 初始化 tracing → 构建协议注册表 → 构建 sink（Kafka，不可用降级 stdout）→ 装配 8 个 connector（失败隔离）→ Ctrl-C 逆序优雅停机。示例配置 `config.example.toml`（workspace 根，无外部服务可冒烟）。
- `crates/connector-{tcp,udp,mqtt,modbus-tcp,http,snmp,request,opcua}`：各协议 connector（票 02-09 已完成）。

## 常用命令

- 构建全部：`cargo build --workspace`
- 检查：`cargo check --workspace`
- 格式化：`cargo fmt --all`（校验：`cargo fmt --all --check`）
- Lint：`cargo clippy --workspace --all-targets -- -D warnings`
- 测试：`cargo test --workspace`；单个：`cargo test -p <crate> <测试名>`
- 运行 gateway：`cargo run -p gateway -- <config.toml>`（或设 `GATEWAY_CONFIG` 环境变量）
- 运行 demo：`cargo run -p learn-demo`

## 依赖白名单（新增须在此登记）

tokio、tokio-util、serde、serde_json、toml、tracing、tracing-subscriber、anyhow、thiserror、async-trait、rdkafka、rumqttc、tokio-modbus、axum、tower（http 测试）、reqwest、mlua、flate2、governor、opcua、cron/tokio-cron-scheduler、chrono（request 时间戳）、socket2（udp SO_BROADCAST 必需）、rand（保留）。snmp 无可用 crate（snmp-rs 不存在于 crates.io），v2c BER 已自写（票08）。

- 依赖统一声明在根 `Cargo.toml` 的 `[workspace.dependencies]`，子 crate 用 `workspace = true` 引用。
- 不引入白名单外、未在 `Cargo.toml` 声明的 crate。
- edition 2024 需 rustc ≥ 1.85。
- `target/` 已 gitignore，构建产物不提交。

## Agent skills

### Issue tracker

Issues 以本地 markdown 形式存放在 `.scratch/<feature-slug>/`。See `docs/agents/issue-tracker.md`.

### Triage labels

使用默认五标签（needs-triage / needs-info / ready-for-agent / ready-for-human / wontfix）。See `docs/agents/triage-labels.md`.

### Domain docs

Single-context：根目录 `GLOSSARY.md` + `docs/adr/`。See `docs/agents/domain.md`.
