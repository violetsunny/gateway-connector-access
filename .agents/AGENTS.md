# AGENTS.md — rust-demo

Rust cargo workspace，edition 2024。结构：根为纯 workspace manifest（无根 package）+ `learn-demo`（rand 学习 demo）+ `crates/connector-common`（trait/Message/Lua 协议引擎/配置/Sink）+ `crates/gateway`（主程序）+ 8 个 connector crate（tcp/udp/mqtt/modbus-tcp/http/snmp/request/opcua）。

当前工程：iot-gateway connectors 的 Rust 重写。真源：`.scratch/rust-connectors/spec.md` + `.scratch/rust-connectors/issues/*.md`（票与状态）+ `review-findings.md`（评审台账）+ `manual-verification.md`（人工验证清单）。

## 命令

- 构建/测试全 workspace：`cargo build --workspace` / `cargo test --workspace`
- 单 crate（并行开发期只用这个，减少 target 锁竞争）：`cargo build -p connector-<name>` / `cargo test -p connector-<name>`
- 改动后验证：`cargo fmt -p <crate> -- --check` → `cargo clippy -p <crate> --all-targets -- -D warnings` → `cargo test -p <crate>`
- gateway 冒烟：`scripts/smoke.sh`（无外部服务，断言 udp/http 上行、kafka 降级、优雅停机，退出码 0=全过）
- 质量闸门：`cp scripts/pre-commit .git/hooks/pre-commit && chmod +x .git/hooks/pre-commit`（commit 时跑 fmt 检查）

## 依赖纪律

- 依赖白名单见根 `AGENTS.md`；依赖统一声明在根 `Cargo.toml` 的 `[workspace.dependencies]`（票 11 已上收），子 crate 用 `workspace = true` 引用；新增依赖先登记白名单
- `learn-demo` 依赖 `rand = "0.10.2"`：API 用 `rand::rng()` + `rng.random_range(..)`（trait `rand::RngExt`），`thread_rng()`/`gen_range()` 编译不过
- edition 2024 需 rustc ≥ 1.85；rdkafka/mlua/opcua 首次编译慢，命令 timeout 设大

## 约定

- 上行唯一出口 `DeviceSink::store_message`；协议一律 Lua 脚本（mlua），字节双形态 hex+JSON
- 解析外部输入必须带资源上限（缓冲长度/解压输出/内存/指令数），超限报错可见（评审 3 个 Critical 同族教训）
- 刻意简化处留 `ponytail:` 注释；失败必须可见（禁静默吞错）
- 提交纪律：每票独立 commit、只 stage 本票文件、禁 `git add -A`、禁 `git push`
