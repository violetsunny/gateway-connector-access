# 票 12 — opcua：SecurityPolicy 配置化 + username 认证修复

Status: done（commit e062817，主 agent 验收：fmt/clippy/test 全绿 21 tests（既有 14 无回退+新增 7）；
偏差裁决：字段命名统一 camelCase 采纳；加密握手需真 server+证书，列人工验证清单第 4 项）
关联：评审 C-2（Critical，用户裁定本轮做）、A组 #4（必改）
Crate：`crates/connector-opcua`（只许改这个 crate）

## 背景

评审发现两个 opcua 安全问题：

1. **#4（A组必改）**：`lib.rs:144-150` 构造 endpoint 时未声明 `UserName` UserTokenPolicy，
   导致配置了 username/password 的会话认证必失败。
2. **C-2（Critical）**：当前仅支持 `SecurityPolicy::None` + `MessageSecurityMode::None`，
   OPC UA 流量明文传输。用户裁定本轮补 SecurityPolicy 配置化。

Java 侧 `OpcuaServerConfig` 无安全字段——本票为 Rust 新增能力，无 Java 基线，
以 crates.io `opcua` 0.12 client API 为准。

## 任务

### 1. 修复 #4：endpoint 补 UserName UserTokenPolicy

配置了 `username` 时，endpoint description 的 `user_identity_tokens` 须包含
`UserTokenPolicy { policy_id: "username", token_type: UserTokenType::UserName, .. }`；
匿名时保留 Anonymous。以 0.12 实际 API 为准（可能是 `EndpointDescription` 字段或 builder）。

### 2. C-2：SecurityPolicy 配置化

`config.rs` 新增可选字段（TOML，全部向后兼容，缺省 = 现状 None/None）：

```toml
security_policy = "Basic256Sha256"   # 缺省 "None"；至少支持 None | Basic256Sha256
security_mode   = "SignAndEncrypt"   # 缺省 "None"；支持 None | Sign | SignAndEncrypt
cert_path       = "certs/opcua-client.der"  # policy != None 时必填
key_path        = "certs/opcua-client.key"  # policy != None 时必填
```

- 未知 policy/mode 字符串 → 配置解析报错（失败可见，禁静默回落 None）
- policy != None 而 cert/key 缺失或文件不存在 → 启动报错，不降级明文
- 会话建立处（`lib.rs` 每轮重建会话的 spawn_blocking 路径）按配置选 policy/mode；
  证书加载用 0.12 提供的 API（`X509`/`PKey` from DER/PEM，以 crate 实际导出为准）
- 服务端不支持所配 policy 时连接失败 → 错误日志含 policy/mode/endpoint，按既有失败隔离路径处理

### 3. 测试

- 配置解析：缺省 None/None；显式 Basic256Sha256+SignAndEncrypt+cert/key 解析成功；
  未知 policy 报错；policy!=None 缺 cert 报错
- #4：构造带 username 的 endpoint description 断言含 UserName token policy（纯单元，不需真服务器）
- 既有 10 个测试不回退

## 验证

```
cargo fmt -p connector-opcua -- --check
cargo clippy -p connector-opcua --all-targets -- -D warnings
cargo test -p connector-opcua
```

## 纪律

- 禁 git commit / git push（主 agent 统一提交）
- 只改 `crates/connector-opcua/`；不新增依赖（opcua 0.12 已在白名单；若 security 需要
  feature flag，改根 Cargo.toml 的 `[workspace.dependencies]` 中 opcua 行并在报告中说明）
- 刻意简化留 `ponytail:` 注释（如仅支持 Basic256Sha256 一种加密策略）
- 参考（只读）：`/Users/kangll13/aiot/java-code/self/gggggggood/agent-skills/key-skills-git/ECC/skills/rust-patterns/SKILL.md`
