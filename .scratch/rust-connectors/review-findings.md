# 评审发现台账（4 路评审 2026-11-02 + 修复收尾 2026-11-03）

## 已修复（commit 3f84bee 评审修复 / e062817 票12）

| 级别 | 项 | 落点 |
|---|---|---|
| Critical | Lua 沙箱（禁 io/os/package/debug/loadfile/dofile/require + 64MB 内存 + 指令 hook） | connector-common/src/protocol.rs |
| Critical | ZLIB 解压炸弹（maxOutputLen 缺省 1MB，超限断连） | connector-tcp/src/codec.rs |
| Critical | JSON_OBJECT maxLength 必填（超限断连） | connector-tcp/src/codec.rs |
| Critical | opcua username 认证必失败（endpoint 补 UserName UserTokenPolicy） | connector-opcua/src/lib.rs |
| Critical | opcua 明文传输（C-2，用户裁定本轮做：SecurityPolicy 配置化，票12） | connector-opcua/{config,lib}.rs |
| 必改 | gateway kafka 判据统一（MqttConnector 第三参 Option<KafkaConfig>） | gateway/main.rs + connector-mqtt |
| 必改 | request auth.rs Instant 溢出 panic（checked_add 折半收敛） | connector-request/src/auth.rs |
| 必改 | HTTP 敏感头剔除（Authorization/Cookie/Proxy-Authorization 不进 Lua params） | connector-http/src/router.rs |
| 必改 | HTTP 错误回显收敛（响应通用错误码，详情仅日志） | connector-http/src/router.rs |
| 必改 | StdoutSink payload 截断（B组 M-3 裁定：元数据 + 256 字符截断） | gateway/main.rs |
| 必改 | scriptDir path 白名单（B组 C-1 裁定）+ 透传全部 8 connector 装配点 | common/protocol.rs + 各 crate |
| Nit | LENGTH_FIELD i64 checked_add | connector-tcp/src/codec.rs |
| Nit | mqtt downlink changed() Err 即 break | connector-mqtt/src/downlink.rs |
| Nit | request 下控响应日志 info→debug | connector-request/src/task.rs |

## 登记不修（Nit/Optional/FYI，升级路径备查）

- 测试断言用裸 `is_err()` 而非匹配具体错误变体——错误消息变更时测试不敏感；升级：断言 error 枚举变体
- 部分测试用固定 sleep 等异步结果——慢机器可能 flaky；升级：tokio time::pause / 条件轮询
- 配置中密码/community 等明文字段未包 `Secret<T>`——Debug 输出可能泄露；升级：secrecy crate 或自写 Debug 脱敏
- HTTP 敏感头黑名单为固定三项——升级路径：可配置黑名单（router.rs ponytail 注释已标）
- auth token 比较未用常量时间算法——本地配置比对场景风险低；升级：subtle crate
- Kafka SASL/TLS 参数未暴露配置——部署项已登记 manual-verification.md 第 1 节
- scriptDir 生产启用为部署项——manual-verification.md 第 8 节
- opcua 加密握手需真 server+证书人工验证——manual-verification.md 第 4 节

## ponytail 简化台账

代码内 `ponytail:` 注释共 36+ 条（本轮修复又新增若干），定位：`grep -rn "ponytail:" crates/`。
交 ponytail-debt 周期审视；每条注释自带天花板与升级路径，不在此重复罗列。

## 评审四路结论存档

- 测试质量：310 tests（修复后），0 假测试、0 对拍错误、负向路径全覆盖——套件可信
- ponytail-review：0 必改，可删仅 ~25-30 行（未删，保持 diff 最小）
- Spec 符合性：19 条决策 15 落实/3 偏差已裁决记录/1 无法判定（人工验证清单承接）；依赖白名单零违规
- 安全：5 Critical + 3 必改全部修复（见上表）
