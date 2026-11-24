# 08: snmp connector

**What to build:** SNMP v2c GET 轮询：按 intervalMinute 周期对配置设备列表的 OID（protocol.params["OIDS"] 逗号分隔）发 GET，响应值 → Metric → Lua decode_multi → DeviceSink 上行；community 来自配置（修 Java 硬编码 "huawei123" 问题）。

**Blocked by:** 01

**Status:** done（commit e3683f3，主 agent 验收：全绿，27 tests；自写 BER 415 行超指引但覆盖完整采纳；community 必填=有意安全修复，配置迁移需补）

- [ ] v2c GET；BER 编解码优先评估 snmp-rs，不满足则最小自写（GET 请求/响应）
- [ ] v3 不支持：配置出现 v3 项启动报错（失败可见）
- [ ] 每轮独立会话、超时配置、错误打日志不中断下轮
- [ ] Seam C 测试：BER 帧编解码 round-trip、响应→Metric 映射、OIDS 解析
- [ ] fmt/clippy/build/test 全绿

## 测试夹具行为边界记录（评审收尾补记 2026-11-03，与票 07 FakeSlave 同等待遇）

spawn_fake_agent（lib.rs:403）对任何 GET 请求返回固定 varbind，不校验 OID 与 community：
- 夹具只验 connector 侧 BER 编解码与轮询链路，不模拟 agent 端校验行为
- 真实 agent 的 OID 匹配、community 校验、超时重传交 manual-verification.md 第 7 项
