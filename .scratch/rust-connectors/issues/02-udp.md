# 02: udp connector

**What to build:** UDP connector 绑定配置端口（含 SO_BROADCAST），收数据报 → 按 pkey/配置选 Lua 协议 decode_multi → 仅 EVENT/INFO/REPORT 三类经 DeviceSink 上行；异常必须打日志（修 Java 空 catch bug）。

**Blocked by:** 01

**Status:** done（commit 70bdd0d，主 agent 验收：全绿，9 tests；偏差已裁决：坏包保活监听=修 Java bug 采纳，socket2 已登记白名单）

- [ ] tokio UdpSocket 绑定，每配置项独立任务
- [ ] 消息类型过滤：仅 DEVICE_EVENT_REQ/DEVICE_INFO_REQ/DEVICE_REPORT_REQ 上行（对齐 Java MessageHandler.java:38-45）
- [ ] 127.0.0.1:0 loopback 端到端测试：发字节 → MockSink 收到 Message；过滤类型被丢弃的负向测试
- [ ] TOML 配置段解析测试（字段对齐 Java UdpConfig）
- [ ] fmt/clippy/build/test 全绿
