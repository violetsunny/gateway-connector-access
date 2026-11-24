# 04: tcp client 端

**What to build:** TCP client 按配置外连远端，复用 03 的拆包器/心跳/会话链路收数上行；写空闲心跳保活；断线按 reconnectDelay 自动重连。

**Blocked by:** 03

**Status:** done（commit 254a947，主 agent 验收：全绿，44 tests；偏差已裁决：reconnectEnabled 生效化/重连单循环采纳）

- [ ] client 模式外连 + 自动重连（对齐 TcpConnectionClient/ReconnectHandler 语义）
- [ ] 写空闲心跳发送（heartbeatInterval）
- [ ] 复用 server 端拆包 codec 与会话逻辑，不复制粘贴
- [ ] loopback 测试：client 连本地 server 收数上行；server 断开后自动重连成功
- [ ] fmt/clippy/build/test 全绿
