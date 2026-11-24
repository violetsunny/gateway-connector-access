# 03: tcp server 端

**What to build:** TCP server 监听配置端口，连接经拆包器切帧 → Lua 协议 decode_multi → DeviceSink 上行；登录帧走 DeviceDirectory（sn→deviceId + 鉴权）建立会话；读空闲心跳超时断开。

**Blocked by:** 01

**Status:** done（commit 28cdbc5，主 agent 验收：全绿，39 tests；偏差已裁决：手写拆包不用 tokio-util 采纳，修 Java 登录永真 bug 采纳）

- [ ] 8 种拆包器：DIRECT/DELIMITER(hex 分隔符)/LENGTH_FIELD(长度字段+字节序可配)/JSON_OBJECT(括号配平)/FIXED_LENGTH/XML(根标签配平)/LINE/ZLIB(flate2)，配置 parserType+parserParams 选择（对齐 ParserProvider.java:33-80 语义）
- [ ] 读空闲心跳（heartbeatInterval 配置，tokio IdleTimeout 等价）
- [ ] 会话管理：登录成功后 sn/deviceId 绑定连接，连接断开清理
- [ ] 下行不实现（Java 死代码，配置出现下行相关项忽略并 warn）
- [ ] loopback 端到端测试：每种拆包器至少 1 个切帧用例；登录成功/鉴权失败/未登录数据路径；心跳超时断开
- [ ] fmt/clippy/build/test 全绿
