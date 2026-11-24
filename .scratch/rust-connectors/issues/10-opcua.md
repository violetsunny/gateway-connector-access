# 10: opcua connector

**What to build:** OPC UA client（全新实现，Java 侧空壳）：username/password 认证连 endpointUrl，按 intervalMinute 周期读取 protocol.params["NODES"]（逗号分隔 NodeId）节点值 → Metric 列表 → Lua decode_multi → DeviceSink 上行；断线重连。

**Blocked by:** 01

**Status:** done（commit 9d91f57，主 agent 验收：全绿，14 tests；偏差已裁决：opcua 0.12 同步 client + spawn_blocking 采纳，async-opcua 升级路径已注；该票依赖引发 serde_json arbitrary_precision 冲突，已由主 agent 修 common 层根治）

- [ ] opcua crate async client；订阅模式、地址空间自动浏览为非目标
- [ ] NODES 解析（含非法 NodeId 报错可见）、读值失败单点不中断整轮
- [ ] 配置字段对齐 OpcuaServerConfig：endpointUrl/username/password/description/intervalMinute/protocol
- [ ] Seam C 测试：NodeId 解析、DataValue→Metric 映射（含状态码非 Good 的处理）
- [ ] fmt/clippy/build/test 全绿
