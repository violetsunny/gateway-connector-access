# 05: http connector

**What to build:** axum HTTP 服务：`POST /api/{pkey}/**` 通配接入（pkey→协议映射来自配置，wildcard 路径与 headers 作为 params 传给 Lua decode_multi）；`/access/std/{rtg,history,event,info}` 标准报文路由；解码结果经 DeviceSink 上行，按 Java 语义返回 response。

**Blocked by:** 01

**Status:** done（commit 336731f，主 agent 验收：全绿，18 tests；偏差已裁决：std/info 改为上行对齐 spec 意图采纳，登记入人工验证清单核对项）

- [ ] 通配路由 + pkey 未知时 4xx（失败可见，不静默）
- [ ] std 路由报文格式由示例 Lua 脚本承载（不移植 Java EnnStandardHttpProtocol）
- [ ] params 注入：wildcardPath、headers、query（对齐 HttpConnectorStater.java:83 调用语义）
- [ ] tower::ServiceExt::oneshot 内存请求测试：通配路由解码上行、std 路由、未知 pkey 负向
- [ ] fmt/clippy/build/test 全绿
