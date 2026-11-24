# 09: request connector

**What to build:** 第三方云平台 HTTP 拉取：任务列表启动时从配置加载，运行期经 Kafka `iot_gateway_trdPlatform_data` 动态增删；tokio-cron 调度（替代 xxl-job）；governor 令牌桶限速；AUTH 型 API token 缓存与过期刷新；响应 → Lua decode_multi → DeviceSink；下行消费 `device_command_topic` → encode → URL 模板替换 → HTTP 发送。

**Blocked by:** 01

**Status:** done（commit ea3d6ea，主 agent 验收：全绿，31 tests；遗留：chrono 白名单登记、CommonError::Http 变体→票11 议）

- [ ] 任务动态增删（Kafka 通道，对齐 RequestConnectorStater.java:99-113 语义）
- [ ] cron 调度 + 令牌桶限速（对齐 TrdPlatformCloudJob.java:38,67-70）
- [ ] AUTH token 刷新流程（对齐 :73-75）
- [ ] 下行 URL 模板替换 + HTTP 发送（对齐 DownRequestHandler.java:138-143）
- [ ] Seam C 测试：响应 JSON→Message、URL 模板替换、限速器行为、任务增删状态机
- [ ] fmt/clippy/build/test 全绿
