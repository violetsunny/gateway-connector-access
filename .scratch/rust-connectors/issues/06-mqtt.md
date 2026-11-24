# 06: mqtt connector

**What to build:** rumqttc 连接多个外部 broker（每 broker 独立 clientId/host/port/认证/TLS/遗嘱/QoS/topics/重连配置）；上行按 topic+payload 动态选协议（ProtocolChooser 等价）decode_multi → DeviceSink；下行消费 Kafka `device_command_topic` → Lua encode → QoS1 publish 到目标 topic。

**Blocked by:** 01

**Status:** done（commit bb25e3c，主 agent 验收：全绿，47 tests；遗留：common MqttConfigServer 缺 lastWill 字段→票11 补）

- [ ] 每 broker 一个 EventLoop task，连接错误重试不 panic（重连延迟配置）
- [ ] 上行：Subscribe 收 Publish → topic 匹配协议 → decode_multi → store_message（对齐 UpMqttHander.java:37-74）
- [ ] 下行：Kafka 消费 → OperationRequest → encode → publish AT_LEAST_ONCE（对齐 DownMqttHandler.java:48-91）
- [ ] Seam C 测试：Notification→Message 映射、topic→协议选择逻辑（含无匹配协议失败路径）；真实 broker 联调进人工验证清单
- [ ] TOML 配置解析测试（字段对齐 MqttConfigServer：clientId/host/port/username/password/tls/tlsCert/tlsKey/qos/topics/reconnectDelay/reconnectEnabled/messageMaxBytes）
- [ ] fmt/clippy/build/test 全绿
