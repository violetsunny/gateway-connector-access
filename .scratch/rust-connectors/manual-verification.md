# 人工验证清单（票 11 交付物）

以下验证需要真实外部服务，agent 无法在本地冒烟中覆盖，交用户人工执行。
通用准备：`cargo build --workspace --release`；配置以 `config.example.toml` 为底本修改。
通用判定：gateway 进程不崩、日志无 panic；上行消息最终出现在 Kafka 对应 topic。

## 1. 真实 Kafka 收发

- 步骤：
  1. `[kafka] bootstrapServers` 填真实 broker（如 `10.0.0.1:9092`）。
  2. 启动 gateway，经 http 链路上行一条：`curl -X POST http://127.0.0.1:8080/api/productA -H 'content-type: application/json' -d '{"messageType":"DEVICE_REPORT_REQ","deviceId":"dev-001","sn":"SN001","metrics":[{"ts":0,"code":"t","value":1}]}'`。
  3. 用 kafka 控制台消费者订阅 `enn_data_iot_metric`。
- 预期：不再出现 "kafka unavailable ... dumped to log" 降级日志；消费者收到该 JSON 消息。
- 判定标准：消息体与 curl 发送的报文一致（messageType/deviceId/metrics）；event/info 类型分别落 `data_iot_event` / `data_iot_device_info`。
- 部署项（评审 C-1 裁定登记）：生产 broker 若要求 SASL/TLS，rdkafka 客户端参数当前未暴露为配置项；
  需要时在 `[kafka]` 段补 `security.protocol`/`sasl.*` 透传（升级路径，见 review-findings.md）。

## 2. MQTT broker 上下行

- 步骤：
  1. `[mqtt]` 的 host/port 指向真实 broker（如 mosquitto/EMQX），topics 保持 `["enn/+/up"]`。
  2. 启动 gateway，用 mosquitto_pub 向 `enn/dev-001/up` 发一条 JSON：`{"messageType":"DEVICE_REPORT_REQ","deviceId":"dev-001","sn":"SN001","metrics":[{"ts":0,"code":"t","value":1}]}`。
  3. 下行：向 kafka `device_command_topic` 发一条 OperationRequest（目标 broker/topic 按 connector-mqtt 下行协议约定）。
  4. 遗嘱验证：直接 kill -9 gateway 进程，订阅 `enn/gw-example-001/status`。
- 预期：上行消息进入 sink（Kafka 或降级日志）；下行命令经 broker 发布到设备 topic；broker 判定掉线后发布遗嘱 `offline`（retain=true）。
- 判定标准：三个方向（上行/下行/遗嘱）各至少一条消息可观测；broker 断开后自动重连日志可见（`等待后重连`）。

## 3. Modbus slave 轮询

- 步骤：
  1. 准备真实 Modbus TCP slave（如 `diagslave -m tcp -p 1502`）或 PLC，`[modbus-tcp]` host/port 指向它。
  2. pointDTOSPath / pointMapDTOSPath 指向真实点表接口（返回格式见 connector-modbus-tcp 票 07 契约），或本地起一个返回固定点表 JSON 的 http 服务。
  3. 启动 gateway，等待一个 readDiff 周期（默认 5s）。
- 预期：日志出现点表拉取成功与轮询读取；寄存器值变化时经 modbus_metric.lua 封装为 DEVICE_REPORT_REQ 上行。
- 判定标准：上行消息 deviceId 来自点位映射、metrics 含 code/value/ts；值未变化时按 readDiff 语义不重复上报。

## 4. OPC UA server 读值

- 步骤：
  1. 准备真实 OPC UA server（如 KEPServerEX、open62541 示例 server 或 Prosys Simulation Server），`[opcua]` endpointUrl 指向它。
  2. NODES 参数改为该 server 上真实存在的节点 ID。
  3. 启动 gateway，等待一个 intervalMinute 周期。
- 预期：连接成功日志（不再 BadNotConnected）；节点值经 opcua_metric.lua 封装上行。
- 判定标准：上行 DEVICE_REPORT_REQ 的 metrics 与 server 端节点实际值一致；server 断开后下轮重试、恢复后自动继续。
- 加密握手（票 12 遗留）：加密路径测试只覆盖到配置解析/校验/endpoint 构造层，真实握手需人工验证——
  server 侧启用 Basic256Sha256+SignAndEncrypt 并信任客户端证书，gateway 配 `securityPolicy/securityMode/certPath/keyPath`
  （见 config.example.toml opcua 段注释）；判定：连接成功且抓包非明文 OPC UA 明文帧。
  注意：缺省 None/None 为明文传输，生产环境建议启用加密。

## 5. 第三方云 API 轮询（request）

- 步骤：
  1. `[request]` url 指向真实云 API（或本地 mock：`python3 -m http.server` 不够，需返回 `{"data":[{"code":"t","value":1,"ts":...}]}` 形态的 JSON 服务）。
  2. 需要鉴权时补 `[request.configuration.auth]` 段（tokenPointer 等，见票 09 契约）。
  3. 启动 gateway，等待 cron 触发（示例为每 30 秒）。
- 预期：任务注册日志（`注册任务 cloud-example-pull-meter`）；每次触发拉取成功并经 cloud_pull.lua 上行。
- 判定标准：上行 metrics 与云 API 返回 data 数组一致；限速（limit）与超时（timeoutMs）生效；auth 配置时请求头带 token。

## 6. http std/info 上行行为差异（Java 对比核对）

- 背景：Java `HttpStdConnector` 的 `/access/info` 只做响应、**不上行** Kafka；Rust 版 std 四路由（rtg/history/event/info）统一走 decode → store_message，**info 也上行**（DEVICE_INFO_REQ → `data_iot_device_info`）。
- 步骤：`curl -X POST http://127.0.0.1:8080/access/std/info -H 'content-type: application/json' -d '{"pKey":"pk","sn":"SN001","extra":"x"}'`，观察 Kafka `data_iot_device_info`。
- 预期：响应 `{"success":true,...,"data":true}`（info 路由 data=true，对齐 Java）；Rust 版额外产生一条 DEVICE_INFO_REQ 上行。
- 判定标准：与下游消费方确认 info 上行是否可接受；若必须对齐 Java（info 不上行），需在 router.rs std_handler 对 info 路由跳过 store（属行为变更，先裁决再改）。

## 7. snmp community 必填（迁移提醒）

- 背景：Java 版 community 硬编码 `"huawei123"`；Rust 版改为必填配置项，缺省启动报错（失败可见）。
- 步骤：迁移 Java 配置时，为每个 `[[snmp.configuration]]` 显式补 `community = "huawei123"`（或真实值）；启动 gateway 用 snmpwalk/真实 agent 验证轮询。
- 预期：不配 community → 启动即报 `community 必须配置`；配齐后轮询正常，OID 值经 snmp_metric.lua 上行。
- 判定标准：上行 DEVICE_REPORT_REQ 的 metrics 与 snmpwalk 读到的 OID 值一致。

## 8. scriptDir 白名单部署项（评审 C-1 裁定登记）

- 背景：Lua 脚本注入风险已做两层防护——运行时沙箱（禁 io/os/package/debug/loadfile/dofile/require +
  64MB 内存限制 + 指令数上限）与加载路径白名单（顶层 `scriptDir`，canonicalize 前缀校验，越界拒绝加载）。
- 部署项：生产配置**应当启用 `scriptDir`**（不设置则不限制，向后兼容）；启用前需把分散在各 crate
  `protocols/` 目录的脚本收敛到同一目录（见 config.example.toml:13-16 注释）。
- 判定标准：配置 `scriptDir` 后，指向目录外脚本的 connector 启动即报"拒绝加载"错误；目录内脚本正常上行。
