# Spec: iot-gateway connectors 的 Rust 重写

Status: ready-for-agent

## Problem Statement

`iot-gateway-new/connectors` 下的 Broker/connector 目前是 Java 8 + Spring Boot 2.7 + Netty 实现，深耦合 Nacos/Redis/Kafka/xxl-job 与 Java 特有的协议 jar 热加载机制（URLClassLoader + ServiceLoader），维护成本高、资源占用大。需要在 `rust-demo` 仓库用 Rust 重写其中仍在使用的 connector，形成可独立运行、配置驱动、协议用 Lua 脚本扩展的轻量 gateway。

## Solution

将 rust-demo 改造为 cargo workspace，提供一个 Rust 版 IoT gateway：

- 8 个 connector：tcp、udp、mqtt、modbus-tcp、http、snmp、request、opcua（opcua 为按配置契约全新实现，Java 侧是空壳）
- 统一 `Connector` trait（start/stop/status）+ `DeviceSink` trait（上行唯一出口，生产实现为 Kafka producer）
- 协议编解码用 Lua（mlua）脚本引擎实现 `Protocol` 契约，字节以 hex 字符串与 JSON 两种形式暴露给脚本
- 配置为本地 TOML 文件，结构对齐 Java 侧 `ennwiot.connector-*` 契约
- 下行仅保留 Java 侧实际生效的活路径：mqtt、request 消费 Kafka `device_command_topic`

## User Stories

1. 作为 gateway 运维者，我想用单个 Rust 二进制 + 一份 TOML 配置启动全部 connector，以便摆脱 JVM/Nacos/Spring 运行时
2. 作为 gateway 运维者，我想每个 connector 的配置结构与 Java 版 `ennwiot.connector-<name>` 字段对齐，以便迁移配置时只做格式转换不做语义映射
3. 作为设备接入工程师，我想 TCP connector 支持 server 监听与 client 外连两种模式，以便同时接入直连设备与主动连平台设备
4. 作为设备接入工程师，我想 TCP 支持 8 种拆包器（DIRECT/DELIMITER(hex)/LENGTH_FIELD(含字节序)/JSON_OBJECT/FIXED_LENGTH/XML/LINE/ZLIB），以便兼容现有设备的粘包协议
5. 作为设备接入工程师，我想 TCP 有读/写空闲心跳检测与断线自动重连，以便长连接会话可自愈
6. 作为设备接入工程师，我想 TCP 登录流程用本地设备清单做 sn→deviceId 映射与鉴权，以便一期不依赖 Redis 也能走通登录链路
7. 作为设备接入工程师，我想 UDP connector 绑定端口（含广播）并按消息类型（EVENT/INFO/REPORT）过滤上行，以便与 Java 版行为一致
8. 作为设备接入工程师，我想 MQTT connector 可同时连多个外部 broker、各自独立 topics/TLS/遗嘱/QoS/重连配置，以便替换手写的 2500 行 Netty MQTT 客户端
9. 作为设备接入工程师，我想 MQTT 上行按 topic+payload 动态选择协议脚本（对齐 ProtocolChooser），以便一个 broker 下多产品共存
10. 作为设备接入工程师，我想 MQTT 下行消费 Kafka `device_command_topic`，经协议 encode 后以 QoS1 publish 到设备，以便云端指令可达
11. 作为工业接入工程师，我想 modbus-tcp connector 以 master 身份按 readDiff 周期轮询 slave，点表从 HTTP URL 拉取 JSON，以便沿用现有点表下发方式
12. 作为工业接入工程师，我想 modbus 数据类型/字节序映射（对齐 DataTypeEnum/IotByteUtils 语义）完整等价重写，以便寄存器值解析结果与 Java 版一致
13. 作为设备接入工程师，我想 HTTP connector 提供 `POST /api/{pkey}/**` 通配接入与 `/access/std/*` 标准报文路由，以便现有 HTTP 设备无感切换
14. 作为设备接入工程师，我想 snmp connector 以 v2c GET 按 intervalMinute 轮询 OID 列表（params["OIDS"]），且 community 来自配置而非硬编码，以便修掉 Java 版硬编码 "huawei123" 的安全问题
15. 作为平台对接工程师，我想 request connector 按任务列表轮询第三方云 API，支持令牌桶限速与 AUTH 型 token 刷新，以便对齐 TrdPlatformCloudJob 行为
16. 作为平台对接工程师，我想 request 的任务可在运行期经 Kafka `iot_gateway_trdPlatform_data` 动态增删，以便不重启 gateway 调整任务
17. 作为平台对接工程师，我想 request 下行消费 `device_command_topic` 后经协议 encode + URL 模板替换发 HTTP 指令，以便云端指令可达第三方平台
18. 作为工业接入工程师，我想 opcua connector 用 username/password 连 endpointUrl，按 intervalMinute 轮询 params["NODES"] 节点值上行，以便接入 OPC UA 设备（Java 侧空壳，此为全新能力）
19. 作为协议开发者，我想用 Lua 脚本实现 decode/decode_multi/encode/login 四个入口，输入字节以 hex 与 JSON 双形态提供、params 以 table 注入，以便不重编译 gateway 就能新增协议
20. 作为协议开发者，我想拿到至少 2 个可运行的示例 Lua 协议（JSON 透传、hex 帧解析），以便照葫芦画瓢
21. 作为平台数据消费方，我想上行消息按类型分别投递 Kafka topic（metric→enn_data_iot_metric、event→data_iot_event、info→data_iot_device_info），且 JSON 字段名与 Java Message 模型一致，以便下游零改动
22. 作为 gateway 运维者，我想有统一 tracing 日志与每 connector 的 status 查询，以便定位接入问题
23. 作为后续维护者，我想 workspace 按 crate 划分（common + 每 connector 一 crate + 主程序），以便单个 connector 可独立编译测试

## Implementation Decisions

- **仓库形态**：rust-demo 改造为 cargo workspace：`crates/connector-common`、`crates/connector-{tcp,udp,mqtt,modbus-tcp,http,snmp,request,opcua}`、`crates/gateway`（主程序，按 TOML 配置装配启停）。现有 `src/main.rs` demo 保留不动。修订根 AGENTS.md：解除"唯一依赖 rand"限制，登记 workspace 命令
- **核心 trait**（connector-common）：
  - `Connector`: 配置在构造期注入（各 connector 持有自己的强类型 config），`async start(sink) / async stop() / status() -> Map`；Java 的 `setupProtocol` 全部为空实现，删除
  - `DeviceSink`: `async store_message(msg)`；生产实现 KafkaSink（rdkafka），测试实现 MockSink（Vec 收集）
  - `DeviceDirectory`（仅 tcp 登录用）: `get_device_id_by_sn / auth_device`；一期实现为 TOML 本地设备清单，生产 Redis/DB 对接留 TODO 并显式标注
  - `Protocol`：由 Lua 引擎统一实现，connector 通过协议名从注册表取
- **消息模型**：serde 结构对齐 Java——`Message{messageId,deviceId,sn,messageType,response}`、`MetricReportRequest{metrics,resume,timeStamp,ingestionTime,changeRpt}`、`Metric{ts,code,value:serde_json::Value}`、`EventReportRequest`、`InfoReportRequest`、`LoginRequest/Response`、`OperationRequest/Response`；`MessageType` 15 个枚举值，序列化用 Java 同名大写下划线字符串
- **Lua 协议引擎**（mlua，Lua 5.4，vendored）：脚本入口 `decode(hex_or_json, params) / decode_multi(...) / encode(msg_table, params) / login(req_table, params)`；字节入参同时提供 hex 字符串与 JSON 数组两种形态；返回 table 由引擎反序列化为 Message；脚本从 `protocol.path` 指定的本地文件加载；协议注册表按配置构建。对齐 Java `type=script`，`type=jar` 不支持（配置中出现即启动报错，失败可见）
- **配置**：单一 TOML 入口（路径由 CLI 参数/环境变量指定），每 connector 一段，字段名对齐 Java `XxxConfig/XxxServerConfig`（camelCase 经 serde rename），内嵌 `protocol{name,path,params,type}`。Kafka broker/topic 独立 `[kafka]` 段
- **connector 要点**：
  - tcp：tokio + tokio-util codecs 实现 8 种拆包（ZLIB 用 flate2）；server/client 双模式；IdleState 等价心跳；会话管理；下行不实现（Java 侧死代码）
  - udp：tokio UdpSocket，SO_BROADCAST；仅 EVENT/INFO/REPORT 三类上行；修掉 Java 版空 catch 与双 handler 覆盖 bug（异常必须打日志）
  - mqtt：rumqttc 替换手写客户端；每 broker 一 EventLoop task；TLS/遗嘱/重连用 crate 原生配置；上行 ProtocolChooser 等价（topic 前缀匹配 + payload 探测）；下行 KafkaListener 等价 task
  - modbus-tcp：tokio-modbus master；点表 reqwest 拉 JSON；tokio interval 轮询；DataType/字节序映射等价重写（这是主要工作量，需逐类型对拍）；事务匹配交给 tokio-modbus；下行不实现
  - http：axum；`POST /api/{pkey}/**` 通配 + `/access/std/{rtg,history,event,info}`；pkey→协议映射来自配置；std 报文格式由示例 Lua 脚本承载（不移植 EnnStandardHttpProtocol Java 源码）
  - snmp：v2c GET；BER 编解码优先评估 `snmp-rs`，不满足则最小自写（GET 请求/响应仅数十行 ASN.1）；community 配置化；intervalMinute 轮询
  - request：reqwest 轮询任务；governor 令牌桶限速；AUTH 型 token 缓存刷新；xxl-job 替换为内置 tokio-cron 调度（cron 表达式进配置）；Kafka 动态任务通道保留；下行 encode→URL 模板替换→HTTP
  - opcua：`opcua` crate async client；username/password；intervalMinute 轮询 params["NODES"]（逗号分隔 NodeId）；读值→Metric 列表→协议 decode_multi→上行
- **依赖白名单**（新增须在此登记）：tokio、tokio-util、serde、serde_json、toml、tracing、tracing-subscriber、anyhow、thiserror、async-trait、rdkafka、rumqttc、tokio-modbus、axum、reqwest、mlua、flate2、governor、opcua、cron/tokio-cron-scheduler、snmp-rs（评估后定）、rand（保留）
- **实现参考**：subagent 实现各 crate 前须阅读 `/Users/kangll13/aiot/java-code/self/gggggggood/agent-skills/key-skills-git/ECC/skills/rust-patterns/SKILL.md` 与 `rust-testing/SKILL.md` 作为编码与测试规范（参考资料，不进运行时）；Java 行为语义以调查报告与源码为准
- **执行方式**：01 基建（workspace + connector-common）由主 agent 或单 subagent 完成并先行合入；8 个 connector 按票扇出 subagent 并行实现；主 agent 逐票检查 + 统一 review

## Testing Decisions

- 好测试 = 只断言外部行为（字节进 → Message 出 / 配置进 → 结构出），不断言内部实现细节
- **Seam A（主）**：MockSink 收集 `store_message`，全部 connector 统一注入
- **Seam B**：Lua 协议引擎纯函数测试——(脚本源码, hex/JSON 输入, params) → Vec<Message>；含错误脚本、类型不匹配、脚本 panic 的失败路径
- **端到端**：tcp/udp 用 127.0.0.1:0 loopback 真收发走 Seam A；http 用 axum `tower::ServiceExt::oneshot` 内存请求
- **Seam C（无法端到端处下沉）**：mqtt（Notification→Message 映射）、modbus-tcp（寄存器字节→Metric 的 DataType/字节序逐类型对拍，对拍基准来自 Java DataTypeEnum/IotByteUtils 语义）、snmp（BER 帧编解码 round-trip）、opcua（DataValue→Metric 映射）、request（响应 JSON→Message、URL 模板替换、限速器行为）
- 配置解析：每个 connector 的 TOML fixture → config struct 断言（含缺字段/错类型失败路径）
- 每票交付门槛：`cargo fmt --check`、`cargo clippy -- -D warnings`、`cargo build`、`cargo test` 全绿；mock broker/真实 Kafka 联调不进单测，列入人工验证清单
- 测试规范参考 rust-testing SKILL（subagent prompt 中注入路径）

## Out of Scope

- 空壳/死代码 connector：ctwing-mq、ftp、tcp-client、opcua-asyncio、ali-mns（源码丢失）、coap（demo 级）——废弃不迁移
- 困难项剔除：ali-mq（ONS 私有协议）、bacnetip（BACnet 栈）——登记为后续独立课题
- tcp 下行、modbus 下行（Java 侧死代码）；snmp v3/USM
- Nacos 配置中心、Redis（设备缓存/协议热更新 pub/sub）、xxl-job、Aviator 表达式、DeviceContext 的 TSL/modelRef 等设备管理方法
- Java 协议 jar 热加载机制与现存 protocols-udf/protocols-std 协议包移植（协议一律 Lua 重写，示例脚本除外不批量迁移）
- WASM 插件、mock MQTT broker/mock Modbus slave 等测试基建
- 性能基准对比、生产部署材料（Dockerfile/CI）

## Further Notes

- Java 侧已知 bug 在 Rust 版修正而非复刻：udp 空 catch 吞异常、udp 双 `.handler()` 覆盖、snmp community 硬编码
- tcp 登录鉴权的本地设备清单是一期简化（`ponytail:` 注释标注天花板：生产需接 Redis/DB），已在 DeviceDirectory 决策中显式化
- 人工验证清单（交付时产出）：真实 Kafka 收发、真实 MQTT broker 上下行、真实 Modbus slave 轮询、真实 OPC UA server 读值、第三方云平台 API 轮询
- 调查报告（16 connector 摸底、公共接口签名、耦合面）存于本会话，关键结论已内化进本 spec；需要溯源时查 Java 源码路径:行号
