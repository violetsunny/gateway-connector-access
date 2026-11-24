# 07: modbus-tcp connector

**What to build:** Modbus TCP master：点表与设备-点位映射从配置 HTTP URL 拉取 JSON；按 readDiff 周期轮询 slave 寄存器；寄存器字节按点表 DataType/字节序解析为 Metric 列表 → Lua decode_multi → DeviceSink 上行；断线重连。

**Blocked by:** 01

**Status:** done（commit c0f0545，主 agent 验收：全绿，47 tests，DataType 20 类型对拍；偏差已裁决：点表失败即报错/不复刻 POINT_CACHE 缺陷采纳）

- [ ] tokio-modbus master 连接 + tokio interval 轮询（scheduleWithFixedDelay 语义：上轮完成后延时）
- [ ] 点表 reqwest 拉取 JSON（启动拉取 + 失败重试可见报错）
- [ ] DataType/字节序映射等价重写（对齐 Java DataTypeEnum 268 行 + IotByteUtils 449 行语义），逐类型对拍测试——本票主要工作量与验收核心
- [ ] 事务匹配交给 tokio-modbus；下行不实现（Java 死代码）
- [ ] Seam C 测试：寄存器字节→Metric 逐 DataType 用例（含大小端、有符号、浮点、位提取）；点表 JSON 解析
- [ ] fmt/clippy/build/test 全绿

## 测试夹具行为边界记录（评审收尾补记 2026-11-03）

FakeSlave（lib.rs:743）对非 `ReadHoldingRegisters(100,_)` 的请求一律回 `IllegalDataAddress`：
- 与 Modbus 规范一致（越界地址即回该异常码）；connector 侧异常码/失败路径由轮询失败计数测试覆盖
- 夹具只模拟单一寄存器段，多段点表与真实 slave 联调交 manual-verification.md 第 3 项
