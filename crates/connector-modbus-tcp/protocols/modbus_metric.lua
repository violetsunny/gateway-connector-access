-- modbus 测点上报协议（票 07）。
--
-- 适用：connector-modbus-tcp 的 readDiff 轮询上行。connector 把每个点的解析结果
-- 整理成单点 JSON `{"deviceId":..,"code":..,"value":..,"ts":..}` 后交给 decode_multi
-- （字节双形态：input.hex / input.json）。寄存器字节→数值（DataType/字节序）与
-- pointId→设备映射查找已在 Rust 侧完成（对齐 Java ModbusDeviceProtocol.decodeMulti
-- 的映射缺失即丢弃语义），本脚本只做消息封装。
--
-- 对齐 Java UpModbusHandler + ModbusDeviceProtocol.decodeMulti(:45-74)：
-- 每点独立封装一条 DEVICE_REPORT_REQ，deviceId 来自点位映射，metrics 单元素数组；
-- Java 不设置 sn/messageId，本脚本同样不输出。
-- 差异：Java 不设置 ingestionTime（恒 0），Rust 版填采集时间戳
-- （与 snmp/opcua 脚本约定一致）。
--
-- 注意：value 为 uint64 且超过 2^63-1 时，经 Lua number（双精度）会丢失精度——
-- mlua 对超 i64 范围的 u64 序列化为 f64，属 Lua 协议引擎的既有特性
-- （snmp/opcua 同样存在），Rust 侧解析本身不丢精度（见 datatype.rs 测试）。
--
-- params：当前未使用（预留）。

function decode_multi(input, params)
  local p = json_decode(string.char(table.unpack(input.json)))
  return {
    {
      deviceId = p.deviceId,
      messageType = "DEVICE_REPORT_REQ",
      metrics = {
        { ts = p.ts, code = p.code, value = p.value },
      },
      resume = "N",
      timeStamp = p.ts,
      ingestionTime = p.ts,
      changeRpt = false,
    },
  }
end

function decode(input, params)
  return decode_multi(input, params)[1]
end

function encode(msg, params)
  return json_encode(msg)
end

function login(req, params)
  return true
end
