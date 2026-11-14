-- opcua 测点上报协议（票 10）。
--
-- 适用：connector-opcua 的 intervalMinute 轮询上行。connector 把一轮 read_values 结果
-- 整理成 Metric JSON 数组 `[{"ts":..,"code":"ns=2;s=Tag","value":..}]` 后交给 decode_multi
-- （字节双形态：input.hex / input.json）。DataValue→Metric 映射（含状态码非 Good 跳过、
-- Variant→JSON 值转换）已在 Rust 侧完成，本脚本只做消息封装。
--
-- params：
--   NODES            逗号分隔 NodeId 列表（connector 用它构造读请求，本脚本不读）
--   deviceId / sn    上行消息归属
--   ts               消息级时间戳（可选，缺省 0）
--
-- 输出：单条 DEVICE_REPORT_REQ，metrics 原样透传。

function decode_multi(input, params)
  local metrics = json_decode(string.char(table.unpack(input.json)))
  return {
    {
      deviceId = params.deviceId,
      sn = params.sn,
      messageType = "DEVICE_REPORT_REQ",
      metrics = metrics,
      resume = "N",
      timeStamp = params.ts or 0,
      ingestionTime = params.ts or 0,
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
