-- snmp 测点映射协议（票 08）。
--
-- 适用：connector-snmp 的 v2c GET 轮询上行。connector 把一轮 GET 响应整理成
-- JSON 对象 `{ "<oid>": "<值字符串>" }` 后交给 decode_multi（字节双形态：input.hex / input.json）。
--
-- params：
--   OIDS     逗号分隔 OID 列表（connector 用它构造 GET 请求，本脚本不读）
--   deviceId / sn  上行消息归属
--   ts       测点时间戳（可选，缺省 0）
--   codes    oid -> 测点 code 映射（可选，缺省用 oid 本身作为 code）
--
-- 输出：单条 DEVICE_REPORT_REQ，metrics 按 code 排序（保证输出确定性）。
-- 值语义：数值型 OID 值为十进制字符串（TimeTicks 为原始百分秒），非数值（如 noSuchObject、
--         设备描述文本）保持字符串，由下游按需处理。

function decode_multi(input, params)
  local map = json_decode(string.char(table.unpack(input.json)))
  local metrics = {}
  for oid, value in pairs(map) do
    local code = (params.codes and params.codes[oid]) or oid
    metrics[#metrics + 1] = {
      ts = params.ts or 0,
      code = code,
      value = tonumber(value) or value,
    }
  end
  table.sort(metrics, function(a, b) return a.code < b.code end)
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
