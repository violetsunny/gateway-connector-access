-- hex 帧解析协议（示例）。
--
-- 适用：固定格式二进制帧。演示用 input.hex（hex 字符串形态）按位切字段。
--
-- 帧格式（示例，3 字节）：
--   byte[0]      = 类型码（此处忽略）
--   byte[1..2]   = big-endian uint16 值
-- 例：字节 [0x01, 0x00, 0x2a] -> hex "01002a" -> 值 42
--
-- params 需提供：deviceId / sn / ts（可选，缺省 0）。
--
-- 引擎注入的全局函数：json_decode / json_encode（本脚本未用到，仅 encode 返回字符串）。

function decode(input, params)
  local h = input.hex
  local value = tonumber(string.sub(h, 3, 6), 16)
  return {
    deviceId = params.deviceId,
    sn = params.sn,
    messageType = "DEVICE_REPORT_REQ",
    metrics = { { ts = params.ts or 0, code = "v", value = value } },
  }
end

function decode_multi(input, params)
  return { decode(input, params) }
end

function encode(msg, params)
  -- 把 metrics[1].value 编回 3 字节帧：类型码 0x01 + big-endian uint16
  local value = msg.metrics[1].value
  return string.char(1, math.floor(value / 256), value % 256)
end

function login(req, params)
  return req.username == params.username and req.password == params.password
end
