-- JSON 透传协议（示例）。
--
-- 适用：设备直接上报 JSON 文本作为 payload。
-- 演示用 input.json（字节整数数组形态）还原字符串并解析。
--
-- 引擎注入的全局函数：
--   json_decode(str) -> table
--   json_encode(table) -> str
-- 入参约定：
--   input  = { hex = "<小写hex字符串>", json = { <字节整数数组> } }
--   params = 配置 protocol.params（table）

function decode(input, params)
  local s = string.char(table.unpack(input.json))
  local obj = json_decode(s)
  return obj
end

function decode_multi(input, params)
  return { decode(input, params) }
end

function encode(msg, params)
  return json_encode(msg)
end

function login(req, params)
  return req.username == params.username and req.password == params.password
end
