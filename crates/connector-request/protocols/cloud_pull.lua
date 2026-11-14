-- 云平台拉取协议（票 09，默认示例脚本）。
--
-- 上行：connector 把 HTTP 响应整理成 JSON 对象（响应头合并进 "headers" 字段，
-- 对齐 Java TrdPlatformCloudServerImpl.sendRequest:104-110）后交给 decode_multi。
-- 本脚本约定响应形如 { data: [ { code, value, ts }, ... ] }；其他平台形态
-- 按平台另配 Lua 脚本，本脚本不做万能适配。
--
-- params：
--   deviceId / sn   上行消息归属
--
-- 下控：encode 透传 OperationRequest 顶层字段（供 URL 模板 ${name} 替换），
-- 并把 param 包装为 { device, cmd }（对齐 DownRequestHandler:141
-- body = operationRequest.getParam() 的第三方平台 body 形态）。

function decode_multi(input, params)
  local res = json_decode(string.char(table.unpack(input.json)))
  local data = res.data
  if type(data) ~= "table" or #data == 0 then
    return {}
  end
  local metrics = {}
  for _, item in ipairs(data) do
    metrics[#metrics + 1] = {
      ts = item.ts or 0,
      code = item.code,
      value = item.value,
    }
  end
  local first_ts = metrics[1].ts
  return {
    {
      deviceId = params.deviceId,
      sn = params.sn,
      messageType = "DEVICE_REPORT_REQ",
      metrics = metrics,
      resume = "N",
      timeStamp = first_ts,
      ingestionTime = first_ts,
      changeRpt = false,
    },
  }
end

function decode(input, params)
  local msgs = decode_multi(input, params)
  return msgs[1] or {}
end

function encode(msg, params)
  msg.param = { device = msg.deviceId, cmd = msg.param }
  return json_encode(msg)
end

function login(req, params)
  return true
end
