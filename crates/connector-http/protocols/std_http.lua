-- 标准 HTTP 报文协议（示例）。
--
-- 承载 /access/std/{rtg,history,event,info} 四路由的报文格式，
-- 替代 Java EnnStandardHttpProtocol（按 spec 决策不移植 Java 源码，用 Lua 重写）。
-- handler 注入 params.route = 请求 URI（如 "/access/std/rtg"）。
--
-- 输入 body（JSON，从 input.json 字节数组还原）：
--   rtg/history: { ver, pKey, sn, ts, devs=[ { dev, ts, resume, d=[ {m,v,ts} ] } ] }
--   event:       { version, pKey, sn, ts, devs=[ { dev, identifier, ts, eventType, value={k=v} } ] }
--   info:        { pKey, sn, ... }（原始格式不固定，整体作为 data 上行）
--
-- 输出 wire message 字段名对齐 Java Message 模型（camelCase）。
--
-- ponytail: messageId / ingestionTime 示例用输入派生（dev+序号 / ts）保持确定性，
-- 未接 wall-clock 与 UUID；生产如需全局唯一/真实摄入时间，由 handler 经 params 注入
-- params.now 与唯一 id，脚本改用之即可（升级路径，不改路由层）。

local function body_of(input)
  return json_decode(string.char(table.unpack(input.json)))
end

-- 时间戳归一：10 位秒级 → 毫秒（对齐 Java String.valueOf(ts).length()==10 判断）。
local function norm_ts(ts)
  if ts == nil then return 0 end
  ts = math.floor(ts)
  if #tostring(ts) == 10 then return ts * 1000 end
  return ts
end

-- rtg/history → MetricReportRequest（DEVICE_REPORT_REQ），对齐 Java transformMessages。
local function transform_rtg(body)
  local out = {}
  for i, dev in ipairs(body.devs or {}) do
    local time = norm_ts(dev.ts)
    local metrics = {}
    for _, dd in ipairs(dev.d or {}) do
      if dd.m ~= nil and dd.v ~= nil then
        metrics[#metrics + 1] = { ts = dd.ts or time, code = dd.m, value = dd.v }
      end
    end
    out[#out + 1] = {
      messageType = "DEVICE_REPORT_REQ",
      deviceId = dev.dev,
      messageId = tostring(dev.dev) .. "-" .. tostring(i),
      timeStamp = time,
      ingestionTime = time,
      resume = dev.resume or "N",
      metrics = metrics,
    }
  end
  return out
end

-- event → EventReportRequest（DEVICE_EVENT_REQ），对齐 Java transformEventMessages。
local function transform_event(body)
  local out = {}
  for i, dev in ipairs(body.devs or {}) do
    local time = norm_ts(dev.ts)
    local metrics = {}
    for k, v in pairs(dev.value or {}) do
      metrics[#metrics + 1] = { ts = time, code = k, value = v }
    end
    out[#out + 1] = {
      messageType = "DEVICE_EVENT_REQ",
      deviceId = dev.dev,
      messageId = tostring(dev.dev) .. "-" .. tostring(i),
      timeStamp = time,
      identifier = dev.identifier,
      type = dev.eventType,
      version = body.version,
      metrics = metrics,
    }
  end
  return out
end

-- info → InfoReportRequest（DEVICE_INFO_REQ）。校验 pKey/sn（对齐 Java BaseAssert，
-- 失败可见：error → handler 返回 4xx）。原始报文整体作为 data 上行。
local function transform_info(body)
  if body.pKey == nil or body.pKey == "" then error("pKey 不能为空") end
  if body.sn == nil or body.sn == "" then error("sn 不能为空") end
  return { {
    messageType = "DEVICE_INFO_REQ",
    deviceId = body.pKey,
    sn = body.sn,
    messageId = tostring(body.sn) .. "-info",
    timeStamp = norm_ts(body.ts),
    data = body,
  } }
end

function decode_multi(input, params)
  local body = body_of(input)
  local route = params.route
  if route == "/access/std/rtg" or route == "/access/std/history" then
    return transform_rtg(body)
  elseif route == "/access/std/event" then
    return transform_event(body)
  elseif route == "/access/std/info" then
    return transform_info(body)
  end
  error("unknown std route: " .. tostring(route))
end

function decode(input, params)
  return decode_multi(input, params)[1]
end

function encode(msg, params)
  return json_encode(msg)
end

function login(req, params)
  return false
end
