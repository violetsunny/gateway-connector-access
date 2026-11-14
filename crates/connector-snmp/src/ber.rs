//! 最小 SNMP v2c BER 编解码：GET 请求构造 + GetResponse 解析。
//!
//! 只覆盖本 connector 需要的子集：v2c（version=1）、community 鉴权、GET 请求、
//! GetResponse 解析、常见应用类型值。不实现 v3/USM、GETNEXT/GETBULK/SET、trap、
//! 不定长 BER 表单。
//!
//! ponytail: 手写 BER 而非引依赖——spec 白名单里的 `snmp-rs` 在 crates.io 不存在，
//! 最接近的 `snmp-min` 是 0.1.0 单维护者新包（rust-version 1.88），且仍需在其上再包一层
//! Java `Variable.toString()` 对齐的值→字符串映射；自写约 250 行、零依赖，round-trip 测试兜底。
//! 升级路径：需要 GETBULK/表遍历/v3 时换成熟栈（如 snmp2），本模块测试即为替换验收基准。

use connector_common::{CommonError, Result};

const TAG_INTEGER: u8 = 0x02;
const TAG_OCTET_STRING: u8 = 0x04;
const TAG_NULL: u8 = 0x05;
const TAG_OID: u8 = 0x06;
const TAG_SEQUENCE: u8 = 0x30;
const TAG_IP_ADDRESS: u8 = 0x40;
const TAG_COUNTER32: u8 = 0x41;
const TAG_GAUGE32: u8 = 0x42;
const TAG_TIME_TICKS: u8 = 0x43;
const TAG_COUNTER64: u8 = 0x46;
const TAG_GET_REQUEST: u8 = 0xA0;
const TAG_GET_RESPONSE: u8 = 0xA2;
const TAG_NO_SUCH_OBJECT: u8 = 0x80;
const TAG_NO_SUCH_INSTANCE: u8 = 0x81;
const TAG_END_OF_MIB_VIEW: u8 = 0x82;

/// v2c 的 version 字段值（v1=0、v2c=1、v3=3）。
const VERSION_V2C: i64 = 1;

/// RFC 3416 error-status 名称，下标即 error-status 值，用于错误信息可读。
const ERROR_NAMES: [&str; 19] = [
    "noError",
    "tooBig",
    "noSuchName",
    "badValue",
    "readOnly",
    "genErr",
    "noAccess",
    "wrongType",
    "wrongLength",
    "wrongEncoding",
    "wrongValue",
    "noCreation",
    "inconsistentValue",
    "resourceUnavailable",
    "commitFailed",
    "undoFailed",
    "authorizationError",
    "notWritable",
    "inconsistentName",
];

fn bad(detail: &str) -> CommonError {
    CommonError::Internal(format!("snmp BER: {detail}"))
}

/// GET 响应 varbind 的取值子集。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SnmpValue {
    Integer(i64),
    OctetString(Vec<u8>),
    Null,
    IpAddress(Vec<u8>),
    Counter32(u64),
    Gauge32(u64),
    TimeTicks(u64),
    Counter64(u64),
    NoSuchObject,
    NoSuchInstance,
    EndOfMibView,
}

impl SnmpValue {
    /// 值 → 上行字符串，对齐 Java `variableBinding.getVariable().toString()`
    /// （`SnmpTimerTask.java:78`）：数值类为十进制、IpAddress 为点分十进制、
    /// OctetString 可打印时为原文否则小写 hex、异常类型为标记名。
    ///
    /// 刻意偏差：TimeTicks 输出原始百分秒数值（如 "12345678"），不做 snmp4j 的
    /// "d:h:m:s.hs" 格式化——下游 Lua 脚本 `tonumber()` 直接可用。
    pub(crate) fn to_wire_string(&self) -> String {
        match self {
            SnmpValue::Integer(v) => v.to_string(),
            // ponytail: Counter/Gauge/TimeTicks 一律十进制数值字符串，天花板是丢掉了
            // snmp4j 的 TimeTicks 人类可读格式；需要时在此按类型分支格式化。
            SnmpValue::Counter32(v)
            | SnmpValue::Gauge32(v)
            | SnmpValue::TimeTicks(v)
            | SnmpValue::Counter64(v) => v.to_string(),
            SnmpValue::OctetString(bytes) => match std::str::from_utf8(bytes) {
                Ok(s) if !s.chars().any(char::is_control) => s.to_string(),
                _ => to_hex(bytes),
            },
            SnmpValue::IpAddress(bytes) => bytes
                .iter()
                .map(u8::to_string)
                .collect::<Vec<_>>()
                .join("."),
            SnmpValue::Null => "Null".to_string(),
            SnmpValue::NoSuchObject => "noSuchObject".to_string(),
            SnmpValue::NoSuchInstance => "noSuchInstance".to_string(),
            SnmpValue::EndOfMibView => "endOfMibView".to_string(),
        }
    }
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// 一个 varbind：OID + 值。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VarBind {
    pub(crate) oid: String,
    pub(crate) value: SnmpValue,
}

/// PDU 头部 + varbind 列表（GET 请求与 GetResponse 共用结构）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Pdu {
    pub(crate) request_id: i64,
    pub(crate) error_status: i64,
    pub(crate) error_index: i64,
    pub(crate) varbinds: Vec<VarBind>,
}

// ---- 编码 ----

fn put_len(out: &mut Vec<u8>, len: usize) {
    if len < 0x80 {
        out.push(len as u8);
        return;
    }
    let be = len.to_be_bytes();
    let start = be.iter().position(|&b| b != 0).unwrap_or(be.len() - 1);
    let sig = &be[start..];
    out.push(0x80 | sig.len() as u8);
    out.extend_from_slice(sig);
}

fn put_tlv(out: &mut Vec<u8>, tag: u8, content: &[u8]) {
    out.push(tag);
    put_len(out, content.len());
    out.extend_from_slice(content);
}

/// 有符号整数的最短二补数大端表示。
fn signed_bytes(v: i64) -> Vec<u8> {
    let be = v.to_be_bytes();
    let mut out = be.to_vec();
    while out.len() > 1
        && ((out[0] == 0x00 && out[1] & 0x80 == 0) || (out[0] == 0xff && out[1] & 0x80 != 0))
    {
        out.remove(0);
    }
    out
}

/// 无符号整数的最短大端表示（最高位为 1 时补前导 0，避免被读成负数）。
/// 仅测试构造响应帧时用（生产路径只发 GET 请求，不含无符号值）。
#[cfg(test)]
fn unsigned_bytes(v: u64) -> Vec<u8> {
    let be = v.to_be_bytes();
    let start = be.iter().position(|&b| b != 0).unwrap_or(be.len() - 1);
    let mut out = be[start..].to_vec();
    if out[0] & 0x80 != 0 {
        out.insert(0, 0x00);
    }
    out
}

fn put_base128(out: &mut Vec<u8>, v: u64) {
    let mut buf = [0u8; 10];
    let mut i = buf.len();
    i -= 1;
    buf[i] = (v & 0x7f) as u8;
    let mut rest = v >> 7;
    while rest > 0 {
        i -= 1;
        buf[i] = ((rest & 0x7f) as u8) | 0x80;
        rest >>= 7;
    }
    out.extend_from_slice(&buf[i..]);
}

/// 点分十进制 OID → OBJECT IDENTIFIER 内容字节。格式非法即报错（失败可见）。
pub(crate) fn encode_oid(oid: &str) -> Result<Vec<u8>> {
    let text = oid.strip_prefix('.').unwrap_or(oid);
    let mut arcs: Vec<u64> = Vec::new();
    for part in text.split('.') {
        if part.is_empty()
            || !part.bytes().all(|b| b.is_ascii_digit())
            || (part.len() > 1 && part.starts_with('0'))
        {
            return Err(bad(&format!("非法 OID '{oid}'")));
        }
        arcs.push(
            part.parse::<u64>()
                .map_err(|_| bad(&format!("OID '{oid}' 的弧段超出 u64")))?,
        );
    }
    if arcs.len() < 2 || arcs[0] > 2 || (arcs[0] < 2 && arcs[1] > 39) {
        return Err(bad(&format!("非法 OID '{oid}'")));
    }
    let mut out = Vec::new();
    put_base128(&mut out, arcs[0] * 40 + arcs[1]);
    for arc in &arcs[2..] {
        put_base128(&mut out, *arc);
    }
    Ok(out)
}

/// 构造 v2c GET 请求帧：`SEQUENCE{version, community, GetRequest-PDU{...}}`。
pub(crate) fn build_get_request(
    request_id: i32,
    community: &str,
    oids: &[String],
) -> Result<Vec<u8>> {
    if oids.is_empty() {
        return Err(CommonError::Config(
            "snmp: OIDS 为空，无法构造 GET 请求".to_string(),
        ));
    }
    let mut varbinds = Vec::new();
    for oid in oids {
        let mut vb = Vec::new();
        put_tlv(&mut vb, TAG_OID, &encode_oid(oid)?);
        put_tlv(&mut vb, TAG_NULL, &[]);
        put_tlv(&mut varbinds, TAG_SEQUENCE, &vb);
    }
    let mut pdu = Vec::new();
    put_tlv(&mut pdu, TAG_INTEGER, &signed_bytes(i64::from(request_id)));
    put_tlv(&mut pdu, TAG_INTEGER, &signed_bytes(0));
    put_tlv(&mut pdu, TAG_INTEGER, &signed_bytes(0));
    put_tlv(&mut pdu, TAG_SEQUENCE, &varbinds);

    let mut msg = Vec::new();
    put_tlv(&mut msg, TAG_INTEGER, &signed_bytes(VERSION_V2C));
    put_tlv(&mut msg, TAG_OCTET_STRING, community.as_bytes());
    put_tlv(&mut msg, TAG_GET_REQUEST, &pdu);

    let mut out = Vec::new();
    put_tlv(&mut out, TAG_SEQUENCE, &msg);
    Ok(out)
}

// ---- 解码 ----

/// 顺序读 BER TLV 的游标。仅支持定长表单。
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn is_empty(&self) -> bool {
        self.pos >= self.buf.len()
    }

    fn byte(&mut self) -> Result<u8> {
        let b = *self.buf.get(self.pos).ok_or_else(|| bad("帧被截断"))?;
        self.pos += 1;
        Ok(b)
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(n).ok_or_else(|| bad("长度字段溢出"))?;
        let slice = self
            .buf
            .get(self.pos..end)
            .ok_or_else(|| bad("内容长度超出帧尾（截断）"))?;
        self.pos = end;
        Ok(slice)
    }

    fn tlv(&mut self) -> Result<(u8, &'a [u8])> {
        let tag = self.byte()?;
        let first = self.byte()?;
        let len = if first < 0x80 {
            usize::from(first)
        } else if first == 0x80 {
            return Err(bad("不支持不定长（indefinite）BER 表单"));
        } else {
            let n = usize::from(first & 0x7f);
            if n > 8 {
                return Err(bad("长度字段字节数过多"));
            }
            let mut acc: usize = 0;
            for _ in 0..n {
                acc = (acc << 8) | usize::from(self.byte()?);
            }
            acc
        };
        Ok((tag, self.take(len)?))
    }

    fn integer(&mut self) -> Result<i64> {
        let (tag, content) = self.tlv()?;
        if tag != TAG_INTEGER {
            return Err(bad(&format!(
                "期望 INTEGER(0x{TAG_INTEGER:02x})，收到 0x{tag:02x}"
            )));
        }
        decode_signed(content)
    }
}

fn decode_signed(content: &[u8]) -> Result<i64> {
    if content.is_empty() || content.len() > 8 {
        return Err(bad(&format!("INTEGER 长度非法: {}", content.len())));
    }
    let mut v: i64 = if content[0] & 0x80 != 0 { -1 } else { 0 };
    for &b in content {
        v = (v << 8) | i64::from(b);
    }
    Ok(v)
}

fn decode_unsigned(content: &[u8]) -> Result<u64> {
    if content.is_empty() || content.len() > 9 || (content.len() == 9 && content[0] != 0) {
        return Err(bad(&format!(
            "无符号整数长度非法或超出 u64: {}",
            content.len()
        )));
    }
    let mut v: u64 = 0;
    for &b in content {
        v = (v << 8) | u64::from(b);
    }
    Ok(v)
}

fn decode_oid(content: &[u8]) -> Result<String> {
    if content.is_empty() {
        return Err(bad("OID 内容为空"));
    }
    if content[content.len() - 1] & 0x80 != 0 {
        return Err(bad("OID 最后一个弧段缺终止字节"));
    }
    // 逐 base-128 组解码；首组是 40*arc0+arc1 的合并值（可跨多字节，如 2.999）。
    let mut groups: Vec<u64> = Vec::new();
    let mut acc: u64 = 0;
    for &b in content {
        if acc >> 57 != 0 {
            return Err(bad("OID 弧段超出 u64"));
        }
        acc = (acc << 7) | u64::from(b & 0x7f);
        if b & 0x80 == 0 {
            groups.push(acc);
            acc = 0;
        }
    }
    let head = groups[0];
    let mut arcs = if head < 40 {
        vec![0, head]
    } else if head < 80 {
        vec![1, head - 40]
    } else {
        vec![2, head - 80]
    };
    arcs.extend_from_slice(&groups[1..]);
    Ok(arcs
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join("."))
}

fn decode_value(tag: u8, content: &[u8]) -> Result<SnmpValue> {
    match tag {
        TAG_INTEGER => Ok(SnmpValue::Integer(decode_signed(content)?)),
        TAG_OCTET_STRING => Ok(SnmpValue::OctetString(content.to_vec())),
        TAG_NULL => Ok(SnmpValue::Null),
        TAG_IP_ADDRESS => Ok(SnmpValue::IpAddress(content.to_vec())),
        TAG_COUNTER32 => Ok(SnmpValue::Counter32(decode_unsigned(content)?)),
        TAG_GAUGE32 => Ok(SnmpValue::Gauge32(decode_unsigned(content)?)),
        TAG_TIME_TICKS => Ok(SnmpValue::TimeTicks(decode_unsigned(content)?)),
        TAG_COUNTER64 => Ok(SnmpValue::Counter64(decode_unsigned(content)?)),
        TAG_NO_SUCH_OBJECT => Ok(SnmpValue::NoSuchObject),
        TAG_NO_SUCH_INSTANCE => Ok(SnmpValue::NoSuchInstance),
        TAG_END_OF_MIB_VIEW => Ok(SnmpValue::EndOfMibView),
        // 0x44 Opaque / 0x45 NsapAddress 等未实现：报错而非静默丢弃（失败可见）。
        other => Err(bad(&format!("不支持的值类型 tag 0x{other:02x}"))),
    }
}

fn parse_pdu(content: &[u8]) -> Result<Pdu> {
    let mut r = Reader::new(content);
    let request_id = r.integer()?;
    let error_status = r.integer()?;
    let error_index = r.integer()?;
    let (vbl_tag, vbl) = r.tlv()?;
    if vbl_tag != TAG_SEQUENCE {
        return Err(bad("varbindlist 不是 SEQUENCE"));
    }
    let mut varbinds = Vec::new();
    let mut vr = Reader::new(vbl);
    while !vr.is_empty() {
        let (vb_tag, vb) = vr.tlv()?;
        if vb_tag != TAG_SEQUENCE {
            return Err(bad("varbind 不是 SEQUENCE"));
        }
        let mut br = Reader::new(vb);
        let (oid_tag, oid_bytes) = br.tlv()?;
        if oid_tag != TAG_OID {
            return Err(bad("varbind 首元素不是 OBJECT IDENTIFIER"));
        }
        let oid = decode_oid(oid_bytes)?;
        let (value_tag, value_bytes) = br.tlv()?;
        let value = decode_value(value_tag, value_bytes)?;
        if !br.is_empty() {
            return Err(bad("varbind 有多余字节"));
        }
        varbinds.push(VarBind { oid, value });
    }
    Ok(Pdu {
        request_id,
        error_status,
        error_index,
        varbinds,
    })
}

/// 拆 SNMP 消息外层：`(version, community, pdu_tag, pdu)`。
fn parse_message(bytes: &[u8]) -> Result<(i64, String, u8, Pdu)> {
    let mut outer = Reader::new(bytes);
    let (tag, msg) = outer.tlv()?;
    if tag != TAG_SEQUENCE {
        return Err(bad("顶层不是 SEQUENCE"));
    }
    if !outer.is_empty() {
        return Err(bad("顶层 SEQUENCE 之后有多余字节"));
    }
    let mut r = Reader::new(msg);
    let version = r.integer()?;
    let (community_tag, community) = r.tlv()?;
    if community_tag != TAG_OCTET_STRING {
        return Err(bad("community 不是 OCTET STRING"));
    }
    let (pdu_tag, pdu_bytes) = r.tlv()?;
    if !r.is_empty() {
        return Err(bad("PDU 之后有多余字节"));
    }
    let pdu = parse_pdu(pdu_bytes)?;
    Ok((
        version,
        String::from_utf8_lossy(community).into_owned(),
        pdu_tag,
        pdu,
    ))
}

fn check_common(version: i64, community: &str, expected: &str) -> Result<()> {
    if version != VERSION_V2C {
        return Err(CommonError::Internal(format!(
            "snmp: 帧 version={version}，Rust 版仅支持 v2c(1)"
        )));
    }
    if community != expected {
        return Err(CommonError::Internal(
            "snmp: 帧 community 与配置不一致，已丢弃".to_string(),
        ));
    }
    Ok(())
}

/// 解析 GetResponse 并校验 version/community/request-id/error-status。
pub(crate) fn parse_get_response(bytes: &[u8], community: &str, request_id: i64) -> Result<Pdu> {
    let (version, frame_community, pdu_tag, pdu) = parse_message(bytes)?;
    check_common(version, &frame_community, community)?;
    if pdu_tag != TAG_GET_RESPONSE {
        return Err(bad(&format!(
            "期望 GetResponse(0x{TAG_GET_RESPONSE:02x})，收到 PDU tag 0x{pdu_tag:02x}"
        )));
    }
    if pdu.request_id != request_id {
        return Err(bad(&format!(
            "request-id 不匹配：期望 {request_id}，收到 {}",
            pdu.request_id
        )));
    }
    if pdu.error_status != 0 {
        let name = ERROR_NAMES
            .get(pdu.error_status as usize)
            .copied()
            .unwrap_or("unknown");
        return Err(CommonError::Internal(format!(
            "snmp: agent 返回 error-status={} ({name}) error-index={}",
            pdu.error_status, pdu.error_index
        )));
    }
    Ok(pdu)
}

// ---- 以下仅测试用：构造响应帧 / 解析请求帧（回环假 agent 与 round-trip 测试） ----

#[cfg(test)]
fn encode_value(value: &SnmpValue) -> (u8, Vec<u8>) {
    match value {
        SnmpValue::Integer(v) => (TAG_INTEGER, signed_bytes(*v)),
        SnmpValue::OctetString(b) => (TAG_OCTET_STRING, b.clone()),
        SnmpValue::Null => (TAG_NULL, Vec::new()),
        SnmpValue::IpAddress(b) => (TAG_IP_ADDRESS, b.clone()),
        SnmpValue::Counter32(v) => (TAG_COUNTER32, unsigned_bytes(*v)),
        SnmpValue::Gauge32(v) => (TAG_GAUGE32, unsigned_bytes(*v)),
        SnmpValue::TimeTicks(v) => (TAG_TIME_TICKS, unsigned_bytes(*v)),
        SnmpValue::Counter64(v) => (TAG_COUNTER64, unsigned_bytes(*v)),
        SnmpValue::NoSuchObject => (TAG_NO_SUCH_OBJECT, Vec::new()),
        SnmpValue::NoSuchInstance => (TAG_NO_SUCH_INSTANCE, Vec::new()),
        SnmpValue::EndOfMibView => (TAG_END_OF_MIB_VIEW, Vec::new()),
    }
}

/// 构造一条完整 SNMP 消息（测试用它伪造 agent 响应 / 断言 version 校验）。
#[cfg(test)]
pub(crate) fn build_message(
    version: i64,
    community: &str,
    pdu_tag: u8,
    request_id: i64,
    error_status: i64,
    error_index: i64,
    varbinds: &[VarBind],
) -> Vec<u8> {
    let mut vbl = Vec::new();
    for vb in varbinds {
        let mut item = Vec::new();
        put_tlv(
            &mut item,
            TAG_OID,
            &encode_oid(&vb.oid).expect("测试 OID 合法"),
        );
        let (tag, content) = encode_value(&vb.value);
        put_tlv(&mut item, tag, &content);
        put_tlv(&mut vbl, TAG_SEQUENCE, &item);
    }
    let mut pdu = Vec::new();
    put_tlv(&mut pdu, TAG_INTEGER, &signed_bytes(request_id));
    put_tlv(&mut pdu, TAG_INTEGER, &signed_bytes(error_status));
    put_tlv(&mut pdu, TAG_INTEGER, &signed_bytes(error_index));
    put_tlv(&mut pdu, TAG_SEQUENCE, &vbl);

    let mut msg = Vec::new();
    put_tlv(&mut msg, TAG_INTEGER, &signed_bytes(version));
    put_tlv(&mut msg, TAG_OCTET_STRING, community.as_bytes());
    put_tlv(&mut msg, pdu_tag, &pdu);

    let mut out = Vec::new();
    put_tlv(&mut out, TAG_SEQUENCE, &msg);
    out
}

#[cfg(test)]
pub(crate) fn build_response(
    request_id: i64,
    community: &str,
    error_status: i64,
    error_index: i64,
    varbinds: &[VarBind],
) -> Vec<u8> {
    build_message(
        VERSION_V2C,
        community,
        TAG_GET_RESPONSE,
        request_id,
        error_status,
        error_index,
        varbinds,
    )
}

#[cfg(test)]
pub(crate) fn parse_get_request(bytes: &[u8], community: &str) -> Result<Pdu> {
    let (version, frame_community, pdu_tag, pdu) = parse_message(bytes)?;
    check_common(version, &frame_community, community)?;
    if pdu_tag != TAG_GET_REQUEST {
        return Err(bad(&format!(
            "期望 GetRequest(0x{TAG_GET_REQUEST:02x})，收到 PDU tag 0x{pdu_tag:02x}"
        )));
    }
    Ok(pdu)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vb(oid: &str, value: SnmpValue) -> VarBind {
        VarBind {
            oid: oid.to_string(),
            value,
        }
    }

    /// 请求构造 round-trip：GET 帧 → 解析回 OID 列表与 request-id。
    #[test]
    fn get_request_round_trip() {
        let oids = vec![
            "1.3.6.1.2.1.1.1.0".to_string(),
            "1.3.6.1.2.1.1.3.0".to_string(),
            "1.3.6.1.4.1.2011.5.25.31.1.1.1.1.5".to_string(),
        ];
        let frame = build_get_request(4242, "public", &oids).unwrap();
        let pdu = parse_get_request(&frame, "public").unwrap();
        assert_eq!(pdu.request_id, 4242);
        assert_eq!(pdu.error_status, 0);
        assert_eq!(pdu.error_index, 0);
        let got: Vec<&str> = pdu.varbinds.iter().map(|v| v.oid.as_str()).collect();
        assert_eq!(got, oids);
        assert!(pdu.varbinds.iter().all(|v| v.value == SnmpValue::Null));
    }

    /// 响应解析 round-trip：覆盖 Integer/OctetString/Counter32/Gauge32/TimeTicks/
    /// Counter64/IpAddress/Null + 三种异常类型（含 NoSuchObject）。
    #[test]
    fn response_round_trip_covers_value_types() {
        let varbinds = vec![
            vb(
                "1.3.6.1.2.1.1.1.0",
                SnmpValue::OctetString(b"Linux router".to_vec()),
            ),
            vb("1.3.6.1.2.1.1.3.0", SnmpValue::TimeTicks(12345678)),
            vb("1.3.6.1.2.1.2.2.1.10.1", SnmpValue::Counter32(4294967295)),
            vb("1.3.6.1.2.1.2.2.1.5.1", SnmpValue::Gauge32(1000)),
            vb("1.3.6.1.2.1.31.1.1.1.6.1", SnmpValue::Counter64(1u64 << 40)),
            vb("1.3.6.1.2.1.4.1.0", SnmpValue::IpAddress(vec![10, 0, 0, 1])),
            vb("1.3.6.1.2.1.1.5.0", SnmpValue::Integer(-5)),
            vb("1.3.6.1.2.1.1.2.0", SnmpValue::Null),
            vb("1.3.6.1.2.1.99.1.0", SnmpValue::NoSuchObject),
            vb("1.3.6.1.2.1.99.2.0", SnmpValue::NoSuchInstance),
            vb("1.3.6.1.2.1.99.3.0", SnmpValue::EndOfMibView),
        ];
        let frame = build_response(7, "huawei-community", 0, 0, &varbinds);
        let pdu = parse_get_response(&frame, "huawei-community", 7).unwrap();
        assert_eq!(pdu.varbinds, varbinds);
    }

    /// 值 → 上行字符串映射（对齐 Java `Variable.toString()`，TimeTicks 为刻意偏差）。
    #[test]
    fn value_to_wire_string() {
        assert_eq!(SnmpValue::Integer(-5).to_wire_string(), "-5");
        assert_eq!(SnmpValue::TimeTicks(12345678).to_wire_string(), "12345678");
        assert_eq!(
            SnmpValue::Counter32(4294967295).to_wire_string(),
            "4294967295"
        );
        assert_eq!(
            SnmpValue::OctetString(b"Linux router".to_vec()).to_wire_string(),
            "Linux router"
        );
        assert_eq!(
            SnmpValue::OctetString(vec![0xde, 0xad, 0x01]).to_wire_string(),
            "dead01"
        );
        assert_eq!(
            SnmpValue::IpAddress(vec![192, 168, 1, 1]).to_wire_string(),
            "192.168.1.1"
        );
        assert_eq!(SnmpValue::Null.to_wire_string(), "Null");
        assert_eq!(SnmpValue::NoSuchObject.to_wire_string(), "noSuchObject");
        assert_eq!(SnmpValue::NoSuchInstance.to_wire_string(), "noSuchInstance");
        assert_eq!(SnmpValue::EndOfMibView.to_wire_string(), "endOfMibView");
    }

    /// 长表单长度（内容 ≥128 字节）编解码一致。
    #[test]
    fn long_form_length_round_trip() {
        let payload = vec![0x41u8; 300];
        let varbinds = vec![vb(
            "1.3.6.1.2.1.1.1.0",
            SnmpValue::OctetString(payload.clone()),
        )];
        let frame = build_response(1, "public", 0, 0, &varbinds);
        let pdu = parse_get_response(&frame, "public", 1).unwrap();
        assert_eq!(pdu.varbinds[0].value, SnmpValue::OctetString(payload));
    }

    /// community 不一致 / request-id 不匹配 / 非 v2c / 非 GetResponse / error-status 非 0 → 报错。
    #[test]
    fn response_rejections_are_visible() {
        let varbinds = vec![vb("1.3.6.1.2.1.1.3.0", SnmpValue::TimeTicks(1))];
        let ok = build_response(9, "public", 0, 0, &varbinds);

        let err = parse_get_response(&ok, "private", 9).unwrap_err();
        assert!(err.to_string().contains("community"), "{err}");

        let err = parse_get_response(&ok, "public", 10).unwrap_err();
        assert!(err.to_string().contains("request-id"), "{err}");

        let v1 = build_message(0, "public", TAG_GET_RESPONSE, 9, 0, 0, &varbinds);
        let err = parse_get_response(&v1, "public", 9).unwrap_err();
        assert!(err.to_string().contains("v2c"), "{err}");

        let trap = build_message(1, "public", 0xA4, 9, 0, 0, &varbinds);
        let err = parse_get_response(&trap, "public", 9).unwrap_err();
        assert!(err.to_string().contains("GetResponse"), "{err}");

        let failed = build_response(9, "public", 2, 1, &varbinds);
        let err = parse_get_response(&failed, "public", 9).unwrap_err();
        assert!(err.to_string().contains("noSuchName"), "{err}");
    }

    /// 截断帧 / 不定长表单 / 非 SEQUENCE 顶层 → 报错且不 panic。
    #[test]
    fn malformed_frames_rejected() {
        let frame = build_response(
            1,
            "public",
            0,
            0,
            &[vb("1.3.6.1.2.1.1.3.0", SnmpValue::Null)],
        );
        for n in 0..frame.len() {
            assert!(
                parse_get_response(&frame[..n], "public", 1).is_err(),
                "截断到 {n} 字节应报错"
            );
        }
        let indefinite = vec![0x30, 0x80, 0x02, 0x01, 0x01, 0x00, 0x00];
        assert!(parse_get_response(&indefinite, "public", 1).is_err());
        assert!(parse_get_response(&[0x02, 0x01, 0x01], "public", 1).is_err());
        assert!(parse_get_response(&[], "public", 1).is_err());
    }

    /// OID 编解码 round-trip（含多字节 base-128 弧段）与非法输入拒绝。
    #[test]
    fn oid_codec() {
        for oid in [
            "1.3.6.1.2.1.1.1.0",
            "1.3.6.1.4.1.2011.5.25.31.1.1.1.1.5",
            "2.999.1",
            "0.0",
            "1.3.6.1.2.1.1.2.18446744073709551615",
        ] {
            let mut content = Vec::new();
            put_tlv(&mut content, TAG_OID, &encode_oid(oid).unwrap());
            let mut r = Reader::new(&content);
            let (tag, bytes) = r.tlv().unwrap();
            assert_eq!(tag, TAG_OID);
            assert_eq!(
                decode_oid(bytes).unwrap(),
                oid,
                "OID round-trip 失败: {oid}"
            );
        }
        for bad_oid in [
            "",
            "1",
            "1.3.",
            "1.3.6.x",
            "3.1",
            "1.40",
            "01.3.6",
            "1.3.99999999999999999999",
        ] {
            assert!(encode_oid(bad_oid).is_err(), "应拒绝非法 OID: '{bad_oid}'");
        }
        // 前导点容忍（对齐 Java `new OID(".1.3.6")`）
        assert_eq!(encode_oid(".1.3.6").unwrap(), encode_oid("1.3.6").unwrap());
    }

    /// 空 OID 列表不产生请求帧（配置校验之外的第二道防线）。
    #[test]
    fn empty_oid_list_rejected() {
        let err = build_get_request(1, "public", &[]).unwrap_err();
        assert!(matches!(err, CommonError::Config(_)), "{err:?}");
    }
}
