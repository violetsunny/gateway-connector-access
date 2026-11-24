//! 手写 MQTT 3.1.1 报文编解码（零新增依赖）。
//!
//! 与 connector-tcp codec 同一手法：`bytes` 不在依赖白名单，不用 tokio-util codecs，
//! 以 `Vec<u8>` 缓冲从头部切出一个完整报文，数据不足返回 `None` 等待更多字节。
//!
//! 资源上限：remaining length 超 [`MAX_PACKET_SIZE`] → Err（连接层断开），
//! 防恶意长度字段耗尽内存（评审 Critical 同族教训）。

use crate::error::{Error, Result};
use crate::topic;

/// remaining length 上限（1 MiB）。ponytail: 固定常量；升级路径：按监听配置可调。
pub const MAX_PACKET_SIZE: usize = 1024 * 1024;

fn malformed(msg: impl Into<String>) -> Error {
    Error::Malformed(msg.into())
}

#[derive(Debug, Clone, PartialEq)]
pub enum Packet {
    Connect(Connect),
    Connack {
        session_present: bool,
        code: u8,
    },
    Publish(Publish),
    Puback(u16),
    Subscribe {
        packet_id: u16,
        filters: Vec<(String, u8)>,
    },
    Suback {
        packet_id: u16,
        codes: Vec<u8>,
    },
    Unsubscribe {
        packet_id: u16,
        filters: Vec<String>,
    },
    Unsuback(u16),
    Pingreq,
    Pingresp,
    Disconnect,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Connect {
    pub protocol_level: u8,
    pub client_id: String,
    pub clean_session: bool,
    pub keep_alive: u16,
    pub username: Option<String>,
    pub password: Option<Vec<u8>>,
    pub will: Option<Will>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Will {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub retain: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Publish {
    pub dup: bool,
    pub qos: u8,
    pub retain: bool,
    pub topic: String,
    /// Some 当且仅当 qos > 0。
    pub packet_id: Option<u16>,
    pub payload: Vec<u8>,
}

/// 从 `buf` 头部切出一个完整报文（并移除）；数据不足返回 `None`。
pub fn decode(buf: &mut Vec<u8>) -> Result<Option<Packet>> {
    if buf.len() < 2 {
        return Ok(None);
    }
    let first = buf[0];
    let mut remaining = 0usize;
    let mut shift = 0u32;
    let mut idx = 1;
    loop {
        if idx >= buf.len() {
            return Ok(None);
        }
        let b = buf[idx];
        idx += 1;
        remaining |= usize::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift > 21 {
            return Err(malformed("remaining length varint exceeds 4 bytes"));
        }
    }
    if remaining > MAX_PACKET_SIZE {
        return Err(malformed(format!(
            "remaining length {remaining} exceeds cap {MAX_PACKET_SIZE}"
        )));
    }
    let total = idx + remaining;
    if buf.len() < total {
        return Ok(None);
    }
    let raw: Vec<u8> = buf.drain(..total).collect();
    let body = &raw[idx..];
    let ptype = first >> 4;
    let flags = first & 0x0f;
    let packet = match ptype {
        1 => Packet::Connect(parse_connect(body)?),
        2 => {
            expect_flags(flags, 0, "CONNACK")?;
            let mut r = Reader::new(body);
            let ack_flags = r.u8()?;
            if ack_flags & 0xfe != 0 {
                return Err(malformed("CONNACK reserved ack flags set"));
            }
            let code = r.u8()?;
            if code > 5 {
                return Err(malformed(format!("CONNACK return code {code} > 5")));
            }
            r.finish()?;
            Packet::Connack {
                session_present: ack_flags & 0x01 != 0,
                code,
            }
        }
        3 => Packet::Publish(parse_publish(flags, body)?),
        4 => {
            expect_flags(flags, 0, "PUBACK")?;
            Packet::Puback(exact_u16(body, "PUBACK")?)
        }
        8 => {
            expect_flags(flags, 2, "SUBSCRIBE")?;
            parse_subscribe(body)?
        }
        9 => {
            expect_flags(flags, 0, "SUBACK")?;
            let mut r = Reader::new(body);
            let packet_id = r.u16()?;
            let codes = r.rest().to_vec();
            if codes.is_empty() {
                return Err(malformed("SUBACK without return codes"));
            }
            if codes.iter().any(|c| !matches!(c, 0 | 1 | 2 | 0x80)) {
                return Err(malformed("SUBACK invalid return code"));
            }
            Packet::Suback { packet_id, codes }
        }
        10 => {
            expect_flags(flags, 2, "UNSUBSCRIBE")?;
            parse_unsubscribe(body)?
        }
        11 => {
            expect_flags(flags, 0, "UNSUBACK")?;
            Packet::Unsuback(exact_u16(body, "UNSUBACK")?)
        }
        12 => {
            expect_flags(flags, 0, "PINGREQ")?;
            expect_empty(body, "PINGREQ")?;
            Packet::Pingreq
        }
        13 => {
            expect_flags(flags, 0, "PINGRESP")?;
            expect_empty(body, "PINGRESP")?;
            Packet::Pingresp
        }
        14 => {
            expect_flags(flags, 0, "DISCONNECT")?;
            expect_empty(body, "DISCONNECT")?;
            Packet::Disconnect
        }
        other => return Err(malformed(format!("unknown packet type {other}"))),
    };
    Ok(Some(packet))
}

/// 编码一个报文追加到 `out`。
pub fn encode(packet: &Packet, out: &mut Vec<u8>) {
    let mut body = Vec::new();
    let (ptype, flags) = match packet {
        Packet::Connect(c) => {
            write_connect(&mut body, c);
            (1, 0)
        }
        Packet::Connack {
            session_present,
            code,
        } => {
            body.push(u8::from(*session_present));
            body.push(*code);
            (2, 0)
        }
        Packet::Publish(p) => {
            let flags = (u8::from(p.dup) << 3) | (p.qos << 1) | u8::from(p.retain);
            write_string(&mut body, &p.topic);
            if let Some(id) = p.packet_id {
                write_u16(&mut body, id);
            }
            body.extend_from_slice(&p.payload);
            (3, flags)
        }
        Packet::Puback(id) => {
            write_u16(&mut body, *id);
            (4, 0)
        }
        Packet::Subscribe { packet_id, filters } => {
            write_u16(&mut body, *packet_id);
            for (f, qos) in filters {
                write_string(&mut body, f);
                body.push(*qos);
            }
            (8, 2)
        }
        Packet::Suback { packet_id, codes } => {
            write_u16(&mut body, *packet_id);
            body.extend_from_slice(codes);
            (9, 0)
        }
        Packet::Unsubscribe { packet_id, filters } => {
            write_u16(&mut body, *packet_id);
            for f in filters {
                write_string(&mut body, f);
            }
            (10, 2)
        }
        Packet::Unsuback(id) => {
            write_u16(&mut body, *id);
            (11, 0)
        }
        Packet::Pingreq => (12, 0),
        Packet::Pingresp => (13, 0),
        Packet::Disconnect => (14, 0),
    };
    out.push((ptype << 4) | flags);
    let mut len = body.len();
    loop {
        let mut b = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            b |= 0x80;
        }
        out.push(b);
        if len == 0 {
            break;
        }
    }
    out.extend_from_slice(&body);
}

fn expect_flags(flags: u8, expect: u8, name: &str) -> Result<()> {
    if flags != expect {
        return Err(malformed(format!(
            "{name} fixed header flags {flags:#x} != {expect:#x}"
        )));
    }
    Ok(())
}

fn expect_empty(body: &[u8], name: &str) -> Result<()> {
    if !body.is_empty() {
        return Err(malformed(format!(
            "{name} expects empty body, got {} bytes",
            body.len()
        )));
    }
    Ok(())
}

fn exact_u16(body: &[u8], name: &str) -> Result<u16> {
    if body.len() != 2 {
        return Err(malformed(format!(
            "{name} expects 2-byte body, got {} bytes",
            body.len()
        )));
    }
    Ok(u16::from_be_bytes([body[0], body[1]]))
}

struct Reader<'a> {
    body: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(body: &'a [u8]) -> Self {
        Self { body, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.pos + n > self.body.len() {
            return Err(malformed("truncated packet body"));
        }
        let s = &self.body[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    fn bytes(&mut self) -> Result<Vec<u8>> {
        let n = self.u16()? as usize;
        Ok(self.take(n)?.to_vec())
    }

    fn string(&mut self) -> Result<String> {
        let b = self.bytes()?;
        if b.contains(&0) {
            return Err(malformed("NUL in utf-8 string"));
        }
        String::from_utf8(b).map_err(|_| malformed("invalid utf-8 string"))
    }

    fn rest(&mut self) -> &'a [u8] {
        let s = &self.body[self.pos..];
        self.pos = self.body.len();
        s
    }

    fn finish(&self) -> Result<()> {
        if self.pos != self.body.len() {
            return Err(malformed(format!(
                "{} trailing bytes",
                self.body.len() - self.pos
            )));
        }
        Ok(())
    }
}

fn parse_connect(body: &[u8]) -> Result<Connect> {
    let mut r = Reader::new(body);
    let name = r.string()?;
    if name != "MQTT" {
        return Err(malformed(format!(
            "unsupported protocol name '{name}' (expect MQTT)"
        )));
    }
    let protocol_level = r.u8()?;
    let flags = r.u8()?;
    if flags & 0x01 != 0 {
        return Err(malformed("CONNECT reserved flag bit set"));
    }
    let keep_alive = r.u16()?;
    let client_id = r.string()?;
    let clean_session = flags & 0x02 != 0;
    let will_flag = flags & 0x04 != 0;
    let will_qos = (flags >> 3) & 0x03;
    let will_retain = flags & 0x20 != 0;
    let password_flag = flags & 0x40 != 0;
    let username_flag = flags & 0x80 != 0;
    if will_flag && will_qos > 2 {
        return Err(malformed("CONNECT will qos 3"));
    }
    if !will_flag && (will_qos != 0 || will_retain) {
        return Err(malformed("CONNECT will qos/retain set without will flag"));
    }
    if password_flag && !username_flag {
        return Err(malformed("CONNECT password flag without username flag"));
    }
    let will = if will_flag {
        let topic = r.string()?;
        let payload = r.bytes()?;
        if !topic::valid_name(&topic) {
            return Err(malformed(format!("invalid will topic '{topic}'")));
        }
        Some(Will {
            topic,
            payload,
            qos: will_qos,
            retain: will_retain,
        })
    } else {
        None
    };
    let username = if username_flag {
        Some(r.string()?)
    } else {
        None
    };
    let password = if password_flag {
        Some(r.bytes()?)
    } else {
        None
    };
    r.finish()?;
    Ok(Connect {
        protocol_level,
        client_id,
        clean_session,
        keep_alive,
        username,
        password,
        will,
    })
}

fn parse_publish(flags: u8, body: &[u8]) -> Result<Publish> {
    let dup = flags & 0x08 != 0;
    let qos = (flags >> 1) & 0x03;
    let retain = flags & 0x01 != 0;
    if qos == 3 {
        return Err(malformed("PUBLISH qos 3"));
    }
    let mut r = Reader::new(body);
    let topic = r.string()?;
    if !topic::valid_name(&topic) {
        return Err(malformed(format!("invalid publish topic '{topic}'")));
    }
    let packet_id = if qos > 0 {
        let id = r.u16()?;
        if id == 0 {
            return Err(malformed("PUBLISH packet id 0"));
        }
        Some(id)
    } else {
        None
    };
    let payload = r.rest().to_vec();
    Ok(Publish {
        dup,
        qos,
        retain,
        topic,
        packet_id,
        payload,
    })
}

fn parse_subscribe(body: &[u8]) -> Result<Packet> {
    let mut r = Reader::new(body);
    let packet_id = r.u16()?;
    if packet_id == 0 {
        return Err(malformed("SUBSCRIBE packet id 0"));
    }
    let mut filters = Vec::new();
    while r.pos < r.body.len() {
        let filter = r.string()?;
        let qos = r.u8()?;
        if qos > 2 {
            return Err(malformed(format!("SUBSCRIBE requested qos {qos} > 2")));
        }
        filters.push((filter, qos));
    }
    if filters.is_empty() {
        return Err(malformed("SUBSCRIBE without topic filters"));
    }
    Ok(Packet::Subscribe { packet_id, filters })
}

fn parse_unsubscribe(body: &[u8]) -> Result<Packet> {
    let mut r = Reader::new(body);
    let packet_id = r.u16()?;
    if packet_id == 0 {
        return Err(malformed("UNSUBSCRIBE packet id 0"));
    }
    let mut filters = Vec::new();
    while r.pos < r.body.len() {
        filters.push(r.string()?);
    }
    if filters.is_empty() {
        return Err(malformed("UNSUBSCRIBE without topic filters"));
    }
    Ok(Packet::Unsubscribe { packet_id, filters })
}

fn write_u16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_be_bytes());
}

fn write_bytes(out: &mut Vec<u8>, b: &[u8]) {
    write_u16(out, b.len() as u16);
    out.extend_from_slice(b);
}

fn write_string(out: &mut Vec<u8>, s: &str) {
    write_bytes(out, s.as_bytes());
}

fn write_connect(out: &mut Vec<u8>, c: &Connect) {
    write_string(out, "MQTT");
    out.push(c.protocol_level);
    let mut flags = 0u8;
    if c.clean_session {
        flags |= 0x02;
    }
    if let Some(w) = &c.will {
        flags |= 0x04 | (w.qos << 3);
        if w.retain {
            flags |= 0x20;
        }
    }
    if c.password.is_some() {
        flags |= 0x40;
    }
    if c.username.is_some() {
        flags |= 0x80;
    }
    out.push(flags);
    write_u16(out, c.keep_alive);
    write_string(out, &c.client_id);
    if let Some(w) = &c.will {
        write_string(out, &w.topic);
        write_bytes(out, &w.payload);
    }
    if let Some(u) = &c.username {
        write_string(out, u);
    }
    if let Some(p) = &c.password {
        write_bytes(out, p);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(packet: Packet) {
        let mut buf = Vec::new();
        encode(&packet, &mut buf);
        let decoded = decode(&mut buf).unwrap().expect("complete packet");
        assert_eq!(decoded, packet);
        assert!(buf.is_empty(), "buffer fully consumed");
    }

    fn sample_connect() -> Connect {
        Connect {
            protocol_level: 4,
            client_id: "client-1".to_string(),
            clean_session: true,
            keep_alive: 60,
            username: Some("user".to_string()),
            password: Some(b"pass".to_vec()),
            will: Some(Will {
                topic: "status/client-1".to_string(),
                payload: b"offline".to_vec(),
                qos: 1,
                retain: true,
            }),
        }
    }

    #[test]
    fn roundtrip_all_packet_types() {
        roundtrip(Packet::Connect(sample_connect()));
        roundtrip(Packet::Connack {
            session_present: true,
            code: 0,
        });
        roundtrip(Packet::Publish(Publish {
            dup: false,
            qos: 0,
            retain: false,
            topic: "a/b".to_string(),
            packet_id: None,
            payload: b"hello".to_vec(),
        }));
        roundtrip(Packet::Publish(Publish {
            dup: true,
            qos: 1,
            retain: true,
            topic: "a/b".to_string(),
            packet_id: Some(42),
            payload: vec![0u8; 300],
        }));
        roundtrip(Packet::Puback(42));
        roundtrip(Packet::Subscribe {
            packet_id: 7,
            filters: vec![("a/+".to_string(), 0), ("b/#".to_string(), 1)],
        });
        roundtrip(Packet::Suback {
            packet_id: 7,
            codes: vec![0, 1, 0x80],
        });
        roundtrip(Packet::Unsubscribe {
            packet_id: 8,
            filters: vec!["a/+".to_string()],
        });
        roundtrip(Packet::Unsuback(8));
        roundtrip(Packet::Pingreq);
        roundtrip(Packet::Pingresp);
        roundtrip(Packet::Disconnect);
    }

    #[test]
    fn connect_minimal_no_optional_fields() {
        roundtrip(Packet::Connect(Connect {
            protocol_level: 4,
            client_id: String::new(),
            clean_session: false,
            keep_alive: 0,
            username: None,
            password: None,
            will: None,
        }));
    }

    #[test]
    fn byte_by_byte_feed_yields_one_packet() {
        let mut wire = Vec::new();
        encode(
            &Packet::Publish(Publish {
                dup: false,
                qos: 1,
                retain: false,
                topic: "t".to_string(),
                packet_id: Some(9),
                payload: b"xyz".to_vec(),
            }),
            &mut wire,
        );
        let mut buf = Vec::new();
        let mut got = None;
        for b in &wire {
            buf.push(*b);
            if let Some(p) = decode(&mut buf).unwrap() {
                assert!(got.is_none(), "exactly one packet");
                got = Some(p);
            }
        }
        assert!(got.is_some());
        assert!(buf.is_empty());
    }

    #[test]
    fn two_concatenated_packets_decode_in_order() {
        let mut buf = Vec::new();
        encode(&Packet::Pingreq, &mut buf);
        encode(&Packet::Disconnect, &mut buf);
        assert_eq!(decode(&mut buf).unwrap(), Some(Packet::Pingreq));
        assert_eq!(decode(&mut buf).unwrap(), Some(Packet::Disconnect));
        assert_eq!(decode(&mut buf).unwrap(), None);
    }

    #[test]
    fn varint_length_boundaries() {
        for payload_len in [0usize, 127, 128, 16383, 16384, MAX_PACKET_SIZE - 3] {
            let p = Packet::Publish(Publish {
                dup: false,
                qos: 0,
                retain: false,
                topic: "t".to_string(),
                packet_id: None,
                payload: vec![0u8; payload_len],
            });
            let mut buf = Vec::new();
            encode(&p, &mut buf);
            assert_eq!(decode(&mut buf).unwrap(), Some(p), "len {payload_len}");
        }
    }

    #[test]
    fn incomplete_body_waits() {
        let mut buf = Vec::new();
        encode(&Packet::Puback(1), &mut buf);
        buf.pop();
        assert_eq!(decode(&mut buf).unwrap(), None);
    }

    #[test]
    fn oversize_remaining_length_errors() {
        // remaining length = MAX_PACKET_SIZE + 1（varint 编码），不等待缓冲直接 Err
        let len = MAX_PACKET_SIZE + 1;
        let mut buf = vec![0x30];
        let mut v = len;
        loop {
            let mut b = (v % 128) as u8;
            v /= 128;
            if v > 0 {
                b |= 0x80;
            }
            buf.push(b);
            if v == 0 {
                break;
            }
        }
        let err = decode(&mut buf).unwrap_err();
        assert!(err.to_string().contains("exceeds cap"), "got {err}");
    }

    #[test]
    fn varint_over_4_bytes_errors() {
        let mut buf = vec![0x30, 0xff, 0xff, 0xff, 0xff, 0x7f];
        assert!(decode(&mut buf).is_err());
    }

    #[test]
    fn publish_qos3_errors() {
        // flags = qos3 << 1 = 0x06
        let mut buf = vec![0x36, 0x00];
        assert!(decode(&mut buf).is_err());
    }

    #[test]
    fn publish_wildcard_topic_errors() {
        let mut buf = Vec::new();
        encode(
            &Packet::Publish(Publish {
                dup: false,
                qos: 0,
                retain: false,
                topic: "a/+".to_string(),
                packet_id: None,
                payload: vec![],
            }),
            &mut buf,
        );
        assert!(decode(&mut buf).is_err());
    }

    #[test]
    fn publish_qos1_zero_packet_id_errors() {
        let mut body = Vec::new();
        write_string(&mut body, "t");
        write_u16(&mut body, 0);
        let mut buf = vec![0x32, body.len() as u8];
        buf.extend_from_slice(&body);
        assert!(decode(&mut buf).is_err());
    }

    #[test]
    fn subscribe_wrong_flags_errors() {
        let mut buf = Vec::new();
        encode(
            &Packet::Subscribe {
                packet_id: 1,
                filters: vec![("a".to_string(), 0)],
            },
            &mut buf,
        );
        buf[0] = 0x80; // flags 0 而非 2
        assert!(decode(&mut buf).is_err());
    }

    #[test]
    fn subscribe_empty_filters_errors() {
        let mut buf = vec![0x82, 0x02, 0x00, 0x01];
        assert!(decode(&mut buf).is_err());
    }

    #[test]
    fn connect_reserved_flag_errors() {
        let mut buf = Vec::new();
        encode(&Packet::Connect(sample_connect()), &mut buf);
        // body: 00 04 M Q T T 04 flags ...，flags 在 body 偏移 7 → wire 偏移 9（2 字节头）
        buf[9] |= 0x01;
        assert!(decode(&mut buf).is_err());
    }

    #[test]
    fn connect_bad_protocol_name_errors() {
        let mut body = Vec::new();
        write_string(&mut body, "MQIsdp");
        body.push(3);
        body.push(0x02);
        write_u16(&mut body, 60);
        write_string(&mut body, "c");
        let mut buf = vec![0x10, body.len() as u8];
        buf.extend_from_slice(&body);
        assert!(decode(&mut buf).is_err());
    }

    #[test]
    fn truncated_connect_body_errors() {
        let mut buf = Vec::new();
        encode(&Packet::Connect(sample_connect()), &mut buf);
        buf.pop();
        buf.pop();
        // 长度字段同步缩小，使报文"完整"但 body 截断
        buf[1] -= 2;
        assert!(decode(&mut buf).is_err());
    }

    #[test]
    fn unknown_packet_type_errors() {
        let mut buf = vec![0x00, 0x00];
        assert!(decode(&mut buf).is_err());
    }

    #[test]
    fn pingreq_with_body_errors() {
        let mut buf = vec![0xc0, 0x01, 0x00];
        assert!(decode(&mut buf).is_err());
    }
}
