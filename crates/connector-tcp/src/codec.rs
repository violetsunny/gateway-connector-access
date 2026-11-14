//! 8 种粘包拆包器，语义对齐 Java `ParserProvider.newFrameDecoder`（ParserProvider.java:33-80）：
//! DIRECT / DELIMITER(hex 分隔符) / LENGTH_FIELD(长度字段偏移+字节序) / JSON_OBJECT(括号配平) /
//! FIXED_LENGTH / XML(根标签配平) / LINE / ZLIB(flate2 zlib 格式)。
//!
//! 独立模块，票 04（tcp client）直接复用。
//!
//! 偏差说明：未用 tokio-util codecs——其 `Decoder` trait 签名要求直接依赖 `bytes`
//! （不在依赖白名单，且并行开发期禁改根 Cargo.toml），故以 `Vec<u8>` 缓冲手写等价实现。
//! 与 Netty 的行为差异统一为「失败可见」：超长/损坏帧返回 Err（由连接层关闭连接），
//! 而非 Netty 的 failFast 丢帧继续。

use connector_common::error::{CommonError, Result};
use serde_json::Value as JsonValue;

/// 拆包器契约：从 `buf` 头部切出一个完整帧（并移除），数据不足返回 `None` 等待更多字节。
pub trait FrameCodec: Send {
    fn decode(&mut self, buf: &mut Vec<u8>) -> Result<Option<Vec<u8>>>;
}

fn cfg(msg: String) -> CommonError {
    CommonError::Config(msg)
}

/// 按 parserType + parserParams 构建拆包器（对齐 ParserProvider.java:33-80）。
///
/// parserType 大小写不敏感（对齐 `ParserType.parse`）；缺失或未知 → Err（失败可见。
/// Java 对缺失 NPE、对未知静默降级为无拆包，均不复刻）。
pub fn build_codec(
    parser_type: Option<&str>,
    params: Option<&JsonValue>,
) -> Result<Box<dyn FrameCodec>> {
    let t = parser_type
        .ok_or_else(|| cfg("tcp parserType required".to_string()))?
        .to_ascii_uppercase();
    match t.as_str() {
        "DIRECT" => Ok(Box::new(DirectCodec)),
        "DELIMITER" => Ok(Box::new(DelimiterCodec {
            delimiter: hex_decode(
                params
                    .and_then(|p| p.get("delimiter"))
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| {
                        cfg("DELIMITER: parserParams.delimiter (hex string) required".to_string())
                    })?,
            )?,
            max_length: req_pos_int(params, "maxLength", "DELIMITER")?,
        })),
        "LENGTH_FIELD" => {
            let field_len = req_pos_int(params, "lengthFieldLength", "LENGTH_FIELD")?;
            if !(1..=8).contains(&field_len) {
                return Err(cfg(format!(
                    "LENGTH_FIELD: lengthFieldLength must be 1..=8, got {field_len}"
                )));
            }
            // 对齐 Java：byteOrder 缺省 BIG_ENDIAN，值等于 "BIG_ENDIAN" 为大端，其余一律小端
            let little_endian = params
                .and_then(|p| p.get("byteOrder"))
                .and_then(JsonValue::as_str)
                .is_some_and(|s| s != "BIG_ENDIAN");
            Ok(Box::new(LengthFieldCodec {
                max_length: req_pos_int(params, "maxLength", "LENGTH_FIELD")?,
                offset: opt_non_neg_int(params, "lengthFieldOffset", "LENGTH_FIELD")?,
                field_len,
                adjustment: opt_int(params, "lengthAdjustment").unwrap_or(0),
                strip: opt_non_neg_int(params, "initialBytesToStrip", "LENGTH_FIELD")?,
                little_endian,
            }))
        }
        "JSON_OBJECT" => Ok(Box::new(JsonObjectCodec::new(req_pos_int(
            params,
            "maxLength",
            "JSON_OBJECT",
        )?))),
        "FIXED_LENGTH" => Ok(Box::new(FixedLengthCodec {
            frame_length: req_pos_int(params, "frameLength", "FIXED_LENGTH")?,
        })),
        "XML" => Ok(Box::new(XmlCodec {
            max_length: req_pos_int(params, "maxLength", "XML")?,
        })),
        "LINE" => Ok(Box::new(LineCodec {
            max_length: req_pos_int(params, "maxLength", "LINE")?,
        })),
        "ZLIB" => {
            // 解压炸弹防护：输出上限可配（maxOutputLen），缺省 1MB
            let max_output = match opt_int(params, "maxOutputLen") {
                Some(v) if v > 0 => v as usize,
                Some(v) => {
                    return Err(cfg(format!(
                        "ZLIB: parserParams.maxOutputLen must be positive, got {v}"
                    )));
                }
                None => DEFAULT_ZLIB_MAX_OUTPUT,
            };
            Ok(Box::new(ZlibCodec::new(max_output)))
        }
        other => Err(cfg(format!("unsupported tcp parserType: {other}"))),
    }
}

/// 取整型参数：兼容 JSON number 与数字字符串（对齐 fastjson getInteger 的强转行为）。
fn opt_int(params: Option<&JsonValue>, key: &str) -> Option<i64> {
    params?.get(key).and_then(|v| {
        v.as_i64()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
    })
}

fn opt_non_neg_int(params: Option<&JsonValue>, key: &str, what: &str) -> Result<usize> {
    let v = opt_int(params, key).unwrap_or(0);
    if v < 0 {
        return Err(cfg(format!(
            "{what}: parserParams.{key} must be >= 0, got {v}"
        )));
    }
    Ok(v as usize)
}

fn req_pos_int(params: Option<&JsonValue>, key: &str, what: &str) -> Result<usize> {
    let v = opt_int(params, key).ok_or_else(|| {
        cfg(format!(
            "{what}: parserParams.{key} required (Java getIntValue 缺省 0 会使 Netty 抛错)"
        ))
    })?;
    if v <= 0 {
        return Err(cfg(format!(
            "{what}: parserParams.{key} must be positive, got {v}"
        )));
    }
    Ok(v as usize)
}

fn hex_decode(s: &str) -> Result<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return Err(cfg(format!("delimiter hex string has odd length: {s}")));
    }
    s.as_bytes()
        .chunks(2)
        .map(|c| Ok(hex_val(c[0])? << 4 | hex_val(c[1])?))
        .collect()
}

fn hex_val(b: u8) -> Result<u8> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(cfg(format!("invalid hex byte in delimiter: {b:#04x}"))),
    }
}

/// DIRECT：无拆包，每次收到的字节块整体作为一帧（对齐 Java DIRECT 不加 decoder，
/// DecoderHandler 一次消费全部可读字节）。
pub struct DirectCodec;

impl FrameCodec for DirectCodec {
    fn decode(&mut self, buf: &mut Vec<u8>) -> Result<Option<Vec<u8>>> {
        if buf.is_empty() {
            Ok(None)
        } else {
            Ok(Some(std::mem::take(buf)))
        }
    }
}

/// DELIMITER：hex 分隔符切帧，帧不含分隔符（对齐 Netty DelimiterBasedFrameDecoder）。
pub struct DelimiterCodec {
    delimiter: Vec<u8>,
    max_length: usize,
}

impl FrameCodec for DelimiterCodec {
    fn decode(&mut self, buf: &mut Vec<u8>) -> Result<Option<Vec<u8>>> {
        // ponytail: windows 线性扫描 O(n*m)，帧长 KB 级可接受；升级路径 Aho-Corasick
        let pos = buf
            .windows(self.delimiter.len())
            .position(|w| w == self.delimiter);
        match pos {
            Some(p) if p > self.max_length => Err(cfg(format!(
                "DELIMITER: frame length {p} exceeds maxLength {}",
                self.max_length
            ))),
            Some(p) => {
                let frame: Vec<u8> = buf.drain(..p).collect();
                buf.drain(..self.delimiter.len());
                Ok(Some(frame))
            }
            None => {
                if buf.len() > self.max_length {
                    return Err(cfg(format!(
                        "DELIMITER: no delimiter within maxLength {}",
                        self.max_length
                    )));
                }
                Ok(None)
            }
        }
    }
}

/// LENGTH_FIELD：长度字段切帧（对齐 Netty LengthFieldBasedFrameDecoder，failFast=true）。
/// frameLength = 长度字段值 + lengthAdjustment + (offset + fieldLen)。
pub struct LengthFieldCodec {
    max_length: usize,
    offset: usize,
    field_len: usize,
    adjustment: i64,
    strip: usize,
    little_endian: bool,
}

impl LengthFieldCodec {
    fn read_length(&self, field: &[u8]) -> u64 {
        // 大端高位在前正序折叠；小端低位在前逆序折叠
        if self.little_endian {
            field
                .iter()
                .rev()
                .fold(0u64, |acc, b| acc << 8 | u64::from(*b))
        } else {
            field.iter().fold(0u64, |acc, b| acc << 8 | u64::from(*b))
        }
    }
}

impl FrameCodec for LengthFieldCodec {
    fn decode(&mut self, buf: &mut Vec<u8>) -> Result<Option<Vec<u8>>> {
        let field_end = self.offset + self.field_len;
        if buf.len() < field_end {
            return Ok(None);
        }
        let value = self.read_length(&buf[self.offset..field_end]) as i64;
        let frame_len = value
            .checked_add(self.adjustment)
            .and_then(|v| v.checked_add(field_end as i64))
            .ok_or_else(|| {
                cfg(format!(
                    "LENGTH_FIELD: frame length overflow: value {value} + adjustment {} + fieldEnd {field_end}",
                    self.adjustment
                ))
            })?;
        if frame_len < field_end as i64 {
            return Err(cfg(format!(
                "LENGTH_FIELD: adjusted frame length {frame_len} < lengthFieldEndOffset {field_end}"
            )));
        }
        let frame_len = frame_len as usize;
        if frame_len > self.max_length {
            return Err(cfg(format!(
                "LENGTH_FIELD: frame length {frame_len} exceeds maxLength {}",
                self.max_length
            )));
        }
        if buf.len() < frame_len {
            return Ok(None);
        }
        let mut frame: Vec<u8> = buf.drain(..frame_len).collect();
        if self.strip > frame.len() {
            return Err(cfg(format!(
                "LENGTH_FIELD: initialBytesToStrip {} > frame length {}",
                self.strip,
                frame.len()
            )));
        }
        frame.drain(..self.strip);
        Ok(Some(frame))
    }
}

/// JSON_OBJECT：括号配平切出完整 JSON 对象/数组（对齐 Netty JsonObjectDecoder）。
/// 字符串内的括号与转义引号不参与配平；起始 `{`/`[` 前只允许空白，其余字节 → Err。
/// maxLength 必填：半开 JSON 缓冲超限 → Err（连接层断开），防无限缓冲。
pub struct JsonObjectCodec {
    max_length: usize,
    started: bool,
    start: usize,
    scanned: usize,
    depth: u32,
    in_string: bool,
    escape: bool,
}

impl JsonObjectCodec {
    fn new(max_length: usize) -> Self {
        Self {
            max_length,
            started: false,
            start: 0,
            scanned: 0,
            depth: 0,
            in_string: false,
            escape: false,
        }
    }

    fn reset(&mut self) {
        *self = Self::new(self.max_length);
    }
}

impl FrameCodec for JsonObjectCodec {
    fn decode(&mut self, buf: &mut Vec<u8>) -> Result<Option<Vec<u8>>> {
        if !self.started {
            while self.scanned < buf.len() && buf[self.scanned].is_ascii_whitespace() {
                self.scanned += 1;
            }
            if self.scanned >= buf.len() {
                if buf.len() > self.max_length {
                    return Err(cfg(format!(
                        "JSON_OBJECT: no JSON start byte within maxLength {}",
                        self.max_length
                    )));
                }
                return Ok(None);
            }
            match buf[self.scanned] {
                b'{' | b'[' => {
                    self.started = true;
                    self.start = self.scanned;
                }
                c => {
                    return Err(cfg(format!(
                        "JSON_OBJECT: invalid start byte {c:#04x} at offset {}",
                        self.scanned
                    )));
                }
            }
        }
        while self.scanned < buf.len() {
            let c = buf[self.scanned];
            self.scanned += 1;
            if self.scanned - self.start > self.max_length {
                return Err(cfg(format!(
                    "JSON_OBJECT: frame length {} exceeds maxLength {}",
                    self.scanned - self.start,
                    self.max_length
                )));
            }
            if self.in_string {
                if self.escape {
                    self.escape = false;
                } else if c == b'\\' {
                    self.escape = true;
                } else if c == b'"' {
                    self.in_string = false;
                }
                continue;
            }
            match c {
                b'"' => self.in_string = true,
                b'{' | b'[' => self.depth += 1,
                b'}' | b']' => {
                    self.depth -= 1;
                    if self.depth == 0 {
                        buf.drain(..self.start);
                        let frame: Vec<u8> = buf.drain(..self.scanned - self.start).collect();
                        self.reset();
                        return Ok(Some(frame));
                    }
                }
                _ => {}
            }
        }
        Ok(None)
    }
}

/// FIXED_LENGTH：定长切帧（对齐 Netty FixedLengthFrameDecoder）。
pub struct FixedLengthCodec {
    frame_length: usize,
}

impl FrameCodec for FixedLengthCodec {
    fn decode(&mut self, buf: &mut Vec<u8>) -> Result<Option<Vec<u8>>> {
        if buf.len() < self.frame_length {
            return Ok(None);
        }
        Ok(Some(buf.drain(..self.frame_length).collect()))
    }
}

/// LINE：按 `\n` 切行，剥离结尾 `\n` 与 `\r\n`（对齐 Netty LineBasedFrameDecoder）。
pub struct LineCodec {
    max_length: usize,
}

impl FrameCodec for LineCodec {
    fn decode(&mut self, buf: &mut Vec<u8>) -> Result<Option<Vec<u8>>> {
        match buf.iter().position(|b| *b == b'\n') {
            Some(pos) if pos > self.max_length => Err(cfg(format!(
                "LINE: line length {pos} exceeds maxLength {}",
                self.max_length
            ))),
            Some(pos) => {
                let mut frame: Vec<u8> = buf.drain(..=pos).collect();
                frame.pop(); // '\n'
                if frame.last() == Some(&b'\r') {
                    frame.pop();
                }
                Ok(Some(frame))
            }
            None => {
                if buf.len() > self.max_length {
                    return Err(cfg(format!(
                        "LINE: no line terminator within maxLength {}",
                        self.max_length
                    )));
                }
                Ok(None)
            }
        }
    }
}

/// XML：根标签配平切帧（对齐 Netty XmlFrameDecoder 的核心语义）。
///
/// ponytail: 朴素扫描——处理注释/CDATA/PI 跳过与同名根标签嵌套配平，但不解析属性值
/// 中的 `<`、DOCTYPE 内部子集、命名空间前缀差异；每次对缓冲全量重扫（O(n²) 最坏，
/// 帧长 KB 级可接受）。升级路径：quick-xml 事件解析器。
pub struct XmlCodec {
    max_length: usize,
}

impl FrameCodec for XmlCodec {
    fn decode(&mut self, buf: &mut Vec<u8>) -> Result<Option<Vec<u8>>> {
        let Some(start) = buf.iter().position(|b| *b == b'<') else {
            return Ok(None);
        };
        match scan_xml(&buf[start..], self.max_length)? {
            Some(end) => {
                buf.drain(..start);
                Ok(Some(buf.drain(..end).collect()))
            }
            None => Ok(None),
        }
    }
}

fn starts_with(hay: &[u8], at: usize, prefix: &[u8]) -> bool {
    hay[at..].starts_with(prefix)
}

/// 从 at 起查找 needle，返回其后的偏移（未找到 None）。
fn find_after(hay: &[u8], from: usize, needle: &[u8]) -> Option<usize> {
    if from >= hay.len() {
        return None;
    }
    hay[from..]
        .windows(needle.len())
        .position(|w| w == needle)
        .map(|p| from + p + needle.len())
}

/// 扫描一段以 `<` 开头的字节，返回完整 XML 帧的结束偏移（不含之后字节）；数据不足返回 None。
fn scan_xml(s: &[u8], max_length: usize) -> Result<Option<usize>> {
    let too_long = |n: usize| {
        cfg(format!(
            "XML: frame length {n} exceeds maxLength {max_length}"
        ))
    };
    // 跳过根元素前的 PI / 注释 / DOCTYPE
    let mut i = 0;
    let root_start = loop {
        if i >= s.len() {
            return Ok(None);
        }
        if s[i] != b'<' {
            i += 1;
            continue;
        }
        if starts_with(s, i, b"<!--") {
            match find_after(s, i + 4, b"-->") {
                Some(e) => i = e,
                None => return Ok(None),
            }
        } else if starts_with(s, i, b"<?") {
            match find_after(s, i + 2, b"?>") {
                Some(e) => i = e,
                None => return Ok(None),
            }
        } else if starts_with(s, i, b"<!") {
            match s[i..].iter().position(|b| *b == b'>') {
                Some(p) => i += p + 1,
                None => return Ok(None),
            }
        } else {
            break i;
        }
    };
    // 解析根标签名
    let name_start = root_start + 1;
    if name_start >= s.len() {
        return Ok(None);
    }
    if s[name_start] == b'/' {
        return Err(cfg("XML: close tag without open tag".to_string()));
    }
    let mut j = name_start;
    while j < s.len() && !matches!(s[j], b' ' | b'\t' | b'\r' | b'\n' | b'>' | b'/') {
        j += 1;
    }
    if j >= s.len() {
        return Ok(None);
    }
    let root = &s[name_start..j];
    if root.is_empty() {
        return Err(cfg("XML: empty root tag name".to_string()));
    }
    let tag_end = match s[j..].iter().position(|b| *b == b'>') {
        Some(p) => j + p + 1,
        None => return Ok(None),
    };
    if s[tag_end - 2] == b'/' {
        // 自闭合根元素 <root/>
        if tag_end > max_length {
            return Err(too_long(tag_end));
        }
        return Ok(Some(tag_end));
    }
    // 扫描正文，只对与根同名的标签配平
    let mut depth = 1usize;
    let mut k = tag_end;
    while k < s.len() {
        if s[k] != b'<' {
            k += 1;
            continue;
        }
        if starts_with(s, k, b"<!--") {
            match find_after(s, k + 4, b"-->") {
                Some(e) => k = e,
                None => return Ok(None),
            }
            continue;
        }
        if starts_with(s, k, b"<![CDATA[") {
            match find_after(s, k + 9, b"]]>") {
                Some(e) => k = e,
                None => return Ok(None),
            }
            continue;
        }
        if starts_with(s, k, b"<?") {
            match find_after(s, k + 2, b"?>") {
                Some(e) => k = e,
                None => return Ok(None),
            }
            continue;
        }
        let mut m = k + 1;
        let is_close = m < s.len() && s[m] == b'/';
        if is_close {
            m += 1;
        }
        let ns = m;
        while m < s.len() && !matches!(s[m], b' ' | b'\t' | b'\r' | b'\n' | b'>' | b'/') {
            m += 1;
        }
        if m >= s.len() {
            return Ok(None);
        }
        let name = &s[ns..m];
        let te = match s[m..].iter().position(|b| *b == b'>') {
            Some(p) => m + p + 1,
            None => return Ok(None),
        };
        if name == root {
            if is_close {
                depth -= 1;
                if depth == 0 {
                    if te > max_length {
                        return Err(too_long(te));
                    }
                    return Ok(Some(te));
                }
            } else if s[te - 2] != b'/' {
                depth += 1;
            }
        }
        k = te;
    }
    if s.len() > max_length {
        return Err(too_long(s.len()));
    }
    Ok(None)
}

/// ZLIB：zlib 格式（含 2 字节头）压缩流切帧，一帧 = 一个完整 zlib 流的解压结果
/// （对齐 Java EnnZlibDecoder 包装 JdkZlibDecoder 的意图：每条报文独立解压）。
/// 解压输出超 `max_output_len` → Err（连接层断开），防解压炸弹耗尽内存。
// ponytail: 缺省上限固定 1MB；升级路径：按设备侧协商压缩比或改流式限速
const DEFAULT_ZLIB_MAX_OUTPUT: usize = 1024 * 1024;

pub struct ZlibCodec {
    decomp: flate2::Decompress,
    out: Vec<u8>,
    fed: usize,
    max_output_len: usize,
}

impl ZlibCodec {
    fn new(max_output_len: usize) -> Self {
        Self {
            decomp: flate2::Decompress::new(true),
            out: Vec::new(),
            fed: 0,
            max_output_len,
        }
    }
}

impl FrameCodec for ZlibCodec {
    fn decode(&mut self, buf: &mut Vec<u8>) -> Result<Option<Vec<u8>>> {
        loop {
            if self.fed >= buf.len() {
                return Ok(None);
            }
            let in_before = self.decomp.total_in();
            let out_before = self.decomp.total_out();
            // 不用 decompress_vec：flate2 1.1.10 在输出 Vec 无 spare capacity 时
            // 吞输入零产出（实测 total_in 前进 total_out=0），改为固定块 decompress
            let mut chunk = [0u8; 8192];
            let status = self
                .decomp
                .decompress(&buf[self.fed..], &mut chunk, flate2::FlushDecompress::None)
                .map_err(|e| CommonError::Internal(format!("zlib inflate: {e}")))?;
            self.fed += (self.decomp.total_in() - in_before) as usize;
            let produced = (self.decomp.total_out() - out_before) as usize;
            self.out.extend_from_slice(&chunk[..produced]);
            if self.out.len() > self.max_output_len {
                return Err(cfg(format!(
                    "ZLIB: decompressed {} bytes exceeds maxOutputLen {} (possible zip bomb), closing connection",
                    self.out.len(),
                    self.max_output_len
                )));
            }
            match status {
                flate2::Status::StreamEnd => {
                    let frame = std::mem::take(&mut self.out);
                    buf.drain(..self.fed);
                    self.decomp = flate2::Decompress::new(true);
                    self.fed = 0;
                    return Ok(Some(frame));
                }
                // 无进展（未消费输入且无产出）→ 等待更多字节，防死循环
                _ if self.decomp.total_in() == in_before && produced == 0 => return Ok(None),
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// 按块喂入并收集所有切出的帧。
    fn drain(codec: &mut dyn FrameCodec, chunks: &[&[u8]]) -> Vec<Vec<u8>> {
        let mut buf = Vec::new();
        let mut out = Vec::new();
        for c in chunks {
            buf.extend_from_slice(c);
            while let Some(f) = codec.decode(&mut buf).unwrap() {
                out.push(f);
            }
        }
        out
    }

    fn build(t: &str, params: JsonValue) -> Box<dyn FrameCodec> {
        build_codec(Some(t), Some(&params)).unwrap()
    }

    fn zlib_compress(data: &[u8]) -> Vec<u8> {
        use std::io::Write;
        let mut e = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        e.write_all(data).unwrap();
        e.finish().unwrap()
    }

    // ---- DIRECT ----

    #[test]
    fn direct_emits_whole_buffer_as_one_frame() {
        let mut c = build("DIRECT", json!({}));
        assert_eq!(drain(c.as_mut(), &[b"abc", b"def"]), vec![b"abc", b"def"]);
    }

    // ---- DELIMITER ----

    #[test]
    fn delimiter_splits_on_hex_delimiter_and_strips_it() {
        // 分隔符 "##" = 0x2323
        let mut c = build("DELIMITER", json!({"maxLength": 100, "delimiter": "2323"}));
        assert_eq!(drain(c.as_mut(), &[b"abc##def##"]), vec![b"abc", b"def"]);
    }

    #[test]
    fn delimiter_waits_for_partial_frame() {
        let mut c = build("DELIMITER", json!({"maxLength": 100, "delimiter": "2323"}));
        assert_eq!(drain(c.as_mut(), &[b"ab", b"c##"]), vec![b"abc"]);
    }

    #[test]
    fn delimiter_rejects_overlong_frame() {
        let mut c = build("DELIMITER", json!({"maxLength": 3, "delimiter": "2323"}));
        let mut buf = b"abcdef##".to_vec();
        let err = c.decode(&mut buf).unwrap_err();
        assert!(matches!(err, CommonError::Config(_)), "got {err:?}");
    }

    // ---- LENGTH_FIELD ----

    #[test]
    fn length_field_big_endian_with_strip() {
        // 2 字节大端长度在 offset 0，strip 2 → 帧体 "abc"
        let mut c = build(
            "LENGTH_FIELD",
            json!({"maxLength": 100, "lengthFieldOffset": 0, "lengthFieldLength": 2,
                   "lengthAdjustment": 0, "initialBytesToStrip": 2}),
        );
        assert_eq!(
            drain(c.as_mut(), &[&[0, 3, b'a', b'b', b'c']]),
            vec![b"abc"]
        );
    }

    #[test]
    fn length_field_little_endian_and_adjustment() {
        // 小端 2 字节长度=3（[3,0]），adjustment=1 → 帧体 4 字节，不 strip
        let mut c = build(
            "LENGTH_FIELD",
            json!({"maxLength": 100, "lengthFieldLength": 2, "lengthAdjustment": 1,
                   "byteOrder": "LITTLE_ENDIAN"}),
        );
        assert_eq!(
            drain(c.as_mut(), &[&[3, 0, b'a', b'b', b'c', b'd']]),
            vec![&[3u8, 0, b'a', b'b', b'c', b'd']]
        );
    }

    #[test]
    fn length_field_waits_for_partial_frame_then_emits_two() {
        let mut c = build(
            "LENGTH_FIELD",
            json!({"maxLength": 100, "lengthFieldLength": 1, "initialBytesToStrip": 1}),
        );
        assert_eq!(
            drain(c.as_mut(), &[&[2, b'a'], &[b'b', 1, b'c']]),
            vec![&b"ab"[..], &b"c"[..]]
        );
    }

    #[test]
    fn length_field_rejects_overlong() {
        let mut c = build(
            "LENGTH_FIELD",
            json!({"maxLength": 4, "lengthFieldLength": 1}),
        );
        let mut buf = vec![200u8, 1, 2];
        assert!(c.decode(&mut buf).is_err());
    }

    #[test]
    fn length_field_length_overflow_returns_error_not_panic() {
        // 8 字节长度字段 = i64::MAX，adjustment=1 → checked_add 溢出 → Err（非 panic/wrap）
        let mut c = build(
            "LENGTH_FIELD",
            json!({"maxLength": 100, "lengthFieldLength": 8,
                   "lengthAdjustment": 1}),
        );
        let mut buf = vec![0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];
        let err = c.decode(&mut buf).unwrap_err();
        assert!(err.to_string().contains("overflow"), "got {err}");
    }

    // ---- JSON_OBJECT ----

    #[test]
    fn json_object_splits_concatenated_objects() {
        let mut c = build("JSON_OBJECT", json!({"maxLength": 4096}));
        assert_eq!(
            drain(c.as_mut(), &[br#"{"a":1}{"b":2}"#]),
            vec![br#"{"a":1}"#, br#"{"b":2}"#]
        );
    }

    #[test]
    fn json_object_balances_nested_and_string_braces() {
        let mut c = build("JSON_OBJECT", json!({"maxLength": 4096}));
        let input = br#"{"a":{"b":"}{"},"c":[1,2]}"#;
        assert_eq!(drain(c.as_mut(), &[input]), vec![input]);
    }

    #[test]
    fn json_object_handles_split_across_chunks_and_arrays() {
        let mut c = build("JSON_OBJECT", json!({"maxLength": 4096}));
        assert_eq!(drain(c.as_mut(), &[b"[1,", b"2]"]), vec![b"[1,2]"]);
    }

    #[test]
    fn json_object_rejects_garbage_start() {
        let mut c = build("JSON_OBJECT", json!({"maxLength": 4096}));
        let mut buf = b"oops{}".to_vec();
        assert!(c.decode(&mut buf).is_err());
    }

    #[test]
    fn json_object_requires_max_length() {
        // maxLength 必填：缺失 → 配置解析报错
        let err = build_codec(Some("JSON_OBJECT"), Some(&json!({})))
            .err()
            .unwrap();
        assert!(err.to_string().contains("maxLength"), "got {err}");
        assert!(build_codec(Some("JSON_OBJECT"), None).is_err());
    }

    #[test]
    fn json_object_half_open_over_max_length_errors() {
        // 半开 JSON 缓冲超 maxLength → 帧错误（连接层断开），错误含实际大小与上限
        let mut c = build("JSON_OBJECT", json!({"maxLength": 10}));
        let mut buf = br#"{"a":"0123456789abcdef"#.to_vec();
        let err = c.decode(&mut buf).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("maxLength 10"), "got {msg}");
        // 起始字节前的空白缓冲同样受限
        let mut c2 = build("JSON_OBJECT", json!({"maxLength": 4}));
        let mut buf2 = b"          ".to_vec();
        assert!(c2.decode(&mut buf2).is_err());
    }

    // ---- FIXED_LENGTH ----

    #[test]
    fn fixed_length_splits_evenly_and_waits() {
        let mut c = build("FIXED_LENGTH", json!({"frameLength": 2}));
        assert_eq!(drain(c.as_mut(), &[b"abc", b"de"]), vec![b"ab", b"cd"]);
        // 剩余 "e" 不足一帧
        let mut buf = b"e".to_vec();
        assert!(c.decode(&mut buf).unwrap().is_none());
    }

    // ---- XML ----

    #[test]
    fn xml_balances_root_tag_with_nesting() {
        let mut c = build("XML", json!({"maxLength": 4096}));
        assert_eq!(
            drain(c.as_mut(), &[b"<a><a></a></a><b/>"]),
            vec![&b"<a><a></a></a>"[..], &b"<b/>"[..]]
        );
    }

    #[test]
    fn xml_ignores_fake_close_in_comment_and_cdata() {
        let mut c = build("XML", json!({"maxLength": 4096}));
        assert_eq!(
            drain(c.as_mut(), &[b"<r><!-- </r> --><![CDATA[</r>]]></r>"]),
            vec![b"<r><!-- </r> --><![CDATA[</r>]]></r>"]
        );
    }

    #[test]
    fn xml_waits_for_partial_frame() {
        let mut c = build("XML", json!({"maxLength": 4096}));
        assert_eq!(
            drain(c.as_mut(), &[b"<r><c", b"/></r>"]),
            vec![b"<r><c/></r>"]
        );
    }

    #[test]
    fn xml_rejects_overlong() {
        let mut c = build("XML", json!({"maxLength": 8}));
        let mut buf = b"<root>0123456789</root>".to_vec();
        assert!(c.decode(&mut buf).is_err());
    }

    // ---- LINE ----

    #[test]
    fn line_splits_lf_and_crlf() {
        let mut c = build("LINE", json!({"maxLength": 100}));
        assert_eq!(drain(c.as_mut(), &[b"a\r\nb\nc"]), vec![b"a", b"b"]);
    }

    #[test]
    fn line_rejects_overlong() {
        let mut c = build("LINE", json!({"maxLength": 3}));
        let mut buf = b"abcdef\n".to_vec();
        assert!(c.decode(&mut buf).is_err());
    }

    // ---- ZLIB ----

    #[test]
    fn zlib_roundtrips_one_stream() {
        let mut c = build("ZLIB", json!({}));
        let payload = b"hello zlib frame";
        let compressed = zlib_compress(payload);
        assert_eq!(drain(c.as_mut(), &[&compressed]), vec![payload]);
    }

    #[test]
    fn zlib_handles_split_and_concatenated_streams() {
        let mut c = build("ZLIB", json!({}));
        let p1 = zlib_compress(b"one");
        let p2 = zlib_compress(b"two");
        let mut joined = p1.clone();
        joined.extend_from_slice(&p2);
        // 前半段不足一个流 → 无帧；补齐后两帧依次切出
        let split_at = p1.len() - 2;
        assert_eq!(
            drain(c.as_mut(), &[&joined[..split_at], &joined[split_at..]]),
            vec![b"one", b"two"]
        );
    }

    #[test]
    fn zlib_rejects_invalid_stream() {
        let mut c = build("ZLIB", json!({}));
        let mut buf = b"not a zlib stream at all".to_vec();
        assert!(c.decode(&mut buf).is_err());
    }

    #[test]
    fn zlib_output_over_limit_errors_with_sizes() {
        // 解压炸弹：解压输出超 maxOutputLen → 帧错误（连接层断开），错误含上限与实际大小
        let mut c = build("ZLIB", json!({"maxOutputLen": 64}));
        let compressed = zlib_compress(&vec![b'A'; 4096]);
        let mut buf = compressed;
        let err = c.decode(&mut buf).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("maxOutputLen 64"), "got {msg}");
        assert!(msg.contains("exceeds"), "got {msg}");
    }

    #[test]
    fn zlib_rejects_non_positive_max_output_len() {
        assert!(build_codec(Some("ZLIB"), Some(&json!({"maxOutputLen": 0}))).is_err());
        assert!(build_codec(Some("ZLIB"), Some(&json!({"maxOutputLen": -1}))).is_err());
    }

    // ---- 工厂 ----

    #[test]
    fn factory_is_case_insensitive_and_rejects_unknown() {
        assert!(build_codec(Some("line"), Some(&json!({"maxLength": 10}))).is_ok());
        assert!(build_codec(Some("NOPE"), None).is_err());
        assert!(build_codec(None, None).is_err());
    }

    #[test]
    fn factory_rejects_missing_or_bad_params() {
        // DELIMITER 缺 delimiter / maxLength
        assert!(build_codec(Some("DELIMITER"), Some(&json!({}))).is_err());
        assert!(build_codec(Some("DELIMITER"), Some(&json!({"delimiter": "2323"}))).is_err());
        // 非法 hex
        assert!(
            build_codec(
                Some("DELIMITER"),
                Some(&json!({"delimiter": "zz", "maxLength": 10}))
            )
            .is_err()
        );
        // LENGTH_FIELD 缺 lengthFieldLength / 非法值
        assert!(build_codec(Some("LENGTH_FIELD"), Some(&json!({"maxLength": 10}))).is_err());
        assert!(
            build_codec(
                Some("LENGTH_FIELD"),
                Some(&json!({"maxLength": 10, "lengthFieldLength": 9}))
            )
            .is_err()
        );
        // FIXED_LENGTH 缺 frameLength
        assert!(build_codec(Some("FIXED_LENGTH"), Some(&json!({}))).is_err());
        // JSON_OBJECT 缺 maxLength
        assert!(build_codec(Some("JSON_OBJECT"), Some(&json!({}))).is_err());
    }

    #[test]
    fn int_params_accept_numeric_strings() {
        // 对齐 fastjson getInteger 对字符串的强转
        let mut c = build("FIXED_LENGTH", json!({"frameLength": "2"}));
        assert_eq!(drain(c.as_mut(), &[b"abcd"]), vec![b"ab", b"cd"]);
    }
}
