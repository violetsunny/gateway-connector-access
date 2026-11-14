//! URL 模板替换，对齐 Java `StringUtil.replaceUrl`（DownRequestHandler.java:140 调用）。
//!
//! 占位符语法 `${name}`（name 限 `[\w]+`，同 Java 正则 `(\$\{)([\w]+)(\})`），
//! 值来自下行 OperationRequest 序列化后的 JSON 对象顶层字段，替换值经
//! Java `URLEncoder.encode(v, "UTF-8")` 等价编码（空格→`+`，非 `[A-Za-z0-9.*-_]`→`%XX`）。
//!
//! 与 Java 的刻意差异：Java 对 map 中不存在的占位符**静默保留原文**，
//! Rust 版报错（失败可见——带 `${...}` 原文的 URL 发出去必然 404，不如就地失败）。

use connector_common::{CommonError, Result};
use serde_json::Value as JsonValue;

/// Java `URLEncoder.encode(s, "UTF-8")` 等价：字母数字与 `.*-_` 原样，
/// 空格 → `+`，其余字节 → `%XX`（大写 hex）。
pub(crate) fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'.' | b'-' | b'*' | b'_' => {
                out.push(*b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// JSON 值 → 替换字符串，对齐 Java `String.valueOf(v)`：
/// 字符串原样，数字/布尔用字面量，对象/数组用紧凑 JSON 文本；null 视为缺失。
fn value_to_string(v: &JsonValue) -> Option<String> {
    match v {
        JsonValue::Null => None,
        JsonValue::String(s) => Some(s.clone()),
        JsonValue::Number(_) | JsonValue::Bool(_) => Some(v.to_string()),
        JsonValue::Object(_) | JsonValue::Array(_) => Some(v.to_string()),
    }
}

fn is_word_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// 替换 `template` 中的 `${name}` 占位符。
///
/// - `vars` 必须是 JSON 对象（下行 encode 后的 OperationRequest）；
/// - 占位符在 `vars` 顶层缺失或值为 null → Err（携带占位符名）；
/// - 非法占位符形态（`${}`、`${a-b}`、未闭合 `${`）按 Java 正则不匹配语义原样保留。
pub(crate) fn replace_url(template: &str, vars: &JsonValue) -> Result<String> {
    let obj = vars.as_object().ok_or_else(|| {
        CommonError::Internal(format!("URL 模板替换需要 JSON 对象作为变量源，实际 {vars}"))
    })?;
    let bytes = template.as_bytes();
    let mut out = String::with_capacity(template.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'{' {
            // 找闭合 '}'；内容必须非空且全为 word 字符（对齐 Java [\w]+）
            if let Some(end) = bytes[i + 2..].iter().position(|&b| b == b'}') {
                let name = &template[i + 2..i + 2 + end];
                if !name.is_empty() && name.bytes().all(is_word_char) {
                    let v = obj.get(name).and_then(value_to_string).ok_or_else(|| {
                        CommonError::Internal(format!(
                            "URL 模板占位符 '${{{name}}}' 在下行消息中无值（消息顶层字段: {}）",
                            obj.keys()
                                .map(String::as_str)
                                .collect::<Vec<_>>()
                                .join(", ")
                        ))
                    })?;
                    out.push_str(&url_encode(&v));
                    i += end + 3; // "${" + name + "}"
                    continue;
                }
            }
        }
        // 非占位符：按 UTF-8 边界推进一个字符
        let ch_len = utf8_char_len(bytes[i]);
        out.push_str(&template[i..i + ch_len]);
        i += ch_len;
    }
    Ok(out)
}

fn utf8_char_len(b: u8) -> usize {
    match b {
        0x00..=0x7F => 1,
        0xC0..=0xDF => 2,
        0xE0..=0xEF => 3,
        _ => 4,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn encodes_like_java_urlencoder() {
        assert_eq!(url_encode("abcXYZ019"), "abcXYZ019");
        assert_eq!(url_encode(".*-_"), ".*-_");
        assert_eq!(url_encode("a b"), "a+b");
        assert_eq!(url_encode("中文"), "%E4%B8%AD%E6%96%87");
        assert_eq!(url_encode("/?&=%"), "%2F%3F%26%3D%25");
    }

    /// 字符串/数字/布尔/对象值替换 + URL 编码（对齐 DownRequestHandler.java:139-140：
    /// 变量源是整个 OperationRequest JSON）。
    #[test]
    fn replaces_placeholders_from_request_json() {
        let vars = json!({
            "deviceId": "dev 1",
            "function": "open",
            "timeStamp": 1700000000000i64,
            "changeRpt": true,
            "param": {"a": 1},
        });
        assert_eq!(
            replace_url("http://x/api?dev=${deviceId}&fn=${function}", &vars).unwrap(),
            "http://x/api?dev=dev+1&fn=open"
        );
        assert_eq!(
            replace_url("http://x/api?ts=${timeStamp}&c=${changeRpt}", &vars).unwrap(),
            "http://x/api?ts=1700000000000&c=true"
        );
        // 对象值 → 紧凑 JSON 文本再编码（Java String.valueOf(JSONObject) 等价）
        assert_eq!(
            replace_url("http://x/api?p=${param}", &vars).unwrap(),
            "http://x/api?p=%7B%22a%22%3A1%7D"
        );
    }

    /// 缺失占位符 / null 值 → Err（失败路径，Java 静默保留原文的修正）。
    #[test]
    fn missing_placeholder_fails() {
        let vars = json!({"deviceId": "d1", "nothing": null});
        let err = replace_url("http://x/api?sn=${sn}", &vars).unwrap_err();
        assert!(err.to_string().contains("${sn}"), "{err}");
        let err = replace_url("http://x/api?n=${nothing}", &vars).unwrap_err();
        assert!(err.to_string().contains("${nothing}"), "{err}");
    }

    /// 无占位符原样返回；非法占位符形态按 Java 正则不匹配语义保留原文。
    #[test]
    fn keeps_literal_when_no_valid_placeholder() {
        let vars = json!({"a": "1"});
        assert_eq!(
            replace_url("http://x/api?plain", &vars).unwrap(),
            "http://x/api?plain"
        );
        // ${} / ${a-b} / 未闭合 ${ —— Java [\w]+ 不匹配 → 原样
        assert_eq!(
            replace_url("http://x/${}?p=${a-b}&q=${unclosed", &vars).unwrap(),
            "http://x/${}?p=${a-b}&q=${unclosed"
        );
        // 中文占位符名不是 \w → 原样
        assert_eq!(
            replace_url("http://x/${中文}", &vars).unwrap(),
            "http://x/${中文}"
        );
    }

    /// 变量源非 JSON 对象 → Err。
    #[test]
    fn non_object_vars_fails() {
        let err = replace_url("http://x/${a}", &json!(["a"])).unwrap_err();
        assert!(err.to_string().contains("JSON 对象"), "{err}");
    }
}
