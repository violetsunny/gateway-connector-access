//! topic → 协议选择（对齐 Java `mqtt/extend/ProtocolChooser`）。
//!
//! Java 侧从 topic 第 4/5 段取 pKey/sn，再用 `DeviceContext`（Redis）查 sn→协议名。Redis 依赖
//! 在本项目 Out of Scope，Rust 版改为配置驱动：每个 broker 的 `topics` 过滤规则绑定该 broker
//! 的协议，按 MQTT 3.1.1 通配语义匹配 topic（精确 / `+` 单层 / `#` 前缀多层）；多条规则同时
//! 命中时按配置顺序探测 payload 可解码者（spec 决策）。
//!
//! 无命中 → [`Choice::NoTopicMatch`]，调用方 warn 丢弃（失败可见，不静默）。

use std::sync::Arc;

use connector_common::{CommonError, Protocol};
use serde_json::Value as JsonValue;

/// 一条 topic 过滤规则：filter 命中即用 protocol + params 解码。
pub struct Rule {
    /// MQTT topic 过滤器（订阅时用的同一个字符串，支持 `+` / `#`）。
    pub filter: String,
    pub protocol: Arc<dyn Protocol>,
    pub params: JsonValue,
}

/// 选择结果。三态显式建模，调用方按态记不同级别日志（无匹配 warn、解码失败 error）。
#[derive(Debug)]
pub enum Choice {
    /// 命中协议并解码成功。
    Decoded {
        protocol: String,
        messages: Vec<JsonValue>,
    },
    /// topic 未命中任何配置规则。
    NoTopicMatch,
    /// 命中规则但解码失败（多规则时全部失败，保留最后一个错误）。
    DecodeFailed {
        protocols: Vec<String>,
        error: CommonError,
    },
}

/// 按配置顺序保存规则的协议选择器。
pub struct ProtocolChooser {
    rules: Vec<Rule>,
}

impl ProtocolChooser {
    pub fn new(rules: Vec<Rule>) -> Self {
        Self { rules }
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// 全部过滤规则（status 与测试用）。
    pub fn filters(&self) -> Vec<&str> {
        self.rules.iter().map(|r| r.filter.as_str()).collect()
    }

    /// topic + payload → 协议 + 解码结果。
    ///
    /// ponytail: 多规则命中时用 `decode_multi` 试解码来选协议，选中者的解码结果直接返回
    /// （不重复解码）；代价是失败候选会白跑一次 Lua。升级路径：给 Protocol 加轻量
    /// `can_decode(payload)` 探针，或按 sn/pKey 建索引（需接 Redis 设备目录）。
    pub fn choose(&self, topic: &str, payload: &[u8]) -> Choice {
        let candidates: Vec<&Rule> = self
            .rules
            .iter()
            .filter(|r| topic_matches(&r.filter, topic))
            .collect();
        if candidates.is_empty() {
            return Choice::NoTopicMatch;
        }

        let mut tried = Vec::with_capacity(candidates.len());
        let mut last_error = None;
        for rule in candidates {
            match rule.protocol.decode_multi(payload, &rule.params) {
                Ok(messages) => {
                    return Choice::Decoded {
                        protocol: rule.protocol.name().to_string(),
                        messages,
                    };
                }
                Err(error) => {
                    tried.push(rule.protocol.name().to_string());
                    last_error = Some(error);
                }
            }
        }

        match last_error {
            Some(error) => Choice::DecodeFailed {
                protocols: tried,
                error,
            },
            // 不可达：candidates 非空时循环必然产出 Decoded 或记录 error
            None => Choice::DecodeFailed {
                protocols: tried,
                error: CommonError::Internal(
                    "protocol chooser: 候选协议非空但既无解码结果也无错误".into(),
                ),
            },
        }
    }
}

/// MQTT 3.1.1 topic 过滤器匹配：段相等、`+` 匹配单层、`#` 匹配剩余任意层（含零层）。
///
/// 例：`enn/+/up` 命中 `enn/dev1/up`，不命中 `enn/dev1/down`、`enn/dev1/a/up`；
/// `enn/#` 命中 `enn`、`enn/a/b`。
pub fn topic_matches(filter: &str, topic: &str) -> bool {
    let mut filter_segments = filter.split('/');
    let mut topic_segments = topic.split('/');
    loop {
        match (filter_segments.next(), topic_segments.next()) {
            (None, None) => return true,
            // `#` 必须是过滤器最后一段，命中后匹配剩余全部层（含零层）
            (Some("#"), _) => return true,
            (Some(f), Some(t)) => {
                if f != "+" && f != t {
                    return false;
                }
            }
            // 层数不一致（过滤器更长或 topic 更长）
            _ => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{JSON_ECHO, protocol};
    use serde_json::json;

    fn chooser(rules: Vec<(&str, &str, &str)>) -> ProtocolChooser {
        ProtocolChooser::new(
            rules
                .into_iter()
                .map(|(filter, name, body)| Rule {
                    filter: filter.to_string(),
                    protocol: protocol(name, body, "return \"\""),
                    params: json!({}),
                })
                .collect(),
        )
    }

    // ---- topic 过滤匹配 ----

    #[test]
    fn topic_matches_exact_and_wildcards() {
        assert!(topic_matches("enn/dev1/up", "enn/dev1/up"));
        assert!(topic_matches("enn/+/up", "enn/dev1/up"));
        assert!(topic_matches("enn/#", "enn/dev1/up"));
        assert!(topic_matches("enn/#", "enn"));
        assert!(topic_matches("#", "any/topic/at/all"));
        assert!(topic_matches("+/+", "a/b"));
    }

    #[test]
    fn topic_rejects_non_matching() {
        assert!(!topic_matches("enn/dev1/up", "enn/dev1/down"));
        assert!(!topic_matches("enn/+/up", "enn/a/b/up"));
        assert!(!topic_matches("enn/+/up", "enn/a"));
        assert!(!topic_matches("enn/a", "enn/a/b"));
        assert!(!topic_matches("enn/#", "other/x"));
        assert!(!topic_matches("+/+", "a/b/c"));
    }

    // ---- 协议选择 ----

    #[test]
    fn chooses_protocol_by_matching_topic_and_decodes() {
        let c = chooser(vec![
            ("enn/+/up", "up_proto", JSON_ECHO),
            ("enn/+/event", "event_proto", JSON_ECHO),
        ]);
        let payload = br#"{"deviceId":"d1","messageType":"DEVICE_EVENT_REQ"}"#;
        match c.choose("enn/dev1/event", payload) {
            Choice::Decoded { protocol, messages } => {
                assert_eq!(protocol, "event_proto");
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0]["deviceId"], "d1");
                assert_eq!(messages[0]["messageType"], "DEVICE_EVENT_REQ");
            }
            other => panic!("expected Decoded, got {other:?}"),
        }
    }

    /// 无规则命中 topic → NoTopicMatch（调用方 warn 丢弃）。
    #[test]
    fn reports_no_topic_match() {
        let c = chooser(vec![("enn/+/up", "up_proto", JSON_ECHO)]);
        assert!(matches!(
            c.choose("other/dev1/up", b"{}"),
            Choice::NoTopicMatch
        ));
        // 空规则表同样无匹配
        assert!(matches!(
            ProtocolChooser::new(vec![]).choose("enn/a/up", b"{}"),
            Choice::NoTopicMatch
        ));
    }

    /// 命中规则但脚本解码失败 → DecodeFailed，携带协议名与错误。
    #[test]
    fn reports_decode_failure() {
        let c = chooser(vec![("enn/+/up", "bad_proto", "error(\"boom\")")]);
        match c.choose("enn/dev1/up", b"whatever") {
            Choice::DecodeFailed { protocols, error } => {
                assert_eq!(protocols, vec!["bad_proto".to_string()]);
                assert!(error.to_string().contains("boom"), "{error}");
            }
            other => panic!("expected DecodeFailed, got {other:?}"),
        }
    }

    /// 多规则命中同一 topic：按配置顺序探测，跳过解码失败者，取第一个可解码的协议。
    #[test]
    fn probes_candidates_in_config_order() {
        let c = chooser(vec![
            ("enn/#", "first_bad", "error(\"cannot handle\")"),
            ("enn/#", "second_ok", JSON_ECHO),
            ("enn/#", "third_ok", JSON_ECHO),
        ]);
        match c.choose("enn/dev1/up", br#"{"sn":"s1"}"#) {
            Choice::Decoded { protocol, messages } => {
                assert_eq!(protocol, "second_ok");
                assert_eq!(messages[0]["sn"], "s1");
            }
            other => panic!("expected Decoded, got {other:?}"),
        }
    }

    /// 多规则全部解码失败 → DecodeFailed 记录全部尝试过的协议名与最后一个错误。
    #[test]
    fn reports_failure_when_all_candidates_fail() {
        let c = chooser(vec![
            ("enn/#", "bad_a", "error(\"a\")"),
            ("enn/#", "bad_b", "error(\"b\")"),
        ]);
        match c.choose("enn/x", b"{}") {
            Choice::DecodeFailed { protocols, error } => {
                assert_eq!(protocols, vec!["bad_a".to_string(), "bad_b".to_string()]);
                // 保留最后一个错误（bad_b），便于定位是哪个脚本失败
                assert!(error.to_string().contains("bad_b"), "{error}");
            }
            other => panic!("expected DecodeFailed, got {other:?}"),
        }
    }

    #[test]
    fn exposes_filters_and_emptiness() {
        let empty = ProtocolChooser::new(vec![]);
        assert!(empty.is_empty());
        assert!(empty.filters().is_empty());

        let c = chooser(vec![("a/#", "p1", JSON_ECHO), ("b/+", "p2", JSON_ECHO)]);
        assert!(!c.is_empty());
        assert_eq!(c.filters(), vec!["a/#", "b/+"]);
    }
}
