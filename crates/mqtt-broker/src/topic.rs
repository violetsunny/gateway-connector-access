//! MQTT 3.1.1 §4.7 topic 名/过滤器校验与通配符匹配。

/// topic 名（PUBLISH/will）：非空、不含通配符与 NUL。
pub fn valid_name(topic: &str) -> bool {
    !topic.is_empty() && !topic.contains(['+', '#', '\0'])
}

/// topic 过滤器（SUBSCRIBE）：`+` 必须独占一层，`#` 必须是最后一层且独占。
pub fn valid_filter(filter: &str) -> bool {
    if filter.is_empty() || filter.contains('\0') {
        return false;
    }
    let levels: Vec<&str> = filter.split('/').collect();
    for (i, level) in levels.iter().enumerate() {
        if level.contains('+') && *level != "+" {
            return false;
        }
        if level.contains('#') && (*level != "#" || i != levels.len() - 1) {
            return false;
        }
    }
    true
}

/// 通配符匹配。通配符开头的过滤器不匹配 `$` 开头 topic（§4.7.2，如 `#` 不匹配 `$SYS/x`）。
/// `sport/#` 匹配 `sport` 本身（`#` 含父层）；`sport/+` 匹配 `sport/` 但不匹配 `sport`。
pub fn matches(filter: &str, topic: &str) -> bool {
    if topic.starts_with('$') && matches!(filter.as_bytes().first(), Some(b'+' | b'#')) {
        return false;
    }
    let mut f = filter.split('/');
    let mut t = topic.split('/');
    loop {
        match (f.next(), t.next()) {
            (Some("#"), _) => return true,
            (Some("+"), Some(_)) => {}
            (Some(a), Some(b)) if a == b => {}
            (None, None) => return true,
            _ => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_validation() {
        assert!(valid_name("sport/tennis/player1"));
        assert!(!valid_name(""));
        assert!(!valid_name("sport/+"));
        assert!(!valid_name("sport/#"));
        assert!(!valid_name("sport\0tennis"));
    }

    #[test]
    fn filter_validation() {
        assert!(valid_filter("sport/tennis/#"));
        assert!(valid_filter("sport/+/player1"));
        assert!(valid_filter("#"));
        assert!(valid_filter("+"));
        assert!(valid_filter("/"));
        assert!(!valid_filter(""));
        assert!(!valid_filter("sport/tennis#"));
        assert!(!valid_filter("sport/#/ranking"));
        assert!(!valid_filter("sport/+player"));
        assert!(!valid_filter("sport/tennis/\0"));
    }

    #[test]
    fn exact_and_wildcard_matching() {
        assert!(matches("sport/tennis/player1", "sport/tennis/player1"));
        assert!(!matches(
            "sport/tennis/player1",
            "sport/tennis/player1/ranking"
        ));
        assert!(matches(
            "sport/tennis/player1/#",
            "sport/tennis/player1/ranking"
        ));
        assert!(matches("sport/tennis/+", "sport/tennis/player1"));
        assert!(!matches("sport/tennis/+", "sport/tennis/player1/ranking"));
        assert!(matches("sport/#", "sport/tennis/player1"));
        assert!(matches("#", "sport/tennis/player1"));
        assert!(matches("+/tennis/#", "sport/tennis/player1"));
        assert!(matches("sport/+/player1", "sport/tennis/player1"));
    }

    #[test]
    fn hash_includes_parent_level() {
        assert!(matches("sport/#", "sport"));
        assert!(matches("sport/#", "sport/"));
    }

    #[test]
    fn plus_matches_empty_level_not_missing_level() {
        assert!(matches("sport/+", "sport/"));
        assert!(!matches("sport/+", "sport"));
        assert!(matches("+/+", "/finance"));
        assert!(matches("/+", "/finance"));
        assert!(!matches("+", "/finance"));
    }

    #[test]
    fn dollar_topics_exempt_from_leading_wildcards() {
        assert!(!matches("#", "$SYS/broker/clients"));
        assert!(!matches("+/monitor/Clients", "$SYS/monitor/Clients"));
        assert!(matches("$SYS/#", "$SYS/broker/clients"));
        assert!(matches("$SYS/monitor/+", "$SYS/monitor/Clients"));
    }
}
