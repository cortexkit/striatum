//! Lossless in-memory representation of conversation hash text.

use std::cmp::Ordering;
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ConversationHash {
    Sha256([u8; 32]),
    Text(Box<str>),
}

impl ConversationHash {
    fn decode(text: &str) -> Option<[u8; 32]> {
        if text.len() != 64 {
            return None;
        }
        let nibble = |b| match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            _ => None,
        };
        let mut bytes = [0; 32];
        for (out, pair) in bytes.iter_mut().zip(text.as_bytes().chunks_exact(2)) {
            *out = nibble(pair[0])? * 16 + nibble(pair[1])?;
        }
        Some(bytes)
    }

    fn encoded(bytes: &[u8; 32]) -> [u8; 64] {
        let mut text = [0; 64];
        for (pair, byte) in text.chunks_exact_mut(2).zip(bytes) {
            pair[0] = b"0123456789abcdef"[(byte >> 4) as usize];
            pair[1] = b"0123456789abcdef"[(byte & 15) as usize];
        }
        text
    }

    pub(crate) fn with_str<R>(&self, f: impl FnOnce(&str) -> R) -> R {
        match self {
            Self::Sha256(bytes) => f(std::str::from_utf8(&Self::encoded(bytes)).unwrap()),
            Self::Text(text) => f(text),
        }
    }

    #[cfg(all(test, feature = "heap-measure"))]
    pub(crate) fn heap_text(&self) -> Option<&str> {
        match self {
            Self::Text(text) => Some(text),
            _ => None,
        }
    }
}

impl From<&str> for ConversationHash {
    fn from(text: &str) -> Self {
        Self::decode(text).map_or_else(|| Self::Text(text.into()), Self::Sha256)
    }
}

impl From<String> for ConversationHash {
    fn from(text: String) -> Self {
        Self::from(text.as_str())
    }
}
impl From<&String> for ConversationHash {
    fn from(text: &String) -> Self {
        Self::from(text.as_str())
    }
}
impl From<&ConversationHash> for ConversationHash {
    fn from(hash: &ConversationHash) -> Self {
        hash.clone()
    }
}

impl fmt::Display for ConversationHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.with_str(|s| f.write_str(s))
    }
}

impl PartialOrd for ConversationHash {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ConversationHash {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Sha256(a), Self::Sha256(b)) => a.cmp(b),
            _ => self.with_str(|a| other.with_str(|b| a.cmp(b))),
        }
    }
}

impl PartialEq<str> for ConversationHash {
    fn eq(&self, other: &str) -> bool {
        match self {
            Self::Sha256(bytes) => Self::decode(other).as_ref() == Some(bytes),
            Self::Text(text) => text.as_ref() == other,
        }
    }
}
impl PartialEq<&str> for ConversationHash {
    fn eq(&self, other: &&str) -> bool {
        self == *other
    }
}
impl PartialEq<String> for ConversationHash {
    fn eq(&self, other: &String) -> bool {
        self == other.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn conversation_hash_preserves_all_bits_and_non_hex_text() {
        let first = "00".repeat(32);
        let last = format!("{}01", "00".repeat(31));
        assert_ne!(
            ConversationHash::from(first.as_str()),
            ConversationHash::from(last.as_str())
        );
        for text in [
            first,
            last,
            "A".repeat(64),
            "h".repeat(64),
            "a".repeat(63),
            "a".repeat(65),
            "雪".into(),
            String::new(),
        ] {
            let hash = ConversationHash::from(text.as_str());
            assert_eq!(hash.to_string(), text);
            assert!(hash == text);
        }
        let mut text = vec![
            "f".repeat(64),
            "g".repeat(64),
            "0".into(),
            "a".repeat(64),
            "A".repeat(64),
        ];
        let mut compact: Vec<_> = text
            .iter()
            .map(|s| ConversationHash::from(s.as_str()))
            .collect();
        text.sort();
        compact.sort();
        assert_eq!(
            compact.iter().map(ToString::to_string).collect::<Vec<_>>(),
            text
        );
    }
}
