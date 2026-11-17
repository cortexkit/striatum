//! Ordered, unique pin membership without a tree allocation for singleton pins.

use std::collections::BTreeSet;

use super::admissions::ConversationKey;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) enum PinMembers {
    #[default]
    Empty,
    One(ConversationKey),
    Many(BTreeSet<ConversationKey>),
}

impl PinMembers {
    pub(crate) fn insert(&mut self, key: ConversationKey) -> bool {
        match self {
            Self::Empty => *self = Self::One(key),
            Self::One(current) if *current == key => return false,
            Self::One(_) => {
                let Self::One(current) = std::mem::take(self) else {
                    unreachable!()
                };
                *self = Self::Many(BTreeSet::from([current, key]));
            }
            Self::Many(keys) => return keys.insert(key),
        }
        true
    }

    pub(crate) fn remove(&mut self, key: &ConversationKey) -> bool {
        match self {
            Self::Empty => false,
            Self::One(current) if current != key => false,
            Self::One(_) => {
                *self = Self::Empty;
                true
            }
            Self::Many(keys) => {
                let removed = keys.remove(key);
                if keys.len() == 1 {
                    *self = Self::One(keys.pop_first().unwrap());
                }
                removed
            }
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &ConversationKey> {
        let (one, many) = match self {
            Self::Empty => (None, None),
            Self::One(key) => (Some(key), None),
            Self::Many(keys) => (None, Some(keys.iter())),
        };
        one.into_iter().chain(many.into_iter().flatten())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pin_members_are_ordered_unique_and_cow() {
        let key = |hash: &str| ("principal".into(), hash.into(), "provider".into());
        let mut members = std::sync::Arc::new(PinMembers::default());
        assert!(std::sync::Arc::make_mut(&mut members).insert(key("b")));
        let original = members.clone();
        assert!(!std::sync::Arc::make_mut(&mut members).insert(key("b")));
        assert!(std::sync::Arc::make_mut(&mut members).insert(key("a")));
        assert_eq!(
            members.iter().cloned().collect::<Vec<_>>(),
            vec![key("a"), key("b")]
        );
        assert_eq!(original.iter().cloned().collect::<Vec<_>>(), vec![key("b")]);
        assert!(!std::sync::Arc::make_mut(&mut members).remove(&key("absent")));
        assert!(std::sync::Arc::make_mut(&mut members).remove(&key("a")));
        assert_eq!(members, original);
        assert!(std::sync::Arc::make_mut(&mut members).remove(&key("b")));
        assert!(members.is_empty());
    }
}
