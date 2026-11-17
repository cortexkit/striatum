//! Value-equal sharing for the state's low-cardinality text columns.

use std::collections::HashSet;
use std::ops::Deref;
use std::sync::{Arc, OnceLock, RwLock};

fn pool() -> &'static RwLock<HashSet<Arc<str>>> {
    static POOL: OnceLock<RwLock<HashSet<Arc<str>>>> = OnceLock::new();
    POOL.get_or_init(RwLock::default)
}

// Only principal, provider, account/credential id, model, affinity model, state
// and request mode enter this pool. It never evicts: these columns have few
// distinct values, and retaining one copy per distinct value avoids a lock on
// every row/key drop. Admission, pin, run and decision ids must stay owned.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct SharedText(Arc<str>);

impl From<&str> for SharedText {
    fn from(text: &str) -> Self {
        if let Some(shared) = pool().read().unwrap().get(text) {
            return Self(shared.clone());
        }
        let mut pool = pool().write().unwrap();
        if let Some(shared) = pool.get(text) {
            return Self(shared.clone());
        }
        let shared: Arc<str> = text.into();
        pool.insert(shared.clone());
        Self(shared)
    }
}
impl From<String> for SharedText {
    fn from(text: String) -> Self {
        Self::from(text.as_str())
    }
}
impl Deref for SharedText {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}
impl SharedText {
    pub(crate) fn as_str(&self) -> &str {
        self
    }
}
impl AsRef<str> for SharedText {
    fn as_ref(&self) -> &str {
        self
    }
}
impl std::fmt::Display for SharedText {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self)
    }
}
impl std::borrow::Borrow<str> for SharedText {
    fn borrow(&self) -> &str {
        self
    }
}
impl PartialEq<str> for SharedText {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}
impl PartialEq<&str> for SharedText {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}
impl PartialEq<String> for SharedText {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}

#[cfg(all(test, feature = "heap-measure"))]
pub(crate) fn take_heap_pool() -> HashSet<Arc<str>> {
    std::mem::take(&mut *pool().write().unwrap())
}

#[cfg(test)]
pub(crate) fn contains(text: &str) -> bool {
    pool().read().unwrap().contains(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_text_is_value_equal_and_interns_only_distinct_values() {
        let a = SharedText::from("interner-test-value");
        let b = SharedText::from(String::from("interner-test-value"));
        assert!(Arc::ptr_eq(&a.0, &b.0));
        let independent = SharedText(Arc::from("interner-test-value"));
        assert_eq!(a, independent);
        assert_ne!(a, SharedText::from("interner-test-other"));
        drop(independent);
        drop(a);
        drop(b);
        let a = SharedText::from("interner-test-value");
        let b = SharedText::from("interner-test-value");
        assert!(Arc::ptr_eq(&a.0, &b.0));
        assert_eq!(
            pool()
                .read()
                .unwrap()
                .iter()
                .filter(|v| v.starts_with("interner-test-"))
                .count(),
            2
        );
    }
}
