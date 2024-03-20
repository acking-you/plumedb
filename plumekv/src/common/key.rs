//! Abstraction and implementation of Key
use std::fmt::Debug;

use bytes::Bytes;

/// Key with generic
pub struct Key<T: AsRef<[u8]>>(T);

/// Key with `&[u8]`
pub type KeySlice<'a> = Key<&'a [u8]>;

/// Key with `Vec<u8>`
pub type KeyVec = Key<Vec<u8>>;

/// Key with [`bytes::Bytes`]
pub type KeyBytes = Key<Bytes>;

impl<'a> From<&'a [u8]> for KeySlice<'a> {
    fn from(value: &'a [u8]) -> Self {
        Self(value)
    }
}

impl<'a> From<&'a [u8]> for KeyVec {
    fn from(value: &'a [u8]) -> Self {
        Self(value.to_vec())
    }
}

impl<'a> From<&'a [u8]> for KeyBytes {
    fn from(value: &'a [u8]) -> Self {
        Self(Bytes::copy_from_slice(value))
    }
}

impl<T: AsRef<[u8]>> Key<T> {
    /// Get inner type
    pub fn into_inner(self) -> T {
        self.0
    }

    /// Get the length of the Key
    pub fn key_len(&self) -> usize {
        self.0.as_ref().len()
    }

    /// Returns `true` if the Key has a length of 0.
    pub fn is_empty(&self) -> bool {
        self.0.as_ref().is_empty()
    }
}

impl Key<Vec<u8>> {
    /// Crate a Key with Vec<u8>
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Clears the key and set ts to 0.
    pub fn clear(&mut self) {
        self.0.clear()
    }

    /// Append a slice to the end of the key
    pub fn append(&mut self, data: &[u8]) {
        self.0.extend(data)
    }

    /// Set the key from a slice without re-allocating.
    pub fn set_from_slice(&mut self, key_slice: KeySlice) {
        self.0.clear();
        self.0.extend(key_slice.0);
    }

    #[allow(missing_docs)]
    pub fn as_key_slice(&self) -> KeySlice {
        Key(self.0.as_slice())
    }

    #[allow(missing_docs)]
    pub fn into_key_bytes(self) -> KeyBytes {
        Key(self.0.into())
    }

    #[allow(missing_docs)]
    pub fn key_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Key<Bytes> {
    /// Crate a Key with [`bytes::Bytes`]
    pub fn new() -> Self {
        Self(Bytes::new())
    }

    #[allow(missing_docs)]
    pub fn as_key_slice(&self) -> KeySlice {
        Key(&self.0)
    }

    /// Create a `KeyBytes` from a `Bytes` and a ts.
    pub fn from_bytes(bytes: Bytes) -> KeyBytes {
        Key(bytes)
    }

    #[allow(missing_docs)]
    pub fn key_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl<'a> Key<&'a [u8]> {
    #[allow(missing_docs)]
    pub fn to_key_vec(self) -> KeyVec {
        Key(self.0.to_vec())
    }

    /// Create a key slice from a slice. Will be removed in week 3.
    pub fn from_slice(slice: &'a [u8]) -> Self {
        Self(slice)
    }

    #[allow(missing_docs)]
    pub fn key_ref(self) -> &'a [u8] {
        self.0
    }
}

impl<T: AsRef<[u8]> + Debug> Debug for Key<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: AsRef<[u8]> + Default> Default for Key<T> {
    fn default() -> Self {
        Self(T::default())
    }
}

impl<T: AsRef<[u8]> + PartialEq> PartialEq for Key<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_ref().eq(other.0.as_ref())
    }
}

impl<T: AsRef<[u8]> + Eq> Eq for Key<T> {}

impl<T: AsRef<[u8]> + Clone> Clone for Key<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: AsRef<[u8]> + Copy> Copy for Key<T> {}

impl<T: AsRef<[u8]> + PartialOrd> PartialOrd for Key<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.as_ref().partial_cmp(other.0.as_ref())
    }
}

impl<T: AsRef<[u8]> + Ord> Ord for Key<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.as_ref().cmp(other.0.as_ref())
    }
}
