//! Id-related operations such as sst_id or memtable_id

use std::fmt::{Debug, Display};
use std::ops::{Add, AddAssign};
use std::sync::atomic::AtomicUsize;

use serde::{Deserialize, Serialize};

/// Represents sst_id or memtable_id
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default)]
pub struct TableId(usize);

impl Debug for TableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<usize> for TableId {
    #[inline]
    fn from(value: usize) -> Self {
        Self(value)
    }
}

impl From<TableId> for usize {
    #[inline]
    fn from(value: TableId) -> Self {
        value.0
    }
}

impl Display for TableId {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Add for TableId {
    type Output = TableId;

    #[inline]
    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}

impl Add<usize> for TableId {
    type Output = TableId;

    #[inline]
    fn add(self, rhs: usize) -> Self::Output {
        Self(self.0 + rhs)
    }
}

impl AddAssign for TableId {
    #[inline]
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0
    }
}

impl AddAssign<usize> for TableId {
    #[inline]
    fn add_assign(&mut self, rhs: usize) {
        self.0 += rhs
    }
}

#[derive(Default)]
pub struct TableIdBuilder(AtomicUsize);

impl TableIdBuilder {
    pub fn new(init: TableId) -> Self {
        Self(AtomicUsize::new(init.0))
    }

    pub fn next_id(&self) -> TableId {
        self.0
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            .into()
    }
}
