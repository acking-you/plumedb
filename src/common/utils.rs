use std::ops::Bound;

use volo_gen::plumedb::bound::Value;

pub(crate) fn map_rpc_bound(value: &Value) -> Bound<&[u8]> {
    match value {
        Value::Unbouned(_) => Bound::Unbounded,
        Value::Include(v) => Bound::Included(v),
        Value::Exclude(v) => Bound::Excluded(v),
    }
}
