//! Per-segment Bloom filters (spec §12.5, ticket F-003). Footer extension tag 2.
//!
//! * Probabilistic **negative** filtering only: "absent" is definitive,
//!   "maybe present" must be confirmed by scanning.
//! * Only **string** values are indexed, and only string equality/IN
//!   predicates may consult a filter (numbers compare across int/float types,
//!   which byte-keyed hashing cannot represent).
//! * Hash v1: `h = splitmix64(fnv1a64(utf8 bytes))`, `h2 = splitmix64(h ^ GOLDEN) | 1`,
//!   bit `i` = `(h + i·h2) mod nbits` (Kirsch–Mitzenmacher double hashing).
//!   Unkeyed: adversarial values can raise the false-positive rate (costing
//!   scans), never cause wrong results.
//! * Sizing: `nbits = max(64, ⌈9.6·n⌉)`, `k = 7` (≈1 % FP); fields with more than
//!   `MAX_ITEMS` distinct values get no filter.
//!
//! ```text
//! u8 version (=1) | uvarint nfilters | per filter:
//!   u8 kind (1 trace_id, 2 span_id, 3 event_id, 4 attribute, 5 resource attribute)
//!   [bytes key — kinds 4 and 5] | u8 k | uvarint nbits | bytes bitset (⌈nbits/8⌉)
//! ```

use std::collections::{BTreeMap, BTreeSet};

use stratadb_common::codec::{Decoder, Encoder};
use stratadb_common::{Error, Result};
use stratadb_schema::Value;

use crate::{Extension, RowRef};

pub const EXT_BLOOM: u16 = 2;
const VERSION: u8 = 1;
const K: u8 = 7;
const BITS_PER_ITEM: f64 = 9.6;
pub const MAX_ITEMS: usize = 4_000_000;

/// A field a filter can cover.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BloomField {
    TraceId,
    SpanId,
    EventId,
    Attr(String),
    Resource(String),
}

impl BloomField {
    pub fn describe(&self) -> String {
        match self {
            BloomField::TraceId => "bloom(trace_id)".into(),
            BloomField::SpanId => "bloom(span_id)".into(),
            BloomField::EventId => "bloom(event_id)".into(),
            BloomField::Attr(k) => format!("bloom(attributes['{k}'])"),
            BloomField::Resource(k) => format!("bloom(resource_attributes['{k}'])"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bloom {
    k: u8,
    nbits: u64,
    bits: Vec<u8>,
}

fn fnv1a64(b: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &x in b {
        h ^= u64::from(x);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

fn splitmix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn hashes(item: &str) -> (u64, u64) {
    let h1 = splitmix64(fnv1a64(item.as_bytes()));
    let h2 = splitmix64(h1 ^ 0x9E37_79B9_7F4A_7C15) | 1;
    (h1, h2)
}

impl Bloom {
    pub fn with_capacity(n: usize) -> Self {
        let nbits = ((n as f64 * BITS_PER_ITEM).ceil() as u64).max(64);
        Self {
            k: K,
            nbits,
            bits: vec![0u8; nbits.div_ceil(8) as usize],
        }
    }

    fn positions(&self, item: &str) -> impl Iterator<Item = u64> + '_ {
        let (h1, h2) = hashes(item);
        (0..u64::from(self.k)).map(move |i| h1.wrapping_add(i.wrapping_mul(h2)) % self.nbits)
    }

    pub fn insert(&mut self, item: &str) {
        let pos: Vec<u64> = self.positions(item).collect();
        for p in pos {
            self.bits[(p / 8) as usize] |= 1 << (p % 8);
        }
    }

    /// False means definitely absent.
    pub fn may_contain(&self, item: &str) -> bool {
        self.positions(item)
            .all(|p| self.bits[(p / 8) as usize] & (1 << (p % 8)) != 0)
    }

    pub fn size_bytes(&self) -> usize {
        self.bits.len()
    }

    /// `u8 k | uvarint nbits | bytes bitset`.
    pub fn encode_into(&self, e: &mut Encoder) {
        e.put_u8(self.k);
        e.put_uvarint(self.nbits);
        e.put_bytes(&self.bits);
    }

    /// Inverse of [`Bloom::encode_into`], validating parameters.
    pub fn decode_from(d: &mut Decoder<'_>, max_bytes: usize) -> Result<Self> {
        let k = d.get_u8()?;
        let nbits = d.get_uvarint()?;
        let bits = d.get_bytes(max_bytes)?.to_vec();
        if k == 0 || k > 32 || nbits == 0 || bits.len() as u64 != nbits.div_ceil(8) {
            return Err(Error::corruption(
                "bloom filter",
                "inconsistent filter parameters",
            ));
        }
        Ok(Self { k, nbits, bits })
    }
}

/// All filters of one segment.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BloomSet {
    filters: BTreeMap<BloomField, Bloom>,
}

impl BloomSet {
    pub fn get(&self, f: &BloomField) -> Option<&Bloom> {
        self.filters.get(f)
    }

    pub fn fields(&self) -> impl Iterator<Item = &BloomField> {
        self.filters.keys()
    }

    pub fn size_bytes(&self) -> usize {
        self.filters.values().map(Bloom::size_bytes).sum()
    }

    /// Build filters for the fixed id columns and the given attribute keys.
    pub fn build(rows: &[RowRef<'_>], attr_keys: &[String], resource_keys: &[String]) -> Self {
        let mut items: BTreeMap<BloomField, BTreeSet<&str>> = BTreeMap::new();
        for f in [BloomField::TraceId, BloomField::SpanId, BloomField::EventId] {
            items.insert(f, BTreeSet::new());
        }
        for k in attr_keys {
            items.insert(BloomField::Attr(k.clone()), BTreeSet::new());
        }
        for k in resource_keys {
            items.insert(BloomField::Resource(k.clone()), BTreeSet::new());
        }
        // Borrow strings from the rows (no copies).
        let mut staged: Vec<(BloomField, &str)> = Vec::new();
        for r in rows {
            let e = r.event;
            if let Some(v) = &e.trace_id {
                staged.push((BloomField::TraceId, v));
            }
            if let Some(v) = &e.span_id {
                staged.push((BloomField::SpanId, v));
            }
            if let Some(v) = &e.event_id {
                staged.push((BloomField::EventId, v));
            }
            for k in attr_keys {
                if let Some(Value::String(v)) = e.attributes.get(k) {
                    staged.push((BloomField::Attr(k.clone()), v));
                }
            }
            for k in resource_keys {
                if let Some(Value::String(v)) = e.resource_attributes.get(k) {
                    staged.push((BloomField::Resource(k.clone()), v));
                }
            }
        }
        for (f, v) in staged {
            if let Some(set) = items.get_mut(&f) {
                set.insert(v);
            }
        }
        let mut filters = BTreeMap::new();
        for (f, vals) in items {
            if vals.len() > MAX_ITEMS {
                continue;
            }
            let mut b = Bloom::with_capacity(vals.len());
            for v in vals {
                b.insert(v);
            }
            filters.insert(f, b);
        }
        Self { filters }
    }

    /// Fields whose filter is missing a value present in `rows` (a false
    /// negative would make queries skip matching data). Empty = sound.
    pub fn false_negatives(&self, rows: &[RowRef<'_>]) -> Vec<String> {
        let mut bad = BTreeSet::new();
        for r in rows {
            let e = r.event;
            for (f, b) in &self.filters {
                let v: Option<&str> = match f {
                    BloomField::TraceId => e.trace_id.as_deref(),
                    BloomField::SpanId => e.span_id.as_deref(),
                    BloomField::EventId => e.event_id.as_deref(),
                    BloomField::Attr(k) => e.attributes.get(k).and_then(Value::as_str),
                    BloomField::Resource(k) => e.resource_attributes.get(k).and_then(Value::as_str),
                };
                if v.is_some_and(|v| !b.may_contain(v)) {
                    bad.insert(f.describe());
                }
            }
        }
        bad.into_iter().collect()
    }

    pub fn to_extension(&self) -> Extension {
        let mut e = Encoder::new();
        e.put_u8(VERSION);
        e.put_uvarint(self.filters.len() as u64);
        for (f, b) in &self.filters {
            match f {
                BloomField::TraceId => e.put_u8(1),
                BloomField::SpanId => e.put_u8(2),
                BloomField::EventId => e.put_u8(3),
                BloomField::Attr(k) => {
                    e.put_u8(4);
                    e.put_str(k);
                }
                BloomField::Resource(k) => {
                    e.put_u8(5);
                    e.put_str(k);
                }
            }
            b.encode_into(&mut e);
        }
        Extension {
            tag: EXT_BLOOM,
            required: false,
            data: e.into_inner(),
        }
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        let bad = |m: &str| Error::corruption("bloom extension", m.to_owned());
        let mut d = Decoder::new(data, "bloom extension");
        if d.get_u8()? != VERSION {
            // Unknown filter versions are ignored by callers (no pruning).
            return Err(Error::unsupported("unknown bloom filter version"));
        }
        let n = d.get_len(1024, 3)?;
        let mut filters = BTreeMap::new();
        for _ in 0..n {
            let f = match d.get_u8()? {
                1 => BloomField::TraceId,
                2 => BloomField::SpanId,
                3 => BloomField::EventId,
                4 => BloomField::Attr(d.get_string(1024)?),
                5 => BloomField::Resource(d.get_string(1024)?),
                _ => return Err(bad("unknown field kind")),
            };
            filters.insert(f, Bloom::decode_from(&mut d, data.len())?);
        }
        d.expect_end()?;
        Ok(Self { filters })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stratadb_common::TenantId;
    use stratadb_schema::Event;

    #[test]
    fn no_false_negatives_and_low_false_positive_rate() {
        let mut b = Bloom::with_capacity(10_000);
        for i in 0..10_000 {
            b.insert(&format!("req_{i}"));
        }
        assert!((0..10_000).all(|i| b.may_contain(&format!("req_{i}"))));
        let fp = (0..100_000)
            .filter(|i| b.may_contain(&format!("absent_{i}")))
            .count();
        assert!(fp < 2_000, "false-positive rate too high: {fp}/100000");
    }

    #[test]
    fn build_encode_decode() {
        let t = TenantId::new("t").unwrap();
        let mut evs = Vec::new();
        for i in 0..50 {
            let mut e = Event::new(t.clone(), 0, 0);
            e.trace_id = Some(format!("{i:032x}"));
            e.attributes
                .insert("request_id".into(), Value::String(format!("r{i}")));
            e.attributes.insert("num".into(), Value::Int(i)); // non-string: not indexed
            evs.push(e);
        }
        let rows: Vec<RowRef<'_>> = evs
            .iter()
            .map(|e| RowRef {
                seq: 1,
                idx: 0,
                event: e,
            })
            .collect();
        let set = BloomSet::build(&rows, &["request_id".into(), "absent_key".into()], &[]);
        let back = BloomSet::decode(&set.to_extension().data).unwrap();
        assert_eq!(back, set);
        let req = back.get(&BloomField::Attr("request_id".into())).unwrap();
        assert!(req.may_contain("r7"));
        assert!(!back
            .get(&BloomField::Attr("absent_key".into()))
            .unwrap()
            .may_contain("x"));
        assert!(back
            .get(&BloomField::TraceId)
            .unwrap()
            .may_contain(&format!("{:032x}", 3)));
        for cut in 0..set.to_extension().data.len() {
            assert!(BloomSet::decode(&set.to_extension().data[..cut]).is_err());
        }
    }
}
