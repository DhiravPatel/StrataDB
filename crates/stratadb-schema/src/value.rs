//! Typed attribute values.
//!
//! Missing vs null vs empty (spec §6.3):
//! * *Missing* — the key is absent from the map / the optional field is `None`.
//! * *Null* — the key is present with [`Value::Null`].
//! * *Empty string* — [`Value::String`] with zero length.

use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    /// Unsigned values that do not fit in `i64`. Values that fit are normalized
    /// to [`Value::Int`] at ingestion so that comparisons are type-consistent.
    UInt(u64),
    /// Finite floats only; NaN and infinities are rejected at validation.
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    /// Unix nanoseconds, UTC.
    Timestamp(i64),
    List(Vec<Value>),
    Map(BTreeMap<String, Value>),
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Bool(_) => "bool",
            Value::Int(_) => "int",
            Value::UInt(_) => "uint",
            Value::Float(_) => "float",
            Value::String(_) => "string",
            Value::Bytes(_) => "bytes",
            Value::Timestamp(_) => "timestamp",
            Value::List(_) => "list",
            Value::Map(_) => "map",
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Approximate in-memory footprint, used for memtable accounting.
    pub fn approx_size(&self) -> usize {
        std::mem::size_of::<Value>()
            + match self {
                Value::String(s) => s.len(),
                Value::Bytes(b) => b.len(),
                Value::List(l) => l.iter().map(Value::approx_size).sum(),
                Value::Map(m) => m.iter().map(|(k, v)| k.len() + 32 + v.approx_size()).sum(),
                _ => 0,
            }
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::String(s.to_owned())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(s)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int(v)
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float(v)
    }
}
