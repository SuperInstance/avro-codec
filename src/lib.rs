use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum AvroValue {
    Null,
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<AvroValue>),
    Map(HashMap<String, AvroValue>),
}

pub fn zigzag_encode(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

pub fn zigzag_decode(n: u64) -> i64 {
    ((n >> 1) as i64) ^ -((n & 1) as i64)
}

pub fn avro_hashcode(value: &AvroValue) -> u64 {
    match value {
        AvroValue::Null => 0,
        AvroValue::Boolean(b) => if *b { 1 } else { 0 },
        AvroValue::Int(i) => *i as u64,
        AvroValue::Long(l) => *l as u64,
        _ => 42,
    }
}
