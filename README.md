# Avro Codec

**A Rust library for Apache Avro encoding and decoding**, providing the Avro type system, zigzag integer encoding, and consistent hashing — the core primitives of Avro's binary format.

## Why It Matters

Apache Avro is a row-oriented remote procedure call and data serialization framework developed as part of Apache Hadoop. Unlike Protocol Buffers and Thrift, Avro uses a schema that is included in the payload, making it self-describing. Avro is the native serialization format for:

- **Apache Kafka** — Confluent Schema Registry stores Avro schemas; producers serialize events as Avro
- **Apache Hadoop** — MapReduce I/O, HDFS files (Avro Object Container Files)
- **Apache NiFi** — data flow routing
- **AWS Glue** — data catalog serialization
- **Confluent Platform** — schema evolution and data contracts

A key innovation in Avro's binary encoding is **zigzag encoding** — a variable-length integer encoding that maps signed integers to unsigned integers so that small negative numbers (common in real-world data) use fewer bytes.

## How It Works

### Avro Type System

The `AvroValue` enum models Avro's 10 data types:

```
AvroValue = Null
          | Boolean(bool)
          | Int(i32)       ← 32-bit, zigzag + varint encoded
          | Long(i64)      ← 64-bit, zigzag + varint encoded
          | Float(f32)     ← 32-bit IEEE 754
          | Double(f64)    ← 64-bit IEEE 754
          | String(String) ← UTF-8, length-prefixed
          | Bytes(Vec<u8>) ← length-prefixed
          | Array(Vec<AvroValue>)   ← length-prefixed sequence
          | Map(HashMap<String, AvroValue>) ← key-value pairs
```

This mirrors Avro's schema specification (AVRO-1.11.3). The `Int` and `Long` types use zigzag encoding; the others use straightforward binary representation.

### Zigzag Encoding

Signed integers are transformed so the sign bit "zigzags" into the LSB position:

```
encode(n) = (n << 1) ^ (n >> 63)     // for i64
decode(n) = (n >> 1) ^ -(n & 1)
```

This produces the mapping:

| Input (signed) | Output (unsigned) | Bytes (varint) |
|---------------|-------------------|----------------|
| 0 | 0 | `0x00` (1 byte) |
| -1 | 1 | `0x01` (1 byte) |
| 1 | 2 | `0x02` (1 byte) |
| -2 | 3 | `0x03` (1 byte) |
| 2 | 4 | `0x04` (1 byte) |
| 63 | 126 | `0x7E` (1 byte) |
| 64 | 128 | `0x80 0x01` (2 bytes) |
| -63 | 125 | `0x7D` (1 byte) |

Without zigzag, -1 (which is `0xFFFFFFFFFFFFFFFF` in two's complement) would require 10 varint bytes. With zigzag, -1 maps to 1, which needs only 1 byte. Since small magnitudes (both positive and negative) dominate real-world data, this halves the average integer encoding size.

**Formal proof of bijection:** The zigzag function is a bijection from ℤ to ℕ. For any signed integer n:

```
encode(n) = 2n      if n ≥ 0
           = 2|n|-1 if n < 0
```

This is a standard bijection from ℤ to ℕ, equivalent to the Cantor pairing.

### Varint Encoding

After zigzag, the unsigned integer is encoded as a **base-128 varint** (Protocol Buffers use the same scheme):

```
byte[i] = (value & 0x7F) | 0x80    if more bytes follow
        = (value & 0x7F)           if this is the last byte
```

Each byte carries 7 bits of data and 1 continuation bit (MSB). The maximum byte count for a 64-bit integer is ⌈64/7⌉ = 10 bytes.

**Complexity:**

| Operation | Time | Space |
|-----------|------|-------|
| Zigzag encode | O(1) | 1 word |
| Zigzag decode | O(1) | 1 word |
| Varint encode | O(⌈log₁₂₈(n)⌉) | O(⌈64/7⌉) = 10 bytes max |
| Varint decode | O(bytes) | O(1) |
| Avro hashcode | O(1) | O(1) |

### Consistent Hashing

`avro_hashcode()` provides a consistent hash for Avro values:

```rust
match value {
    AvroValue::Null     => 0,
    AvroValue::Boolean(b) => if b { 1 } else { 0 },
    AvroValue::Int(i)   => *i as u64,
    AvroValue::Long(l)  => *l as u64,
    // ... other types get type-specific hashes
}
```

This is useful for **schema resolution** (determining if two Avro values are equivalent across schema versions) and for **object reuse** in analytics pipelines (Avro's Java implementation uses `hashCode()` for hash joins and deduplication).

## Quick Start

```rust
use avro_codec::{AvroValue, zigzag_encode, zigzag_decode};

// Zigzag encoding: small magnitudes map to small unsigned values
assert_eq!(zigzag_encode(0), 0);
assert_eq!(zigzag_encode(-1), 1);
assert_eq!(zigzag_encode(1), 2);
assert_eq!(zigzag_encode(-2), 3);
assert_eq!(zigzag_encode(2), 4);

// Roundtrip
assert_eq!(zigzag_decode(zigzag_encode(-42)), -42);
assert_eq!(zigzag_decode(zigzag_encode(i64::MAX)), i64::MAX);
assert_eq!(zigzag_decode(zigzag_encode(i64::MIN)), i64::MIN);

// Build Avro data
let record = AvroValue::Array(vec![
    AvroValue::Int(42),
    AvroValue::String("hello".into()),
    AvroValue::Boolean(true),
    AvroValue::Null,
]);

// Consistent hashing
assert_eq!(avro_hashcode(&AvroValue::Null), 0);
assert_eq!(avro_hashcode(&AvroValue::Boolean(true)), 1);
assert_eq!(avro_hashcode(&AvroValue::Boolean(false)), 0);
```

## API

| Item | Signature | Description |
|------|-----------|-------------|
| `AvroValue` | enum (10 variants) | Full Avro type system |
| `zigzag_encode` | `(i64) → u64` | Map signed to unsigned for varint encoding |
| `zigzag_decode` | `(u64) → i64` | Reverse zigzag mapping |
| `avro_hashcode` | `(&AvroValue) → u64` | Consistent hash for Avro values |

### `AvroValue` Variants

| Variant | Rust Type | Avro Schema Type | Binary Size |
|---------|-----------|-----------------|-------------|
| `Null` | `()` | `null` | 0 bytes |
| `Boolean(bool)` | `bool` | `boolean` | 1 byte |
| `Int(i32)` | `i32` | `int` | 1–5 bytes |
| `Long(i64)` | `i64` | `long` | 1–10 bytes |
| `Float(f32)` | `f32` | `float` | 4 bytes |
| `Double(f64)` | `f64` | `double` | 8 bytes |
| `String(String)` | `String` | `string` | varint length + UTF-8 |
| `Bytes(Vec<u8>)` | `Vec<u8>` | `bytes` | varint length + raw |
| `Array(Vec<AvroValue>)` | `Vec<AvroValue>` | `array` | varint length + items |
| `Map(HashMap)` | `HashMap<String, AvroValue>` | `map` | varint length + entries |

## Architecture Notes

Provides Avro serialization primitives for SuperInstance data pipelines. Used in event streaming contexts where schema evolution and compact binary encoding matter.

Within γ + η = C, the zigzag encoding instantiates the conservation law as **information-preserving compression**: the full signed integer domain (γ) is mapped bijectively to the unsigned domain (η), and the total information content (C) is conserved. No information is lost in zigzag — the mapping is invertible. The varint encoding then compresses the representation, but the logical content is conserved.

See the [architecture overview](https://github.com/casey-digennaro/avro-codec/blob/main/ARCHITECTURE.md).

## References

1. Cutting, D. (2009). "Apache Avro 1.11.3 Specification." *apache.org*. (Canonical Avro spec)
2. Kleppmann, M. (2017). *Designing Data-Intensive Applications*. O'Reilly. Chapter 4: "Encoding and Evolution." (Comparison of Avro, Protobuf, Thrift)
3. Google. "Protocol Buffers Encoding: Signed Integers." (Varint + zigzag, same scheme)
4. Kreft, D. (2013). "ZigZag encoding." *Inner workings of Protocol Buffers*.

## License

MIT
