# Avro Codec

**A Rust library for Apache Avro encoding and decoding**, providing the Avro type system and zigzag integer encoding used in Avro's binary format.

## Why It Matters

Apache Avro is a row-oriented remote procedure call and data serialization framework developed as part of Apache Hadoop. Unlike Protocol Buffers and Thrift, Avro uses a schema that is included in the payload, making it self-describing. Avro is the native serialization format for Kafka, Hadoop MapReduce, and Confluent Schema Registry.

A key innovation in Avro's binary encoding is **zigzag encoding** — a variable-length integer encoding that maps signed integers to unsigned integers so that small negative numbers (common in real-world data) use fewer bytes. `0 → 0`, `-1 → 1`, `1 → 2`, `-2 → 3`, etc.

## How It Works

**Type System**: The `AvroValue` enum models Avro's 11 data types: `Null`, `Boolean`, `Int` (32-bit), `Long` (64-bit), `Float`, `Double`, `String`, `Bytes`, `Array`, `Map`, plus `Tag` for logical types. This mirrors Avro's schema specification.

**Zigzag Encoding**: Signed integers are transformed so the sign bit "zigzags" into the LSB position: `encode(n) = (n << 1) ^ (n >> 63)` for `i64`. This maps `0, -1, 1, -2, 2, ...` to `0, 1, 2, 3, 4, ...`. The result is then varint-encoded (7 bits per byte, MSB continuation bit), so small magnitudes — regardless of sign — take one byte. Decoding reverses the process: `decode(n) = (n >> 1) ^ -(n & 1)`.

**Hashing**: `avro_hashcode()` provides a consistent hash for Avro values, useful for object equality in schema resolution.

## Quick Start

```rust
use avro_codec::{AvroValue, zigzag_encode, zigzag_decode};

// Zigzag encoding
assert_eq!(zigzag_encode(0), 0);
assert_eq!(zigzag_encode(-1), 1);
assert_eq!(zigzag_encode(1), 2);
assert_eq!(zigzag_decode(zigzag_encode(-42)), -42);

// Build Avro data
let record = AvroValue::Array(vec![
    AvroValue::Int(42),
    AvroValue::String("hello".into()),
    AvroValue::Null,
]);
```

## API

- **`AvroValue`** — Enum covering all Avro data types (Null, Boolean, Int, Long, Float, Double, String, Bytes, Array, Map)
- **`zigzag_encode(i64) → u64`** — Map signed to unsigned for varint encoding
- **`zigzag_decode(u64) → i64`** — Reverse zigzag mapping
- **`avro_hashcode(&AvroValue) → u64`** — Consistent hash for Avro values

## Architecture Notes

Provides Avro serialization primitives for SuperInstance data pipelines. Used in event streaming contexts where schema evolution and compact binary encoding matter. See the [architecture overview](https://github.com/SuperInstance/SuperInstance/blob/main/ARCHITECTURE.md).

## License

MIT
