# Event encoding v1

Used inside WAL EventBatch payloads. Varints are LEB128 (canonical; overlong
forms rejected); signed varints are zig-zag.

```text
u8      encoding_version = 1
ivarint event_time            (Unix ns)
ivarint ingest_time           (Unix ns)
bytes   tenant_id             (uvarint len + UTF-8, validated alphabet)
u8      severity              (0,1,5,9,13,17,21)
opt     service, severity_text, trace_id, span_id, event_id
          (u8 0 = missing | u8 1 + bytes)
optval  body                  (u8 0 = missing | u8 1 + value)
map     attributes
map     resource_attributes
uvarint schema_version
```

`value` = tag u8 + payload: 0 null, 1 false, 2 true, 3 int (ivarint),
4 uint (uvarint), 5 float (f64 LE bits), 6 string (bytes), 7 bytes,
8 timestamp (ivarint ns), 9 list (uvarint n + n values), 10 map.
`map` = uvarint n + n × (key bytes, value); **keys strictly increasing**
(canonical; duplicates rejected). Decoders bound nesting depth (64) and never
allocate more than the remaining input.
