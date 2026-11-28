// StrataQL tables and columns (docs/query-language/grammar.md,
// semantics.md; crates/stratadb-query/src/ast.rs). StrataDB has no schema
// endpoint yet, so the shape is fixed here (see docs/studio/STATUS.md).

const EVENT_COLUMNS = [
  { name: 'event_time', type: 'timestamp', doc: 'Event time (ns, UTC)' },
  { name: 'ingest_time', type: 'timestamp', doc: 'When the server accepted the event' },
  { name: 'service', type: 'string', doc: 'Emitting service (or missing)' },
  { name: 'severity', type: 'enum', doc: 'UNSPECIFIED TRACE DEBUG INFO WARN ERROR FATAL' },
  { name: 'severity_number', type: 'int', doc: '0, 1, 5, 9, 13, 17, 21' },
  { name: 'severity_text', type: 'string', doc: 'Original severity text' },
  { name: 'body', type: 'any', doc: 'Message body: any value or missing' },
  { name: 'trace_id', type: 'string', doc: 'Trace id (hex)' },
  { name: 'span_id', type: 'string', doc: 'Span id (hex)' },
  { name: 'event_id', type: 'string', doc: 'Event id' },
  { name: 'tenant_id', type: 'string', doc: "Constant: the caller's tenant" },
  { name: 'schema_version', type: 'int', doc: 'Event schema version' },
  { name: 'attributes', type: 'map', doc: "attributes['key'] selects one value" },
  { name: 'resource_attributes', type: 'map', doc: "resource_attributes['key'] selects one value" },
];

/** Attributes of a system.query_log row (docs/api/http.md). */
export const QUERY_LOG_ATTRIBUTES = [
  ['query_id', 'string'], ['fingerprint', 'string'], ['status', 'string'],
  ['queue_us', 'int'], ['elapsed_us', 'int'], ['serialize_us', 'int'], ['result_rows', 'int'],
  ['table', 'string'], ['parse_us', 'int'], ['plan_us', 'int'], ['exec_us', 'int'], ['cpu_us', 'int'],
  ['rows_scanned', 'int'], ['rows_matched', 'int'], ['bytes_read', 'int'],
  ['segments_total', 'int'], ['segments_scanned', 'int'], ['memory_rows_scanned', 'int'],
  ['memory_peak', 'int'], ['scan_workers', 'int'],
];

export const TABLES = [
  {
    name: 'logs',
    doc: "The tenant's events",
    columns: EVENT_COLUMNS,
    sample: 'SELECT *\nFROM logs\nORDER BY event_time DESC\nLIMIT 100',
  },
  {
    name: 'system.query_log',
    doc: 'One row per query the tenant ran',
    columns: EVENT_COLUMNS,
    attributes: QUERY_LOG_ATTRIBUTES,
    sample: "SELECT event_time, attributes['fingerprint'] AS fingerprint, attributes['status'] AS status,\n       attributes['elapsed_us'] AS elapsed_us, body\nFROM system.query_log\nORDER BY event_time DESC\nLIMIT 100",
  },
];

export const KEYWORDS = [
  'SELECT', 'FROM', 'WHERE', 'GROUP', 'BY', 'ORDER', 'LIMIT', 'AS', 'ASC', 'DESC', 'AND', 'OR', 'NOT',
  'IN', 'IS', 'NULL', 'MISSING', 'LIKE', 'ILIKE', 'TRUE', 'FALSE', 'TIMESTAMP', 'INTERVAL',
  'EXPLAIN', 'ANALYZE',
];

export const FUNCTIONS = ['COUNT', 'MIN', 'MAX', 'SUM', 'AVG', 'TIME_BUCKET'];
