# StrataQL v0 grammar (normative)

Implementation: `crates/stratadb-query/src/parser.rs`. Anything not in this
grammar is rejected with `invalid_input` and a byte offset. Keywords are
case-insensitive. `--` starts a comment to end of line.

```ebnf
statement   = [ "EXPLAIN" [ "ANALYZE" ] ] query [ ";" ] ;
query       = "SELECT" ( "*" | item { "," item } )
              "FROM" table
              [ "WHERE" expr ]
              [ "GROUP" "BY" expr { "," expr } ]
              [ "ORDER" "BY" order { "," order } ]
              [ "LIMIT" integer ] ;
table       = "logs" | "system" "." "query_log" ;
item        = expr [ "AS" ident ] ;
order       = ( expr | integer | alias ) [ "ASC" | "DESC" ] ;
expr        = and { "OR" and } ;
and         = not { "AND" not } ;
not         = "NOT" not | predicate ;
predicate   = primary [ cmp primary
                      | "IS" [ "NOT" ] ( "NULL" | "MISSING" )
                      | [ "NOT" ] "IN" "(" literal { "," literal } ")"
                      | [ "NOT" ] ( "LIKE" | "ILIKE" ) string ] ;
cmp         = "=" | "==" | "!=" | "<>" | "<" | "<=" | ">" | ">=" ;
primary     = literal | column | aggregate | bucket | "(" expr ")" ;
literal     = string | [ "-" ] number | "TRUE" | "FALSE" | "NULL"
            | "TIMESTAMP" string ;
column      = canonical | ( "attributes" | "resource_attributes" ) "[" string "]" ;
canonical   = "event_time" | "ingest_time" | "service" | "severity"
            | "severity_number" | "severity_text" | "body" | "trace_id"
            | "span_id" | "event_id" | "tenant_id" | "schema_version"
            | "attributes" | "resource_attributes" ;
aggregate   = "COUNT" "(" ( "*" | expr ) ")" | ( "MIN" | "MAX" | "SUM" | "AVG" ) "(" expr ")" ;
bucket      = "time_bucket" "(" "INTERVAL" string "," expr ")" ;
string      = "'" { char | "''" } "'" ;
```

`TIMESTAMP` strings are RFC 3339; without a zone designator they are UTC.
`INTERVAL` strings are `<positive integer> <unit>` with units
`ns, us, ms, s|second(s), m|min|minute(s), h|hour(s), d|day(s)`.

Limits (defaults, `ParseLimits`): query ≤ 64 KiB, expression nesting ≤ 64,
IN list ≤ 10 000 items.

Not supported (rejected): joins, subqueries, arithmetic, `HAVING`, `DISTINCT`,
`OFFSET`, window functions, regex, functions other than those listed, any
statement other than `SELECT`/`EXPLAIN`.
