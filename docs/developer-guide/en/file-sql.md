# File as a database (file SQL)

seekdb can read local text and columnar files directly inside a SQL statement. The
files do not need to be imported and no table has to be created first: `read_csv`,
`read_json`, `read_ndjson` and `read_parquet` are table functions that expose the
file content as a table, and `glob()` lists files. Filtering, projection, ordering,
aggregation and joins are still executed by the seekdb SQL engine.

## Enabling the feature

The feature is disabled by default. Enable it for the session (or for the tenant):

```sql
SET enable_file_sql = 1;         -- current session
SET GLOBAL enable_file_sql = 1;  -- new sessions of the tenant
```

Even when the feature is enabled, every path has to be allowed by `secure_file_priv`
(the same check that protects `LOAD DATA` and `SELECT ... INTO OUTFILE`). The default
value is `/`, so every absolute path is allowed; set it to a directory to restrict the
accessible files:

```sql
SET GLOBAL secure_file_priv = '/data/file_sql';
```

The check is performed when the statement is resolved, on the canonicalized path, so
`..` escapes and symbolic links cannot be used to leave the allowed directory.

## Reading files

```sql
SELECT * FROM read_csv('/data/people.csv');
SELECT count(*) FROM read_csv('/data/people.csv');
SELECT id, name FROM read_csv('/data/people.csv') WHERE id > 10 ORDER BY id DESC LIMIT 5;
SELECT dept, count(*), avg(score) FROM read_csv('/data/people.csv') GROUP BY dept;
SELECT * FROM read_ndjson('/data/events.jsonl');
SELECT * FROM read_parquet('/data/events.parquet');
```

Each read function is a normal table source, therefore an alias, a column list, a
`JOIN` (including a join across formats) and the usual SQL clauses work as usual:

```sql
SELECT c.id, o.amount
FROM read_csv('/data/customers.csv') AS c
JOIN read_parquet('/data/orders.parquet') AS o ON c.id = o.customer_id
WHERE o.amount > 100;
```

### Named arguments

The first argument is always the file path, the remaining arguments are named with
`name => value` and are optional:

| argument | meaning | default |
| --- | --- | --- |
| `delimiter` (`delim`, `sep`) | field separator | `,` |
| `quote` | quote character | `"` |
| `escape` | escape character | `"` (doubled quote) |
| `header` | the first line contains column names | `true` |
| `null_str` (`nullstr`) | text that represents NULL | empty field |
| `sample_size` | must be `-1`, full-file inference | `-1` |
| `max_line_size` | maximum size of one line/field | 64MB |
| `max_columns` | maximum number of columns | 4096 |
| `max_records` | stop after this many records, `0` means all | `0` |

```sql
SELECT * FROM read_csv('/data/t.csv', delimiter => ';', nullstr => 'NA');
SELECT * FROM read_csv('/data/t.csv', header => false);
```

CSV and JSON lines are fully scanned to derive the schema, which is why `sample_size`
only accepts `-1`.

### Listing files

`glob()` only discovers files, it does not read them. It returns a single column
named `file` with the matching paths, sorted in ascending order:

```sql
SELECT * FROM glob('/data/*.csv');
```

`*` and `?` are supported in the file name part of the pattern.

### Column metadata

The columns of a file table are resolved at statement resolution time from the file
content, so they are visible to `EXPLAIN` / `DESCRIBE`:

```sql
EXPLAIN SELECT * FROM read_csv('/data/people.csv');
DESCRIBE SELECT * FROM read_csv('/data/people.csv');
```

Because the schema is read from the file, a change of the file between the resolve and
the execution phase is detected and reported instead of returning inconsistent data.

## Type inference

The reader infers one of the following types for each column:

| inferred type | SQL type | notes |
| --- | --- | --- |
| BIGINT | `bigint` | `INT64_MIN` .. `INT64_MAX` are accepted exactly |
| DOUBLE | `double` | values that do not fit a bigint or carry a fraction |
| BOOLEAN | `tinyint(1)` | `true` / `false` |
| VARCHAR | `varchar` | fallback for any other text |
| DATE | `date` | `YYYY-MM-DD`, validated (month, day, leap year) |
| DATETIME | `datetime` | `YYYY-MM-DD HH:MM:SS[.ffffff]` |
| NULL | `varchar` | a column that is NULL in every row |

The inference is deterministic and uses the full file:

* `NULL` widens to any other type;
* `BIGINT` and `DOUBLE` widen to `DOUBLE`;
* `DATE` and `DATETIME` widen to `DATETIME`;
* every other combination widens to `VARCHAR`.

CSV values are classified as boolean only for `true` / `false`, so a column of `1`/`0`
stays numeric. Quoted values are never treated as NULL.

JSON lines values are typed from the JSON value: integer numbers are `BIGINT`, numbers
with a fraction or an exponent are `DOUBLE`, booleans are `BOOLEAN`, strings are
`VARCHAR` and `null` is `NULL`. The keys of all rows are unioned in their first
appearance order, a key that is missing in a row is NULL. Type conflicts widen the
same way as for CSV. Nested objects and arrays are rejected with the line number and
the key that caused the problem, because only flat schemas are supported.

## Reading details

* UTF-8 is assumed; a UTF-8 BOM at the beginning of a file is skipped.
* `LF` and `CRLF` line endings are both accepted, a final line without a newline is
  accepted too.
* A quoted CSV field may contain the delimiter, quotes (doubled or escaped) and
  newlines.
* An empty unquoted field is NULL. When `nullstr` is set, the exact text is matched
  instead.
* A row with a different number of fields than the header is reported with its line
  number and the expected/actual field counts.
* Files are read in a streaming fashion in 64KB chunks. Only the current row (and, for
  parquet, the current row group of the projected columns) is kept in memory, so the
  memory usage does not grow with the file size.
* Parquet files are read directly: the footer schema, multiple row groups, nullable
  columns, plain and dictionary encoding and Snappy compression are supported. Only the
  columns referenced by the query are decoded (column pruning). Nested and repeated
  columns are rejected.

## Exporting a CSV file

`SELECT ... INTO OUTFILE` remains the supported way to write a CSV file that can be
read back with `read_csv`.

## Limitations

* A file table is read only; `INSERT` / `UPDATE` / `DELETE` on it is not supported.
* `sample_size` values other than `-1` are rejected.
* Nested structures (JSON objects/arrays, nested or repeated parquet columns) are not
  supported.
* Multi-file merging and remote files are not supported; `glob()` only lists files.
