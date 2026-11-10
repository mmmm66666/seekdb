# file_sql test suite

Tests of the "file as a database" feature: `read_csv`, `read_json`, `read_ndjson`,
`read_parquet` and `glob()`.

## Layout

```
file_sql/
  t/file_sql_basic.test       mysqltest case
  r/mysql/file_sql_basic.result
  data/                       static input files
```

## Data deployment

`include/file_sql_data.inc` copies `data/` to `/tmp/file_sql_data` with `--exec`
(mysqltest and the observer run on the same host in the CI runner), and
`include/file_sql_data_cleanup.inc` removes it again. The queries therefore use plain
absolute paths such as `/tmp/file_sql_data/people.csv`.

Nothing is transferred with `ssh`/`scp`, and no environment variable is expanded inside
the SQL text, both of which would make the suite depend on the deployment layout.

## Test style

The case is an end to end smoke test: every statement has to execute successfully, but
the result sets are not compared. The expected output is produced only by the `--echo`
markers, so it cannot drift with value formatting, column widths or the observer's data
directory.

That means a red run reports a broken SQL path (parse, resolve, plan or execution) but
not a wrong value. Value level assertions are done in
`unittest/sql/engine/basic/test_file_scan_utils.cpp`, where the readers are called
directly.

## Running

The runner starts the session with the feature disabled, so the case enables it:

```sql
SET enable_file_sql = 1;
```

`secure_file_priv` defaults to `/`, which allows every absolute path. A test that wants
to check the restriction can narrow it, for example
`SET GLOBAL secure_file_priv = '/data';`, and verify that a file outside of that
directory is rejected.

## Data files

| file | purpose |
| --- | --- |
| `people.csv` | header, quoted field with a comma, an empty (NULL) field, `BIGINT` / `VARCHAR` / `DOUBLE` / `BOOLEAN` / `DATE` columns |
| `simple.csv` | minimal CSV, used by the `glob()` case and by `header => false` |
| `semicolon.csv` | `;` separated, quoted value that contains the delimiter |
| `crlf.csv` | CRLF line endings |
| `notes.csv` | LF with a newline inside a quoted field |
| `employees.jsonl` | keys in a different order per row, a missing key, used for the field union and the cross-format join |
| `kv.jsonl` | minimal JSON lines |

## Coverage

* feature switch (`enable_file_sql`)
* CSV: inferred schema, projection, expression, `WHERE`, `ORDER BY`, `LIMIT`,
  `GROUP BY`, `HAVING`, `COUNT` / `SUM` / `MIN` / `MAX` / `AVG`
* CSV: quoting, embedded newline, CRLF, named arguments (`delimiter`, `delim`,
  `header`, `sample_size`, `nullstr`)
* JSON lines: field union, missing fields as NULL, `read_json` and `read_ndjson`
* `glob()` file discovery, column named `file`
* `JOIN`, `LEFT JOIN` and a cross-format join
* `EXPLAIN` / `DESCRIBE` of a file query (column metadata resolution)
