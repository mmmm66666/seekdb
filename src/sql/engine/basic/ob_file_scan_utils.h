/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <stdint.h>
#include <stdio.h>
#include <string>
#include <utility>
#include <vector>

#include "lib/string/ob_string.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace sql
{

// File table function formats, the value is persisted in the physical plan, so the
// numeric value of each enumerator must stay stable.
enum ObFileSqlFormat
{
  FILE_SQL_FORMAT_CSV = 0,
  FILE_SQL_FORMAT_JSON = 1,
  FILE_SQL_FORMAT_NDJSON = 2,
  FILE_SQL_FORMAT_PARQUET = 3,
  FILE_SQL_FORMAT_GLOB = 4,
  FILE_SQL_FORMAT_MAX = 5,
};

// Logical type of a column inferred from file content.
enum ObFileSqlType
{
  FILE_SQL_TYPE_NULL = 0,
  FILE_SQL_TYPE_BOOLEAN = 1,
  FILE_SQL_TYPE_BIGINT = 2,
  FILE_SQL_TYPE_DOUBLE = 3,
  FILE_SQL_TYPE_VARCHAR = 4,
  FILE_SQL_TYPE_DATE = 5,
  FILE_SQL_TYPE_DATETIME = 6,
  FILE_SQL_TYPE_MAX = 7,
};

const char *ob_file_sql_format_name(const ObFileSqlFormat format);
const char *ob_file_sql_type_name(const ObFileSqlType type);
// Widen two types, the result is the smallest type that can hold both values.
ObFileSqlType ob_file_sql_widen_type(const ObFileSqlType left, const ObFileSqlType right);
// Try to parse a text value as the given type, return true when the value conforms.
bool ob_file_sql_value_match_type(const char *str, const int64_t len, const ObFileSqlType type);
// Strict date/datetime parsing, used by type inference and by the scan operator.
// The date is returned as days since 1970-01-01, the datetime as microseconds since
// 1970-01-01 00:00:00 which is the internal representation of the storage layer.
int ob_file_sql_parse_date(const char *str, const int64_t len, int32_t &days);
int ob_file_sql_parse_datetime(const char *str, const int64_t len, int64_t &usec);
// Parse a full text value into the storage representation of the target type.
int ob_file_sql_parse_int64(const char *str, const int64_t len, int64_t &value);
int ob_file_sql_parse_double(const char *str, const int64_t len, double &value);
int ob_file_sql_parse_bool(const char *str, const int64_t len, bool &value);
// Nullability: an empty, unquoted CSV field is treated as NULL by default.
bool ob_file_sql_is_blank(const char *str, const int64_t len);

struct ObFileSqlColumnSchema
{
  ObFileSqlColumnSchema() : name_(), type_(FILE_SQL_TYPE_NULL) {}
  std::string name_;
  ObFileSqlType type_;
};

struct ObFileSqlSchema
{
  ObFileSqlSchema() : columns_() {}
  void reset() { columns_.clear(); }
  int64_t count() const { return static_cast<int64_t>(columns_.size()); }
  const ObFileSqlColumnSchema &at(const int64_t i) const
  {
    return columns_[static_cast<size_t>(i)];
  }
  std::vector<ObFileSqlColumnSchema> columns_;
};

// Default resource limits, they prevent a single file from exhausting server memory.
const int64_t OB_FILE_SQL_DEFAULT_MAX_LINE_SIZE = 64L * 1024L * 1024L;   // 64MB
const int64_t OB_FILE_SQL_DEFAULT_MAX_COLUMNS = 4096;
const int64_t OB_FILE_SQL_DEFAULT_MAX_RECORDS = 0;                       // 0 means unlimited
const int64_t OB_FILE_SQL_MAX_GLOB_FILES = 100000;

// Reading options of one file function invocation. The struct can be encoded into a
// single string so that it can be carried by the physical plan spec.
struct ObFileSqlOptions
{
  ObFileSqlOptions()
    : format_(FILE_SQL_FORMAT_CSV),
      delimiter_(','),
      quote_('"'),
      escape_('"'),
      null_str_(),
      has_header_(true),
      header_explicit_(false),
      sample_size_(-1),
      max_line_size_(OB_FILE_SQL_DEFAULT_MAX_LINE_SIZE),
      max_columns_(OB_FILE_SQL_DEFAULT_MAX_COLUMNS),
      max_records_(OB_FILE_SQL_DEFAULT_MAX_RECORDS),
      null_unquoted_only_(true),
      trim_space_(false)
  {}

  void reset() { *this = ObFileSqlOptions(); }

  // Apply one named argument, name is case insensitive.
  int set_named_arg(const std::string &name, const std::string &value, std::string &err_msg);
  // Whether the field should be treated as NULL.
  bool is_null_field(const char *str, const int64_t len, const bool quoted) const;
  // Serialize / deserialize the options carried by the physical plan.
  int encode(std::string &out) const;
  int decode(const std::string &in, std::string &err_msg);

  ObFileSqlFormat format_;
  char delimiter_;
  char quote_;
  char escape_;
  std::string null_str_;
  bool has_header_;
  bool header_explicit_;
  int64_t sample_size_;
  int64_t max_line_size_;
  int64_t max_columns_;
  int64_t max_records_;
  bool null_unquoted_only_;
  bool trim_space_;
};

// One cell of a file row. Values are normalized to text so that all formats share the
// same conversion path in the executor, NULL is kept as an explicit flag.
struct ObFileSqlCell
{
  ObFileSqlCell() : is_null_(true), quoted_(false), str_() {}
  void reset() { is_null_ = true; quoted_ = false; str_.clear(); }
  bool is_null_;
  bool quoted_;
  std::string str_;
};

// File identity used to detect that a file changed between the resolve phase and the
// execution phase.
struct ObFileSqlFileStamp
{
  ObFileSqlFileStamp() : exists_(false), size_(0), mtime_sec_(0), mtime_nsec_(0) {}
  bool exists_;
  int64_t size_;
  int64_t mtime_sec_;
  int64_t mtime_nsec_;
  bool same_as(const ObFileSqlFileStamp &other) const
  {
    return exists_ == other.exists_ && size_ == other.size_ &&
           mtime_sec_ == other.mtime_sec_ && mtime_nsec_ == other.mtime_nsec_;
  }
};

int ob_file_sql_get_file_stamp(const std::string &path, ObFileSqlFileStamp &stamp);

// Expand a glob pattern, only '*' and '?' are supported, matched files are returned in
// ascending lexicographical order.
int ob_file_sql_glob(const std::string &pattern, std::vector<std::string> &files,
                     std::string &err_msg);

// Streaming reader of one file. The same class serves schema inference and execution,
// it never materializes the whole file.
class ObFileSqlReader
{
public:
  ObFileSqlReader();
  ~ObFileSqlReader();

  // Open the file and derive the schema. When expected_schema is not NULL the derived
  // schema must be identical to it, this detects a file change between resolve and
  // execute.
  int open(const std::string &path,
           const ObFileSqlOptions &options,
           const ObFileSqlSchema *expected_schema);
  // Read one row, eof is set when the file is exhausted.
  int next_row(std::vector<ObFileSqlCell> &row, bool &eof);
  // Rewind to the first data row, used by rescan.
  int rewind();
  // Restrict the columns that must be decoded, only honored by the parquet reader.
  int set_projection(const std::vector<char> &projected);
  void close();

  const ObFileSqlSchema &get_schema() const { return schema_; }
  const ObFileSqlOptions &get_options() const { return options_; }
  const std::string &get_error_msg() const { return err_msg_; }
  int64_t get_row_index() const { return row_index_; }
  int64_t get_scanned_bytes() const { return scanned_bytes_; }
  bool is_opened() const { return is_opened_; }
  const ObFileSqlFileStamp &get_stamp() const { return stamp_; }

  // Used by the glob() table function, the schema is a single varchar column.
  int open_glob(const std::string &pattern, const ObFileSqlOptions &options);
  int next_glob_row(std::vector<ObFileSqlCell> &row, bool &eof);

private:
  int detect_csv_schema(ObFileSqlSchema &schema, std::string &err_msg);
  int detect_jsonl_schema(ObFileSqlSchema &schema, std::string &err_msg);
  int detect_parquet_schema(ObFileSqlSchema &schema, std::string &err_msg);

  int open_csv();
  int next_csv_row(std::vector<ObFileSqlCell> &row, bool &eof);
  int read_csv_record(std::vector<ObFileSqlCell> &row, bool &eof);
  int open_jsonl();
  int next_jsonl_row(std::vector<ObFileSqlCell> &row, bool &eof);
  int open_parquet();
  int next_parquet_row(std::vector<ObFileSqlCell> &row, bool &eof);

  int read_more();
  int ensure_buffered(const int64_t need);
  int peek_byte(char &ch, bool &eof);
  int take_byte(char &ch, bool &eof);
  int skip_bom();
  int read_text_line(std::string &line, bool &eof);
  int report_error(const int ret, const char *fmt, ...);
  int reset_stream();

private:
  bool is_opened_;
  ObFileSqlFormat format_;
  ObFileSqlOptions options_;
  ObFileSqlSchema schema_;
  ObFileSqlFileStamp stamp_;
  std::string path_;
  std::string err_msg_;
  int64_t row_index_;
  int64_t scanned_bytes_;

  // buffered input shared by csv and jsonl
  FILE *fp_;
  std::vector<char> buf_;
  int64_t buf_pos_;
  int64_t buf_len_;
  bool file_eof_;

  // csv state
  bool csv_header_consumed_;
  std::vector<std::string> csv_header_;

  // jsonl state
  std::vector<std::string> json_keys_;

  // parquet state, implemented in ob_parquet_reader.cpp
  void *parquet_;
  std::vector<std::string> glob_files_;
};

// Serialize / deserialize the inferred schema, it is carried by the physical plan so
// that a file changed between resolve and execute can be detected.
int ob_file_sql_encode_schema(const ObFileSqlSchema &schema, std::string &out);
int ob_file_sql_decode_schema(const std::string &in, ObFileSqlSchema &schema);

// Entry point used by the resolver: open the file, infer the schema and validate that
// the options are legal for the format.
int ob_file_sql_infer_schema(const std::string &path,
                             const ObFileSqlOptions &options,
                             ObFileSqlSchema &schema,
                             ObFileSqlFileStamp &stamp,
                             std::string &err_msg);

// CSV helpers exposed for testing.
int ob_file_sql_split_csv_record(const char *data, const int64_t len,
                                 const ObFileSqlOptions &options,
                                 std::vector<ObFileSqlCell> &fields,
                                 bool &has_unescaped_newline);
} // namespace sql
} // namespace oceanbase
