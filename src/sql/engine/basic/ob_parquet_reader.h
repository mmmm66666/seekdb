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
#include <string>
#include <vector>

#include "sql/engine/basic/ob_file_scan_utils.h"

namespace oceanbase
{
namespace sql
{

// Parquet physical types.
enum ObFileSqlParquetType
{
  OB_PARQUET_TYPE_BOOLEAN = 0,
  OB_PARQUET_TYPE_INT32 = 1,
  OB_PARQUET_TYPE_INT64 = 2,
  OB_PARQUET_TYPE_INT96 = 3,
  OB_PARQUET_TYPE_FLOAT = 4,
  OB_PARQUET_TYPE_DOUBLE = 5,
  OB_PARQUET_TYPE_BYTE_ARRAY = 6,
  OB_PARQUET_TYPE_FIXED_LEN_BYTE_ARRAY = 7,
};

enum ObFileSqlParquetCodec
{
  OB_PARQUET_CODEC_UNCOMPRESSED = 0,
  OB_PARQUET_CODEC_SNAPPY = 1,
  OB_PARQUET_CODEC_GZIP = 2,
  OB_PARQUET_CODEC_LZO = 3,
  OB_PARQUET_CODEC_BROTLI = 4,
  OB_PARQUET_CODEC_LZ4 = 5,
  OB_PARQUET_CODEC_ZSTD = 6,
};

enum ObFileSqlParquetEncoding
{
  OB_PARQUET_ENCODING_PLAIN = 0,
  OB_PARQUET_ENCODING_PLAIN_DICTIONARY = 2,
  OB_PARQUET_ENCODING_RLE = 3,
  OB_PARQUET_ENCODING_BIT_PACKED = 4,
  OB_PARQUET_ENCODING_DELTA_BINARY_PACKED = 5,
  OB_PARQUET_ENCODING_DELTA_LENGTH_BYTE_ARRAY = 6,
  OB_PARQUET_ENCODING_DELTA_BYTE_ARRAY = 7,
  OB_PARQUET_ENCODING_RLE_DICTIONARY = 8,
};

// Decompress one raw snappy block, returns OB_NOT_SUPPORTED on malformed input.
int ob_file_sql_snappy_decompress(const char *src, const int64_t src_len,
                                  std::string &dst, std::string &err_msg);

struct ObFileSqlParquetColumn
{
  ObFileSqlParquetColumn()
    : name_(),
      physical_type_(OB_PARQUET_TYPE_BYTE_ARRAY),
      converted_type_(0),
      max_def_level_(0),
      max_rep_level_(0),
      schema_type_(FILE_SQL_TYPE_VARCHAR)
  {}
  std::string name_;
  int32_t physical_type_;
  int32_t converted_type_;
  int32_t max_def_level_;
  int32_t max_rep_level_;
  ObFileSqlType schema_type_;
};

struct ObFileSqlParquetChunk
{
  ObFileSqlParquetChunk()
    : column_index_(0),
      physical_type_(0),
      codec_(0),
      num_values_(0),
      data_page_offset_(0),
      dictionary_page_offset_(-1),
      total_compressed_size_(0)
  {}
  int64_t column_index_;
  int32_t physical_type_;
  int32_t codec_;
  int64_t num_values_;
  int64_t data_page_offset_;
  int64_t dictionary_page_offset_;
  int64_t total_compressed_size_;
};

struct ObFileSqlParquetRowGroup
{
  ObFileSqlParquetRowGroup() : num_rows_(0), total_byte_size_(0), chunks_() {}
  int64_t num_rows_;
  int64_t total_byte_size_;
  std::vector<ObFileSqlParquetChunk> chunks_;
};

// A reader of a flat parquet file. Multiple row groups, nullable columns and column
// pruning are supported, nested and repeated columns are rejected.
class ObFileSqlParquetReader
{
public:
  ObFileSqlParquetReader();
  ~ObFileSqlParquetReader();

  int open(const std::string &path, const ObFileSqlOptions &options, std::string &err_msg);
  void close();
  int rewind();

  const ObFileSqlSchema &get_schema() const { return schema_; }
  int64_t get_column_count() const { return static_cast<int64_t>(columns_.size()); }
  // Mark the columns that must be decoded, unselected columns are skipped and return
  // NULL cells.
  int set_projection(const std::vector<char> &projected);
  int read_row(std::vector<ObFileSqlCell> &row, bool &eof);

private:
  int load_footer(std::string &err_msg);
  int load_row_group(const int64_t row_group_idx, std::string &err_msg);
  int read_column_pages(const ObFileSqlParquetChunk &chunk,
                        std::vector<bool> &nulls,
                        std::vector<std::string> &values,
                        std::string &err_msg);

private:
  FILE *fp_;
  std::string path_;
  ObFileSqlOptions options_;
  ObFileSqlSchema schema_;
  std::vector<ObFileSqlParquetColumn> columns_;
  std::vector<ObFileSqlParquetRowGroup> row_groups_;
  std::vector<char> projected_;
  // decoded values of the current row group, indexed by column
  std::vector<char> col_loaded_;
  std::vector<std::vector<bool> > col_nulls_;
  std::vector<std::vector<std::string> > col_values_;
  int64_t cur_row_group_;
  int64_t row_in_group_;
  int64_t total_rows_;
  int64_t file_size_;
  bool is_opened_;
};

// Read only the footer schema, used by the resolver.
int ob_file_sql_parquet_read_schema(const std::string &path, ObFileSqlSchema &schema,
                                    std::string &err_msg);

} // namespace sql
} // namespace oceanbase
