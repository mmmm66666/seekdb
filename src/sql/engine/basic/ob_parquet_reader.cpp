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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/basic/ob_parquet_reader.h"

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>

namespace oceanbase
{
namespace sql
{

namespace
{

const int32_t PARQUET_CONVERTED_UTF8 = 0;
const int32_t PARQUET_CONVERTED_DATE = 6;
const int32_t PARQUET_CONVERTED_TIMESTAMP_MILLIS = 9;
const int32_t PARQUET_CONVERTED_TIMESTAMP_MICROS = 10;

const int32_t PARQUET_PAGE_DATA = 0;
const int32_t PARQUET_PAGE_INDEX = 1;
const int32_t PARQUET_PAGE_DICTIONARY = 2;
const int32_t PARQUET_PAGE_DATA_V2 = 3;

const int32_t PARQUET_ENCODING_PLAIN = 0;
const int32_t PARQUET_ENCODING_PLAIN_DICTIONARY = 2;
const int32_t PARQUET_ENCODING_RLE_DICTIONARY = 8;

const uint8_t THRIFT_STOP = 0;
const uint8_t THRIFT_BOOL_TRUE = 1;
const uint8_t THRIFT_BOOL_FALSE = 2;
const uint8_t THRIFT_BYTE = 3;
const uint8_t THRIFT_I16 = 4;
const uint8_t THRIFT_I32 = 5;
const uint8_t THRIFT_I64 = 6;
const uint8_t THRIFT_DOUBLE = 7;
const uint8_t THRIFT_BINARY = 8;
const uint8_t THRIFT_LIST = 9;
const uint8_t THRIFT_SET = 10;
const uint8_t THRIFT_MAP = 11;
const uint8_t THRIFT_STRUCT = 12;

static int64_t ob_file_sql_floor_div(const int64_t value, const int64_t divisor)
{
  int64_t quotient = value / divisor;
  if ((value % divisor != 0) && ((value < 0) != (divisor < 0))) {
    --quotient;
  }
  return quotient;
}

static void ob_file_sql_format_days(int64_t days, std::string &out)
{
  days += 719468;
  const int64_t era = (days >= 0 ? days : days - 146096) / 146097;
  const int64_t doe = days - era * 146097;
  const int64_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
  int64_t year = yoe + era * 400;
  const int64_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
  const int64_t mp = (5 * doy + 2) / 153;
  const int64_t day = doy - (153 * mp + 2) / 5 + 1;
  const int64_t month = mp + (mp < 10 ? 3 : -9);
  year += (month <= 2 ? 1 : 0);
  char buf[32];
  snprintf(buf, sizeof(buf), "%04lld-%02lld-%02lld",
           static_cast<long long>(year), static_cast<long long>(month),
           static_cast<long long>(day));
  out.assign(buf);
}

static void ob_file_sql_format_datetime_us(int64_t usec, std::string &out)
{
  const int64_t days = ob_file_sql_floor_div(usec, 86400000000LL);
  int64_t rest = usec - days * 86400000000LL;
  if (rest < 0) {
    rest += 86400000000LL;
  }
  const int64_t hour = rest / 3600000000LL;
  rest -= hour * 3600000000LL;
  const int64_t minute = rest / 60000000LL;
  rest -= minute * 60000000LL;
  const int64_t second = rest / 1000000LL;
  const int64_t micro = rest - second * 1000000LL;
  std::string date;
  ob_file_sql_format_days(days, date);
  char buf[64];
  if (0 == micro) {
    snprintf(buf, sizeof(buf), "%s %02lld:%02lld:%02lld", date.c_str(),
             static_cast<long long>(hour), static_cast<long long>(minute),
             static_cast<long long>(second));
  } else {
    char frac[16];
    snprintf(frac, sizeof(frac), "%06lld", static_cast<long long>(micro));
    int64_t frac_len = 6;
    while (frac_len > 1 && '0' == frac[frac_len - 1]) {
      --frac_len;
    }
    frac[frac_len] = '\0';
    snprintf(buf, sizeof(buf), "%s %02lld:%02lld:%02lld.%s", date.c_str(),
             static_cast<long long>(hour), static_cast<long long>(minute),
             static_cast<long long>(second), frac);
  }
  out.assign(buf);
}

static uint32_t ob_file_sql_load_le32(const uint8_t *data)
{
  return static_cast<uint32_t>(data[0]) | (static_cast<uint32_t>(data[1]) << 8) |
         (static_cast<uint32_t>(data[2]) << 16) | (static_cast<uint32_t>(data[3]) << 24);
}

static uint64_t ob_file_sql_load_le64(const uint8_t *data)
{
  uint64_t value = 0;
  for (int i = 7; i >= 0; --i) {
    value = (value << 8) | static_cast<uint64_t>(data[i]);
  }
  return value;
}

// Minimal reader of the thrift compact protocol, only the subset used by parquet
// metadata and page headers is implemented.
class ObFileSqlThriftReader
{
public:
  ObFileSqlThriftReader() : data_(NULL), len_(0), pos_(0), last_field_id_(0) {}
  void init(const uint8_t *data, const int64_t len)
  {
    data_ = data;
    len_ = len;
    pos_ = 0;
    last_field_id_ = 0;
  }
  int64_t pos() const { return pos_; }
  bool is_valid() const { return NULL != data_; }

  int read_byte(uint8_t &value)
  {
    int ret = OB_SUCCESS;
    if (pos_ >= len_) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      value = data_[pos_++];
    }
    return ret;
  }

  int read_varint(uint64_t &value)
  {
    int ret = OB_SUCCESS;
    int shift = 0;
    value = 0;
    bool done = false;
    while (OB_SUCC(ret) && !done) {
      uint8_t byte = 0;
      if (OB_FAIL(read_byte(byte))) {
      } else if (shift > 63) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        value |= (static_cast<uint64_t>(byte & 0x7f) << shift);
        if (0 == (byte & 0x80)) {
          done = true;
        } else {
          shift += 7;
        }
      }
    }
    return ret;
  }

  int read_zigzag(int64_t &value)
  {
    int ret = OB_SUCCESS;
    uint64_t raw = 0;
    if (OB_FAIL(read_varint(raw))) {
    } else {
      value = static_cast<int64_t>((raw >> 1) ^ (~(raw & 1) + 1));
    }
    return ret;
  }

  int skip_bytes(const int64_t count)
  {
    int ret = OB_SUCCESS;
    if (count < 0 || pos_ + count > len_) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      pos_ += count;
    }
    return ret;
  }

  int read_binary(std::string &value)
  {
    int ret = OB_SUCCESS;
    uint64_t size = 0;
    value.clear();
    if (OB_FAIL(read_varint(size))) {
    } else if (static_cast<int64_t>(size) > len_ - pos_) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      value.assign(reinterpret_cast<const char *>(data_ + pos_), static_cast<size_t>(size));
      pos_ += static_cast<int64_t>(size);
    }
    return ret;
  }

  int read_field_header(uint8_t &type, int16_t &field_id)
  {
    int ret = OB_SUCCESS;
    uint8_t byte = 0;
    if (OB_FAIL(read_byte(byte))) {
    } else {
      type = static_cast<uint8_t>(byte & 0x0f);
      const uint8_t delta = static_cast<uint8_t>((byte & 0xf0) >> 4);
      if (THRIFT_STOP == type) {
        field_id = 0;
      } else if (0 == delta) {
        int64_t wide_id = 0;
        if (OB_FAIL(read_zigzag(wide_id))) {
        } else {
          field_id = static_cast<int16_t>(wide_id);
          last_field_id_ = field_id;
        }
      } else {
        field_id = static_cast<int16_t>(last_field_id_ + delta);
        last_field_id_ = field_id;
      }
    }
    return ret;
  }

  int read_list_header(uint8_t &elem_type, int64_t &size)
  {
    int ret = OB_SUCCESS;
    uint8_t byte = 0;
    if (OB_FAIL(read_byte(byte))) {
    } else {
      elem_type = static_cast<uint8_t>(byte & 0x0f);
      size = static_cast<int64_t>((byte & 0xf0) >> 4);
      if (15 == size) {
        uint64_t wide_size = 0;
        if (OB_FAIL(read_varint(wide_size))) {
        } else {
          size = static_cast<int64_t>(wide_size);
        }
      }
      last_field_id_ = 0;
    }
    return ret;
  }

  int skip(const uint8_t type)
  {
    int ret = OB_SUCCESS;
    switch (type) {
      case THRIFT_STOP:
        break;
      case THRIFT_BOOL_TRUE:
      case THRIFT_BOOL_FALSE:
        break;
      case THRIFT_BYTE:
        ret = skip_bytes(1);
        break;
      case THRIFT_I16:
      case THRIFT_I32:
      case THRIFT_I64: {
        uint64_t raw = 0;
        ret = read_varint(raw);
        break;
      }
      case THRIFT_DOUBLE:
        ret = skip_bytes(8);
        break;
      case THRIFT_BINARY: {
        std::string ignored;
        ret = read_binary(ignored);
        break;
      }
      case THRIFT_LIST:
      case THRIFT_SET: {
        uint8_t elem_type = 0;
        int64_t size = 0;
        if (OB_FAIL(read_list_header(elem_type, size))) {
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
            ret = skip(elem_type);
          }
        }
        break;
      }
      case THRIFT_MAP: {
        uint8_t byte = 0;
        if (OB_FAIL(read_byte(byte))) {
        } else {
          const uint8_t key_type = static_cast<uint8_t>((byte & 0xf0) >> 4);
          const uint8_t value_type = static_cast<uint8_t>(byte & 0x0f);
          int64_t size = 0;
          uint64_t wide_size = 0;
          if (OB_FAIL(read_varint(wide_size))) {
          } else {
            size = static_cast<int64_t>(wide_size);
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
            if (OB_FAIL(skip(key_type))) {
            } else {
              ret = skip(value_type);
            }
          }
        }
        break;
      }
      case THRIFT_STRUCT: {
        bool done = false;
        while (OB_SUCC(ret) && !done) {
          uint8_t field_type = 0;
          int16_t field_id = 0;
          if (OB_FAIL(read_field_header(field_type, field_id))) {
          } else if (THRIFT_STOP == field_type) {
            done = true;
          } else {
            ret = skip(field_type);
          }
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        break;
    }
    return ret;
  }

private:
  const uint8_t *data_;
  int64_t len_;
  int64_t pos_;
  int16_t last_field_id_;
};

struct ObFileSqlParquetSchemaElement
{
  ObFileSqlParquetSchemaElement()
    : type_(-1), type_length_(0), repetition_type_(-1), name_(), num_children_(0),
      converted_type_(-1)
  {}
  int32_t type_;
  int32_t type_length_;
  int32_t repetition_type_;
  std::string name_;
  int32_t num_children_;
  int32_t converted_type_;
};

int read_schema_element(ObFileSqlThriftReader &reader,
                        ObFileSqlParquetSchemaElement &element)
{
  int ret = OB_SUCCESS;
  bool done = false;
  while (OB_SUCC(ret) && !done) {
    uint8_t type = 0;
    int16_t field_id = 0;
    if (OB_FAIL(reader.read_field_header(type, field_id))) {
    } else if (THRIFT_STOP == type) {
      done = true;
    } else if (1 == field_id && THRIFT_I32 == type) {
      int64_t value = 0;
      if (OB_FAIL(reader.read_zigzag(value))) {
      } else {
        element.type_ = static_cast<int32_t>(value);
      }
    } else if (2 == field_id && THRIFT_I32 == type) {
      int64_t value = 0;
      if (OB_FAIL(reader.read_zigzag(value))) {
      } else {
        element.type_length_ = static_cast<int32_t>(value);
      }
    } else if (3 == field_id && THRIFT_I32 == type) {
      int64_t value = 0;
      if (OB_FAIL(reader.read_zigzag(value))) {
      } else {
        element.repetition_type_ = static_cast<int32_t>(value);
      }
    } else if (4 == field_id && THRIFT_BINARY == type) {
      ret = reader.read_binary(element.name_);
    } else if (5 == field_id && THRIFT_I32 == type) {
      int64_t value = 0;
      if (OB_FAIL(reader.read_zigzag(value))) {
      } else {
        element.num_children_ = static_cast<int32_t>(value);
      }
    } else if (6 == field_id && THRIFT_I32 == type) {
      int64_t value = 0;
      if (OB_FAIL(reader.read_zigzag(value))) {
      } else {
        element.converted_type_ = static_cast<int32_t>(value);
      }
    } else if (10 == field_id && THRIFT_STRUCT == type) {
      // logicalType, only the DATE / TIMESTAMP / STRING members are interpreted
      bool logical_done = false;
      while (OB_SUCC(ret) && !logical_done) {
        uint8_t logical_type = 0;
        int16_t logical_id = 0;
        if (OB_FAIL(reader.read_field_header(logical_type, logical_id))) {
        } else if (THRIFT_STOP == logical_type) {
          logical_done = true;
        } else if (1 == logical_id && THRIFT_STRUCT == logical_type) {
          // STRING
          std::string ignored;
          (void)ignored;
          if (element.converted_type_ < 0) {
            element.converted_type_ = PARQUET_CONVERTED_UTF8;
          }
          ret = reader.skip(THRIFT_STRUCT);
        } else if (6 == logical_id && THRIFT_STRUCT == logical_type) {
          if (element.converted_type_ < 0) {
            element.converted_type_ = PARQUET_CONVERTED_DATE;
          }
          ret = reader.skip(THRIFT_STRUCT);
        } else if (8 == logical_id && THRIFT_STRUCT == logical_type) {
          bool timestamp_done = false;
          int32_t unit = 0;
          while (OB_SUCC(ret) && !timestamp_done) {
            uint8_t ts_type = 0;
            int16_t ts_id = 0;
            if (OB_FAIL(reader.read_field_header(ts_type, ts_id))) {
            } else if (THRIFT_STOP == ts_type) {
              timestamp_done = true;
            } else if (2 == ts_id && THRIFT_STRUCT == ts_type) {
              bool unit_done = false;
              while (OB_SUCC(ret) && !unit_done) {
                uint8_t unit_type = 0;
                int16_t unit_id = 0;
                if (OB_FAIL(reader.read_field_header(unit_type, unit_id))) {
                } else if (THRIFT_STOP == unit_type) {
                  unit_done = true;
                } else if (1 == unit_id) {
                  unit = 1;
                  ret = reader.skip(unit_type);
                } else if (2 == unit_id) {
                  unit = 2;
                  ret = reader.skip(unit_type);
                } else if (3 == unit_id) {
                  unit = 3;
                  ret = reader.skip(unit_type);
                } else {
                  ret = reader.skip(unit_type);
                }
              }
            } else {
              ret = reader.skip(ts_type);
            }
          }
          if (OB_SUCC(ret) && element.converted_type_ < 0) {
            element.converted_type_ = (1 == unit) ? PARQUET_CONVERTED_TIMESTAMP_MILLIS
                                                  : PARQUET_CONVERTED_TIMESTAMP_MICROS;
          }
        } else {
          ret = reader.skip(logical_type);
        }
      }
    } else {
      ret = reader.skip(type);
    }
  }
  return ret;
}

struct ObFileSqlParquetColumnMeta
{
  ObFileSqlParquetColumnMeta()
    : type_(-1), codec_(0), num_values_(0), total_uncompressed_size_(0),
      total_compressed_size_(0), data_page_offset_(0), dictionary_page_offset_(-1)
  {}
  int32_t type_;
  int32_t codec_;
  int64_t num_values_;
  int64_t total_uncompressed_size_;
  int64_t total_compressed_size_;
  int64_t data_page_offset_;
  int64_t dictionary_page_offset_;
};

int read_column_meta(ObFileSqlThriftReader &reader, ObFileSqlParquetColumnMeta &meta)
{
  int ret = OB_SUCCESS;
  bool done = false;
  while (OB_SUCC(ret) && !done) {
    uint8_t type = 0;
    int16_t field_id = 0;
    if (OB_FAIL(reader.read_field_header(type, field_id))) {
    } else if (THRIFT_STOP == type) {
      done = true;
    } else if (1 == field_id && THRIFT_I32 == type) {
      int64_t value = 0;
      if (OB_FAIL(reader.read_zigzag(value))) {
      } else {
        meta.type_ = static_cast<int32_t>(value);
      }
    } else if (4 == field_id && THRIFT_I32 == type) {
      int64_t value = 0;
      if (OB_FAIL(reader.read_zigzag(value))) {
      } else {
        meta.codec_ = static_cast<int32_t>(value);
      }
    } else if (5 == field_id && THRIFT_I64 == type) {
      int64_t value = 0;
      if (OB_FAIL(reader.read_zigzag(value))) {
      } else {
        meta.num_values_ = value;
      }
    } else if (6 == field_id && THRIFT_I64 == type) {
      int64_t value = 0;
      if (OB_FAIL(reader.read_zigzag(value))) {
      } else {
        meta.total_uncompressed_size_ = value;
      }
    } else if (7 == field_id && THRIFT_I64 == type) {
      int64_t value = 0;
      if (OB_FAIL(reader.read_zigzag(value))) {
      } else {
        meta.total_compressed_size_ = value;
      }
    } else if (9 == field_id && THRIFT_I64 == type) {
      int64_t value = 0;
      if (OB_FAIL(reader.read_zigzag(value))) {
      } else {
        meta.data_page_offset_ = value;
      }
    } else if (11 == field_id && THRIFT_I64 == type) {
      int64_t value = 0;
      if (OB_FAIL(reader.read_zigzag(value))) {
      } else {
        meta.dictionary_page_offset_ = value;
      }
    } else {
      ret = reader.skip(type);
    }
  }
  return ret;
}

int read_column_chunk(ObFileSqlThriftReader &reader, ObFileSqlParquetChunk &chunk,
                      int64_t &column_index)
{
  int ret = OB_SUCCESS;
  bool done = false;
  bool has_meta = false;
  ObFileSqlParquetColumnMeta meta;
  while (OB_SUCC(ret) && !done) {
    uint8_t type = 0;
    int16_t field_id = 0;
    if (OB_FAIL(reader.read_field_header(type, field_id))) {
    } else if (THRIFT_STOP == type) {
      done = true;
    } else if (3 == field_id && THRIFT_STRUCT == type) {
      if (OB_FAIL(read_column_meta(reader, meta))) {
      } else {
        has_meta = true;
      }
    } else {
      ret = reader.skip(type);
    }
  }
  if (OB_SUCC(ret) && has_meta) {
    chunk.column_index_ = column_index;
    chunk.physical_type_ = meta.type_;
    chunk.codec_ = meta.codec_;
    chunk.num_values_ = meta.num_values_;
    chunk.data_page_offset_ = meta.data_page_offset_;
    chunk.dictionary_page_offset_ = meta.dictionary_page_offset_;
    chunk.total_compressed_size_ = meta.total_compressed_size_;
  } else if (OB_SUCC(ret)) {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

struct ObFileSqlParquetPageHeader
{
  ObFileSqlParquetPageHeader()
    : type_(-1), uncompressed_page_size_(0), compressed_page_size_(0), num_values_(0),
      encoding_(0), def_level_encoding_(0), rep_level_encoding_(0),
      v2_num_values_(0), v2_num_nulls_(0), v2_num_rows_(0), v2_encoding_(0),
      v2_def_levels_byte_length_(0), v2_rep_levels_byte_length_(0), v2_is_compressed_(true),
      dict_num_values_(0), dict_encoding_(0)
  {}
  int32_t type_;
  int32_t uncompressed_page_size_;
  int32_t compressed_page_size_;
  // data page v1
  int32_t num_values_;
  int32_t encoding_;
  int32_t def_level_encoding_;
  int32_t rep_level_encoding_;
  // data page v2
  int32_t v2_num_values_;
  int32_t v2_num_nulls_;
  int32_t v2_num_rows_;
  int32_t v2_encoding_;
  int32_t v2_def_levels_byte_length_;
  int32_t v2_rep_levels_byte_length_;
  bool v2_is_compressed_;
  // dictionary page
  int32_t dict_num_values_;
  int32_t dict_encoding_;
};

int read_page_header_into(ObFileSqlThriftReader &reader, ObFileSqlParquetPageHeader &header)
{
  int ret = OB_SUCCESS;
  bool done = false;
  while (OB_SUCC(ret) && !done) {
    uint8_t type = 0;
    int16_t field_id = 0;
    if (OB_FAIL(reader.read_field_header(type, field_id))) {
    } else if (THRIFT_STOP == type) {
      done = true;
    } else if (1 == field_id && THRIFT_I32 == type) {
      int64_t value = 0;
      if (OB_FAIL(reader.read_zigzag(value))) {
      } else {
        header.type_ = static_cast<int32_t>(value);
      }
    } else if (2 == field_id && THRIFT_I32 == type) {
      int64_t value = 0;
      if (OB_FAIL(reader.read_zigzag(value))) {
      } else {
        header.uncompressed_page_size_ = static_cast<int32_t>(value);
      }
    } else if (3 == field_id && THRIFT_I32 == type) {
      int64_t value = 0;
      if (OB_FAIL(reader.read_zigzag(value))) {
      } else {
        header.compressed_page_size_ = static_cast<int32_t>(value);
      }
    } else if (5 == field_id && THRIFT_STRUCT == type) {
      bool page_done = false;
      while (OB_SUCC(ret) && !page_done) {
        uint8_t page_type = 0;
        int16_t page_id = 0;
        if (OB_FAIL(reader.read_field_header(page_type, page_id))) {
        } else if (THRIFT_STOP == page_type) {
          page_done = true;
        } else if (1 == page_id && THRIFT_I32 == page_type) {
          int64_t value = 0;
          if (OB_FAIL(reader.read_zigzag(value))) {
          } else {
            header.num_values_ = static_cast<int32_t>(value);
          }
        } else if (2 == page_id && THRIFT_I32 == page_type) {
          int64_t value = 0;
          if (OB_FAIL(reader.read_zigzag(value))) {
          } else {
            header.encoding_ = static_cast<int32_t>(value);
          }
        } else if (3 == page_id && THRIFT_I32 == page_type) {
          int64_t value = 0;
          if (OB_FAIL(reader.read_zigzag(value))) {
          } else {
            header.def_level_encoding_ = static_cast<int32_t>(value);
          }
        } else if (4 == page_id && THRIFT_I32 == page_type) {
          int64_t value = 0;
          if (OB_FAIL(reader.read_zigzag(value))) {
          } else {
            header.rep_level_encoding_ = static_cast<int32_t>(value);
          }
        } else {
          ret = reader.skip(page_type);
        }
      }
    } else if (7 == field_id && THRIFT_STRUCT == type) {
      bool page_done = false;
      while (OB_SUCC(ret) && !page_done) {
        uint8_t page_type = 0;
        int16_t page_id = 0;
        if (OB_FAIL(reader.read_field_header(page_type, page_id))) {
        } else if (THRIFT_STOP == page_type) {
          page_done = true;
        } else if (1 == page_id && THRIFT_I32 == page_type) {
          int64_t value = 0;
          if (OB_FAIL(reader.read_zigzag(value))) {
          } else {
            header.dict_num_values_ = static_cast<int32_t>(value);
          }
        } else if (2 == page_id && THRIFT_I32 == page_type) {
          int64_t value = 0;
          if (OB_FAIL(reader.read_zigzag(value))) {
          } else {
            header.dict_encoding_ = static_cast<int32_t>(value);
          }
        } else {
          ret = reader.skip(page_type);
        }
      }
    } else if (8 == field_id && THRIFT_STRUCT == type) {
      bool page_done = false;
      while (OB_SUCC(ret) && !page_done) {
        uint8_t page_type = 0;
        int16_t page_id = 0;
        if (OB_FAIL(reader.read_field_header(page_type, page_id))) {
        } else if (THRIFT_STOP == page_type) {
          page_done = true;
        } else if (1 == page_id && THRIFT_I32 == page_type) {
          int64_t value = 0;
          if (OB_FAIL(reader.read_zigzag(value))) {
          } else {
            header.v2_num_values_ = static_cast<int32_t>(value);
          }
        } else if (2 == page_id && THRIFT_I32 == page_type) {
          int64_t value = 0;
          if (OB_FAIL(reader.read_zigzag(value))) {
          } else {
            header.v2_num_nulls_ = static_cast<int32_t>(value);
          }
        } else if (3 == page_id && THRIFT_I32 == page_type) {
          int64_t value = 0;
          if (OB_FAIL(reader.read_zigzag(value))) {
          } else {
            header.v2_num_rows_ = static_cast<int32_t>(value);
          }
        } else if (4 == page_id && THRIFT_I32 == page_type) {
          int64_t value = 0;
          if (OB_FAIL(reader.read_zigzag(value))) {
          } else {
            header.v2_encoding_ = static_cast<int32_t>(value);
          }
        } else if (5 == page_id && THRIFT_I32 == page_type) {
          int64_t value = 0;
          if (OB_FAIL(reader.read_zigzag(value))) {
          } else {
            header.v2_def_levels_byte_length_ = static_cast<int32_t>(value);
          }
        } else if (6 == page_id && THRIFT_I32 == page_type) {
          int64_t value = 0;
          if (OB_FAIL(reader.read_zigzag(value))) {
          } else {
            header.v2_rep_levels_byte_length_ = static_cast<int32_t>(value);
          }
        } else if (7 == page_id &&
                   (THRIFT_BOOL_TRUE == page_type || THRIFT_BOOL_FALSE == page_type)) {
          header.v2_is_compressed_ = (THRIFT_BOOL_TRUE == page_type);
        } else {
          ret = reader.skip(page_type);
        }
      }
    } else {
      ret = reader.skip(type);
    }
  }
  return ret;
}

// little endian bit reader used by the RLE / bit-packed hybrid decoder
int rle_hybrid_decode(const uint8_t *data, const int64_t len, const int64_t bit_width,
                      const int64_t num_values, std::vector<int64_t> &values,
                      int64_t &consumed, std::string &err_msg)
{
  int ret = OB_SUCCESS;
  values.clear();
  consumed = 0;
  if (bit_width < 0 || bit_width > 64) {
    ret = OB_ERR_UNEXPECTED;
    err_msg.assign("invalid parquet level bit width");
  } else if (0 == bit_width) {
    // all levels are the maximum value, no bytes are stored
    values.assign(static_cast<size_t>(num_values), 0);
  } else {
    const int64_t required = (num_values + 7) / 8;
    (void)required;
    int64_t pos = 0;
    while (OB_SUCC(ret) && pos < len &&
           static_cast<int64_t>(values.size()) < num_values) {
      uint64_t header = 0;
      int shift = 0;
      bool done = false;
      while (OB_SUCC(ret) && !done) {
        if (pos >= len) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          const uint8_t byte = data[pos++];
          if (shift > 63) {
            ret = OB_ERR_UNEXPECTED;
          } else {
            header |= (static_cast<uint64_t>(byte & 0x7f) << shift);
            if (0 == (byte & 0x80)) {
              done = true;
            } else {
              shift += 7;
            }
          }
        }
      }
      if (OB_FAIL(ret)) {
        err_msg.assign("truncated parquet RLE header");
      } else if (0 == (header & 1)) {
        // RLE run
        const int64_t run_length = static_cast<int64_t>(header >> 1);
        const int64_t byte_width = (bit_width + 7) / 8;
        int64_t value = 0;
        if (pos + byte_width > len) {
          ret = OB_ERR_UNEXPECTED;
          err_msg.assign("truncated parquet RLE run");
        } else {
          for (int64_t i = 0; i < byte_width; ++i) {
            value |= (static_cast<int64_t>(data[pos + i]) << (8 * i));
          }
          pos += byte_width;
          for (int64_t i = 0; OB_SUCC(ret) && i < run_length &&
                             static_cast<int64_t>(values.size()) < num_values; ++i) {
            values.push_back(value);
          }
        }
      } else {
        // bit-packed run
        const int64_t groups = static_cast<int64_t>(header >> 1);
        const int64_t count = groups * 8;
        for (int64_t i = 0; OB_SUCC(ret) && i < count &&
                           static_cast<int64_t>(values.size()) < num_values; ++i) {
          const int64_t bit_offset = i * bit_width;
          const int64_t byte_offset = pos + bit_offset / 8;
          const int64_t shift_bits = bit_offset % 8;
          if (byte_offset + ((shift_bits + bit_width + 7) / 8) > len) {
            ret = OB_ERR_UNEXPECTED;
            err_msg.assign("truncated parquet bit-packed run");
          } else {
            uint64_t value = 0;
            const int64_t bytes_needed = (shift_bits + bit_width + 7) / 8;
            for (int64_t b = 0; b < bytes_needed; ++b) {
              value |= (static_cast<uint64_t>(data[byte_offset + b]) << (8 * b));
            }
            value >>= shift_bits;
            if (bit_width < 64) {
              value &= ((static_cast<uint64_t>(1) << bit_width) - 1);
            }
            values.push_back(static_cast<int64_t>(value));
          }
        }
        if (OB_SUCC(ret)) {
          pos += (count * bit_width + 7) / 8;
        }
      }
    }
    consumed = pos;
    if (OB_SUCC(ret) && static_cast<int64_t>(values.size()) < num_values) {
      ret = OB_ERR_UNEXPECTED;
      err_msg.assign("parquet level run is shorter than expected");
    }
  }
  return ret;
}

int32_t bit_width_of(const int64_t max_value)
{
  int32_t width = 0;
  int64_t value = max_value;
  while (value > 0) {
    ++width;
    value >>= 1;
  }
  return width;
}

int read_plain_value(const ObFileSqlParquetColumn &column, const uint8_t *data,
                     const int64_t len, int64_t &pos, std::string &out,
                     std::string &err_msg)
{
  int ret = OB_SUCCESS;
  out.clear();
  switch (column.physical_type_) {
    case OB_PARQUET_TYPE_BOOLEAN: {
      if (pos >= len) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        const uint8_t byte = data[pos];
        pos += 1;
        out.assign((byte & 1) ? "true" : "false");
      }
      break;
    }
    case OB_PARQUET_TYPE_INT32: {
      if (pos + 4 > len) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        const int32_t value = static_cast<int32_t>(ob_file_sql_load_le32(data + pos));
        pos += 4;
        if (PARQUET_CONVERTED_DATE == column.converted_type_) {
          ob_file_sql_format_days(value, out);
        } else {
          char buf[32];
          snprintf(buf, sizeof(buf), "%d", value);
          out.assign(buf);
        }
      }
      break;
    }
    case OB_PARQUET_TYPE_INT64: {
      if (pos + 8 > len) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        const int64_t value = static_cast<int64_t>(ob_file_sql_load_le64(data + pos));
        pos += 8;
        if (PARQUET_CONVERTED_TIMESTAMP_MILLIS == column.converted_type_) {
          ob_file_sql_format_datetime_us(value * 1000LL, out);
        } else if (PARQUET_CONVERTED_TIMESTAMP_MICROS == column.converted_type_) {
          ob_file_sql_format_datetime_us(value, out);
        } else {
          char buf[32];
          snprintf(buf, sizeof(buf), "%lld", static_cast<long long>(value));
          out.assign(buf);
        }
      }
      break;
    }
    case OB_PARQUET_TYPE_INT96: {
      if (pos + 12 > len) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        const int64_t nanos = static_cast<int64_t>(ob_file_sql_load_le64(data + pos));
        const int32_t julian_day = static_cast<int32_t>(ob_file_sql_load_le32(data + pos + 8));
        pos += 12;
        const int64_t days = static_cast<int64_t>(julian_day) - 2440588;
        ob_file_sql_format_datetime_us(days * 86400000000LL + nanos / 1000, out);
      }
      break;
    }
    case OB_PARQUET_TYPE_FLOAT: {
      if (pos + 4 > len) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        const uint32_t raw = ob_file_sql_load_le32(data + pos);
        pos += 4;
        float value = 0.0f;
        memcpy(&value, &raw, sizeof(value));
        char buf[64];
        snprintf(buf, sizeof(buf), "%.9g", static_cast<double>(value));
        out.assign(buf);
      }
      break;
    }
    case OB_PARQUET_TYPE_DOUBLE: {
      if (pos + 8 > len) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        const uint64_t raw = ob_file_sql_load_le64(data + pos);
        pos += 8;
        double value = 0.0;
        memcpy(&value, &raw, sizeof(value));
        char buf[64];
        snprintf(buf, sizeof(buf), "%.17g", value);
        out.assign(buf);
      }
      break;
    }
    case OB_PARQUET_TYPE_BYTE_ARRAY: {
      if (pos + 4 > len) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        const uint32_t value_len = ob_file_sql_load_le32(data + pos);
        pos += 4;
        if (value_len > static_cast<uint32_t>(len - pos)) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          out.assign(reinterpret_cast<const char *>(data + pos), static_cast<size_t>(value_len));
          pos += static_cast<int64_t>(value_len);
        }
      }
      break;
    }
    case OB_PARQUET_TYPE_FIXED_LEN_BYTE_ARRAY: {
      if (pos + 1 > len) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        out.assign(1, static_cast<char>(data[pos]));
        pos += 1;
      }
      break;
    }
    default:
      ret = OB_NOT_SUPPORTED;
      break;
  }
  if (OB_FAIL(ret)) {
    err_msg.assign("failed to decode a parquet value");
  }
  return ret;
}

} // namespace

int ob_file_sql_snappy_decompress(const char *src, const int64_t src_len,
                                  std::string &dst, std::string &err_msg)
{
  int ret = OB_SUCCESS;
  dst.clear();
  if (NULL == src || src_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    err_msg.assign("empty snappy block");
  } else {
    const uint8_t *data = reinterpret_cast<const uint8_t *>(src);
    int64_t pos = 0;
    uint64_t uncompressed_len = 0;
    int shift = 0;
    bool header_done = false;
    while (OB_SUCC(ret) && !header_done) {
      if (pos >= src_len) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        const uint8_t byte = data[pos++];
        if (shift > 63) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          uncompressed_len |= (static_cast<uint64_t>(byte & 0x7f) << shift);
          if (0 == (byte & 0x80)) {
            header_done = true;
          } else {
            shift += 7;
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
      err_msg.assign("invalid snappy block header");
    } else if (uncompressed_len > static_cast<uint64_t>(1) << 31) {
      ret = OB_SIZE_OVERFLOW;
      err_msg.assign("snappy block is too large");
    } else {
      dst.reserve(static_cast<size_t>(uncompressed_len));
      while (OB_SUCC(ret) && pos < src_len &&
             static_cast<uint64_t>(dst.size()) < uncompressed_len) {
        const uint8_t tag = data[pos++];
        const uint8_t kind = static_cast<uint8_t>(tag & 0x03);
        if (0 == kind) {
          // literal
          uint64_t literal_len = static_cast<uint64_t>(tag >> 2);
          if (0x3c == literal_len) {
            if (pos >= src_len) {
              ret = OB_ERR_UNEXPECTED;
            } else {
              literal_len = static_cast<uint64_t>(data[pos++]);
            }
          } else if (0x3d == literal_len) {
            if (pos + 1 >= src_len) {
              ret = OB_ERR_UNEXPECTED;
            } else {
              literal_len = static_cast<uint64_t>(data[pos]) |
                            (static_cast<uint64_t>(data[pos + 1]) << 8);
              pos += 2;
            }
          } else if (0x3e == literal_len) {
            if (pos + 2 >= src_len) {
              ret = OB_ERR_UNEXPECTED;
            } else {
              literal_len = static_cast<uint64_t>(data[pos]) |
                            (static_cast<uint64_t>(data[pos + 1]) << 8) |
                            (static_cast<uint64_t>(data[pos + 2]) << 16);
              pos += 3;
            }
          } else if (0x3f == literal_len) {
            if (pos + 3 >= src_len) {
              ret = OB_ERR_UNEXPECTED;
            } else {
              literal_len = static_cast<uint64_t>(data[pos]) |
                            (static_cast<uint64_t>(data[pos + 1]) << 8) |
                            (static_cast<uint64_t>(data[pos + 2]) << 16) |
                            (static_cast<uint64_t>(data[pos + 3]) << 24);
              pos += 4;
            }
          }
          ++literal_len;
          if (OB_FAIL(ret)) {
          } else if (pos + static_cast<int64_t>(literal_len) > src_len) {
            ret = OB_ERR_UNEXPECTED;
          } else {
            dst.append(reinterpret_cast<const char *>(data + pos),
                       static_cast<size_t>(literal_len));
            pos += static_cast<int64_t>(literal_len);
          }
        } else {
          // copy with 1/2/4 byte offset
          int64_t copy_len = 0;
          int64_t offset = 0;
          if (1 == kind) {
            if (pos + 1 >= src_len) {
              ret = OB_ERR_UNEXPECTED;
            } else {
              copy_len = ((tag >> 2) & 0x07) + 4;
              offset = ((static_cast<int64_t>(tag >> 5) & 0x07) << 8) | data[pos];
              ++pos;
            }
          } else if (2 == kind) {
            if (pos + 2 >= src_len) {
              ret = OB_ERR_UNEXPECTED;
            } else {
              copy_len = (tag >> 2) + 1;
              offset = static_cast<int64_t>(data[pos]) |
                       (static_cast<int64_t>(data[pos + 1]) << 8);
              pos += 2;
            }
          } else {
            if (pos + 4 >= src_len) {
              ret = OB_ERR_UNEXPECTED;
            } else {
              copy_len = (tag >> 2) + 1;
              offset = static_cast<int64_t>(data[pos]) |
                       (static_cast<int64_t>(data[pos + 1]) << 8) |
                       (static_cast<int64_t>(data[pos + 2]) << 16) |
                       (static_cast<int64_t>(data[pos + 3]) << 24);
              pos += 4;
            }
          }
          if (OB_FAIL(ret)) {
            err_msg.assign("truncated snappy copy tag");
          } else if (offset <= 0 || static_cast<int64_t>(dst.size()) < offset) {
            ret = OB_ERR_UNEXPECTED;
            err_msg.assign("invalid snappy copy offset");
          } else {
            const size_t base = dst.size() - static_cast<size_t>(offset);
            for (int64_t i = 0; i < copy_len; ++i) {
              dst.push_back(dst[base + static_cast<size_t>(i)]);
            }
          }
        }
      }
      if (OB_SUCC(ret) && static_cast<uint64_t>(dst.size()) != uncompressed_len) {
        ret = OB_ERR_UNEXPECTED;
      }
      if (OB_FAIL(ret) && err_msg.empty()) {
        err_msg.assign("failed to decompress a snappy block");
      }
    }
  }
  return ret;
}

ObFileSqlParquetReader::ObFileSqlParquetReader()
  : fp_(NULL),
    path_(),
    options_(),
    schema_(),
    columns_(),
    row_groups_(),
    projected_(),
    group_loaded_(),
    group_nulls_(),
    group_values_(),
    cur_row_group_(-1),
    row_in_group_(0),
    total_rows_(0),
    file_size_(0),
    is_opened_(false)
{
}

ObFileSqlParquetReader::~ObFileSqlParquetReader()
{
  close();
}

void ObFileSqlParquetReader::close()
{
  if (NULL != fp_) {
    ::fclose(fp_);
    fp_ = NULL;
  }
  columns_.clear();
  row_groups_.clear();
  projected_.clear();
  col_loaded_.clear();
  col_nulls_.clear();
  col_values_.clear();
  is_opened_ = false;
}

int ObFileSqlParquetReader::open(const std::string &path,
                                 const ObFileSqlOptions &options,
                                 std::string &err_msg)
{
  int ret = OB_SUCCESS;
  close();
  path_ = path;
  options_ = options;
  fp_ = ::fopen(path.c_str(), "rb");
  if (NULL == fp_) {
    ret = OB_FILE_NOT_EXIST;
    err_msg.assign("can not open parquet file: ").append(path);
  } else if (0 != ::fseek(fp_, 0, SEEK_END)) {
    ret = OB_IO_ERROR;
    err_msg.assign("can not seek parquet file: ").append(path);
  } else {
    file_size_ = static_cast<int64_t>(::ftell(fp_));
    if (OB_FAIL(load_footer(err_msg))) {
    } else {
      projected_.assign(columns_.size(), 1);
      is_opened_ = true;
    }
  }
  if (OB_FAIL(ret) && err_msg.empty()) {
    err_msg.assign("failed to open parquet file: ").append(path);
  }
  return ret;
}

int ObFileSqlParquetReader::load_footer(std::string &err_msg)
{
  int ret = OB_SUCCESS;
  if (file_size_ < 12) {
    ret = OB_INVALID_ARGUMENT;
    err_msg.assign("not a parquet file: the file is too small");
  } else {
    char tail[8];
    if (0 != ::fseek(fp_, file_size_ - 8, SEEK_SET) ||
        8 != static_cast<int64_t>(::fread(tail, 1, 8, fp_))) {
      ret = OB_IO_ERROR;
      err_msg.assign("failed to read the parquet footer");
    } else if (0 != memcmp(tail + 4, "PAR1", 4)) {
      ret = OB_INVALID_ARGUMENT;
      err_msg.assign("not a parquet file: missing PAR1 magic");
    } else {
      const uint8_t *len_bytes = reinterpret_cast<const uint8_t *>(tail);
      const uint32_t footer_len = ob_file_sql_load_le32(len_bytes);
      if (static_cast<int64_t>(footer_len) + 8 > file_size_ || footer_len > (64L << 20)) {
        ret = OB_INVALID_ARGUMENT;
        err_msg.assign("invalid parquet footer length");
      } else {
        std::string footer;
        footer.resize(footer_len);
        if (0 != ::fseek(fp_, file_size_ - 8 - static_cast<int64_t>(footer_len), SEEK_SET) ||
            0 != ::fread(&footer[0], 1, footer_len, fp_)) {
          ret = OB_IO_ERROR;
          err_msg.assign("failed to read the parquet footer");
        } else {
          ObFileSqlThriftReader reader;
          reader.init(reinterpret_cast<const uint8_t *>(footer.data()),
                      static_cast<int64_t>(footer.size()));
          std::vector<ObFileSqlParquetSchemaElement> elements;
          bool done = false;
          while (OB_SUCC(ret) && !done) {
            uint8_t type = 0;
            int16_t field_id = 0;
            if (OB_FAIL(reader.read_field_header(type, field_id))) {
            } else if (THRIFT_STOP == type) {
              done = true;
            } else if (2 == field_id && THRIFT_LIST == type) {
              uint8_t elem_type = 0;
              int64_t size = 0;
              if (OB_FAIL(reader.read_list_header(elem_type, size))) {
              } else if (THRIFT_STRUCT != elem_type) {
                ret = OB_ERR_UNEXPECTED;
              } else {
                for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
                  ObFileSqlParquetSchemaElement element;
                  if (OB_FAIL(read_schema_element(reader, element))) {
                  } else {
                    elements.push_back(element);
                  }
                }
              }
            } else if (3 == field_id && THRIFT_I64 == type) {
              int64_t value = 0;
              if (OB_FAIL(reader.read_zigzag(value))) {
              } else {
                total_rows_ = value;
              }
            } else if (4 == field_id && THRIFT_LIST == type) {
              uint8_t elem_type = 0;
              int64_t size = 0;
              if (OB_FAIL(reader.read_list_header(elem_type, size))) {
              } else if (THRIFT_STRUCT != elem_type) {
                ret = OB_ERR_UNEXPECTED;
              } else {
                for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
                  ObFileSqlParquetRowGroup group;
                  bool group_done = false;
                  while (OB_SUCC(ret) && !group_done) {
                    uint8_t group_type = 0;
                    int16_t group_id = 0;
                    if (OB_FAIL(reader.read_field_header(group_type, group_id))) {
                    } else if (THRIFT_STOP == group_type) {
                      group_done = true;
                    } else if (1 == group_id && THRIFT_LIST == group_type) {
                      uint8_t chunk_type = 0;
                      int64_t chunk_count = 0;
                      if (OB_FAIL(reader.read_list_header(chunk_type, chunk_count))) {
                      } else if (THRIFT_STRUCT != chunk_type) {
                        ret = OB_ERR_UNEXPECTED;
                      } else {
                        for (int64_t c = 0; OB_SUCC(ret) && c < chunk_count; ++c) {
                          ObFileSqlParquetChunk chunk;
                          int64_t column_index = static_cast<int64_t>(group.chunks_.size());
                          if (OB_FAIL(read_column_chunk(reader, chunk, column_index))) {
                          } else {
                            group.chunks_.push_back(chunk);
                          }
                        }
                      }
                    } else if (2 == group_id && THRIFT_I64 == group_type) {
                      int64_t value = 0;
                      if (OB_FAIL(reader.read_zigzag(value))) {
                      } else {
                        group.total_byte_size_ = value;
                      }
                    } else if (3 == group_id && THRIFT_I64 == group_type) {
                      int64_t value = 0;
                      if (OB_FAIL(reader.read_zigzag(value))) {
                      } else {
                        group.num_rows_ = value;
                      }
                    } else {
                      ret = reader.skip(group_type);
                    }
                  }
                  if (OB_SUCC(ret)) {
                    row_groups_.push_back(group);
                  }
                }
              }
            } else {
              ret = reader.skip(type);
            }
          }
          if (OB_FAIL(ret)) {
            err_msg.assign("failed to parse the parquet footer");
          } else if (elements.empty()) {
            ret = OB_INVALID_ARGUMENT;
            err_msg.assign("parquet file has no schema");
          } else if (elements[0].num_children_ <= 0) {
            ret = OB_INVALID_ARGUMENT;
            err_msg.assign("parquet file has no column");
          } else {
            int64_t index = 1;
            for (int64_t i = 0; OB_SUCC(ret) && i < elements[0].num_children_; ++i) {
              if (index >= static_cast<int64_t>(elements.size())) {
                ret = OB_ERR_UNEXPECTED;
                err_msg.assign("invalid parquet schema");
              } else if (elements[index].num_children_ > 0) {
                ret = OB_NOT_SUPPORTED;
                err_msg.assign("nested parquet columns are not supported");
              } else if (2 == elements[index].repetition_type_) {
                ret = OB_NOT_SUPPORTED;
                err_msg.assign("repeated parquet columns are not supported");
              } else {
                ObFileSqlParquetColumn column;
                column.name_ = elements[index].name_;
                column.physical_type_ = elements[index].type_;
                column.converted_type_ = elements[index].converted_type_;
                column.max_rep_level_ = 0;
                column.max_def_level_ = (1 == elements[index].repetition_type_) ? 1 : 0;
                switch (column.physical_type_) {
                  case OB_PARQUET_TYPE_BOOLEAN:
                    column.schema_type_ = FILE_SQL_TYPE_BOOLEAN;
                    break;
                  case OB_PARQUET_TYPE_INT32:
                    column.schema_type_ = (PARQUET_CONVERTED_DATE == column.converted_type_)
                                              ? FILE_SQL_TYPE_DATE
                                              : FILE_SQL_TYPE_BIGINT;
                    break;
                  case OB_PARQUET_TYPE_INT64:
                    column.schema_type_ =
                        (PARQUET_CONVERTED_TIMESTAMP_MILLIS == column.converted_type_ ||
                         PARQUET_CONVERTED_TIMESTAMP_MICROS == column.converted_type_)
                            ? FILE_SQL_TYPE_DATETIME
                            : FILE_SQL_TYPE_BIGINT;
                    break;
                  case OB_PARQUET_TYPE_FLOAT:
                  case OB_PARQUET_TYPE_DOUBLE:
                    column.schema_type_ = FILE_SQL_TYPE_DOUBLE;
                    break;
                  case OB_PARQUET_TYPE_BYTE_ARRAY:
                    column.schema_type_ = FILE_SQL_TYPE_VARCHAR;
                    break;
                  case OB_PARQUET_TYPE_INT96:
                    column.schema_type_ = FILE_SQL_TYPE_DATETIME;
                    break;
                  case OB_PARQUET_TYPE_FIXED_LEN_BYTE_ARRAY:
                    column.schema_type_ = FILE_SQL_TYPE_VARCHAR;
                    break;
                  default:
                    ret = OB_NOT_SUPPORTED;
                    err_msg.assign("unsupported parquet physical type");
                    break;
                }
                if (OB_SUCC(ret)) {
                  columns_.push_back(column);
                  ObFileSqlColumnSchema schema_column;
                  schema_column.name_ = column.name_;
                  schema_column.type_ = column.schema_type_;
                  schema_.columns_.push_back(schema_column);
                }
                ++index;
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObFileSqlParquetReader::set_projection(const std::vector<char> &projected)
{
  int ret = OB_SUCCESS;
  if (projected.size() != columns_.size()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    projected_ = projected;
  }
  return ret;
}

int ObFileSqlParquetReader::rewind()
{
  int ret = OB_SUCCESS;
  cur_row_group_ = -1;
  row_in_group_ = 0;
  col_loaded_.assign(columns_.size(), 0);
  col_nulls_.resize(columns_.size());
  col_values_.resize(columns_.size());
  return ret;
}

int ObFileSqlParquetReader::read_column_pages(const ObFileSqlParquetChunk &chunk,
                                              std::vector<bool> &nulls,
                                              std::vector<std::string> &values,
                                              std::string &err_msg)
{
  int ret = OB_SUCCESS;
  nulls.clear();
  values.clear();
  int64_t start = chunk.data_page_offset_;
  bool has_dictionary_page = false;
  ObFileSqlParquetColumn &column = columns_[static_cast<size_t>(chunk.column_index_)];
  if (chunk.dictionary_page_offset_ > 0 && chunk.dictionary_page_offset_ < start) {
    start = chunk.dictionary_page_offset_;
    has_dictionary_page = true;
  }
  int64_t chunk_len = chunk.total_compressed_size_;
  if (chunk_len <= 0 || start + chunk_len > file_size_) {
    chunk_len = file_size_ - start;
  }
  if (start < 0 || chunk_len <= 0) {
    ret = OB_ERR_UNEXPECTED;
    err_msg.assign("invalid parquet column chunk offset");
  } else {
    std::string chunk_data;
    chunk_data.resize(static_cast<size_t>(chunk_len));
    if (0 != ::fseek(fp_, start, SEEK_SET) ||
        0 != ::fread(&chunk_data[0], 1, static_cast<size_t>(chunk_len), fp_)) {
      ret = OB_IO_ERROR;
      err_msg.assign("failed to read a parquet column chunk");
    } else {
      const uint8_t *raw = reinterpret_cast<const uint8_t *>(chunk_data.data());
      const int64_t raw_len = chunk_len;
      int64_t pos = 0;
      std::vector<std::string> dictionary;
      bool dictionary_ready = false;
      const int32_t def_bit_width = bit_width_of(column.max_def_level_);
      while (OB_SUCC(ret) && pos < raw_len &&
             static_cast<int64_t>(nulls.size()) < chunk.num_values_) {
        ObFileSqlThriftReader reader;
        reader.init(raw + pos, raw_len - pos);
        ObFileSqlParquetPageHeader header;
        if (OB_FAIL(read_page_header_into(reader, header))) {
          err_msg.assign("failed to parse a parquet page header");
        } else if (header.compressed_page_size_ < 0 ||
                   reader.pos() + header.compressed_page_size_ > raw_len - pos) {
          ret = OB_ERR_UNEXPECTED;
          err_msg.assign("truncated parquet page");
        } else {
          const int64_t header_len = reader.pos();
          const uint8_t *page_data = raw + pos + header_len;
          const int64_t page_len = header.compressed_page_size_;
          pos += header_len + page_len;
          std::string decompressed;
          std::string combined;
          const uint8_t *body = page_data;
          int64_t body_len = page_len;
          if (PARQUET_PAGE_INDEX == header.type_) {
            // index pages carry no value
          } else {
            bool need_decompress = true;
            int64_t level_bytes = 0;
            if (PARQUET_PAGE_DATA_V2 == header.type_) {
              // in a v2 page the repetition and definition levels are stored in front of
              // the page and are never compressed
              level_bytes = static_cast<int64_t>(header.v2_rep_levels_byte_length_) +
                            static_cast<int64_t>(header.v2_def_levels_byte_length_);
              need_decompress = header.v2_is_compressed_;
            }
            if (OB_PARQUET_CODEC_UNCOMPRESSED == chunk.codec_) {
              need_decompress = false;
            }
            if (level_bytes < 0 || level_bytes > page_len) {
              ret = OB_ERR_UNEXPECTED;
              err_msg.assign("invalid parquet v2 level length");
            } else if (need_decompress) {
              if (OB_PARQUET_CODEC_SNAPPY == chunk.codec_) {
                if (OB_FAIL(ob_file_sql_snappy_decompress(
                        reinterpret_cast<const char *>(page_data + level_bytes),
                        page_len - level_bytes, decompressed, err_msg))) {
                } else if (level_bytes > 0 &&
                           FALSE_IT(combined.assign(reinterpret_cast<const char *>(page_data),
                                                    static_cast<size_t>(level_bytes)))) {
                } else if (level_bytes > 0 &&
                           FALSE_IT(combined.append(decompressed))) {
                } else if (level_bytes > 0) {
                  body = reinterpret_cast<const uint8_t *>(combined.data());
                  body_len = static_cast<int64_t>(combined.size());
                } else {
                  body = reinterpret_cast<const uint8_t *>(decompressed.data());
                  body_len = static_cast<int64_t>(decompressed.size());
                }
              } else {
                ret = OB_NOT_SUPPORTED;
                err_msg.assign("only snappy compressed parquet files are supported");
              }
            }
            if (OB_SUCC(ret) && PARQUET_PAGE_DICTIONARY == header.type_) {
              dictionary.clear();
              if (!has_dictionary_page) {
                // dictionaries are still decoded, some writers only set the data page offset
              }
              int64_t dict_pos = 0;
              for (int32_t i = 0; OB_SUCC(ret) && i < header.dict_num_values_; ++i) {
                std::string value;
                if (OB_FAIL(read_plain_value(column, body, body_len, dict_pos, value, err_msg))) {
                } else {
                  dictionary.push_back(value);
                }
              }
              dictionary_ready = true;
            } else if (OB_SUCC(ret) &&
                       (PARQUET_PAGE_DATA == header.type_ ||
                        PARQUET_PAGE_DATA_V2 == header.type_)) {
              const bool is_v2 = (PARQUET_PAGE_DATA_V2 == header.type_);
              const int32_t page_num_values =
                  is_v2 ? header.v2_num_values_ : header.num_values_;
              std::vector<int64_t> def_levels;
              int64_t level_pos = 0;
              if (column.max_def_level_ > 0) {
                if (is_v2) {
                  const int64_t def_len = header.v2_def_levels_byte_length_;
                  if (def_len < 0 || level_pos + def_len > body_len) {
                    ret = OB_ERR_UNEXPECTED;
                    err_msg.assign("invalid parquet v2 definition level length");
                  } else {
                    std::vector<int64_t> levels;
                    int64_t consumed = 0;
                    if (OB_FAIL(rle_hybrid_decode(body + level_pos, def_len, def_bit_width,
                                                  page_num_values, levels, consumed, err_msg))) {
                    } else {
                      def_levels = levels;
                      level_pos += def_len;
                    }
                  }
                } else {
                  if (level_pos + 4 > body_len) {
                    ret = OB_ERR_UNEXPECTED;
                    err_msg.assign("truncated parquet definition levels");
                  } else {
                    const uint32_t level_size = ob_file_sql_load_le32(body + level_pos);
                    level_pos += 4;
                    if (level_size > 0 && level_pos + static_cast<int64_t>(level_size) <= body_len) {
                      std::vector<int64_t> levels;
                      int64_t consumed = 0;
                      if (OB_FAIL(rle_hybrid_decode(body + level_pos, level_size, def_bit_width,
                                                    page_num_values, levels, consumed, err_msg))) {
                      } else {
                        def_levels = levels;
                        level_pos += static_cast<int64_t>(level_size);
                      }
                    }
                  }
                }
              }
              if (OB_SUCC(ret)) {
                const int32_t encoding = is_v2 ? header.v2_encoding_ : header.encoding_;
                int64_t value_pos = level_pos;
                if (PARQUET_ENCODING_PLAIN == encoding) {
                  const bool is_boolean =
                      (OB_PARQUET_TYPE_BOOLEAN == column.physical_type_);
                  const int64_t bool_base = value_pos;
                  int64_t bool_index = 0;
                  for (int32_t i = 0; OB_SUCC(ret) && i < page_num_values; ++i) {
                    const bool is_null =
                        (column.max_def_level_ > 0 &&
                         static_cast<int64_t>(def_levels.size()) > i &&
                         def_levels[static_cast<size_t>(i)] < column.max_def_level_);
                    std::string value;
                    if (is_null) {
                      // only the non null values are stored
                      nulls.push_back(true);
                      values.push_back(std::string());
                    } else if (is_boolean) {
                      const int64_t byte_offset = bool_base + bool_index / 8;
                      if (byte_offset >= body_len) {
                        ret = OB_ERR_UNEXPECTED;
                        err_msg.assign("truncated parquet boolean values");
                      } else {
                        const uint8_t byte = body[byte_offset];
                        const bool bit = (0 != ((byte >> (bool_index % 8)) & 1));
                        nulls.push_back(false);
                        values.push_back(bit ? std::string("true") : std::string("false"));
                        ++bool_index;
                      }
                    } else if (OB_FAIL(read_plain_value(column, body, body_len, value_pos,
                                                        value, err_msg))) {
                    } else {
                      nulls.push_back(false);
                      values.push_back(value);
                    }
                  }
                  if (OB_SUCC(ret) && is_boolean) {
                    value_pos = bool_base + (bool_index + 7) / 8;
                  }
                } else if (PARQUET_ENCODING_PLAIN_DICTIONARY == encoding ||
                           PARQUET_ENCODING_RLE_DICTIONARY == encoding) {
                  if (!dictionary_ready) {
                    ret = OB_ERR_UNEXPECTED;
                    err_msg.assign("parquet dictionary page is missing");
                  } else if (value_pos >= body_len) {
                    ret = OB_ERR_UNEXPECTED;
                    err_msg.assign("truncated parquet dictionary encoded page");
                  } else {
                    const int64_t dict_bit_width = body[value_pos];
                    ++value_pos;
                    std::vector<int64_t> indexes;
                    int64_t consumed = 0;
                    if (OB_FAIL(rle_hybrid_decode(body + value_pos, body_len - value_pos,
                                                  dict_bit_width, page_num_values, indexes,
                                                  consumed, err_msg))) {
                    } else {
                      for (int32_t i = 0; OB_SUCC(ret) && i < page_num_values; ++i) {
                        const bool is_null =
                            (column.max_def_level_ > 0 &&
                             static_cast<int64_t>(def_levels.size()) > i &&
                             def_levels[static_cast<size_t>(i)] < column.max_def_level_);
                        if (is_null) {
                          nulls.push_back(true);
                          values.push_back(std::string());
                        } else {
                          const int64_t dict_index =
                              (static_cast<size_t>(i) < indexes.size()) ? indexes[static_cast<size_t>(i)] : -1;
                          if (dict_index < 0 ||
                              dict_index >= static_cast<int64_t>(dictionary.size())) {
                            ret = OB_ERR_UNEXPECTED;
                            err_msg.assign("invalid parquet dictionary index");
                          } else {
                            nulls.push_back(false);
                            values.push_back(dictionary[static_cast<size_t>(dict_index)]);
                          }
                        }
                      }
                    }
                  }
                } else {
                  ret = OB_NOT_SUPPORTED;
                  err_msg.assign("unsupported parquet value encoding");
                }
              }
            }
          }
        }
      }
      if (OB_SUCC(ret) && static_cast<int64_t>(nulls.size()) < chunk.num_values_) {
        ret = OB_ERR_UNEXPECTED;
        err_msg.assign("parquet column chunk has fewer values than expected");
      }
    }
  }
  return ret;
}

int ObFileSqlParquetReader::load_row_group(const int64_t row_group_idx,
                                           std::string &err_msg)
{
  int ret = OB_SUCCESS;
  if (row_group_idx < 0 || row_group_idx >= static_cast<int64_t>(row_groups_.size())) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    const ObFileSqlParquetRowGroup &group = row_groups_[static_cast<size_t>(row_group_idx)];
    if (col_loaded_.size() != columns_.size()) {
      col_loaded_.assign(columns_.size(), 0);
      col_nulls_.resize(columns_.size());
      col_values_.resize(columns_.size());
    }
    for (size_t c = 0; OB_SUCC(ret) && c < columns_.size(); ++c) {
      col_loaded_[c] = 0;
      col_nulls_[c].clear();
      col_values_[c].clear();
    }
    for (size_t c = 0; OB_SUCC(ret) && c < group.chunks_.size(); ++c) {
      const ObFileSqlParquetChunk &chunk = group.chunks_[c];
      const int64_t column_index = chunk.column_index_;
      if (column_index < 0 || column_index >= static_cast<int64_t>(columns_.size())) {
        ret = OB_ERR_UNEXPECTED;
        err_msg.assign("invalid parquet column chunk index");
      } else if (projected_.empty() || !projected_[static_cast<size_t>(column_index)]) {
        // column pruning: this column is not referenced by the query, its pages are
        // never read and the output cells stay NULL
        col_loaded_[static_cast<size_t>(column_index)] = 1;
      } else if (OB_FAIL(read_column_pages(chunk,
                                           col_nulls_[static_cast<size_t>(column_index)],
                                           col_values_[static_cast<size_t>(column_index)],
                                           err_msg))) {
      } else {
        col_loaded_[static_cast<size_t>(column_index)] = 1;
      }
    }
    if (OB_SUCC(ret)) {
      cur_row_group_ = row_group_idx;
      row_in_group_ = 0;
    }
  }
  return ret;
}

int ObFileSqlParquetReader::read_row(std::vector<ObFileSqlCell> &row, bool &eof)
{
  int ret = OB_SUCCESS;
  row.clear();
  eof = false;
  if (!is_opened_) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    bool ready = false;
    while (OB_SUCC(ret) && !ready) {
      if (cur_row_group_ < 0 ||
          row_in_group_ >= row_groups_[static_cast<size_t>(cur_row_group_)].num_rows_) {
        const int64_t next_group = cur_row_group_ + 1;
        if (next_group >= static_cast<int64_t>(row_groups_.size())) {
          eof = true;
          break;
        } else {
          std::string err_msg;
          if (OB_FAIL(load_row_group(next_group, err_msg))) {
          } else {
            ready = true;
          }
        }
      } else {
        ready = true;
      }
    }
    if (OB_SUCC(ret) && !eof && ready) {
      const int64_t idx = row_in_group_;
      for (size_t c = 0; OB_SUCC(ret) && c < columns_.size(); ++c) {
        ObFileSqlCell cell;
        cell.is_null_ = true;
        if (c < col_loaded_.size() && col_loaded_[c] &&
            idx < static_cast<int64_t>(col_values_[c].size()) &&
            idx < static_cast<int64_t>(col_nulls_[c].size())) {
          cell.is_null_ = col_nulls_[c][static_cast<size_t>(idx)];
          if (!cell.is_null_) {
            cell.str_ = col_values_[c][static_cast<size_t>(idx)];
          }
        }
        row.push_back(cell);
      }
      ++row_in_group_;
    }
  }
  return ret;
}

int ob_file_sql_parquet_read_schema(const std::string &path, ObFileSqlSchema &schema,
                                    std::string &err_msg)
{
  int ret = OB_SUCCESS;
  schema.reset();
  err_msg.clear();
  ObFileSqlParquetReader reader;
  ObFileSqlOptions options;
  options.format_ = FILE_SQL_FORMAT_PARQUET;
  if (OB_FAIL(reader.open(path, options, err_msg))) {
  } else {
    schema = reader.get_schema();
    reader.close();
  }
  return ret;
}

int ObFileSqlReader::open_parquet()
{
  int ret = OB_SUCCESS;
  ObFileSqlParquetReader *reader = new (std::nothrow) ObFileSqlParquetReader();
  if (NULL == reader) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    err_msg_.assign("failed to allocate a parquet reader");
  } else {
    parquet_ = reader;
    if (OB_FAIL(reader->open(path_, options_, err_msg_))) {
    }
  }
  return ret;
}

int ObFileSqlReader::detect_parquet_schema(ObFileSqlSchema &schema, std::string &err_msg)
{
  int ret = OB_SUCCESS;
  ObFileSqlParquetReader *reader = static_cast<ObFileSqlParquetReader *>(parquet_);
  if (NULL == reader) {
    ret = OB_ERR_UNEXPECTED;
    err_msg.assign("parquet reader is not opened");
  } else {
    schema = reader->get_schema();
    if (schema.count() <= 0) {
      ret = OB_INVALID_ARGUMENT;
      err_msg.assign("parquet file has no column");
    }
  }
  return ret;
}

int ObFileSqlReader::next_parquet_row(std::vector<ObFileSqlCell> &row, bool &eof)
{
  int ret = OB_SUCCESS;
  ObFileSqlParquetReader *reader = static_cast<ObFileSqlParquetReader *>(parquet_);
  if (NULL == reader) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(reader->read_row(row, eof))) {
  } else if (!eof) {
    ++row_index_;
  }
  return ret;
}

int ObFileSqlReader::set_projection(const std::vector<char> &projected)
{
  int ret = OB_SUCCESS;
  if (FILE_SQL_FORMAT_PARQUET == format_ && NULL != parquet_) {
    ObFileSqlParquetReader *reader = static_cast<ObFileSqlParquetReader *>(parquet_);
    ret = reader->set_projection(projected);
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
