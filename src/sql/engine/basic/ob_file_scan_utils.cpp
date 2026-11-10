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

#include "sql/engine/basic/ob_file_scan_utils.h"

#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#ifndef _WIN32
#include <strings.h>
#endif

#include "lib/oblog/ob_log_module.h"
#include "sql/engine/basic/ob_parquet_reader.h"

namespace oceanbase
{
namespace sql
{

namespace
{

const int64_t OB_FILE_SQL_BUF_SIZE = 64L * 1024L;
const int64_t OB_FILE_SQL_MAX_JSON_DEPTH = 32;
const int64_t OB_FILE_SQL_MAX_DATETIME_SCALE = 6;

static char ob_file_sql_lower(const char c)
{
  return (c >= 'A' && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c;
}

static void ob_file_sql_lower_str(const std::string &in, std::string &out)
{
  out.clear();
  out.reserve(in.size());
  for (size_t i = 0; i < in.size(); ++i) {
    out.push_back(ob_file_sql_lower(in[i]));
  }
}

static bool ob_file_sql_str_eq_ci(const std::string &left, const char *right)
{
  const size_t right_len = strlen(right);
  if (left.size() != right_len) {
    return false;
  }
  for (size_t i = 0; i < right_len; ++i) {
    if (ob_file_sql_lower(left[i]) != ob_file_sql_lower(right[i])) {
      return false;
    }
  }
  return true;
}

static bool ob_file_sql_starts_with_ci(const std::string &str, const char *prefix)
{
  const size_t prefix_len = strlen(prefix);
  if (str.size() < prefix_len) {
    return false;
  }
  for (size_t i = 0; i < prefix_len; ++i) {
    if (ob_file_sql_lower(str[i]) != ob_file_sql_lower(prefix[i])) {
      return false;
    }
  }
  return true;
}

static bool ob_file_sql_is_digit(const char c) { return c >= '0' && c <= '9'; }

static bool ob_file_sql_is_space(const char c)
{
  return ' ' == c || '\t' == c || '\n' == c || '\r' == c || '\f' == c || '\v' == c;
}

static std::string ob_file_sql_trim(const std::string &str)
{
  size_t begin = 0;
  size_t end = str.size();
  while (begin < end && ob_file_sql_is_space(str[begin])) {
    ++begin;
  }
  while (end > begin && ob_file_sql_is_space(str[end - 1])) {
    --end;
  }
  return str.substr(begin, end - begin);
}

// Days since 1970-01-01 for a civil date, valid for the whole supported range.
static int64_t ob_file_sql_days_from_civil(int64_t year, int64_t month, int64_t day)
{
  year -= month <= 2 ? 1 : 0;
  const int64_t era = (year >= 0 ? year : year - 399) / 400;
  const int64_t yoe = year - era * 400;
  const int64_t doy = (153 * (month + (month > 2 ? -3 : 9)) + 2) / 5 + day - 1;
  const int64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  return era * 146097 + doe - 719468;
}

static bool ob_file_sql_is_leap_year(const int64_t year)
{
  return (0 == year % 4 && 0 != year % 100) || 0 == year % 400;
}

static int64_t ob_file_sql_days_in_month(const int64_t year, const int64_t month)
{
  static const int64_t days[13] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  int64_t ret = 0;
  if (month < 1 || month > 12) {
    ret = 0;
  } else if (2 == month && ob_file_sql_is_leap_year(year)) {
    ret = 29;
  } else {
    ret = days[month];
  }
  return ret;
}

static bool ob_file_sql_read_ndigits(const char *str, const int64_t len, int64_t &pos,
                                     const int64_t n, int64_t &value)
{
  bool valid = pos + n <= len;
  value = 0;
  for (int64_t i = 0; valid && i < n; ++i) {
    const char c = str[pos + i];
    if (!ob_file_sql_is_digit(c)) {
      valid = false;
    } else {
      value = value * 10 + (c - '0');
    }
  }
  if (valid) {
    pos += n;
  }
  return valid;
}

static int ob_file_sql_parse_date_parts(const char *str, const int64_t len,
                                        int64_t &year, int64_t &month, int64_t &day,
                                        int64_t &pos)
{
  int ret = OB_SUCCESS;
  year = 0;
  month = 0;
  day = 0;
  pos = 0;
  if (len < 10) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!ob_file_sql_read_ndigits(str, len, pos, 4, year) || pos >= len || '-' != str[pos]) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ++pos;
    if (!ob_file_sql_read_ndigits(str, len, pos, 2, month) || pos >= len || '-' != str[pos]) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      ++pos;
      if (!ob_file_sql_read_ndigits(str, len, pos, 2, day)) {
        ret = OB_INVALID_ARGUMENT;
      } else if (year < 1 || year > 9999 || month < 1 || month > 12 ||
                 day < 1 || day > ob_file_sql_days_in_month(year, month)) {
        ret = OB_INVALID_ARGUMENT;
      }
    }
  }
  return ret;
}

struct ObFileSqlJsonValue
{
  enum Kind
  {
    J_NULL = 0,
    J_BOOL,
    J_INT,
    J_DOUBLE,
    J_STRING,
    J_OBJECT,
    J_ARRAY,
  };
  ObFileSqlJsonValue() : kind_(J_NULL), bool_(false), int_(0), dbl_(0.0), str_() {}
  Kind kind_;
  bool bool_;
  int64_t int_;
  double dbl_;
  std::string str_;
};

// A minimal strict JSON parser, only used for jsonl files. Nested objects and arrays
// are parsed so that the scanner can skip them, but the caller rejects them.
class ObFileSqlJsonParser
{
public:
  ObFileSqlJsonParser() : data_(NULL), len_(0), pos_(0) {}
  void init(const char *data, const int64_t len)
  {
    data_ = data;
    len_ = len;
    pos_ = 0;
  }
  // Parse one object at the current position, keys and values are appended in the
  // order they appear in the document.
  int parse_object(std::vector<std::string> &keys, std::vector<ObFileSqlJsonValue> &values,
                   int64_t &err_pos);

private:
  int skip_space();
  int parse_string(std::string &value, int64_t &err_pos);
  int parse_number(ObFileSqlJsonValue &value, int64_t &err_pos);
  int parse_value(ObFileSqlJsonValue &value, int64_t &err_pos, const int64_t depth);
  int skip_literal(const char *literal);

private:
  const char *data_;
  int64_t len_;
  int64_t pos_;
};

int ObFileSqlJsonParser::skip_space()
{
  int ret = OB_SUCCESS;
  while (pos_ < len_ && ob_file_sql_is_space(data_[pos_])) {
    ++pos_;
  }
  return ret;
}

int ObFileSqlJsonParser::skip_literal(const char *literal)
{
  int ret = OB_SUCCESS;
  const int64_t literal_len = static_cast<int64_t>(strlen(literal));
  if (pos_ + literal_len > len_ || 0 != strncmp(data_ + pos_, literal, literal_len)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    pos_ += literal_len;
  }
  return ret;
}

int ObFileSqlJsonParser::parse_string(std::string &value, int64_t &err_pos)
{
  int ret = OB_SUCCESS;
  value.clear();
  err_pos = pos_;
  if (pos_ >= len_ || '"' != data_[pos_]) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    bool closed = false;
    ++pos_;
    while (OB_SUCC(ret) && pos_ < len_ && !closed) {
      const unsigned char c = static_cast<unsigned char>(data_[pos_]);
      if ('"' == c) {
        ++pos_;
        closed = true;
      } else if ('\\' == c) {
        ++pos_;
        if (pos_ >= len_) {
          ret = OB_ERR_UNEXPECTED;
          err_pos = pos_;
        } else {
          const char esc = data_[pos_++];
          switch (esc) {
            case '"': value.push_back('"'); break;
            case '\\': value.push_back('\\'); break;
            case '/': value.push_back('/'); break;
            case 'b': value.push_back('\b'); break;
            case 'f': value.push_back('\f'); break;
            case 'n': value.push_back('\n'); break;
            case 'r': value.push_back('\r'); break;
            case 't': value.push_back('\t'); break;
            case 'u': {
              int32_t code = 0;
              if (pos_ + 4 > len_) {
                ret = OB_ERR_UNEXPECTED;
                err_pos = pos_;
              } else {
                bool hex_valid = true;
                for (int64_t i = 0; i < 4 && hex_valid; ++i) {
                  const char hc = data_[pos_ + i];
                  int32_t digit = -1;
                  if (hc >= '0' && hc <= '9') {
                    digit = hc - '0';
                  } else if (hc >= 'a' && hc <= 'f') {
                    digit = hc - 'a' + 10;
                  } else if (hc >= 'A' && hc <= 'F') {
                    digit = hc - 'A' + 10;
                  } else {
                    hex_valid = false;
                  }
                  if (hex_valid) {
                    code = code * 16 + digit;
                  }
                }
                if (!hex_valid) {
                  ret = OB_ERR_UNEXPECTED;
                  err_pos = pos_;
                } else {
                  pos_ += 4;
                  // encode to utf-8, surrogate pairs are not merged
                  if (code < 0x80) {
                    value.push_back(static_cast<char>(code));
                  } else if (code < 0x800) {
                    value.push_back(static_cast<char>(0xC0 | (code >> 6)));
                    value.push_back(static_cast<char>(0x80 | (code & 0x3F)));
                  } else {
                    value.push_back(static_cast<char>(0xE0 | (code >> 12)));
                    value.push_back(static_cast<char>(0x80 | ((code >> 6) & 0x3F)));
                    value.push_back(static_cast<char>(0x80 | (code & 0x3F)));
                  }
                }
              }
              break;
            }
            default: {
              ret = OB_ERR_UNEXPECTED;
              err_pos = pos_ - 1;
              break;
            }
          }
        }
      } else if (c < 0x20) {
        ret = OB_ERR_UNEXPECTED;
        err_pos = pos_;
      } else {
        value.push_back(static_cast<char>(c));
        ++pos_;
      }
    }
    if (OB_SUCC(ret) && !closed) {
      ret = OB_ERR_UNEXPECTED;
      err_pos = pos_;
    }
  }
  return ret;
}

int ObFileSqlJsonParser::parse_number(ObFileSqlJsonValue &value, int64_t &err_pos)
{
  int ret = OB_SUCCESS;
  const int64_t start = pos_;
  bool is_double = false;
  if (pos_ < len_ && ('-' == data_[pos_] || '+' == data_[pos_])) {
    ++pos_;
  }
  int64_t int_digits = 0;
  while (pos_ < len_ && ob_file_sql_is_digit(data_[pos_])) {
    ++pos_;
    ++int_digits;
  }
  if (pos_ < len_ && '.' == data_[pos_]) {
    is_double = true;
    ++pos_;
    while (pos_ < len_ && ob_file_sql_is_digit(data_[pos_])) {
      ++pos_;
    }
  }
  if (pos_ < len_ && ('e' == data_[pos_] || 'E' == data_[pos_])) {
    is_double = true;
    ++pos_;
    if (pos_ < len_ && ('-' == data_[pos_] || '+' == data_[pos_])) {
      ++pos_;
    }
    while (pos_ < len_ && ob_file_sql_is_digit(data_[pos_])) {
      ++pos_;
    }
  }
  err_pos = start;
  if (0 == int_digits) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    const std::string text(data_ + start, static_cast<size_t>(pos_ - start));
    if (is_double) {
      char *end_ptr = NULL;
      const double dbl = strtod(text.c_str(), &end_ptr);
      if (NULL == end_ptr || '\0' != *end_ptr) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        value.kind_ = ObFileSqlJsonValue::J_DOUBLE;
        value.dbl_ = dbl;
      }
    } else {
      char *end_ptr = NULL;
      errno = 0;
      const long long int_val = strtoll(text.c_str(), &end_ptr, 10);
      if (0 != errno || NULL == end_ptr || '\0' != *end_ptr) {
        // integer overflow, degrade to double
        char *dbl_end = NULL;
        const double dbl = strtod(text.c_str(), &dbl_end);
        if (NULL == dbl_end || '\0' != *dbl_end) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          value.kind_ = ObFileSqlJsonValue::J_DOUBLE;
          value.dbl_ = dbl;
        }
      } else {
        value.kind_ = ObFileSqlJsonValue::J_INT;
        value.int_ = static_cast<int64_t>(int_val);
      }
    }
  }
  return ret;
}

int ObFileSqlJsonParser::parse_value(ObFileSqlJsonValue &value, int64_t &err_pos,
                                     const int64_t depth)
{
  int ret = OB_SUCCESS;
  if (depth > OB_FILE_SQL_MAX_JSON_DEPTH) {
    ret = OB_SIZE_OVERFLOW;
    err_pos = pos_;
  } else if (OB_FAIL(skip_space())) {
  } else if (pos_ >= len_) {
    ret = OB_ERR_UNEXPECTED;
    err_pos = pos_;
  } else {
    const char c = data_[pos_];
    if ('"' == c) {
      value.kind_ = ObFileSqlJsonValue::J_STRING;
      ret = parse_string(value.str_, err_pos);
    } else if ('{' == c || '[' == c) {
      value.kind_ = ('{' == c) ? ObFileSqlJsonValue::J_OBJECT : ObFileSqlJsonValue::J_ARRAY;
      const char open = c;
      const char close = ('{' == c) ? '}' : ']';
      int64_t level = 0;
      bool closed = false;
      while (OB_SUCC(ret) && pos_ < len_ && !closed) {
        const char cur = data_[pos_];
        if ('"' == cur) {
          std::string skipped;
          if (OB_FAIL(parse_string(skipped, err_pos))) {
          }
        } else if (cur == open) {
          ++level;
          ++pos_;
        } else if (cur == close) {
          --level;
          ++pos_;
          if (0 == level) {
            closed = true;
          }
        } else {
          ++pos_;
        }
      }
      if (OB_SUCC(ret) && !closed) {
        ret = OB_ERR_UNEXPECTED;
        err_pos = pos_;
      }
    } else if (0 == strncmp(data_ + pos_, "true", 4) && pos_ + 4 <= len_) {
      pos_ += 4;
      value.kind_ = ObFileSqlJsonValue::J_BOOL;
      value.bool_ = true;
    } else if (0 == strncmp(data_ + pos_, "false", 5) && pos_ + 5 <= len_) {
      pos_ += 5;
      value.kind_ = ObFileSqlJsonValue::J_BOOL;
      value.bool_ = false;
    } else if (0 == strncmp(data_ + pos_, "null", 4) && pos_ + 4 <= len_) {
      pos_ += 4;
      value.kind_ = ObFileSqlJsonValue::J_NULL;
    } else {
      ret = parse_number(value, err_pos);
    }
  }
  return ret;
}

int ObFileSqlJsonParser::parse_object(std::vector<std::string> &keys,
                                      std::vector<ObFileSqlJsonValue> &values,
                                      int64_t &err_pos)
{
  int ret = OB_SUCCESS;
  keys.clear();
  values.clear();
  err_pos = pos_;
  if (OB_FAIL(skip_space())) {
  } else if (pos_ >= len_ || '{' != data_[pos_]) {
    ret = OB_ERR_UNEXPECTED;
    err_pos = pos_;
  } else {
    ++pos_;
    bool closed = false;
    while (OB_SUCC(ret) && !closed) {
      if (OB_FAIL(skip_space())) {
      } else if (pos_ >= len_) {
        ret = OB_ERR_UNEXPECTED;
        err_pos = pos_;
      } else if ('}' == data_[pos_]) {
        ++pos_;
        closed = true;
      } else {
        std::string key;
        err_pos = pos_;
        if (OB_FAIL(parse_string(key, err_pos))) {
        } else if (OB_FAIL(skip_space())) {
        } else if (pos_ >= len_ || ':' != data_[pos_]) {
          ret = OB_ERR_UNEXPECTED;
          err_pos = pos_;
        } else {
          ++pos_;
          ObFileSqlJsonValue value;
          if (OB_FAIL(parse_value(value, err_pos, 1))) {
          } else {
            keys.push_back(key);
            values.push_back(value);
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(skip_space())) {
            } else if (pos_ < len_ && ',' == data_[pos_]) {
              ++pos_;
            } else if (pos_ < len_ && '}' == data_[pos_]) {
              ++pos_;
              closed = true;
            } else {
              ret = OB_ERR_UNEXPECTED;
              err_pos = pos_;
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && !closed) {
      ret = OB_ERR_UNEXPECTED;
      err_pos = pos_;
    }
  }
  return ret;
}

static int ob_file_sql_typename_to_type(const std::string &name, ObFileSqlType &type)
{
  int ret = OB_SUCCESS;
  if (ob_file_sql_str_eq_ci(name, "csv")) {
    type = FILE_SQL_TYPE_VARCHAR;
  } else if (ob_file_sql_str_eq_ci(name, "bigint") || ob_file_sql_str_eq_ci(name, "int")
             || ob_file_sql_str_eq_ci(name, "integer")) {
    type = FILE_SQL_TYPE_BIGINT;
  } else if (ob_file_sql_str_eq_ci(name, "double")) {
    type = FILE_SQL_TYPE_DOUBLE;
  } else if (ob_file_sql_str_eq_ci(name, "boolean") || ob_file_sql_str_eq_ci(name, "bool")) {
    type = FILE_SQL_TYPE_BOOLEAN;
  } else if (ob_file_sql_str_eq_ci(name, "varchar") || ob_file_sql_str_eq_ci(name, "string")
             || ob_file_sql_str_eq_ci(name, "text")) {
    type = FILE_SQL_TYPE_VARCHAR;
  } else if (ob_file_sql_str_eq_ci(name, "date")) {
    type = FILE_SQL_TYPE_DATE;
  } else if (ob_file_sql_str_eq_ci(name, "datetime") || ob_file_sql_str_eq_ci(name, "timestamp")) {
    type = FILE_SQL_TYPE_DATETIME;
  } else {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

static int ob_file_sql_parse_char_arg(const std::string &value, char &ch, std::string &err_msg)
{
  int ret = OB_SUCCESS;
  if (1 == value.size()) {
    ch = value[0];
  } else if (2 == value.size() && '\\' == value[0]) {
    switch (value[1]) {
      case 't': ch = '\t'; break;
      case 'n': ch = '\n'; break;
      case 'r': ch = '\r'; break;
      case '0': ch = '\0'; break;
      case '\\': ch = '\\'; break;
      default:
        ret = OB_INVALID_ARGUMENT;
        break;
    }
    if (OB_FAIL(ret)) {
      err_msg.assign("invalid escape sequence for a single character option: ").append(value);
    }
  } else if (ob_file_sql_str_eq_ci(value, "tab")) {
    ch = '\t';
  } else {
    ret = OB_INVALID_ARGUMENT;
    err_msg.assign("option value must be a single character, got: ").append(value);
  }
  return ret;
}

} // namespace

const char *ob_file_sql_format_name(const ObFileSqlFormat format)
{
  const char *name = "csv";
  switch (format) {
    case FILE_SQL_FORMAT_CSV: name = "csv"; break;
    case FILE_SQL_FORMAT_JSON: name = "json"; break;
    case FILE_SQL_FORMAT_NDJSON: name = "ndjson"; break;
    case FILE_SQL_FORMAT_PARQUET: name = "parquet"; break;
    case FILE_SQL_FORMAT_GLOB: name = "glob"; break;
    default: name = "unknown"; break;
  }
  return name;
}

const char *ob_file_sql_type_name(const ObFileSqlType type)
{
  const char *name = "varchar";
  switch (type) {
    case FILE_SQL_TYPE_NULL: name = "null"; break;
    case FILE_SQL_TYPE_BOOLEAN: name = "boolean"; break;
    case FILE_SQL_TYPE_BIGINT: name = "bigint"; break;
    case FILE_SQL_TYPE_DOUBLE: name = "double"; break;
    case FILE_SQL_TYPE_VARCHAR: name = "varchar"; break;
    case FILE_SQL_TYPE_DATE: name = "date"; break;
    case FILE_SQL_TYPE_DATETIME: name = "datetime"; break;
    default: name = "unknown"; break;
  }
  return name;
}

ObFileSqlType ob_file_sql_widen_type(const ObFileSqlType left, const ObFileSqlType right)
{
  ObFileSqlType type = FILE_SQL_TYPE_VARCHAR;
  if (left == right) {
    type = left;
  } else if (FILE_SQL_TYPE_NULL == left) {
    type = right;
  } else if (FILE_SQL_TYPE_NULL == right) {
    type = left;
  } else if ((FILE_SQL_TYPE_BIGINT == left || FILE_SQL_TYPE_BIGINT == right) &&
             (FILE_SQL_TYPE_DOUBLE == left || FILE_SQL_TYPE_DOUBLE == right)) {
    type = FILE_SQL_TYPE_DOUBLE;
  } else if ((FILE_SQL_TYPE_DATE == left || FILE_SQL_TYPE_DATE == right) &&
             (FILE_SQL_TYPE_DATETIME == left || FILE_SQL_TYPE_DATETIME == right)) {
    type = FILE_SQL_TYPE_DATETIME;
  } else {
    type = FILE_SQL_TYPE_VARCHAR;
  }
  return type;
}

bool ob_file_sql_is_blank(const char *str, const int64_t len)
{
  bool blank = (0 == len);
  bool all_space = (len > 0);
  for (int64_t i = 0; all_space && i < len; ++i) {
    if (!ob_file_sql_is_space(str[i])) {
      all_space = false;
    }
  }
  return blank || all_space;
}

int ob_file_sql_parse_int64(const char *str, const int64_t len, int64_t &value)
{
  int ret = OB_SUCCESS;
  value = 0;
  int64_t pos = 0;
  bool negative = false;
  if (0 == len) {
    ret = OB_INVALID_ARGUMENT;
  } else if ('-' == str[0] || '+' == str[0]) {
    negative = ('-' == str[0]);
    pos = 1;
  }
  if (OB_SUCC(ret)) {
    if (pos >= len) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      // accumulate in the negative space so that INT64_MIN is representable
      const int64_t cutoff = negative ? INT64_MIN : -INT64_MAX;
      int64_t acc = 0;
      for (; OB_SUCC(ret) && pos < len; ++pos) {
        const char c = str[pos];
        if (!ob_file_sql_is_digit(c)) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          const int64_t digit = c - '0';
          if (acc < (cutoff + digit) / 10) {
            ret = OB_INVALID_ARGUMENT;
          } else {
            acc = acc * 10 - digit;
          }
        }
      }
      if (OB_SUCC(ret)) {
        value = negative ? acc : -acc;
      }
    }
  }
  return ret;
}

int ob_file_sql_parse_double(const char *str, const int64_t len, double &value)
{
  int ret = OB_SUCCESS;
  value = 0.0;
  if (0 == len || len >= 512) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    char buf[512];
    MEMCPY(buf, str, len);
    buf[len] = '\0';
    char *end_ptr = NULL;
    errno = 0;
    const double dbl = strtod(buf, &end_ptr);
    if (0 != errno || NULL == end_ptr || '\0' != *end_ptr) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      value = dbl;
    }
  }
  return ret;
}

int ob_file_sql_parse_bool(const char *str, const int64_t len, bool &value)
{
  int ret = OB_SUCCESS;
  value = false;
  if (0 == len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (1 == len && ('t' == ob_file_sql_lower(str[0]))) {
    value = true;
  } else if (1 == len && ('f' == ob_file_sql_lower(str[0]))) {
    value = false;
  } else if (4 == len && 0 == strncasecmp(str, "true", 4)) {
    value = true;
  } else if (5 == len && 0 == strncasecmp(str, "false", 5)) {
    value = false;
  } else if (3 == len && 0 == strncasecmp(str, "yes", 3)) {
    value = true;
  } else if (2 == len && 0 == strncasecmp(str, "no", 2)) {
    value = false;
  } else {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

int ob_file_sql_parse_date(const char *str, const int64_t len, int32_t &days)
{
  int ret = OB_SUCCESS;
  days = 0;
  int64_t pos = 0;
  int64_t year = 0;
  int64_t month = 0;
  int64_t day = 0;
  if (OB_FAIL(ob_file_sql_parse_date_parts(str, len, year, month, day, pos))) {
  } else if (pos != len) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    days = static_cast<int32_t>(ob_file_sql_days_from_civil(year, month, day));
  }
  return ret;
}

int ob_file_sql_parse_datetime(const char *str, const int64_t len, int64_t &usec)
{
  int ret = OB_SUCCESS;
  usec = 0;
  int64_t pos = 0;
  int64_t year = 0;
  int64_t month = 0;
  int64_t day = 0;
  int64_t hour = 0;
  int64_t minute = 0;
  int64_t second = 0;
  int64_t fraction = 0;
  int64_t scale = 0;
  if (OB_FAIL(ob_file_sql_parse_date_parts(str, len, year, month, day, pos))) {
  } else if (pos >= len || (' ' != str[pos] && 'T' != str[pos] && 't' != str[pos])) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ++pos;
    if (!ob_file_sql_read_ndigits(str, len, pos, 2, hour) || pos >= len || ':' != str[pos]) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      ++pos;
      if (!ob_file_sql_read_ndigits(str, len, pos, 2, minute) || pos >= len || ':' != str[pos]) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        ++pos;
        if (!ob_file_sql_read_ndigits(str, len, pos, 2, second)) {
          ret = OB_INVALID_ARGUMENT;
        } else if (pos < len && '.' == str[pos]) {
          ++pos;
          while (pos < len && ob_file_sql_is_digit(str[pos]) && scale < OB_FILE_SQL_MAX_DATETIME_SCALE) {
            fraction = fraction * 10 + (str[pos] - '0');
            ++scale;
            ++pos;
          }
          while (pos < len && ob_file_sql_is_digit(str[pos])) {
            ++pos;
          }
          for (int64_t i = scale; i < OB_FILE_SQL_MAX_DATETIME_SCALE; ++i) {
            fraction *= 10;
          }
        }
        if (OB_SUCC(ret) && pos != len) {
          ret = OB_INVALID_ARGUMENT;
        }
        if (OB_SUCC(ret) &&
            (hour > 23 || minute > 59 || second > 59)) {
          ret = OB_INVALID_ARGUMENT;
        }
        if (OB_SUCC(ret)) {
          usec = (ob_file_sql_days_from_civil(year, month, day) * 86400 +
                  hour * 3600 + minute * 60 + second) * 1000000 + fraction;
        }
      }
    }
  }
  return ret;
}

bool ob_file_sql_value_match_type(const char *str, const int64_t len, const ObFileSqlType type)
{
  bool match = false;
  if (FILE_SQL_TYPE_VARCHAR == type) {
    match = true;
  } else if (FILE_SQL_TYPE_BOOLEAN == type) {
    bool value = false;
    match = (OB_SUCCESS == ob_file_sql_parse_bool(str, len, value));
  } else if (FILE_SQL_TYPE_BIGINT == type) {
    int64_t value = 0;
    match = (OB_SUCCESS == ob_file_sql_parse_int64(str, len, value));
  } else if (FILE_SQL_TYPE_DOUBLE == type) {
    double value = 0.0;
    match = (OB_SUCCESS == ob_file_sql_parse_double(str, len, value));
  } else if (FILE_SQL_TYPE_DATE == type) {
    int32_t value = 0;
    match = (OB_SUCCESS == ob_file_sql_parse_date(str, len, value));
  } else if (FILE_SQL_TYPE_DATETIME == type) {
    int64_t value = 0;
    match = (OB_SUCCESS == ob_file_sql_parse_datetime(str, len, value));
  } else {
    match = false;
  }
  return match;
}

// Classify one non null text value, used by CSV type inference.
static ObFileSqlType ob_file_sql_classify_value(const char *str, const int64_t len)
{
  ObFileSqlType type = FILE_SQL_TYPE_VARCHAR;
  int32_t date_value = 0;
  int64_t datetime_value = 0;
  int64_t int_value = 0;
  double dbl_value = 0.0;
  if (4 == len && 0 == strncasecmp(str, "true", 4)) {
    type = FILE_SQL_TYPE_BOOLEAN;
  } else if (5 == len && 0 == strncasecmp(str, "false", 5)) {
    type = FILE_SQL_TYPE_BOOLEAN;
  } else if (OB_SUCCESS == ob_file_sql_parse_int64(str, len, int_value)) {
    type = FILE_SQL_TYPE_BIGINT;
  } else if (OB_SUCCESS == ob_file_sql_parse_double(str, len, dbl_value)) {
    type = FILE_SQL_TYPE_DOUBLE;
  } else if (OB_SUCCESS == ob_file_sql_parse_date(str, len, date_value)) {
    type = FILE_SQL_TYPE_DATE;
  } else if (OB_SUCCESS == ob_file_sql_parse_datetime(str, len, datetime_value)) {
    type = FILE_SQL_TYPE_DATETIME;
  } else {
    type = FILE_SQL_TYPE_VARCHAR;
  }
  return type;
}

bool ObFileSqlOptions::is_null_field(const char *str, const int64_t len, const bool quoted) const
{
  bool is_null = false;
  if (null_str_.empty()) {
    if (null_unquoted_only_ && quoted) {
      is_null = false;
    } else if (trim_space_ && !quoted) {
      is_null = ob_file_sql_is_blank(str, len);
    } else {
      is_null = (0 == len);
    }
  } else if (!quoted || !null_unquoted_only_) {
    is_null = (static_cast<int64_t>(null_str_.size()) == len &&
               0 == (len > 0 ? memcmp(str, null_str_.data(), static_cast<size_t>(len)) : 0));
  }
  return is_null;
}

int ObFileSqlOptions::set_named_arg(const std::string &name, const std::string &value,
                                    std::string &err_msg)
{
  int ret = OB_SUCCESS;
  const std::string trimmed = ob_file_sql_trim(value);
  if (ob_file_sql_str_eq_ci(name, "delimiter") || ob_file_sql_str_eq_ci(name, "delim")
      || ob_file_sql_str_eq_ci(name, "sep") || ob_file_sql_str_eq_ci(name, "separator")) {
    ret = ob_file_sql_parse_char_arg(value, delimiter_, err_msg);
  } else if (ob_file_sql_str_eq_ci(name, "quote")) {
    ret = ob_file_sql_parse_char_arg(value, quote_, err_msg);
  } else if (ob_file_sql_str_eq_ci(name, "escape")) {
    ret = ob_file_sql_parse_char_arg(value, escape_, err_msg);
  } else if (ob_file_sql_str_eq_ci(name, "nullstr") || ob_file_sql_str_eq_ci(name, "null_str")
             || ob_file_sql_str_eq_ci(name, "null")) {
    null_str_ = value;
  } else if (ob_file_sql_str_eq_ci(name, "header")) {
    if (ob_file_sql_str_eq_ci(trimmed, "true") || ob_file_sql_str_eq_ci(trimmed, "on")
        || ob_file_sql_str_eq_ci(trimmed, "yes") || 0 == trimmed.compare("1")) {
      has_header_ = true;
      header_explicit_ = true;
    } else if (ob_file_sql_str_eq_ci(trimmed, "false") || ob_file_sql_str_eq_ci(trimmed, "off")
               || ob_file_sql_str_eq_ci(trimmed, "no") || 0 == trimmed.compare("0")) {
      has_header_ = false;
      header_explicit_ = true;
    } else {
      ret = OB_INVALID_ARGUMENT;
      err_msg.assign("header option expects a boolean value, got: ").append(trimmed);
    }
  } else if (ob_file_sql_str_eq_ci(name, "sample_size")) {
    int64_t sample_size = 0;
    if (OB_FAIL(ob_file_sql_parse_int64(trimmed.data(), static_cast<int64_t>(trimmed.size()),
                                        sample_size))) {
      err_msg.assign("sample_size expects an integer value, got: ").append(trimmed);
    } else {
      sample_size_ = sample_size;
    }
  } else if (ob_file_sql_str_eq_ci(name, "max_line_size")) {
    int64_t limit = 0;
    if (OB_FAIL(ob_file_sql_parse_int64(trimmed.data(), static_cast<int64_t>(trimmed.size()),
                                        limit)) ||
        limit <= 0) {
      ret = OB_INVALID_ARGUMENT;
      err_msg.assign("max_line_size expects a positive integer, got: ").append(trimmed);
    } else {
      max_line_size_ = limit;
    }
  } else if (ob_file_sql_str_eq_ci(name, "max_columns")) {
    int64_t limit = 0;
    if (OB_FAIL(ob_file_sql_parse_int64(trimmed.data(), static_cast<int64_t>(trimmed.size()),
                                        limit)) ||
        limit <= 0) {
      ret = OB_INVALID_ARGUMENT;
      err_msg.assign("max_columns expects a positive integer, got: ").append(trimmed);
    } else {
      max_columns_ = limit;
    }
  } else if (ob_file_sql_str_eq_ci(name, "max_records")) {
    int64_t limit = -1;
    if (OB_FAIL(ob_file_sql_parse_int64(trimmed.data(), static_cast<int64_t>(trimmed.size()),
                                        limit)) ||
        limit < 0) {
      ret = OB_INVALID_ARGUMENT;
      err_msg.assign("max_records expects a non negative integer, got: ").append(trimmed);
    } else {
      max_records_ = limit;
    }
  } else if (ob_file_sql_str_eq_ci(name, "trim_space") || ob_file_sql_str_eq_ci(name, "trim")) {
    if (ob_file_sql_str_eq_ci(trimmed, "true") || 0 == trimmed.compare("1")) {
      trim_space_ = true;
    } else if (ob_file_sql_str_eq_ci(trimmed, "false") || 0 == trimmed.compare("0")) {
      trim_space_ = false;
    } else {
      ret = OB_INVALID_ARGUMENT;
      err_msg.assign("trim_space option expects a boolean value, got: ").append(trimmed);
    }
  } else if (ob_file_sql_str_eq_ci(name, "format")) {
    if (ob_file_sql_str_eq_ci(trimmed, "csv")) {
      format_ = FILE_SQL_FORMAT_CSV;
    } else if (ob_file_sql_str_eq_ci(trimmed, "json")) {
      format_ = FILE_SQL_FORMAT_JSON;
    } else if (ob_file_sql_str_eq_ci(trimmed, "ndjson") || ob_file_sql_str_eq_ci(trimmed, "jsonl")) {
      format_ = FILE_SQL_FORMAT_NDJSON;
    } else if (ob_file_sql_str_eq_ci(trimmed, "parquet")) {
      format_ = FILE_SQL_FORMAT_PARQUET;
    } else {
      ret = OB_INVALID_ARGUMENT;
      err_msg.assign("unknown file format: ").append(trimmed);
    }
  } else if (ob_file_sql_str_eq_ci(name, "types") || ob_file_sql_str_eq_ci(name, "columns")) {
    // column type overrides, the value is a comma separated list of type names
    ret = OB_NOT_SUPPORTED;
    err_msg.assign("the option is not supported yet: ").append(name);
  } else {
    ret = OB_INVALID_ARGUMENT;
    err_msg.assign("unknown named argument: ").append(name);
  }
  return ret;
}

namespace
{

static void ob_file_sql_append_escaped(std::string &out, const std::string &in)
{
  static const char *hex = "0123456789ABCDEF";
  for (size_t i = 0; i < in.size(); ++i) {
    const unsigned char c = static_cast<unsigned char>(in[i]);
    if (('%' == c) || (';' == c) || ('=' == c) || (':' == c) || (c < 0x20) || (c > 0x7e)) {
      out.push_back('%');
      out.push_back(hex[(c >> 4) & 0x0f]);
      out.push_back(hex[c & 0x0f]);
    } else {
      out.push_back(static_cast<char>(c));
    }
  }
}

static int ob_file_sql_hex_value(const char c)
{
  int value = -1;
  if (c >= '0' && c <= '9') {
    value = c - '0';
  } else if (c >= 'A' && c <= 'F') {
    value = c - 'A' + 10;
  } else if (c >= 'a' && c <= 'f') {
    value = c - 'a' + 10;
  }
  return value;
}

static int ob_file_sql_unescape(const std::string &in, std::string &out, std::string &err_msg)
{
  int ret = OB_SUCCESS;
  out.clear();
  out.reserve(in.size());
  for (size_t i = 0; OB_SUCC(ret) && i < in.size(); ++i) {
    if ('%' == in[i]) {
      if (i + 2 >= in.size()) {
        ret = OB_ERR_UNEXPECTED;
        err_msg.assign("invalid file scan option encoding");
      } else {
        const int high = ob_file_sql_hex_value(in[i + 1]);
        const int low = ob_file_sql_hex_value(in[i + 2]);
        if (high < 0 || low < 0) {
          ret = OB_ERR_UNEXPECTED;
          err_msg.assign("invalid file scan option encoding");
        } else {
          out.push_back(static_cast<char>((high << 4) | low));
          i += 2;
        }
      }
    } else {
      out.push_back(in[i]);
    }
  }
  return ret;
}

} // namespace

int ObFileSqlOptions::encode(std::string &out) const
{
  int ret = OB_SUCCESS;
  out.clear();
  char num_buf[64];
  std::string tmp;
  out.append("format=");
  ob_file_sql_append_escaped(out, ob_file_sql_format_name(format_));
  out.append(";delimiter=");
  tmp.assign(1, delimiter_);
  ob_file_sql_append_escaped(out, tmp);
  out.append(";quote=");
  tmp.assign(1, quote_);
  ob_file_sql_append_escaped(out, tmp);
  out.append(";escape=");
  tmp.assign(1, escape_);
  ob_file_sql_append_escaped(out, tmp);
  out.append(";nullstr=");
  ob_file_sql_append_escaped(out, null_str_);
  out.append(";header=");
  out.append(has_header_ ? "1" : "0");
  out.append(header_explicit_ ? "1" : "0");
  snprintf(num_buf, sizeof(num_buf), "%lld", static_cast<long long>(sample_size_));
  out.append(";sample_size=");
  out.append(num_buf);
  snprintf(num_buf, sizeof(num_buf), "%lld", static_cast<long long>(max_line_size_));
  out.append(";max_line_size=");
  out.append(num_buf);
  snprintf(num_buf, sizeof(num_buf), "%lld", static_cast<long long>(max_columns_));
  out.append(";max_columns=");
  out.append(num_buf);
  snprintf(num_buf, sizeof(num_buf), "%lld", static_cast<long long>(max_records_));
  out.append(";max_records=");
  out.append(num_buf);
  out.append(";flags=");
  out.append(null_unquoted_only_ ? "1" : "0");
  out.append(trim_space_ ? "1" : "0");
  out.append(";");
  return ret;
}

int ObFileSqlOptions::decode(const std::string &in, std::string &err_msg)
{
  int ret = OB_SUCCESS;
  ObFileSqlOptions decoded;
  size_t pos = 0;
  while (OB_SUCC(ret) && pos < in.size()) {
    const size_t sep = in.find(';', pos);
    if (std::string::npos == sep) {
      ret = OB_ERR_UNEXPECTED;
      err_msg.assign("invalid file scan option encoding");
    } else {
      const std::string item = in.substr(pos, sep - pos);
      const size_t eq = item.find('=');
      if (std::string::npos == eq) {
        ret = OB_ERR_UNEXPECTED;
        err_msg.assign("invalid file scan option encoding");
      } else {
        const std::string key = item.substr(0, eq);
        std::string raw = item.substr(eq + 1);
        std::string value;
        if (OB_FAIL(ob_file_sql_unescape(raw, value, err_msg))) {
        } else if ("format" == key) {
          if (ob_file_sql_str_eq_ci(value, "csv")) {
            decoded.format_ = FILE_SQL_FORMAT_CSV;
          } else if (ob_file_sql_str_eq_ci(value, "json")) {
            decoded.format_ = FILE_SQL_FORMAT_JSON;
          } else if (ob_file_sql_str_eq_ci(value, "ndjson")) {
            decoded.format_ = FILE_SQL_FORMAT_NDJSON;
          } else if (ob_file_sql_str_eq_ci(value, "parquet")) {
            decoded.format_ = FILE_SQL_FORMAT_PARQUET;
          } else if (ob_file_sql_str_eq_ci(value, "glob")) {
            decoded.format_ = FILE_SQL_FORMAT_GLOB;
          } else {
            ret = OB_ERR_UNEXPECTED;
            err_msg.assign("invalid file format in plan options");
          }
        } else if ("delimiter" == key) {
          if (value.size() != 1) {
            ret = OB_ERR_UNEXPECTED;
            err_msg.assign("invalid delimiter in plan options");
          } else {
            decoded.delimiter_ = value[0];
          }
        } else if ("quote" == key) {
          if (value.size() != 1) {
            ret = OB_ERR_UNEXPECTED;
            err_msg.assign("invalid quote in plan options");
          } else {
            decoded.quote_ = value[0];
          }
        } else if ("escape" == key) {
          if (value.size() != 1) {
            ret = OB_ERR_UNEXPECTED;
            err_msg.assign("invalid escape in plan options");
          } else {
            decoded.escape_ = value[0];
          }
        } else if ("nullstr" == key) {
          decoded.null_str_ = value;
        } else if ("header" == key) {
          decoded.has_header_ = (value.size() >= 1 && '1' == value[0]);
          decoded.header_explicit_ = (value.size() >= 2 && '1' == value[1]);
        } else if ("sample_size" == key) {
          ret = ob_file_sql_parse_int64(value.data(), static_cast<int64_t>(value.size()),
                                        decoded.sample_size_);
        } else if ("max_line_size" == key) {
          ret = ob_file_sql_parse_int64(value.data(), static_cast<int64_t>(value.size()),
                                        decoded.max_line_size_);
        } else if ("max_columns" == key) {
          ret = ob_file_sql_parse_int64(value.data(), static_cast<int64_t>(value.size()),
                                        decoded.max_columns_);
        } else if ("max_records" == key) {
          ret = ob_file_sql_parse_int64(value.data(), static_cast<int64_t>(value.size()),
                                        decoded.max_records_);
        } else if ("flags" == key) {
          decoded.null_unquoted_only_ = (value.size() >= 1 && '1' == value[0]);
          decoded.trim_space_ = (value.size() >= 2 && '1' == value[1]);
        } else {
          ret = OB_ERR_UNEXPECTED;
          err_msg.assign("unknown key in plan options: ").append(key);
        }
      }
      if (OB_SUCC(ret)) {
        pos = sep + 1;
      }
    }
  }
  if (OB_SUCC(ret)) {
    *this = decoded;
  }
  return ret;
}

int ob_file_sql_get_file_stamp(const std::string &path, ObFileSqlFileStamp &stamp)
{
  int ret = OB_SUCCESS;
  struct stat file_stat;
  MEMSET(&file_stat, 0, sizeof(file_stat));
  if (0 != ::stat(path.c_str(), &file_stat)) {
    ret = OB_FILE_NOT_EXIST;
    stamp = ObFileSqlFileStamp();
  } else if (!S_ISREG(file_stat.st_mode)) {
    ret = OB_FILE_NOT_EXIST;
    stamp = ObFileSqlFileStamp();
  } else {
    stamp.exists_ = true;
    stamp.size_ = static_cast<int64_t>(file_stat.st_size);
    stamp.mtime_sec_ = static_cast<int64_t>(file_stat.st_mtime);
    stamp.mtime_nsec_ = 0;
#ifdef __linux__
    stamp.mtime_nsec_ = static_cast<int64_t>(file_stat.st_mtim.tv_nsec);
#endif
  }
  return ret;
}

namespace
{

// Case sensitive wildcard match with '*' and '?'.
static bool ob_file_sql_wildcard_match(const char *pattern, const int64_t pattern_len,
                                       const char *text, const int64_t text_len)
{
  int64_t p = 0;
  int64_t t = 0;
  int64_t star_p = -1;
  int64_t star_t = 0;
  bool matched = true;
  while (matched && t < text_len) {
    if (p < pattern_len && ('?' == pattern[p])) {
      ++p;
      ++t;
    } else if (p < pattern_len && ('*' == pattern[p])) {
      star_p = p;
      star_t = t;
      ++p;
    } else if (p < pattern_len && pattern[p] == text[t]) {
      ++p;
      ++t;
    } else if (star_p >= 0) {
      p = star_p + 1;
      ++star_t;
      t = star_t;
    } else {
      matched = false;
    }
  }
  while (matched && p < pattern_len && '*' == pattern[p]) {
    ++p;
  }
  return matched && p == pattern_len;
}

static int ob_file_sql_glob_dir(const std::string &dir, const std::string &pattern,
                                std::vector<std::string> &files, std::string &err_msg)
{
  int ret = OB_SUCCESS;
  const std::string dir_path = dir.empty() ? std::string(".") : dir;
  DIR *handle = ::opendir(dir_path.c_str());
  if (NULL == handle) {
    ret = OB_FILE_NOT_EXIST;
    err_msg.assign("directory does not exist or is not readable: ").append(dir_path);
  } else {
    struct dirent *entry = NULL;
    while (OB_SUCC(ret) && NULL != (entry = ::readdir(handle))) {
      const char *name = entry->d_name;
      if (0 == strcmp(name, ".") || 0 == strcmp(name, "..")) {
        continue;
      }
      const int64_t name_len = static_cast<int64_t>(strlen(name));
      if (!ob_file_sql_wildcard_match(pattern.data(), static_cast<int64_t>(pattern.size()),
                                      name, name_len)) {
        continue;
      }
      std::string full = dir.empty() ? std::string(name) : (dir + "/" + name);
      struct stat file_stat;
      MEMSET(&file_stat, 0, sizeof(file_stat));
      if (0 == ::stat(full.c_str(), &file_stat) && S_ISREG(file_stat.st_mode)) {
        files.push_back(full);
        if (static_cast<int64_t>(files.size()) > OB_FILE_SQL_MAX_GLOB_FILES) {
          ret = OB_SIZE_OVERFLOW;
          err_msg.assign("too many files matched by the glob pattern");
        }
      }
    }
    ::closedir(handle);
  }
  return ret;
}

} // namespace

int ob_file_sql_glob(const std::string &pattern, std::vector<std::string> &files,
                     std::string &err_msg)
{
  int ret = OB_SUCCESS;
  files.clear();
  if (pattern.empty()) {
    ret = OB_INVALID_ARGUMENT;
    err_msg.assign("glob pattern is empty");
  } else {
    const size_t slash = pattern.find_last_of('/');
    std::string dir;
    std::string name_pattern = pattern;
    if (std::string::npos != slash) {
      dir = pattern.substr(0, slash);
      name_pattern = pattern.substr(slash + 1);
      if (dir.empty()) {
        dir = "/";
      }
    }
    bool has_wildcard = false;
    for (size_t i = 0; i < name_pattern.size(); ++i) {
      if ('*' == name_pattern[i] || '?' == name_pattern[i]) {
        has_wildcard = true;
      }
    }
    if (!has_wildcard) {
      ObFileSqlFileStamp stamp;
      if (OB_FAIL(ob_file_sql_get_file_stamp(pattern, stamp))) {
        err_msg.assign("file does not exist: ").append(pattern);
      } else {
        files.push_back(pattern);
      }
    } else if (name_pattern.find('/') != std::string::npos) {
      ret = OB_NOT_SUPPORTED;
      err_msg.assign("glob pattern does not support wildcards in directory names: ").append(pattern);
    } else {
      ret = ob_file_sql_glob_dir(dir, name_pattern, files, err_msg);
      if (OB_SUCC(ret)) {
        std::sort(files.begin(), files.end());
      }
    }
  }
  return ret;
}

int ob_file_sql_split_csv_record(const char *data, const int64_t len,
                                 const ObFileSqlOptions &options,
                                 std::vector<ObFileSqlCell> &fields,
                                 bool &has_unescaped_newline)
{
  int ret = OB_SUCCESS;
  fields.clear();
  has_unescaped_newline = false;
  ObFileSqlCell cell;
  bool in_quotes = false;
  bool field_quoted = false;
  bool record_done = false;
  int64_t i = 0;
  while (OB_SUCC(ret) && i <= len && !record_done) {
    if (i == len) {
      cell.is_null_ = options.is_null_field(cell.str_.data(),
                                            static_cast<int64_t>(cell.str_.size()),
                                            field_quoted);
      cell.quoted_ = field_quoted;
      fields.push_back(cell);
      record_done = true;
    } else {
      const char c = data[i];
      if (in_quotes) {
        if (c == options.escape_ && options.escape_ != options.quote_) {
          if (i + 1 < len) {
            cell.str_.push_back(data[i + 1]);
            i += 2;
          } else {
            ret = OB_ERR_UNEXPECTED;
          }
        } else if (c == options.quote_) {
          if (i + 1 < len && data[i + 1] == options.quote_) {
            cell.str_.push_back(options.quote_);
            i += 2;
          } else {
            in_quotes = false;
            ++i;
          }
        } else {
          cell.str_.push_back(c);
          ++i;
        }
      } else if (c == options.delimiter_) {
        cell.is_null_ = options.is_null_field(cell.str_.data(),
                                              static_cast<int64_t>(cell.str_.size()),
                                              field_quoted);
        cell.quoted_ = field_quoted;
        fields.push_back(cell);
        cell = ObFileSqlCell();
        field_quoted = false;
        ++i;
      } else if ('\r' == c || '\n' == c) {
        record_done = true;
        if ('\r' == c && i + 1 < len && '\n' == data[i + 1]) {
          ++i;
        }
        ++i;
        cell.is_null_ = options.is_null_field(cell.str_.data(),
                                              static_cast<int64_t>(cell.str_.size()),
                                              field_quoted);
        cell.quoted_ = field_quoted;
        fields.push_back(cell);
      } else if (c == options.quote_ && cell.str_.empty()) {
        in_quotes = true;
        field_quoted = true;
        ++i;
      } else {
        cell.str_.push_back(c);
        ++i;
      }
    }
    if (OB_SUCC(ret) && !record_done && in_quotes) {
      has_unescaped_newline = true;
    }
  }
  return ret;
}

ObFileSqlReader::ObFileSqlReader()
  : is_opened_(false),
    format_(FILE_SQL_FORMAT_CSV),
    options_(),
    schema_(),
    stamp_(),
    path_(),
    err_msg_(),
    row_index_(0),
    scanned_bytes_(0),
    fp_(NULL),
    buf_(),
    buf_pos_(0),
    buf_len_(0),
    file_eof_(false),
    csv_header_consumed_(false),
    csv_header_(),
    json_keys_(),
    parquet_(NULL),
    glob_files_()
{
}

ObFileSqlReader::~ObFileSqlReader()
{
  close();
}

void ObFileSqlReader::close()
{
  if (NULL != fp_) {
    ::fclose(fp_);
    fp_ = NULL;
  }
  if (NULL != parquet_) {
    ObFileSqlParquetReader *reader = static_cast<ObFileSqlParquetReader *>(parquet_);
    delete reader;
    parquet_ = NULL;
  }
  buf_.clear();
  buf_pos_ = 0;
  buf_len_ = 0;
  file_eof_ = false;
  is_opened_ = false;
}

int ObFileSqlReader::report_error(const int ret_code, const char *fmt, ...)
{
  char msg[1024];
  msg[0] = '\0';
  va_list args;
  va_start(args, fmt);
  vsnprintf(msg, sizeof(msg), fmt, args);
  va_end(args);
  err_msg_.assign(msg);
  return ret_code;
}

int ObFileSqlReader::read_more()
{
  int ret = OB_SUCCESS;
  if (NULL == fp_) {
    ret = OB_ERR_UNEXPECTED;
  } else if (buf_pos_ < buf_len_) {
    // still has buffered data
  } else if (file_eof_) {
    // nothing to read
  } else {
    buf_pos_ = 0;
    buf_len_ = 0;
    const size_t read_size = ::fread(&buf_[0], 1, buf_.size(), fp_);
    if (0 != ::ferror(fp_)) {
      ret = report_error(OB_IO_ERROR, "failed to read file '%s'", path_.c_str());
    } else {
      buf_len_ = static_cast<int64_t>(read_size);
      scanned_bytes_ += buf_len_;
      if (static_cast<size_t>(buf_len_) < buf_.size()) {
        file_eof_ = true;
      }
    }
  }
  return ret;
}

int ObFileSqlReader::ensure_buffered(const int64_t need)
{
  int ret = OB_SUCCESS;
  if (need <= 0 || NULL == fp_) {
    ret = OB_ERR_UNEXPECTED;
  } else if (buf_len_ - buf_pos_ >= need) {
    // enough data is buffered
  } else {
    const int64_t cap = static_cast<int64_t>(buf_.size());
    if (need > cap) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      const int64_t remain = buf_len_ - buf_pos_;
      if (remain > 0 && buf_pos_ > 0) {
        memmove(&buf_[0], &buf_[buf_pos_], static_cast<size_t>(remain));
      }
      buf_pos_ = 0;
      buf_len_ = remain;
      while (OB_SUCC(ret) && buf_len_ < need && !file_eof_) {
        const size_t read_size = ::fread(&buf_[buf_len_], 1,
                                         static_cast<size_t>(cap - buf_len_), fp_);
        if (0 != ::ferror(fp_)) {
          ret = report_error(OB_IO_ERROR, "failed to read file '%s'", path_.c_str());
        } else {
          scanned_bytes_ += static_cast<int64_t>(read_size);
          buf_len_ += static_cast<int64_t>(read_size);
          if (0 == read_size) {
            file_eof_ = true;
          }
        }
      }
    }
  }
  return ret;
}

int ObFileSqlReader::peek_byte(char &ch, bool &eof)
{
  int ret = OB_SUCCESS;
  ch = '\0';
  eof = true;
  if (OB_FAIL(ensure_buffered(1))) {
  } else if (buf_pos_ >= buf_len_) {
    eof = true;
  } else {
    eof = false;
    ch = buf_[buf_pos_];
  }
  return ret;
}

int ObFileSqlReader::take_byte(char &ch, bool &eof)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(peek_byte(ch, eof))) {
  } else if (!eof) {
    ++buf_pos_;
  }
  return ret;
}

int ObFileSqlReader::reset_stream()
{
  int ret = OB_SUCCESS;
  if (NULL == fp_) {
    ret = OB_ERR_UNEXPECTED;
  } else if (0 != ::fseek(fp_, 0, SEEK_SET)) {
    ret = report_error(OB_IO_ERROR, "failed to rewind file '%s'", path_.c_str());
  } else {
    buf_pos_ = 0;
    buf_len_ = 0;
    file_eof_ = false;
    row_index_ = 0;
    scanned_bytes_ = 0;
    csv_header_consumed_ = false;
    if (OB_FAIL(skip_bom())) {
    }
  }
  return ret;
}

int ObFileSqlReader::skip_bom()
{
  int ret = OB_SUCCESS;
  char ch = 0;
  bool eof = false;
  if (NULL == fp_) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(peek_byte(ch, eof))) {
  } else if (eof || static_cast<unsigned char>(ch) != 0xEF) {
    // no BOM
  } else if (OB_FAIL(ensure_buffered(3))) {
  } else if (buf_pos_ + 2 < buf_len_ &&
             static_cast<unsigned char>(buf_[buf_pos_ + 1]) == 0xBB &&
             static_cast<unsigned char>(buf_[buf_pos_ + 2]) == 0xBF) {
    buf_pos_ += 3;
  }
  return ret;
}

int ObFileSqlReader::read_text_line(std::string &line, bool &eof)
{
  int ret = OB_SUCCESS;
  line.clear();
  eof = false;
  bool line_done = false;
  while (OB_SUCC(ret) && !line_done) {
    char ch = 0;
    bool at_end = false;
    if (OB_FAIL(peek_byte(ch, at_end))) {
    } else if (at_end) {
      line_done = true;
      if (line.empty()) {
        eof = true;
      }
    } else if ('\n' == ch) {
      (void)take_byte(ch, at_end);
      line_done = true;
    } else if ('\r' == ch) {
      (void)take_byte(ch, at_end);
      char next_ch = 0;
      bool next_end = false;
      if (OB_FAIL(peek_byte(next_ch, next_end))) {
      } else if (!next_end && '\n' == next_ch) {
        (void)take_byte(next_ch, next_end);
      }
      line_done = true;
    } else {
      line.push_back(ch);
      (void)take_byte(ch, at_end);
    }
    if (OB_SUCC(ret) && static_cast<int64_t>(line.size()) > options_.max_line_size_) {
      ret = report_error(OB_SIZE_OVERFLOW, "line %lld is too long, the limit is %lld bytes",
                         static_cast<long long>(row_index_ + 1),
                         static_cast<long long>(options_.max_line_size_));
    }
  }
  return ret;
}

int ObFileSqlReader::open(const std::string &path, const ObFileSqlOptions &options,
                          const ObFileSqlSchema *expected_schema)
{
  int ret = OB_SUCCESS;
  close();
  err_msg_.clear();
  path_ = path;
  options_ = options;
  format_ = options.format_;
  schema_.reset();
  row_index_ = 0;
  scanned_bytes_ = 0;
  csv_header_consumed_ = false;
  csv_header_.clear();
  json_keys_.clear();
  glob_files_.clear();

  if (FILE_SQL_FORMAT_GLOB == format_) {
    // a glob pattern does not name a single file, the matched files are discovered here
    if (OB_FAIL(open_glob(path, options))) {
    }
  } else if (OB_FAIL(ob_file_sql_get_file_stamp(path, stamp_))) {
    report_error(OB_FILE_NOT_EXIST, "file does not exist or is not readable: %s", path.c_str());
  } else if (FILE_SQL_FORMAT_PARQUET == format_) {
    if (OB_FAIL(open_parquet())) {
    } else if (OB_FAIL(detect_parquet_schema(schema_, err_msg_))) {
    }
  } else {
    fp_ = ::fopen(path.c_str(), "rb");
    if (NULL == fp_) {
      ret = report_error(OB_FILE_NOT_EXIST, "can not open file '%s'", path.c_str());
    } else {
      buf_.resize(static_cast<size_t>(OB_FILE_SQL_BUF_SIZE));
      buf_pos_ = 0;
      buf_len_ = 0;
      file_eof_ = false;
      // detect the schema from the beginning of the file, the UTF-8 BOM is skipped by
      // reset_stream so that it never becomes a part of the first column name
      if (OB_FAIL(reset_stream())) {
      } else if (FILE_SQL_FORMAT_CSV == format_) {
        if (OB_FAIL(detect_csv_schema(schema_, err_msg_))) {
        } else if (OB_FAIL(reset_stream())) {
        } else if (OB_FAIL(open_csv())) {
        }
      } else if (FILE_SQL_FORMAT_JSON == format_ || FILE_SQL_FORMAT_NDJSON == format_) {
        if (OB_FAIL(detect_jsonl_schema(schema_, err_msg_))) {
        } else if (OB_FAIL(reset_stream())) {
        } else if (OB_FAIL(open_jsonl())) {
        }
      } else {
        ret = report_error(OB_INVALID_ARGUMENT, "unsupported file format");
      }
    }
  }

  if (OB_SUCC(ret) && NULL != expected_schema) {
    bool same = (expected_schema->count() == schema_.count());
    for (int64_t i = 0; same && i < schema_.count(); ++i) {
      if (expected_schema->at(i).name_ != schema_.at(i).name_ ||
          expected_schema->at(i).type_ != schema_.at(i).type_) {
        same = false;
      }
    }
    if (!same) {
      ret = report_error(OB_ERR_UNEXPECTED,
                         "file '%s' changed: the schema differs from the one used to "
                         "resolve the statement", path.c_str());
    }
  }
  if (OB_SUCC(ret)) {
    is_opened_ = true;
  } else {
    close();
    if (err_msg_.empty()) {
      err_msg_.assign("failed to open file: ").append(path);
    }
  }
  return ret;
}

int ObFileSqlReader::open_csv()
{
  int ret = OB_SUCCESS;
  if (NULL == fp_) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    row_index_ = 0;
    csv_header_.clear();
    csv_header_consumed_ = false;
    if (options_.has_header_) {
      std::vector<ObFileSqlCell> header_row;
      bool eof = false;
      if (OB_FAIL(read_csv_record(header_row, eof))) {
      } else if (eof) {
        // an empty file yields an empty schema, detected during inference
      } else {
        for (size_t i = 0; i < header_row.size(); ++i) {
          csv_header_.push_back(header_row[i].is_null_ ? std::string() : header_row[i].str_);
        }
        csv_header_consumed_ = true;
      }
    }
  }
  return ret;
}

int ObFileSqlReader::next_csv_row(std::vector<ObFileSqlCell> &row, bool &eof)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(read_csv_record(row, eof))) {
  } else if (eof) {
  } else if (static_cast<int64_t>(row.size()) != schema_.count()) {
    ret = report_error(OB_ERR_UNEXPECTED,
                       "bad csv row at line %lld: expected %lld fields but got %lld",
                       static_cast<long long>(row_index_ + 1),
                       static_cast<long long>(schema_.count()),
                       static_cast<long long>(row.size()));
  }
  if (OB_SUCC(ret) && !eof) {
    ++row_index_;
  }
  return ret;
}

int ObFileSqlReader::read_csv_record(std::vector<ObFileSqlCell> &row, bool &eof)
{
  int ret = OB_SUCCESS;
  row.clear();
  eof = false;
  bool any_consumed = false;
  bool in_quotes = false;
  bool field_quoted = false;
  bool record_done = false;
  ObFileSqlCell cell;
  int64_t field_bytes = 0;
  while (OB_SUCC(ret) && !record_done) {
    char ch = 0;
    bool at_end = false;
    if (OB_FAIL(peek_byte(ch, at_end))) {
    } else if (at_end) {
      if (!any_consumed && row.empty() && cell.str_.empty() && !field_quoted) {
        eof = true;
      } else {
        cell.is_null_ = options_.is_null_field(cell.str_.data(),
                                              static_cast<int64_t>(cell.str_.size()),
                                              field_quoted);
        cell.quoted_ = field_quoted;
        row.push_back(cell);
      }
      record_done = true;
    } else if (in_quotes) {
      if (ch == options_.escape_ && options_.escape_ != options_.quote_) {
        char next_ch = 0;
        bool next_end = false;
        if (OB_FAIL(take_byte(ch, at_end))) {
        } else if (OB_FAIL(peek_byte(next_ch, next_end))) {
        } else if (next_end) {
          ret = report_error(OB_ERR_UNEXPECTED,
                             "unterminated quoted field at line %lld",
                             static_cast<long long>(row_index_ + 1));
        } else {
          cell.str_.push_back(next_ch);
          (void)take_byte(next_ch, next_end);
        }
      } else if (ch == options_.quote_) {
        char next_ch = 0;
        bool next_end = false;
        if (OB_FAIL(take_byte(ch, at_end))) {
        } else if (OB_FAIL(peek_byte(next_ch, next_end))) {
        } else if (!next_end && options_.escape_ == options_.quote_ &&
                   next_ch == options_.quote_) {
          cell.str_.push_back(options_.quote_);
          (void)take_byte(next_ch, next_end);
        } else {
          in_quotes = false;
        }
      } else {
        cell.str_.push_back(ch);
        (void)take_byte(ch, at_end);
      }
    } else if (ch == options_.delimiter_) {
      cell.is_null_ = options_.is_null_field(cell.str_.data(),
                                            static_cast<int64_t>(cell.str_.size()),
                                            field_quoted);
      cell.quoted_ = field_quoted;
      row.push_back(cell);
      cell = ObFileSqlCell();
      field_quoted = false;
      field_bytes = 0;
      any_consumed = true;
      (void)take_byte(ch, at_end);
    } else if ('\r' == ch || '\n' == ch) {
      (void)take_byte(ch, at_end);
      if ('\r' == ch) {
        char next_ch = 0;
        bool next_end = false;
        if (OB_FAIL(peek_byte(next_ch, next_end))) {
        } else if (!next_end && '\n' == next_ch) {
          (void)take_byte(next_ch, next_end);
        }
      }
      cell.is_null_ = options_.is_null_field(cell.str_.data(),
                                            static_cast<int64_t>(cell.str_.size()),
                                            field_quoted);
      cell.quoted_ = field_quoted;
      row.push_back(cell);
      record_done = true;
      any_consumed = true;
    } else if (ch == options_.quote_ && cell.str_.empty() && !field_quoted) {
      in_quotes = true;
      field_quoted = true;
      any_consumed = true;
      (void)take_byte(ch, at_end);
    } else {
      cell.str_.push_back(ch);
      ++field_bytes;
      any_consumed = true;
      (void)take_byte(ch, at_end);
    }
    if (OB_SUCC(ret) && field_bytes > options_.max_line_size_) {
      ret = report_error(OB_SIZE_OVERFLOW,
                         "field at line %lld exceeds the max_field_size limit (%lld bytes)",
                         static_cast<long long>(row_index_ + 1),
                         static_cast<long long>(options_.max_line_size_));
    }
  }
  if (OB_SUCC(ret) && row.size() > static_cast<size_t>(options_.max_columns_)) {
    ret = report_error(OB_SIZE_OVERFLOW,
                       "line %lld has %lld fields which exceeds the max_columns limit (%lld)",
                       static_cast<long long>(row_index_ + 1),
                       static_cast<long long>(row.size()),
                       static_cast<long long>(options_.max_columns_));
  }
  return ret;
}

int ObFileSqlReader::detect_csv_schema(ObFileSqlSchema &schema, std::string &err_msg)
{
  int ret = OB_SUCCESS;
  schema.reset();
  std::vector<ObFileSqlCell> row;
  bool eof = false;
  std::vector<std::string> names;
  std::vector<ObFileSqlType> types;
  int64_t data_rows = 0;
  if (OB_FAIL(read_csv_record(row, eof))) {
    err_msg = err_msg_;
  } else if (eof) {
    ret = OB_INVALID_ARGUMENT;
    err_msg.assign("can not infer the schema of an empty file");
  } else if (options_.has_header_) {
    for (size_t i = 0; i < row.size(); ++i) {
      std::string name = row[i].is_null_ ? std::string() : ob_file_sql_trim(row[i].str_);
      if (name.empty()) {
        char buf[64];
        snprintf(buf, sizeof(buf), "column%lld", static_cast<long long>(i));
        name.assign(buf);
      }
      names.push_back(name);
    }
    types.assign(names.size(), FILE_SQL_TYPE_NULL);
  } else {
    for (size_t i = 0; i < row.size(); ++i) {
      char buf[64];
      snprintf(buf, sizeof(buf), "column%lld", static_cast<long long>(i));
      names.push_back(buf);
    }
    types.assign(names.size(), FILE_SQL_TYPE_NULL);
    for (size_t i = 0; i < row.size(); ++i) {
      if (!row[i].is_null_) {
        types[i] = ob_file_sql_widen_type(types[i],
                                          ob_file_sql_classify_value(row[i].str_.data(),
                                                                     static_cast<int64_t>(row[i].str_.size())));
      }
    }
    ++data_rows;
  }
  if (OB_FAIL(ret)) {
  } else if (static_cast<int64_t>(names.size()) > options_.max_columns_) {
    ret = OB_SIZE_OVERFLOW;
    err_msg.assign("the file has too many columns");
  } else {
    while (OB_SUCC(ret)) {
      if (options_.max_records_ > 0 && data_rows >= options_.max_records_) {
        break;
      }
      bool read_eof = false;
      if (OB_FAIL(read_csv_record(row, read_eof))) {
        err_msg = err_msg_;
      } else if (read_eof) {
        break;
      } else if (row.size() != names.size()) {
        ret = OB_ERR_UNEXPECTED;
        char buf[256];
        snprintf(buf, sizeof(buf),
                 "bad csv row at line %lld: expected %lld fields but got %lld",
                 static_cast<long long>(data_rows + 1),
                 static_cast<long long>(names.size()),
                 static_cast<long long>(row.size()));
        err_msg.assign(buf);
      } else {
        for (size_t i = 0; i < row.size(); ++i) {
          if (!row[i].is_null_) {
            types[i] = ob_file_sql_widen_type(types[i],
                                              ob_file_sql_classify_value(row[i].str_.data(),
                                                                         static_cast<int64_t>(row[i].str_.size())));
          }
        }
        ++data_rows;
      }
    }
  }
  if (OB_SUCC(ret)) {
    std::vector<std::string> unique_names;
    for (size_t i = 0; i < names.size(); ++i) {
      std::string candidate = names[i];
      bool duplicated = true;
      int64_t suffix = 1;
      while (duplicated) {
        duplicated = false;
        for (size_t j = 0; j < unique_names.size(); ++j) {
          if (0 == strcasecmp(unique_names[j].c_str(), candidate.c_str())) {
            duplicated = true;
            break;
          }
        }
        if (duplicated) {
          ++suffix;
          char buf[32];
          snprintf(buf, sizeof(buf), "%lld", static_cast<long long>(suffix));
          candidate = names[i] + "_" + buf;
        }
      }
      unique_names.push_back(candidate);
    }
    for (size_t i = 0; i < unique_names.size(); ++i) {
      ObFileSqlColumnSchema column;
      column.name_ = unique_names[i];
      column.type_ = types[i];
      if (FILE_SQL_TYPE_NULL == column.type_) {
        // an all NULL column is exposed as a nullable varchar column so that it can be
        // referenced by ordinary SQL expressions
        column.type_ = FILE_SQL_TYPE_VARCHAR;
      }
      schema.columns_.push_back(column);
    }
  }
  return ret;
}

int ObFileSqlReader::open_jsonl()
{
  int ret = OB_SUCCESS;
  row_index_ = 0;
  return ret;
}

int ObFileSqlReader::next_jsonl_row(std::vector<ObFileSqlCell> &row, bool &eof)
{
  int ret = OB_SUCCESS;
  row.clear();
  eof = false;
  std::string line;
  bool got_line = false;
  while (OB_SUCC(ret) && !got_line) {
    bool line_eof = false;
    if (OB_FAIL(read_text_line(line, line_eof))) {
    } else if (line_eof) {
      eof = true;
      break;
    } else {
      ++row_index_;
      if (!ob_file_sql_is_blank(line.data(), static_cast<int64_t>(line.size()))) {
        got_line = true;
      }
    }
  }
  if (OB_SUCC(ret) && got_line) {
    ObFileSqlJsonParser parser;
    parser.init(line.data(), static_cast<int64_t>(line.size()));
    std::vector<std::string> keys;
    std::vector<ObFileSqlJsonValue> values;
    int64_t err_pos = 0;
    if (OB_FAIL(parser.parse_object(keys, values, err_pos))) {
      ret = report_error(OB_ERR_UNEXPECTED, "invalid json at line %lld near column %lld",
                         static_cast<long long>(row_index_),
                         static_cast<long long>(err_pos + 1));
    } else {
      for (size_t i = 0; OB_SUCC(ret) && i < values.size(); ++i) {
        if (ObFileSqlJsonValue::J_OBJECT == values[i].kind_ ||
            ObFileSqlJsonValue::J_ARRAY == values[i].kind_) {
          ret = report_error(OB_NOT_SUPPORTED,
                             "nested object or array is not supported at line %lld, key '%s'",
                             static_cast<long long>(row_index_), keys[i].c_str());
        }
      }
      if (OB_SUCC(ret)) {
        for (int64_t i = 0; i < schema_.count(); ++i) {
          ObFileSqlCell cell;
          cell.is_null_ = true;
          for (size_t j = 0; j < keys.size(); ++j) {
            if (keys[j] == schema_.at(i).name_) {
              if (ObFileSqlJsonValue::J_NULL != values[j].kind_) {
                cell.is_null_ = false;
                if (ObFileSqlJsonValue::J_BOOL == values[j].kind_) {
                  cell.str_.assign(values[j].bool_ ? "true" : "false");
                } else if (ObFileSqlJsonValue::J_INT == values[j].kind_) {
                  char buf[32];
                  snprintf(buf, sizeof(buf), "%lld", static_cast<long long>(values[j].int_));
                  cell.str_.assign(buf);
                } else if (ObFileSqlJsonValue::J_DOUBLE == values[j].kind_) {
                  char buf[64];
                  snprintf(buf, sizeof(buf), "%.17g", values[j].dbl_);
                  cell.str_.assign(buf);
                } else {
                  cell.str_.assign(values[j].str_);
                }
              }
              break;
            }
          }
          row.push_back(cell);
        }
      }
    }
  }
  return ret;
}

int ObFileSqlReader::detect_jsonl_schema(ObFileSqlSchema &schema, std::string &err_msg)
{
  int ret = OB_SUCCESS;
  schema.reset();
  json_keys_.clear();
  std::vector<ObFileSqlType> types;
  int64_t line_count = 0;
  std::string line;
  while (OB_SUCC(ret)) {
    if (options_.max_records_ > 0 && line_count >= options_.max_records_) {
      break;
    }
    bool line_eof = false;
    if (OB_FAIL(read_text_line(line, line_eof))) {
      err_msg = err_msg_;
    } else if (line_eof) {
      break;
    } else if (ob_file_sql_is_blank(line.data(), static_cast<int64_t>(line.size()))) {
      // skip blank lines
    } else {
      ++line_count;
      ObFileSqlJsonParser parser;
      parser.init(line.data(), static_cast<int64_t>(line.size()));
      std::vector<std::string> keys;
      std::vector<ObFileSqlJsonValue> values;
      int64_t err_pos = 0;
      if (OB_FAIL(parser.parse_object(keys, values, err_pos))) {
        ret = OB_ERR_UNEXPECTED;
        char buf[256];
        snprintf(buf, sizeof(buf), "invalid json at line %lld near column %lld",
                 static_cast<long long>(line_count),
                 static_cast<long long>(err_pos + 1));
        err_msg.assign(buf);
      } else {
        for (size_t i = 0; OB_SUCC(ret) && i < values.size(); ++i) {
          if (ObFileSqlJsonValue::J_OBJECT == values[i].kind_ ||
              ObFileSqlJsonValue::J_ARRAY == values[i].kind_) {
            ret = OB_NOT_SUPPORTED;
            char buf[256];
            snprintf(buf, sizeof(buf),
                     "nested object or array is not supported at line %lld, key '%s'",
                     static_cast<long long>(line_count), keys[i].c_str());
            err_msg.assign(buf);
          }
        }
      }
      for (size_t i = 0; OB_SUCC(ret) && i < keys.size(); ++i) {
        size_t idx = 0;
        bool found = false;
        for (size_t j = 0; j < json_keys_.size() && !found; ++j) {
          if (json_keys_[j] == keys[i]) {
            idx = j;
            found = true;
          }
        }
        if (!found) {
          if (static_cast<int64_t>(json_keys_.size()) >= options_.max_columns_) {
            ret = OB_SIZE_OVERFLOW;
            err_msg.assign("the json file has too many distinct keys");
          } else {
            json_keys_.push_back(keys[i]);
            types.push_back(FILE_SQL_TYPE_NULL);
            idx = json_keys_.size() - 1;
          }
        }
        if (OB_SUCC(ret)) {
          ObFileSqlType value_type = FILE_SQL_TYPE_NULL;
          if (ObFileSqlJsonValue::J_BOOL == values[i].kind_) {
            value_type = FILE_SQL_TYPE_BOOLEAN;
          } else if (ObFileSqlJsonValue::J_INT == values[i].kind_) {
            value_type = FILE_SQL_TYPE_BIGINT;
          } else if (ObFileSqlJsonValue::J_DOUBLE == values[i].kind_) {
            value_type = FILE_SQL_TYPE_DOUBLE;
          } else if (ObFileSqlJsonValue::J_STRING == values[i].kind_) {
            value_type = FILE_SQL_TYPE_VARCHAR;
          } else {
            value_type = FILE_SQL_TYPE_NULL;
          }
          types[idx] = ob_file_sql_widen_type(types[idx], value_type);
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (json_keys_.empty()) {
      ret = OB_INVALID_ARGUMENT;
      err_msg.assign("can not infer the schema of an empty json file");
    } else {
      for (size_t i = 0; i < json_keys_.size(); ++i) {
        ObFileSqlColumnSchema column;
        column.name_ = json_keys_[i];
        column.type_ = types[i];
        if (FILE_SQL_TYPE_NULL == column.type_) {
          column.type_ = FILE_SQL_TYPE_VARCHAR;
        }
        schema.columns_.push_back(column);
      }
    }
  }
  return ret;
}

int ObFileSqlReader::open_glob(const std::string &pattern, const ObFileSqlOptions &options)
{
  int ret = OB_SUCCESS;
  close();
  err_msg_.clear();
  path_ = pattern;
  options_ = options;
  format_ = FILE_SQL_FORMAT_GLOB;
  schema_.reset();
  row_index_ = 0;
  ObFileSqlColumnSchema column;
  column.name_.assign("file");
  column.type_ = FILE_SQL_TYPE_VARCHAR;
  schema_.columns_.push_back(column);
  if (OB_FAIL(ob_file_sql_glob(pattern, glob_files_, err_msg_))) {
  } else {
    is_opened_ = true;
  }
  return ret;
}

int ObFileSqlReader::next_glob_row(std::vector<ObFileSqlCell> &row, bool &eof)
{
  int ret = OB_SUCCESS;
  row.clear();
  eof = (row_index_ >= static_cast<int64_t>(glob_files_.size()));
  if (!eof) {
    ObFileSqlCell cell;
    cell.is_null_ = false;
    cell.str_ = glob_files_[static_cast<size_t>(row_index_)];
    row.push_back(cell);
    ++row_index_;
  }
  return ret;
}

int ObFileSqlReader::next_row(std::vector<ObFileSqlCell> &row, bool &eof)
{
  int ret = OB_SUCCESS;
  if (!is_opened_) {
    ret = OB_ERR_UNEXPECTED;
  } else if (FILE_SQL_FORMAT_CSV == format_) {
    ret = next_csv_row(row, eof);
  } else if (FILE_SQL_FORMAT_JSON == format_ || FILE_SQL_FORMAT_NDJSON == format_) {
    ret = next_jsonl_row(row, eof);
  } else if (FILE_SQL_FORMAT_PARQUET == format_) {
    ret = next_parquet_row(row, eof);
  } else if (FILE_SQL_FORMAT_GLOB == format_) {
    ret = next_glob_row(row, eof);
  } else {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

int ObFileSqlReader::rewind()
{
  int ret = OB_SUCCESS;
  if (!is_opened_) {
    ret = OB_ERR_UNEXPECTED;
  } else if (FILE_SQL_FORMAT_GLOB == format_) {
    row_index_ = 0;
  } else if (FILE_SQL_FORMAT_PARQUET == format_) {
    ObFileSqlParquetReader *reader = static_cast<ObFileSqlParquetReader *>(parquet_);
    if (NULL == reader) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = reader->rewind();
      row_index_ = 0;
    }
  } else if (OB_FAIL(reset_stream())) {
  } else if (FILE_SQL_FORMAT_CSV == format_ && OB_FAIL(open_csv())) {
  } else if ((FILE_SQL_FORMAT_JSON == format_ || FILE_SQL_FORMAT_NDJSON == format_) &&
             OB_FAIL(open_jsonl())) {
  }
  return ret;
}

int ob_file_sql_encode_schema(const ObFileSqlSchema &schema, std::string &out)
{
  int ret = OB_SUCCESS;
  out.clear();
  char num_buf[32];
  snprintf(num_buf, sizeof(num_buf), "%lld", static_cast<long long>(schema.count()));
  out.append(num_buf);
  out.push_back(';');
  for (int64_t i = 0; i < schema.count(); ++i) {
    ob_file_sql_append_escaped(out, schema.at(i).name_);
    out.push_back(':');
    snprintf(num_buf, sizeof(num_buf), "%lld",
             static_cast<long long>(schema.at(i).type_));
    out.append(num_buf);
    out.push_back(';');
  }
  return ret;
}

int ob_file_sql_decode_schema(const std::string &in, ObFileSqlSchema &schema)
{
  int ret = OB_SUCCESS;
  schema.reset();
  size_t pos = 0;
  const size_t sep = in.find(';', pos);
  int64_t count = 0;
  if (std::string::npos == sep ||
      OB_FAIL(ob_file_sql_parse_int64(in.data(), static_cast<int64_t>(sep), count)) ||
      count < 0) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    pos = sep + 1;
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      const size_t end = in.find(';', pos);
      if (std::string::npos == end) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        const std::string item = in.substr(pos, end - pos);
        const size_t colon = item.rfind(':');
        if (std::string::npos == colon) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          std::string name;
          std::string unescape_err;
          int64_t type = 0;
          if (OB_FAIL(ob_file_sql_unescape(item.substr(0, colon), name, unescape_err))) {
          } else if (OB_FAIL(ob_file_sql_parse_int64(item.data() + colon + 1,
                                                     static_cast<int64_t>(item.size() - colon - 1),
                                                     type))) {
          } else if (type < 0 || type >= FILE_SQL_TYPE_MAX) {
            ret = OB_ERR_UNEXPECTED;
          } else {
            ObFileSqlColumnSchema column;
            column.name_ = name;
            column.type_ = static_cast<ObFileSqlType>(type);
            schema.columns_.push_back(column);
            pos = end + 1;
          }
        }
      }
    }
  }
  return ret;
}

int ob_file_sql_infer_schema(const std::string &path,                             const ObFileSqlOptions &options,
                             ObFileSqlSchema &schema,
                             ObFileSqlFileStamp &stamp,
                             std::string &err_msg)
{
  int ret = OB_SUCCESS;
  schema.reset();
  err_msg.clear();
  if (OB_FAIL(ob_file_sql_get_file_stamp(path, stamp))) {
    err_msg.assign("file does not exist or is not readable: ").append(path);
  } else if (options.sample_size_ != -1) {
    ret = OB_NOT_SUPPORTED;
    err_msg.assign("file sql only supports sample_size = -1 (full schema inference)");
  } else if (FILE_SQL_FORMAT_PARQUET == options.format_) {
    ret = ob_file_sql_parquet_read_schema(path, schema, err_msg);
  } else {
    ObFileSqlReader reader;
    if (OB_FAIL(reader.open(path, options, NULL))) {
      err_msg = reader.get_error_msg();
    } else {
      schema = reader.get_schema();
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
