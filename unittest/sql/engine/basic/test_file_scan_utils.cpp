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

#include <gtest/gtest.h>

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "sql/engine/basic/ob_file_scan_utils.h"
#include "sql/engine/basic/ob_parquet_reader.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

namespace
{

// create a small temporary file and return its absolute-ish name
std::string make_file(const char *name, const std::string &content)
{
  std::string path = std::string("/tmp/ob_file_sql_ut_") + name;
  FILE *fp = std::fopen(path.c_str(), "wb");
  EXPECT_TRUE(NULL != fp);
  if (NULL != fp) {
    std::fwrite(content.data(), 1, content.size(), fp);
    std::fclose(fp);
  }
  return path;
}

ObFileSqlOptions csv_options(const std::string &extra = "")
{
  ObFileSqlOptions opts;
  opts.format_ = FILE_SQL_FORMAT_CSV;
  (void)extra;
  return opts;
}

int infer(const std::string &path, const ObFileSqlOptions &opts, ObFileSqlSchema &schema)
{
  ObFileSqlFileStamp stamp;
  std::string err_msg;
  return ob_file_sql_infer_schema(path, opts, schema, stamp, err_msg);
}

} // namespace

class TestFileScanUtils : public ::testing::Test
{
public:
  void TearDown() override
  {
    // best effort cleanup of the generated temp files
    std::remove("/tmp/ob_file_sql_ut_people.csv");
    std::remove("/tmp/ob_file_sql_ut_people_crlf.csv");
    std::remove("/tmp/ob_file_sql_ut_mixed.jsonl");
    std::remove("/tmp/ob_file_sql_ut_changed.csv");
    std::remove("/tmp/ob_file_sql_ut_utf8.csv");
  }
};

TEST_F(TestFileScanUtils, SnappyLiteralAndCopy)
{
  std::string out;
  std::string err;
  // a pure literal block "hello world"
  const char literal[] = {0x0b, 0x28, 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd'};
  ASSERT_EQ(OB_SUCCESS, ob_file_sql_snappy_decompress(literal, sizeof(literal), out, err));
  EXPECT_EQ(std::string("hello world"), out);

  // literal "abc" followed by a one-byte-offset copy tag, repeating the tail
  const char block[] = {0x07, 0x08, 'a', 'b', 'c', 0x01, 0x03};
  out.clear();
  ASSERT_EQ(OB_SUCCESS, ob_file_sql_snappy_decompress(block, sizeof(block), out, err));
  EXPECT_EQ(std::string("abcabca"), out);

  // truncated input must fail cleanly
  out.clear();
  EXPECT_EQ(OB_ERR_UNEXPECTED, ob_file_sql_snappy_decompress(literal, 2, out, err));
}

TEST_F(TestFileScanUtils, CsvInferTypes)
{
  const std::string path = make_file(
      "people.csv",
      "id,name,score,active,birth\n"
      "1,Alice,88.5,true,1990-05-01\n"
      "2,Bob,92,false,1988-12-31\n"
      "3,\"Carol, Jr.\",75.25,true,1995-07-04\n"
      "4,,60,false,2000-02-29\n");
  ObFileSqlOptions opts;
  opts.format_ = FILE_SQL_FORMAT_CSV;
  opts.sample_size_ = -1;
  ObFileSqlSchema schema;
  ASSERT_EQ(OB_SUCCESS, infer(path, opts, schema));
  ASSERT_EQ(5, schema.count());
  EXPECT_EQ("id", schema.at(0).name_);
  EXPECT_EQ(FILE_SQL_TYPE_BIGINT, schema.at(0).type_);
  EXPECT_EQ("name", schema.at(1).name_);
  EXPECT_EQ(FILE_SQL_TYPE_VARCHAR, schema.at(1).type_);
  EXPECT_EQ(FILE_SQL_TYPE_DOUBLE, schema.at(2).type_);
  EXPECT_EQ(FILE_SQL_TYPE_BOOLEAN, schema.at(3).type_);
  EXPECT_EQ(FILE_SQL_TYPE_DATE, schema.at(4).type_);

  ObFileSqlReader reader;
  std::vector<ObFileSqlCell> row;
  bool eof = false;
  ASSERT_EQ(OB_SUCCESS, reader.open(path, opts, &schema));
  // first data row: 1,Alice,88.5,true,1990-05-01
  ASSERT_EQ(OB_SUCCESS, reader.next_row(row, eof));
  EXPECT_FALSE(eof);
  ASSERT_EQ(5, (int)row.size());
  EXPECT_FALSE(row[0].is_null_);
  EXPECT_EQ("1", row[0].str_);
  EXPECT_FALSE(row[1].is_null_);
  EXPECT_EQ("Alice", row[1].str_);
  EXPECT_FALSE(row[3].is_null_);
  EXPECT_EQ("false", row[3].str_);
  // the quoted field keeps the comma: 3,"Carol, Jr.",75.25,true,1995-07-04
  ASSERT_EQ(OB_SUCCESS, reader.next_row(row, eof));
  ASSERT_EQ(OB_SUCCESS, reader.next_row(row, eof));
  ASSERT_EQ(5, (int)row.size());
  EXPECT_EQ("Carol, Jr.", row[1].str_);
  // the last row has an empty (unquoted) name field, which is NULL
  ASSERT_EQ(OB_SUCCESS, reader.next_row(row, eof));
  ASSERT_EQ(5, (int)row.size());
  EXPECT_TRUE(row[1].is_null_);
  reader.close();
}

TEST_F(TestFileScanUtils, CsvCrlfAndQuotedNewline)
{
  const std::string path = make_file(
      "people_crlf.csv",
      "id,note\r\n"
      "1,\"line1\r\nline2\"\r\n"
      "2,plain\r\n");
  ObFileSqlOptions opts = csv_options();
  ObFileSqlSchema schema;
  ASSERT_EQ(OB_SUCCESS, infer(path, opts, schema));
  ASSERT_EQ(2, schema.count());
  ObFileSqlReader reader;
  std::vector<ObFileSqlCell> row;
  bool eof = false;
  ASSERT_EQ(OB_SUCCESS, reader.open(path, opts, &schema));
  ASSERT_EQ(OB_SUCCESS, reader.next_row(row, eof));
  ASSERT_EQ(2, (int)row.size());
  EXPECT_EQ("line1\r\nline2", row[1].str_);
  // the quoted field keeps the embedded CRLF, which is a single row
  ASSERT_EQ(OB_SUCCESS, reader.next_row(row, eof));
  EXPECT_EQ("2", row[0].str_);
  reader.close();
}

TEST_F(TestFileScanUtils, CsvBadRowDiagnosed)
{
  const std::string path = make_file(
      "bad.csv",
      "a,b,c\n"
      "1,2\n");
  ObFileSqlOptions opts = csv_options();
  ObFileSqlSchema schema;
  std::string err_msg;
  ObFileSqlFileStamp stamp;
  int rt = ob_file_sql_infer_schema(path, opts, schema, stamp, err_msg);
  EXPECT_EQ(OB_ERR_UNEXPECTED, rt);
  EXPECT_FALSE(err_msg.empty());
}

TEST_F(TestFileScanUtils, JsonlFieldUnion)
{
  const std::string path = make_file(
      "mixed.jsonl",
      "{\"name\":\"Alice\",\"dept\":\"eng\",\"level\":3}\n"
      "{\"dept\":\"sales\",\"level\":5,\"name\":\"Bob\"}\n"
      "{\"name\":\"Carol\"}\n");
  ObFileSqlOptions opts;
  opts.format_ = FILE_SQL_FORMAT_NDJSON;
  opts.sample_size_ = -1;
  ObFileSqlSchema schema;
  ASSERT_EQ(OB_SUCCESS, infer(path, opts, schema));
  ASSERT_EQ(3, schema.count());
  EXPECT_EQ("name", schema.at(0).name_);
  EXPECT_EQ("dept", schema.at(1).name_);
  EXPECT_EQ("level", schema.at(2).name_);
  EXPECT_EQ(FILE_SQL_TYPE_BIGINT, schema.at(2).type_);

  ObFileSqlReader reader;
  std::vector<ObFileSqlCell> row;
  bool eof = false;
  ASSERT_EQ(OB_SUCCESS, reader.open(path, opts, &schema));
  // first row
  ASSERT_EQ(OB_SUCCESS, reader.next_row(row, eof));
  ASSERT_EQ(3, (int)row.size());
  EXPECT_EQ("Alice", row[0].str_);
  EXPECT_EQ("3", row[2].str_);
  // last row has a missing dept and level
  ASSERT_EQ(OB_SUCCESS, reader.next_row(row, eof));
  ASSERT_EQ(OB_SUCCESS, reader.next_row(row, eof));
  EXPECT_TRUE(row[1].is_null_);
  EXPECT_TRUE(row[2].is_null_);
  reader.close();
}

TEST_F(TestFileScanUtils, JsonlRejectNested)
{
  const std::string path = make_file(
      "nested.jsonl",
      "{\"a\":1,\"b\":{\"c\":2}}\n");
  ObFileSqlOptions opts;
  opts.format_ = FILE_SQL_FORMAT_NDJSON;
  std::string err_msg;
  ObFileSqlSchema schema;
  ObFileSqlFileStamp stamp;
  EXPECT_EQ(OB_NOT_SUPPORTED, ob_file_sql_infer_schema(path, opts, schema, stamp, err_msg));
}

TEST_F(TestFileScanUtils, FileChangeDetection)
{
  const std::string path = make_file("changed.csv", "a,b\n1,2\n");
  ObFileSqlFileStamp stamp1;
  ASSERT_EQ(OB_SUCCESS, ob_file_sql_get_file_stamp(path, stamp1));
  ObFileSqlFileStamp stamp2 = stamp1;
  EXPECT_TRUE(stamp1.same_as(stamp2));
  // rewrite the file with different content and a different mtime/size
  std::remove(path.c_str());
  FILE *fp = std::fopen(path.c_str(), "wb");
  const char content[] = "a,b,c\n1,2,3\n";
  std::fwrite(content, 1, sizeof(content) - 1, fp);
  std::fclose(fp);
  ObFileSqlFileStamp stamp3;
  ASSERT_EQ(OB_SUCCESS, ob_file_sql_get_file_stamp(path, stamp3));
  EXPECT_FALSE(stamp1.same_as(stamp3));
  std::remove(path.c_str());
}

TEST_F(TestFileScanUtils, Utf8Content)
{
  const std::string path = make_file(
      "utf8.csv",
      "name,city\n"
      "\xe6\x9d\x8e\xe5\x9b\x9b,\xe5\x8c\x97\xe4\xba\xac\n");
  ObFileSqlOptions opts = csv_options();
  ObFileSqlSchema schema;
  ASSERT_EQ(OB_SUCCESS, infer(path, opts, schema));
  ObFileSqlReader reader;
  std::vector<ObFileSqlCell> row;
  bool eof = false;
  ASSERT_EQ(OB_SUCCESS, reader.open(path, opts, &schema));
  ASSERT_EQ(OB_SUCCESS, reader.next_row(row, eof));
  ASSERT_EQ(2, (int)row.size());
  EXPECT_EQ("\xe6\x9d\x8e\xe5\x9b\x9b", row[0].str_);
  reader.close();
}

TEST_F(TestFileScanUtils, SchemaEncodeDecodeRoundTrip)
{
  ObFileSqlSchema schema;
  ObFileSqlColumnSchema c;
  c.name_ = "a b:c;d";
  c.type_ = FILE_SQL_TYPE_BIGINT;
  schema.columns_.push_back(c);
  c.name_ = "e";
  c.type_ = FILE_SQL_TYPE_VARCHAR;
  schema.columns_.push_back(c);
  std::string encoded;
  ASSERT_EQ(OB_SUCCESS, ob_file_sql_encode_schema(schema, encoded));
  ObFileSqlSchema decoded;
  ASSERT_EQ(OB_SUCCESS, ob_file_sql_decode_schema(encoded, decoded));
  ASSERT_EQ(2, decoded.count());
  EXPECT_EQ("a b:c;d", decoded.at(0).name_);
  EXPECT_EQ(FILE_SQL_TYPE_BIGINT, decoded.at(0).type_);
  EXPECT_EQ("e", decoded.at(1).name_);
}

TEST_F(TestFileScanUtils, OptionsRoundTrip)
{
  ObFileSqlOptions opts;
  opts.format_ = FILE_SQL_FORMAT_CSV;
  std::string name_str = "delimiter";
  std::string value_str = ";";
  std::string err;
  ASSERT_EQ(OB_SUCCESS, opts.set_named_arg(name_str, value_str, err));
  name_str.assign("header");
  value_str.assign("false");
  ASSERT_EQ(OB_SUCCESS, opts.set_named_arg(name_str, value_str, err));
  name_str.assign("nullstr");
  value_str.assign("NA");
  ASSERT_EQ(OB_SUCCESS, opts.set_named_arg(name_str, value_str, err));
  EXPECT_EQ(';', opts.delimiter_);
  EXPECT_FALSE(opts.has_header_);
  EXPECT_EQ(std::string("NA"), opts.null_str_);

  std::string encoded;
  ASSERT_EQ(OB_SUCCESS, opts.encode(encoded));
  ObFileSqlOptions decoded;
  ASSERT_EQ(OB_SUCCESS, decoded.decode(encoded, err));
  EXPECT_EQ(FILE_SQL_FORMAT_CSV, decoded.format_);
  EXPECT_EQ(';', decoded.delimiter_);
  EXPECT_FALSE(decoded.has_header_);
  EXPECT_EQ(std::string("NA"), decoded.null_str_);
}

} // namespace sql
} // namespace oceanbase