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

#include <cstring>

#include "sql/parser/ob_parser.h"
#include "sql/parser/parse_node.h"
#include "sql/parser/ob_item_type.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

namespace
{

// recursively search the parse tree for a node type
bool find_node_type(const ParseNode *node, const ObItemType type, int64_t &count)
{
  bool found = false;
  if (NULL != node) {
    if (node->type_ == type) {
      ++count;
      found = true;
    }
    for (int32_t i = 0; i < node->num_child_; ++i) {
      if (find_node_type(node->children_[i], type, count)) {
        found = true;
      }
    }
  }
  return found;
}

bool find_node_name(const ParseNode *node, const ObItemType type, const char *name)
{
  bool found = false;
  if (NULL != node) {
    if (node->type_ == type && NULL != node->str_value_ && NULL != name &&
        static_cast<int64_t>(strlen(name)) == node->str_len_ &&
        0 == strncasecmp(node->str_value_, name, static_cast<size_t>(node->str_len_))) {
      found = true;
    }
    for (int32_t i = 0; !found && i < node->num_child_; ++i) {
      found = find_node_name(node->children_[i], type, name);
    }
  }
  return found;
}

} // namespace

class TestFileSqlParser : public ::testing::Test
{
public:
  int parse_sql(const char *sql, ParseResult &result)
  {
    // ParseResult is a plain C structure and must be zeroed before parsing
    memset(&result, 0, sizeof(result));
    ObParser parser(allocator_, 0 /* default sql mode */);
    return parser.parse(ObString::make_string(sql), result);
  }

  ObArenaAllocator allocator_;
};

TEST_F(TestFileSqlParser, ParseReadCsv)
{
  ParseResult result;
  ASSERT_EQ(OB_SUCCESS, parse_sql("select * from read_csv('/tmp/a.csv')", result));
  ASSERT_TRUE(NULL != result.result_tree_);
  int64_t count = 0;
  EXPECT_TRUE(find_node_type(result.result_tree_, T_FILE_TABLE_EXPRESSION, count));
  EXPECT_EQ(1, count);
  EXPECT_TRUE(find_node_name(result.result_tree_, T_IDENT, "read_csv"));
}

TEST_F(TestFileSqlParser, ParseAllReadFunctions)
{
  const char *sqls[] = {
      "select * from read_csv('/tmp/a.csv')",
      "select * from read_json('/tmp/a.jsonl')",
      "select * from read_ndjson('/tmp/a.jsonl')",
      "select * from read_parquet('/tmp/a.parquet')",
      "select * from glob('/tmp/*.csv')",
  };
  const char *names[] = {"read_csv", "read_json", "read_ndjson", "read_parquet", "glob"};
  for (int64_t i = 0; i < 5; ++i) {
    ParseResult result;
    ASSERT_EQ(OB_SUCCESS, parse_sql(sqls[i], result));
    int64_t count = 0;
    EXPECT_TRUE(find_node_type(result.result_tree_, T_FILE_TABLE_EXPRESSION, count));
    EXPECT_TRUE(find_node_name(result.result_tree_, T_IDENT, names[i]));
  }
}

TEST_F(TestFileSqlParser, ParseAlias)
{
  ParseResult result;
  ASSERT_EQ(OB_SUCCESS,
            parse_sql("select t.id from read_csv('/tmp/a.csv') as t", result));
  int64_t count = 0;
  ASSERT_TRUE(find_node_type(result.result_tree_, T_FILE_TABLE_EXPRESSION, count));
  EXPECT_TRUE(find_node_name(result.result_tree_, T_IDENT, "t"));
}

TEST_F(TestFileSqlParser, ParseNamedArguments)
{
  ParseResult result;
  ASSERT_EQ(OB_SUCCESS,
            parse_sql("select * from read_csv('/tmp/a.csv', delimiter => ';', "
                      "header => false)",
                      result));
  int64_t count = 0;
  EXPECT_TRUE(find_node_type(result.result_tree_, T_FILE_TABLE_ARG, count));
  EXPECT_EQ(2, count);
}

TEST_F(TestFileSqlParser, ParseDescribeReadCsv)
{
  ParseResult result;
  ASSERT_EQ(OB_SUCCESS,
            parse_sql("describe select * from read_csv('/tmp/a.csv')", result));
  int64_t count = 0;
  EXPECT_TRUE(find_node_type(result.result_tree_, T_FILE_TABLE_EXPRESSION, count));
}

TEST_F(TestFileSqlParser, ParseJoin)
{
  ParseResult result;
  ASSERT_EQ(OB_SUCCESS,
            parse_sql("select * from read_csv('/tmp/a.csv') c "
                      "join read_parquet('/tmp/b.parquet') p on c.id = p.id",
                      result));
  int64_t count = 0;
  EXPECT_TRUE(find_node_type(result.result_tree_, T_FILE_TABLE_EXPRESSION, count));
  EXPECT_EQ(2, count);
}

TEST_F(TestFileSqlParser, RejectPlainFunctionInFrom)
{
  ParseResult result;
  // a random function is not a table source
  EXPECT_NE(OB_SUCCESS, parse_sql("select * from not_a_file_func('/tmp/a.csv')", result));
}

} // namespace sql
} // namespace oceanbase