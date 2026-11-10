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

#define USING_LOG_PREFIX SQL_OPT

#include "sql/optimizer/ob_log_file_scan.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/engine/basic/ob_file_scan_utils.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

int ObLogFileScan::generate_access_exprs()
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = get_stmt();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_size(); ++i) {
      const ColumnItem *col_item = stmt->get_column_item(i);
      if (OB_ISNULL(col_item) || OB_ISNULL(col_item->expr_)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (col_item->table_id_ == table_id_ &&
                 col_item->expr_->is_explicited_reference() &&
                 OB_FAIL(access_exprs_.push_back(col_item->expr_))) {
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObLogFileScan::get_op_exprs(ObIArray<ObRawExpr *> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_access_exprs())) {
  } else if (OB_FAIL(append(all_exprs, access_exprs_))) {
  } else if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
  }
  return ret;
}

int ObLogFileScan::allocate_expr_post(ObAllocExprContext &ctx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < access_exprs_.count(); ++i) {
    ObRawExpr *value_col = access_exprs_.at(i);
    if (OB_FAIL(mark_expr_produced(value_col, branch_id_, id_, ctx))) {
    } else if (!is_plan_root() && OB_FAIL(add_var_to_array_no_dup(output_exprs_, value_col))) {
    } else { /*do nothing*/ }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObLogicalOperator::allocate_expr_post(ctx))) {
    }
  }
  return ret;
}

int ObLogFileScan::get_plan_item_info(PlanText &plan_text, ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = get_stmt();
  if (OB_FAIL(ObLogicalOperator::get_plan_item_info(plan_text, plan_item))) {
  } else if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    const TableItem *table_item = stmt->get_table_item_by_id(table_id_);
    const ObFileTableDef *file_def =
        (NULL == table_item) ? NULL : table_item->file_table_def_;
    if (NULL == file_def) {
      BUF_PRINT_STR("file", plan_item.special_predicates_,
                    plan_item.special_predicates_len_);
    } else {
      const char *format_name = ob_file_sql_format_name(
          static_cast<ObFileSqlFormat>(file_def->format_));
      BEGIN_BUF_PRINT;
      if (OB_FAIL(BUF_PRINTF("format: %s, file: %.*s", format_name,
                             file_def->path_.length(), file_def->path_.ptr()))) {
      } else {
        END_BUF_PRINT(plan_item.special_predicates_,
                      plan_item.special_predicates_len_);
      }
    }
  }
  if (OB_SUCC(ret)) {
    const ObString &name = get_table_name();
    BUF_PRINT_OB_STR(name.ptr(), name.length(),
                     plan_item.object_alias_, plan_item.object_alias_len_);
    BUF_PRINT_STR("FILE SCAN", plan_item.object_type_, plan_item.object_type_len_);
  }
  return ret;
}

uint64_t ObLogFileScan::hash(uint64_t seed) const
{
  seed = do_hash(table_name_, seed);
  seed = ObLogicalOperator::hash(seed);
  return seed;
}

int ObLogFileScan::is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed)
{
  is_fixed = ObOptimizerUtil::find_item(access_exprs_, expr);
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase
