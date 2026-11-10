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

#include "sql/engine/basic/ob_file_scan_op.h"

#include <string>

#include "sql/engine/ob_exec_context.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObFileScanSpec, ObOpSpec), file_path_, file_options_, file_schema_,
                    project_map_, column_exprs_);

ObFileScanOp::ObFileScanOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObOperator(exec_ctx, spec, input),
    reader_(),
    reader_inited_(false),
    options_(),
    projected_(),
    cells_()
{
}

void ObFileScanOp::destroy()
{
  reader_.close();
  reader_inited_ = false;
  ObOperator::destroy();
}

int ObFileScanOp::init_reader()
{
  int ret = OB_SUCCESS;
  const ObString &path = MY_SPEC.file_path_;
  const ObString &options_str = MY_SPEC.file_options_;
  const ObString &schema_str = MY_SPEC.file_schema_;
  ObFileSqlSchema expected_schema;
  std::string decode_err;
  if (path.empty()) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(options_.decode(std::string(options_str.ptr(), options_str.length()),
                                     decode_err))) {
    LOG_WARN("failed to decode the file scan options", K(ret), K(decode_err.c_str()));
  } else {
    std::string err_msg;
    if (!schema_str.empty() &&
        OB_FAIL(ob_file_sql_decode_schema(std::string(schema_str.ptr(), schema_str.length()),
                                          expected_schema))) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(reader_.open(std::string(path.ptr(), path.length()), options_,
                                    schema_str.empty() ? NULL : &expected_schema))) {
      err_msg = reader_.get_error_msg();
      if (!err_msg.empty()) {
        LOG_USER_ERROR(OB_ERR_UNEXPECTED, err_msg.c_str());
      }
    } else if (OB_FAIL(init_projection())) {
    } else {
      reader_inited_ = true;
    }
  }
  return ret;
}

int ObFileScanOp::init_projection()
{
  int ret = OB_SUCCESS;
  const int64_t column_count = reader_.get_schema().count();
  projected_.assign(static_cast<size_t>(column_count), 1);
  const ObString &project_map = MY_SPEC.project_map_;
  if (!project_map.empty()) {
    const int64_t map_len = project_map.length();
    for (int64_t i = 0; i < column_count; ++i) {
      const char flag = (i < map_len) ? project_map.ptr()[i] : '0';
      projected_[static_cast<size_t>(i)] = ('1' == flag) ? 1 : 0;
    }
  }
  if (OB_SUCC(ret) && FILE_SQL_FORMAT_PARQUET == options_.format_) {
    ret = reader_.set_projection(projected_);
  }
  return ret;
}

int ObFileScanOp::fill_column(const int64_t column_idx, const ObFileSqlCell &cell)
{
  int ret = OB_SUCCESS;
  ObExpr *expr = NULL;
  if (column_idx < 0 || column_idx >= MY_SPEC.column_exprs_.count()) {
    // the column is not part of the output, it is pruned
  } else if (column_idx < static_cast<int64_t>(projected_.size()) &&
             !projected_[static_cast<size_t>(column_idx)]) {
    // the column is not referenced by the query, skip converting it
  } else if (OB_ISNULL(expr = MY_SPEC.column_exprs_.at(column_idx))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (cell.is_null_) {
    expr->locate_datum_for_write(eval_ctx_).set_null();
    expr->set_evaluated_projected(eval_ctx_);
  } else {
    ObObj obj;
    const char *text = cell.str_.data();
    const int64_t text_len = static_cast<int64_t>(cell.str_.size());
    const ObObjType target_type = expr->datum_meta_.type_;
    if (ObIntType == target_type) {
      int64_t value = 0;
      double dbl_value = 0.0;
      if (OB_SUCCESS == ob_file_sql_parse_int64(text, text_len, value)) {
        obj.set_int(value);
      } else if (OB_SUCCESS == ob_file_sql_parse_double(text, text_len, dbl_value)) {
        obj.set_int(static_cast<int64_t>(dbl_value));
      } else {
        ret = OB_INVALID_ARGUMENT;
      }
    } else if (ObDoubleType == target_type || ObFloatType == target_type) {
      double value = 0.0;
      int64_t int_value = 0;
      if (OB_SUCCESS == ob_file_sql_parse_double(text, text_len, value)) {
        obj.set_double(value);
      } else if (OB_SUCCESS == ob_file_sql_parse_int64(text, text_len, int_value)) {
        obj.set_double(static_cast<double>(int_value));
      } else {
        ret = OB_INVALID_ARGUMENT;
      }
    } else if (ObTinyIntType == target_type) {
      bool bool_value = false;
      int64_t int_value = 0;
      if (OB_SUCCESS == ob_file_sql_parse_bool(text, text_len, bool_value)) {
        obj.set_tinyint(static_cast<int8_t>(bool_value ? 1 : 0));
      } else if (OB_SUCCESS == ob_file_sql_parse_int64(text, text_len, int_value)) {
        obj.set_tinyint(static_cast<int8_t>(int_value));
      } else {
        ret = OB_INVALID_ARGUMENT;
      }
    } else if (ObInt32Type == target_type || ObSmallIntType == target_type ||
               ObMediumIntType == target_type || ObUInt64Type == target_type) {
      int64_t int_value = 0;
      double dbl_value = 0.0;
      if (OB_SUCCESS == ob_file_sql_parse_int64(text, text_len, int_value)) {
        obj.set_int(int_value);
      } else if (OB_SUCCESS == ob_file_sql_parse_double(text, text_len, dbl_value)) {
        obj.set_int(static_cast<int64_t>(dbl_value));
      } else {
        ret = OB_INVALID_ARGUMENT;
      }
    } else if (ObNumberType == target_type) {
      // only used when an explicit cast to a decimal type is requested
      double dbl_value = 0.0;
      int64_t int_value = 0;
      if (OB_SUCCESS == ob_file_sql_parse_int64(text, text_len, int_value)) {
        obj.set_int(int_value);
      } else if (OB_SUCCESS == ob_file_sql_parse_double(text, text_len, dbl_value)) {
        obj.set_double(dbl_value);
      } else {
        ret = OB_INVALID_ARGUMENT;
      }
    } else if (ObDateType == target_type) {
      int32_t days = 0;
      if (OB_SUCCESS == ob_file_sql_parse_date(text, text_len, days)) {
        obj.set_date(days);
      } else {
        int64_t msec = 0;
        if (OB_SUCCESS == ob_file_sql_parse_datetime(text, text_len, msec)) {
          obj.set_date(static_cast<int32_t>(msec / 86400000000LL));
        } else {
          ret = OB_INVALID_ARGUMENT;
        }
      }
    } else if (ObDateTimeType == target_type) {
      int64_t usec = 0;
      if (OB_SUCCESS == ob_file_sql_parse_datetime(text, text_len, usec)) {
        obj.set_datetime(usec);
      } else {
        int32_t days = 0;
        if (OB_SUCCESS == ob_file_sql_parse_date(text, text_len, days)) {
          obj.set_datetime(static_cast<int64_t>(days) * 86400000000LL);
        } else {
          ret = OB_INVALID_ARGUMENT;
        }
      }
    } else if (ob_is_string_type(target_type)) {
      obj.set_varchar(ObString(static_cast<int32_t>(text_len), text));
      obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      obj.set_collation_level(CS_LEVEL_IMPLICIT);
    } else {
      ret = OB_NOT_SUPPORTED;
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to convert a file value to the column type", K(ret),
               "target_type", ob_obj_type_str(target_type), "value", cell.str_.c_str());
    } else {
      ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
      if (OB_FAIL(datum.from_obj(obj, expr->obj_datum_map_))) {
      } else {
        expr->set_evaluated_projected(eval_ctx_);
      }
    }
  }
  return ret;
}

int ObFileScanOp::convert_row()
{
  int ret = OB_SUCCESS;
  const int64_t column_count = static_cast<int64_t>(cells_.size());
  for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
    if (OB_FAIL(fill_column(i, cells_[static_cast<size_t>(i)]))) {
    }
  }
  return ret;
}

int ObFileScanOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_open())) {
  } else if (OB_FAIL(init_reader())) {
  }
  return ret;
}

int ObFileScanOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
  } else if (reader_inited_ && OB_FAIL(reader_.rewind())) {
  } else if (!reader_inited_ && OB_FAIL(init_reader())) {
  }
  return ret;
}

int ObFileScanOp::inner_close()
{
  int ret = OB_SUCCESS;
  reader_.close();
  reader_inited_ = false;
  if (OB_FAIL(ObOperator::inner_close())) {
  }
  return ret;
}

int ObFileScanOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  bool eof = false;
  clear_evaluated_flag();
  if (!reader_inited_ && OB_FAIL(init_reader())) {
  } else if (OB_FAIL(ctx_.check_status())) {
  } else if (OB_FAIL(reader_.next_row(cells_, eof))) {
    const std::string &err_msg = reader_.get_error_msg();
    if (!err_msg.empty()) {
      LOG_USER_ERROR(OB_ERR_UNEXPECTED, err_msg.c_str());
    }
  } else if (eof) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(convert_row())) {
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
