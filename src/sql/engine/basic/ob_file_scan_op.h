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

#include <vector>

#include "sql/engine/ob_operator.h"
#include "sql/engine/basic/ob_file_scan_utils.h"

namespace oceanbase
{
namespace sql
{

class ObExpr;

// Physical spec of a file scan. The file path, the reading options and the inferred
// schema are computed at resolve time, the output columns are carried by
// column_exprs_ and project_map_ tells which of them are referenced by the query.
class ObFileScanSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObFileScanSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
      file_path_(),
      file_options_(),
      file_schema_(),
      project_map_(),
      column_exprs_(alloc)
  {}
  common::ObString file_path_;
  common::ObString file_options_;
  common::ObString file_schema_;
  // one character per file column, '1' means the column is referenced by the query
  common::ObString project_map_;
  common::ObFixedArray<ObExpr *, common::ObIAllocator> column_exprs_;
};

// Reads the rows of a local file. The scan is streaming: only the current row is kept
// in memory, so a file larger than the memory limit can still be queried.
class ObFileScanOp : public ObOperator
{
public:
  ObFileScanOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual ~ObFileScanOp() {}
  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  virtual int inner_close() override;
  virtual void destroy() override;

private:
  int init_reader();
  int init_projection();
  int convert_row();
  int fill_column(const int64_t column_idx, const ObFileSqlCell &cell);

private:
  ObFileSqlReader reader_;
  bool reader_inited_;
  ObFileSqlOptions options_;
  std::vector<char> projected_;
  std::vector<ObFileSqlCell> cells_;
};

} // namespace sql
} // namespace oceanbase
