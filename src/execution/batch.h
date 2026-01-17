#pragma once

#include <algorithm>

#include "src/core/column.h"
#include "src/core/schema.h"
#include "src/util/assert.h"

namespace ngn {

class Batch {
 public:
  Batch(std::vector<Column> columns, Schema schema)
      : columns_(std::move(columns)), schema_(std::move(schema)), row_count_(0) {
    ASSERT(schema_.Fields().size() == columns_.size());

    for (size_t i = 0; i < columns_.size(); ++i) {
      ASSERT(columns_[i].GetType() == schema_.Fields()[i].type);
    }

    if (!columns_.empty()) {
      row_count_ = columns_[0].Size();
    }
  }

  Batch(int64_t row_count, Schema schema) : schema_(std::move(schema)), row_count_(row_count) {
    ASSERT(schema_.Fields().empty());
    ASSERT(row_count >= 0);
  }

  int64_t Rows() const { return row_count_; }

  const Schema& GetSchema() const { return schema_; }
  const std::vector<Column>& Columns() const { return columns_; }

  Column ColumnByName(const std::string& name) const {
    const auto& fields = schema_.Fields();
    auto iter = std::find_if(fields.begin(), fields.end(), [&name](const Field& field) { return field.name == name; });
    ASSERT_WITH_MESSAGE(iter != fields.end(), "Column '" + name + "' is not found in batch");
    size_t index = iter - fields.begin();
    return columns_[index];
  }

 private:
  std::vector<Column> columns_;
  Schema schema_;
  int64_t row_count_;
};

}  // namespace ngn
