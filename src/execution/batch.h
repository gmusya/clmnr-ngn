#pragma once

#include <algorithm>

#include "src/core/column.h"
#include "src/core/schema.h"

namespace ngn {

class Batch {
 public:
  Batch(std::vector<Column> columns, Schema schema) : columns_(std::move(columns)), schema_(std::move(schema)) {
    ASSERT(!columns.empty());
    ASSERT(schema.Fields().size() == columns.size());

    for (size_t i = 0; i < columns.size(); ++i) {
      ASSERT(columns[i].GetType() == schema.Fields()[i].type);
    }
  }

  int64_t Rows() const { return columns_[0].Size(); }

  Column ColumnByName(const std::string& name) {
    const auto& fields = schema_.Fields();
    auto iter = std::find_if(fields.begin(), fields.end(), [&name](const Field& field) { return field.name == name; });
    ASSERT(iter != fields.end());

    size_t index = iter - fields.begin();
    return columns_[index];
  }

 private:
  std::vector<Column> columns_;
  Schema schema_;
};

}  // namespace ngn
