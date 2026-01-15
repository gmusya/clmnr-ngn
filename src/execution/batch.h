#pragma once

#include <algorithm>

#include "src/core/column.h"
#include "src/core/schema.h"
#include "src/util/assert.h"

namespace ngn {

class Batch {
 public:
  Batch(std::vector<Column> columns, Schema schema) : columns_(std::move(columns)), schema_(std::move(schema)) {
    ASSERT(!columns_.empty());
    ASSERT(schema_.Fields().size() == columns_.size());

    for (size_t i = 0; i < columns_.size(); ++i) {
      ASSERT(columns_[i].GetType() == schema_.Fields()[i].type);
    }
  }

  int64_t Rows() const { return columns_[0].Size(); }

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
};

}  // namespace ngn
