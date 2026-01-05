#pragma once

#include <fstream>
#include <ios>
#include <iostream>
#include <string>

#include "src/core/column.h"
#include "src/util/assert.h"

namespace ngn {

class FileWriter {
 public:
  explicit FileWriter(const std::string& path) : path_(path) {}

  void AppendColumn(Column column) { columns_.emplace_back(std::move(column)); }

  void Finalize() && {
    std::ofstream output(path_);
    ASSERT(output.good());

    std::vector<uint64_t> column_offsets;

    for (auto& column : columns_) {
      auto& values = column.Values();

      column_offsets.emplace_back(output.tellp());

      uint64_t size = values.size();
      output.write(reinterpret_cast<const char*>(&size), sizeof(size));
      output.write(reinterpret_cast<const char*>(values.data()), values.size() * sizeof(values[0]));
    }

    {
      uint64_t size = column_offsets.size();
      output.write(reinterpret_cast<const char*>(&size), sizeof(size));
      output.write(reinterpret_cast<const char*>(column_offsets.data()),
                   column_offsets.size() * sizeof(column_offsets[0]));
    }

    uint64_t columns_count = columns_.size();
    output.write(reinterpret_cast<const char*>(&columns_count), sizeof(columns_count));

    output.close();
  }

 private:
  std::string path_;
  std::vector<Column> columns_;
};

class FileReader {
 public:
  explicit FileReader(const std::string& path) : file_(path) { ASSERT(file_.good()); }

  uint64_t ColumnCount() const {
    constexpr int64_t kShift = sizeof(uint64_t);
    file_.seekg(-kShift, std::ios::end);

    uint64_t columns_count;
    file_.read(reinterpret_cast<char*>(&columns_count), sizeof(columns_count));

    return columns_count;
  }

  std::vector<int64_t> Column(uint64_t idx) const {
    uint64_t column_count = ColumnCount();
    ASSERT(idx < column_count);

    constexpr int64_t kShift = sizeof(uint64_t);
    file_.seekg(-(1 + column_count - idx) * kShift, std::ios::end);

    uint64_t offset;
    file_.read(reinterpret_cast<char*>(&offset), sizeof(offset));

    file_.seekg(offset, std::ios::beg);

    uint64_t size;
    file_.read(reinterpret_cast<char*>(&size), sizeof(size));

    std::vector<int64_t> values(size);
    file_.read(reinterpret_cast<char*>(values.data()), sizeof(values[0]) * size);

    return values;
  }

 private:
  mutable std::ifstream file_;
};

}  // namespace ngn
