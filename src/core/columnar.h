#pragma once

#include <fstream>
#include <ios>
#include <iostream>
#include <string>

#include "src/core/column.h"
#include "src/core/schema.h"
#include "src/core/serde.h"
#include "src/core/type.h"
#include "src/util/assert.h"
#include "src/util/macro.h"

namespace ngn {

class Metadata {
 public:
  Metadata(Schema schema, std::vector<int64_t> offsets) : schema_(std::move(schema)), offsets_(std::move(offsets)) {}

  std::string Serialize() const {
    std::stringstream out;
    std::string serialized_schema = schema_.Serialize();
    Write(serialized_schema, out);

    int64_t sz = offsets_.size();
    Write(sz, out);

    for (int64_t offset : offsets_) {
      Write(offset, out);
    }

    return out.str();
  }

  static Metadata Deserialize(const std::string& data) {
    std::stringstream in(data);
    std::string serialized_schema = Read<std::string>(in);
    Schema schema = Schema::Deserialize(serialized_schema);

    int64_t sz = Read<int64_t>(in);
    std::vector<int64_t> offsets(sz);

    for (size_t i = 0; i < offsets.size(); ++i) {
      offsets[i] = Read<int64_t>(in);
    }

    return Metadata(std::move(schema), std::move(offsets));
  }

  const Schema& GetSchema() const { return schema_; }
  const std::vector<int64_t>& GetOffsets() const { return offsets_; }

 private:
  Schema schema_;
  std::vector<int64_t> offsets_;
};

class FileWriter {
 public:
  explicit FileWriter(const std::string& path, Schema schema) : path_(path), schema_(std::move(schema)) {}

  void AppendColumn(Column column) { columns_.emplace_back(std::move(column)); }

  void Finalize() && {
    std::ofstream output(path_);
    ASSERT(output.good());

    ASSERT(schema_.Fields().size() == columns_.size());

    std::vector<int64_t> column_offsets;

    for (size_t i = 0; i < columns_.size(); ++i) {
      const auto& column = columns_[i];
      ASSERT(column.GetType() == schema_.Fields()[i].type);

      column_offsets.emplace_back(output.tellp());

      std::visit([this, &output](const auto& typed_column) { this->WriteColumn(typed_column, output); },
                 column.Values());
    }

    {
      std::string serialized_metadata = Metadata(schema_, column_offsets).Serialize();
      Write(serialized_metadata, output);

      int64_t metadata_size = serialized_metadata.size() + sizeof(int64_t);
      Write(metadata_size, output);
    }

    output.close();
  }

 private:
  template <Type type>
  void WriteColumnCommon(const ArrayType<type>& values, std::ofstream& output) {
    int64_t size = values.size();
    Write(size, output);
    for (const auto& value : values) {
      Write(value, output);
    }
  }

  void WriteColumn(const ArrayType<Type::kInt64>& values, std::ofstream& output) {
    WriteColumnCommon<Type::kInt64>(values, output);
  }

  void WriteColumn(const ArrayType<Type::kString>& values, std::ofstream& output) {
    WriteColumnCommon<Type::kString>(values, output);
  }

  std::string path_;
  Schema schema_;
  std::vector<Column> columns_;
};

namespace internal {
template <Type type>
ArrayType<type> ReadColumn(std::ifstream& input);

template <>
ArrayType<Type::kString> ReadColumn(std::ifstream& input) {
  int64_t size = Read<int64_t>(input);
  ArrayType<Type::kString> result;
  result.reserve(size);
  for (int64_t i = 0; i < size; ++i) {
    result.emplace_back(Read<std::string>(input));
  }
  return result;
}

template <>
ArrayType<Type::kInt64> ReadColumn(std::ifstream& input) {
  int64_t size = Read<int64_t>(input);
  ArrayType<Type::kInt64> result;
  result.reserve(size);
  for (int64_t i = 0; i < size; ++i) {
    result.emplace_back(Read<int64_t>(input));
  }
  return result;
}
}  // namespace internal

class FileReader {
 public:
  explicit FileReader(const std::string& path)
      : file_(path), metadata_([&]() {
          ASSERT(file_.good());

          constexpr int64_t kShift = sizeof(int64_t);
          file_.seekg(-kShift, std::ios::end);

          int64_t metadata_size = Read<int64_t>(file_);
          file_.seekg(-(metadata_size + kShift), std::ios::end);

          std::string serialized_metadata = Read<std::string>(file_);
          ASSERT(metadata_size == static_cast<int64_t>(serialized_metadata.size() + sizeof(int64_t)));

          return Metadata::Deserialize(serialized_metadata);
        }()) {}

  uint64_t ColumnCount() const { return metadata_.GetOffsets().size(); }

  Column ReadColumn(uint64_t idx) const {
    uint64_t column_count = ColumnCount();
    ASSERT(idx < column_count);

    int64_t offset = metadata_.GetOffsets()[idx];
    file_.seekg(offset, std::ios::beg);

    switch (metadata_.GetSchema().Fields()[idx].type) {
      case Type::kInt64:
        return Column(internal::ReadColumn<Type::kInt64>(file_));
      case Type::kString:
        return Column(internal::ReadColumn<Type::kString>(file_));
      default:
        THROW_NOT_IMPLEMENTED;
    }
  }

 private:
  mutable std::ifstream file_;
  Metadata metadata_;
};

}  // namespace ngn
