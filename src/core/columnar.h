#pragma once

#include <fstream>
#include <ios>
#include <iostream>
#include <sstream>
#include <string>

#include "src/core/column.h"
#include "src/core/schema.h"
#include "src/core/serde.h"
#include "src/core/type.h"
#include "src/util/assert.h"
#include "src/util/macro.h"

namespace ngn {

static constexpr int64_t kColumnarFooterMagic = 0x434C4D4E52524733;  // "CLMNRRG3"

class Metadata {
 public:
  Metadata(Schema schema, std::vector<int64_t> row_group_offsets, std::vector<int64_t> row_group_row_counts)
      : schema_(std::move(schema)),
        row_group_offsets_(std::move(row_group_offsets)),
        row_group_row_counts_(std::move(row_group_row_counts)) {}

  std::string Serialize() const {
    std::stringstream out;
    std::string serialized_schema = schema_.Serialize();
    Write(serialized_schema, out);

    ASSERT(row_group_offsets_.size() == row_group_row_counts_.size());

    int64_t sz = row_group_offsets_.size();
    Write(sz, out);

    for (int64_t offset : row_group_offsets_) {
      Write(offset, out);
    }

    for (int64_t row_count : row_group_row_counts_) {
      Write(row_count, out);
    }

    return out.str();
  }

  static Metadata Deserialize(const std::string& data) {
    std::stringstream in(data);
    std::string serialized_schema = Read<std::string>(in);
    Schema schema = Schema::Deserialize(serialized_schema);

    int64_t sz = Read<int64_t>(in);
    std::vector<int64_t> row_group_offsets(sz);

    for (size_t i = 0; i < row_group_offsets.size(); ++i) {
      row_group_offsets[i] = Read<int64_t>(in);
    }

    std::vector<int64_t> row_group_row_counts(sz);
    for (size_t i = 0; i < row_group_row_counts.size(); ++i) {
      row_group_row_counts[i] = Read<int64_t>(in);
    }

    return Metadata(std::move(schema), std::move(row_group_offsets), std::move(row_group_row_counts));
  }

  const Schema& GetSchema() const { return schema_; }
  const std::vector<int64_t>& GetRowGroupOffsets() const { return row_group_offsets_; }
  const std::vector<int64_t>& GetRowGroupRowCounts() const { return row_group_row_counts_; }

 private:
  Schema schema_;
  std::vector<int64_t> row_group_offsets_;
  std::vector<int64_t> row_group_row_counts_;
};

class FileWriter {
 public:
  explicit FileWriter(const std::string& path, Schema schema)
      : path_(path), schema_(std::move(schema)), output_(path, std::ios::binary) {
    ASSERT(output_.good());
  }

  void AppendRowGroup(std::vector<Column> columns) {
    ASSERT(!columns.empty());
    ASSERT(schema_.Fields().size() == columns.size());

    const int64_t row_count = static_cast<int64_t>(columns[0].Size());
    for (const auto& col : columns) {
      ASSERT(static_cast<int64_t>(col.Size()) == row_count);
    }

    for (size_t i = 0; i < columns.size(); ++i) {
      ASSERT(columns[i].GetType() == schema_.Fields()[i].type);
    }

    const int64_t row_group_start = output_.tellp();
    row_group_offsets_.emplace_back(row_group_start);
    row_group_row_counts_.emplace_back(row_count);

    Write(row_count, output_);

    // Column offsets are stored as int64_t, relative to row_group_start.
    // Layout:
    //   row_count:int64
    //   column_offsets[column_count]:int64
    //   column_0 ...
    //   column_1 ...
    //   ...
    const int64_t column_count = static_cast<int64_t>(columns.size());
    const std::streampos offsets_pos = output_.tellp();
    for (int64_t i = 0; i < column_count; ++i) {
      Write(int64_t{0}, output_);
    }

    std::vector<int64_t> column_offsets;
    column_offsets.reserve(columns.size());
    for (const auto& column : columns) {
      column_offsets.emplace_back(static_cast<int64_t>(output_.tellp()) - row_group_start);
      std::visit([this](const auto& typed_column) { this->WriteColumn(typed_column, output_); }, column.Values());
    }

    const std::streampos end_pos = output_.tellp();
    output_.seekp(offsets_pos, std::ios::beg);
    for (int64_t off : column_offsets) {
      Write(off, output_);
    }
    output_.seekp(end_pos, std::ios::beg);
  }

  void Finalize() && {
    {
      std::string serialized_metadata = Metadata(schema_, row_group_offsets_, row_group_row_counts_).Serialize();
      Write(serialized_metadata, output_);

      int64_t metadata_size = serialized_metadata.size() + sizeof(int64_t);
      Write(metadata_size, output_);
      Write(kColumnarFooterMagic, output_);
    }

    output_.close();
  }

 private:
  template <Type type>
  void WriteColumn(const ArrayType<type>& values, std::ostream& output) {
    const int64_t size = static_cast<int64_t>(values.size());
    Write(size, output);
    for (const auto& value : values) {
      Write(value, output);
    }
  }

  std::string path_;
  Schema schema_;

  std::ofstream output_;

  std::vector<int64_t> row_group_offsets_;
  std::vector<int64_t> row_group_row_counts_;
};

namespace internal {
template <Type type>
ArrayType<type> ReadColumn(std::istream& input) {
  const int64_t size = Read<int64_t>(input);
  ArrayType<type> result;
  result.reserve(size);
  for (int64_t i = 0; i < size; ++i) {
    result.emplace_back(Read<PhysicalType<type>>(input));
  }
  return result;
}
}  // namespace internal

class FileReader {
 public:
  explicit FileReader(const std::string& path)
      : file_(path, std::ios::binary), metadata_([&]() {
          ASSERT(file_.good());

          constexpr int64_t kShift = sizeof(int64_t);

          // Footer layout:
          //   ... data ...
          //   Write(serialized_metadata)  // string = [len:int64][bytes...]
          //   Write(metadata_size:int64)  // includes string length prefix
          //   Write(kColumnarFooterMagic:int64)
          file_.seekg(-kShift, std::ios::end);
          int64_t magic = Read<int64_t>(file_);
          ASSERT(magic == kColumnarFooterMagic);

          file_.seekg(-(2 * kShift), std::ios::end);
          int64_t metadata_size = Read<int64_t>(file_);
          file_.seekg(-(metadata_size + 2 * kShift), std::ios::end);

          std::string serialized_metadata = Read<std::string>(file_);
          ASSERT(metadata_size == static_cast<int64_t>(serialized_metadata.size() + sizeof(int64_t)));

          return Metadata::Deserialize(serialized_metadata);
        }()) {}

  const Schema& GetSchema() const { return metadata_.GetSchema(); }

  uint64_t ColumnCount() const { return metadata_.GetSchema().Fields().size(); }
  uint64_t RowGroupCount() const { return metadata_.GetRowGroupOffsets().size(); }

  int64_t RowGroupRowCount(uint64_t row_group_idx) const {
    ASSERT(row_group_idx < RowGroupCount());
    return metadata_.GetRowGroupRowCounts()[row_group_idx];
  }

  std::vector<Column> ReadRowGroup(uint64_t row_group_idx) const {
    ASSERT(row_group_idx < RowGroupCount());

    const int64_t offset = metadata_.GetRowGroupOffsets()[row_group_idx];
    file_.seekg(offset, std::ios::beg);

    const int64_t row_count = Read<int64_t>(file_);
    ASSERT(row_count == metadata_.GetRowGroupRowCounts()[row_group_idx]);

    std::vector<int64_t> column_offsets(ColumnCount());
    for (uint64_t i = 0; i < ColumnCount(); ++i) {
      column_offsets[i] = Read<int64_t>(file_);
      ASSERT(column_offsets[i] >= 0);
    }

    std::vector<Column> result;
    result.reserve(ColumnCount());
    for (uint64_t col_idx = 0; col_idx < ColumnCount(); ++col_idx) {
      const auto& field = metadata_.GetSchema().Fields()[col_idx];
      file_.seekg(offset + column_offsets[col_idx], std::ios::beg);
      switch (field.type) {
        case Type::kBool: {
          auto col = internal::ReadColumn<Type::kBool>(file_);
          ASSERT(static_cast<int64_t>(col.size()) == row_count);
          result.emplace_back(std::move(col));
          break;
        }
        case Type::kInt16: {
          auto col = internal::ReadColumn<Type::kInt16>(file_);
          ASSERT(static_cast<int64_t>(col.size()) == row_count);
          result.emplace_back(std::move(col));
          break;
        }
        case Type::kInt32: {
          auto col = internal::ReadColumn<Type::kInt32>(file_);
          ASSERT(static_cast<int64_t>(col.size()) == row_count);
          result.emplace_back(std::move(col));
          break;
        }
        case Type::kInt64: {
          auto col = internal::ReadColumn<Type::kInt64>(file_);
          ASSERT(static_cast<int64_t>(col.size()) == row_count);
          result.emplace_back(std::move(col));
          break;
        }
        case Type::kInt128: {
          auto col = internal::ReadColumn<Type::kInt128>(file_);
          ASSERT(static_cast<int64_t>(col.size()) == row_count);
          result.emplace_back(std::move(col));
          break;
        }
        case Type::kString: {
          auto col = internal::ReadColumn<Type::kString>(file_);
          ASSERT(static_cast<int64_t>(col.size()) == row_count);
          result.emplace_back(std::move(col));
          break;
        }
        case Type::kDate: {
          auto col = internal::ReadColumn<Type::kDate>(file_);
          ASSERT(static_cast<int64_t>(col.size()) == row_count);
          result.emplace_back(std::move(col));
          break;
        }
        case Type::kTimestamp: {
          auto col = internal::ReadColumn<Type::kTimestamp>(file_);
          ASSERT(static_cast<int64_t>(col.size()) == row_count);
          result.emplace_back(std::move(col));
          break;
        }
        case Type::kChar: {
          auto col = internal::ReadColumn<Type::kChar>(file_);
          ASSERT(static_cast<int64_t>(col.size()) == row_count);
          result.emplace_back(std::move(col));
          break;
        }
        default:
          THROW_NOT_IMPLEMENTED;
      }
    }

    return result;
  }

  Column ReadRowGroupColumn(uint64_t row_group_idx, uint64_t column_idx) const {
    ASSERT(row_group_idx < RowGroupCount());
    ASSERT(column_idx < ColumnCount());

    const int64_t offset = metadata_.GetRowGroupOffsets()[row_group_idx];
    file_.seekg(offset, std::ios::beg);

    const int64_t row_count = Read<int64_t>(file_);
    ASSERT(row_count == metadata_.GetRowGroupRowCounts()[row_group_idx]);

    // Read offsets table and jump directly to the requested column.
    std::vector<int64_t> column_offsets(ColumnCount());
    for (uint64_t i = 0; i < ColumnCount(); ++i) {
      column_offsets[i] = Read<int64_t>(file_);
      ASSERT(column_offsets[i] >= 0);
    }

    const Type col_type = metadata_.GetSchema().Fields()[column_idx].type;
    file_.seekg(offset + column_offsets[column_idx], std::ios::beg);

    switch (col_type) {
      case Type::kBool: {
        auto col = internal::ReadColumn<Type::kBool>(file_);
        ASSERT(static_cast<int64_t>(col.size()) == row_count);
        return Column(std::move(col));
      }
      case Type::kInt16: {
        auto col = internal::ReadColumn<Type::kInt16>(file_);
        ASSERT(static_cast<int64_t>(col.size()) == row_count);
        return Column(std::move(col));
      }
      case Type::kInt32: {
        auto col = internal::ReadColumn<Type::kInt32>(file_);
        ASSERT(static_cast<int64_t>(col.size()) == row_count);
        return Column(std::move(col));
      }
      case Type::kInt64: {
        auto col = internal::ReadColumn<Type::kInt64>(file_);
        ASSERT(static_cast<int64_t>(col.size()) == row_count);
        return Column(std::move(col));
      }
      case Type::kInt128: {
        auto col = internal::ReadColumn<Type::kInt128>(file_);
        ASSERT(static_cast<int64_t>(col.size()) == row_count);
        return Column(std::move(col));
      }
      case Type::kString: {
        auto col = internal::ReadColumn<Type::kString>(file_);
        ASSERT(static_cast<int64_t>(col.size()) == row_count);
        return Column(std::move(col));
      }
      case Type::kDate: {
        auto col = internal::ReadColumn<Type::kDate>(file_);
        ASSERT(static_cast<int64_t>(col.size()) == row_count);
        return Column(std::move(col));
      }
      case Type::kTimestamp: {
        auto col = internal::ReadColumn<Type::kTimestamp>(file_);
        ASSERT(static_cast<int64_t>(col.size()) == row_count);
        return Column(std::move(col));
      }
      case Type::kChar: {
        auto col = internal::ReadColumn<Type::kChar>(file_);
        ASSERT(static_cast<int64_t>(col.size()) == row_count);
        return Column(std::move(col));
      }
      default:
        THROW_NOT_IMPLEMENTED;
    }
  }

 private:
  mutable std::ifstream file_;
  Metadata metadata_;
};

}  // namespace ngn
