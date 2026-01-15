#include "src/execution/operator.h"

#include <algorithm>
#include <numeric>

#include "src/core/columnar.h"
#include "src/execution/aggregation_executor.h"
#include "src/execution/batch.h"
#include "src/execution/stream.h"
#include "src/util/assert.h"
#include "src/util/macro.h"

namespace ngn {

class AggregationStream : public IStream<std::shared_ptr<Batch>> {
 public:
  AggregationStream(std::shared_ptr<AggregateOperator> aggregation) : op_(std::move(aggregation)) {}

  std::optional<std::shared_ptr<Batch>> Next() override {
    if (first_) {
      first_ = false;

      auto stream = Execute(op_->child);
      return Evaluate(stream, op_->aggregation);
    }

    return std::nullopt;
  }

 private:
  bool first_ = true;

  std::shared_ptr<AggregateOperator> op_;
};

class ScanStream : public IStream<std::shared_ptr<Batch>> {
 public:
  ScanStream(std::shared_ptr<ScanOperator> scan) : reader_(scan->input_path), op_(std::move(scan)) {}

  std::optional<std::shared_ptr<Batch>> Next() override {
    if (row_group_index_ >= reader_.RowGroupCount()) {
      return std::nullopt;
    }

    auto columns = reader_.ReadRowGroup(row_group_index_);
    ++row_group_index_;

    if (op_->schema != reader_.GetSchema()) {
      THROW_NOT_IMPLEMENTED;
    }

    return std::make_shared<Batch>(std::move(columns), op_->schema);
  }

 private:
  FileReader reader_;

  std::shared_ptr<ScanOperator> op_;

  size_t row_group_index_ = 0;
};

class FilterStream : public IStream<std::shared_ptr<Batch>> {
 public:
  FilterStream(std::shared_ptr<FilterOperator> filter) : op_(std::move(filter)) { stream_ = Execute(op_->child); }

  std::optional<std::shared_ptr<Batch>> Next() override {
    std::optional<std::shared_ptr<Batch>> batch = stream_->Next();
    if (batch) {
      Column condition_result = Evaluate(batch.value(), op_->condition);
      ASSERT(condition_result.GetType() == Type::kBool);

      return ApplyFilterColumn(batch.value(), std::get<ArrayType<Type::kBool>>(condition_result.Values()));
    }
    return std::nullopt;
  }

 private:
  std::shared_ptr<Batch> ApplyFilterColumn(std::shared_ptr<Batch> batch, ArrayType<Type::kBool> filter) {
    ASSERT(batch->Rows() == static_cast<int64_t>(filter.size()));

    std::vector<Column> filtered_columns;

    filtered_columns.reserve(batch->Columns().size());
    for (const auto& column : batch->Columns()) {
      std::visit([&filtered_columns]<typename T>(const T&) -> void { filtered_columns.emplace_back(T{}); },
                 column.Values());
    }

    for (int64_t i = 0; i < batch->Rows(); ++i) {
      if (filter[i].value) {
        for (size_t j = 0; j < batch->Columns().size(); ++j) {
          Column::GenericColumn& column = filtered_columns.at(j).Values();
          Value::GenericValue value = batch->Columns().at(j)[i].GetValue();

          std::visit(
              [&value]<Type type>(ArrayType<type>& column) -> void {
                if (std::holds_alternative<PhysicalType<type>>(value)) {
                  column.emplace_back(std::get<PhysicalType<type>>(value));
                } else {
                  THROW_RUNTIME_ERROR("Type mismatch, type = " + std::to_string(static_cast<int>(type)) +
                                      ", vs index " + std::to_string(value.index()));
                }
              },
              column);
        }
      }
    }

    return std::make_shared<Batch>(std::move(filtered_columns), batch->GetSchema());
  }

  std::shared_ptr<FilterOperator> op_;
  std::shared_ptr<IStream<std::shared_ptr<Batch>>> stream_;
};

class ProjectStream : public IStream<std::shared_ptr<Batch>> {
 public:
  ProjectStream(std::shared_ptr<ProjectOperator> project) : op_(std::move(project)) { stream_ = Execute(op_->child); }

  std::optional<std::shared_ptr<Batch>> Next() override {
    std::optional<std::shared_ptr<Batch>> batch = stream_->Next();
    if (batch) {
      std::vector<Column> projected_columns;
      projected_columns.reserve(op_->projections.size());

      std::vector<Field> fields;
      for (const auto& projection : op_->projections) {
        projected_columns.emplace_back(Evaluate(batch.value(), projection.expression));

        fields.emplace_back(Field(projection.name, projected_columns.back().GetType()));
      }
      return std::make_shared<Batch>(std::move(projected_columns), Schema(fields));
    }
    return std::nullopt;
  }

 private:
  std::shared_ptr<ProjectOperator> op_;
  std::shared_ptr<IStream<std::shared_ptr<Batch>>> stream_;
};

class SortStream : public IStream<std::shared_ptr<Batch>> {
 public:
  SortStream(std::shared_ptr<SortOperator> sort) : op_(sort) { stream_ = Execute(sort->child); }

  std::optional<std::shared_ptr<Batch>> Next() override {
    if (returned_) {
      return std::nullopt;
    }
    returned_ = true;

    // 1. Read all batches from the child stream
    std::vector<std::shared_ptr<Batch>> batches;
    while (auto batch = stream_->Next()) {
      batches.emplace_back(batch.value());
    }

    if (batches.empty()) {
      return std::nullopt;
    }

    // 2. Merge all batches into a single batch
    std::shared_ptr<Batch> merged = MergeBatches(batches);
    const int64_t num_rows = merged->Rows();

    if (num_rows == 0) {
      return merged;
    }

    // 3. Evaluate sort key columns
    std::vector<Column> sort_columns;
    sort_columns.reserve(op_->sort_keys.size());
    for (const auto& sort_key : op_->sort_keys) {
      sort_columns.emplace_back(Evaluate(merged, sort_key.expression));
    }

    // 4. Create index array and sort it
    std::vector<int64_t> indices(num_rows);
    std::iota(indices.begin(), indices.end(), 0);

    std::sort(indices.begin(), indices.end(), [&](int64_t a, int64_t b) {
      for (size_t k = 0; k < sort_columns.size(); ++k) {
        std::strong_ordering cmp = sort_columns[k][a] <=> sort_columns[k][b];
        if (cmp != 0) {
          return op_->sort_keys[k].is_ascending ? (cmp < 0) : (cmp > 0);
        }
      }
      return false;
    });

    // 5. Reorder all columns according to sorted indices
    std::vector<Column> sorted_columns = ReorderColumns(merged->Columns(), indices);

    return std::make_shared<Batch>(std::move(sorted_columns), merged->GetSchema());
  }

 private:
  static std::shared_ptr<Batch> MergeBatches(const std::vector<std::shared_ptr<Batch>>& batches) {
    ASSERT(!batches.empty());

    const Schema& schema = batches[0]->GetSchema();
    const size_t num_columns = batches[0]->Columns().size();

    // Calculate total rows
    int64_t total_rows = 0;
    for (const auto& batch : batches) {
      total_rows += batch->Rows();
    }

    // Create empty columns with reserved capacity
    std::vector<Column> merged_columns;
    merged_columns.reserve(num_columns);
    for (size_t col_idx = 0; col_idx < num_columns; ++col_idx) {
      std::visit(
          [&merged_columns, total_rows]<Type type>(const ArrayType<type>&) {
            ArrayType<type> arr;
            arr.reserve(total_rows);
            merged_columns.emplace_back(std::move(arr));
          },
          batches[0]->Columns()[col_idx].Values());
    }

    // Copy data from all batches
    for (const auto& batch : batches) {
      for (size_t col_idx = 0; col_idx < num_columns; ++col_idx) {
        std::visit(
            [&batch, col_idx]<Type type>(ArrayType<type>& dest) {
              const auto& src = std::get<ArrayType<type>>(batch->Columns()[col_idx].Values());
              dest.insert(dest.end(), src.begin(), src.end());
            },
            merged_columns[col_idx].Values());
      }
    }

    return std::make_shared<Batch>(std::move(merged_columns), schema);
  }

  static std::vector<Column> ReorderColumns(const std::vector<Column>& columns, const std::vector<int64_t>& indices) {
    std::vector<Column> result;
    result.reserve(columns.size());

    for (const auto& column : columns) {
      std::visit(
          [&result, &indices]<Type type>(const ArrayType<type>& src) {
            ArrayType<type> dest;
            dest.reserve(indices.size());
            for (int64_t idx : indices) {
              dest.emplace_back(src[idx]);
            }
            result.emplace_back(std::move(dest));
          },
          column.Values());
    }

    return result;
  }

  bool returned_ = false;
  std::shared_ptr<SortOperator> op_;
  std::shared_ptr<IStream<std::shared_ptr<Batch>>> stream_;
};

std::shared_ptr<IStream<std::shared_ptr<Batch>>> Execute(std::shared_ptr<Operator> op) {
  ASSERT(op != nullptr);

  switch (op->type) {
    case OperatorType::kAggregate:
      return std::make_shared<AggregationStream>(std::static_pointer_cast<AggregateOperator>(op));
    case OperatorType::kScan:
      return std::make_shared<ScanStream>(std::static_pointer_cast<ScanOperator>(op));
    case OperatorType::kFilter:
      return std::make_shared<FilterStream>(std::static_pointer_cast<FilterOperator>(op));
    case OperatorType::kProject:
      return std::make_shared<ProjectStream>(std::static_pointer_cast<ProjectOperator>(op));
    case OperatorType::kSort:
      return std::make_shared<SortStream>(std::static_pointer_cast<SortOperator>(op));
    case OperatorType::kLimit:
      THROW_NOT_IMPLEMENTED;
    default:
      THROW_NOT_IMPLEMENTED;
  }

  THROW_NOT_IMPLEMENTED;
}

}  // namespace ngn
