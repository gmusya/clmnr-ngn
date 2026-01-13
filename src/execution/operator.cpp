#include "src/execution/operator.h"

#include "src/core/columnar.h"
#include "src/execution/aggregation_executor.h"
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
      THROW_NOT_IMPLEMENTED;
    case OperatorType::kLimit:
      THROW_NOT_IMPLEMENTED;
    default:
      THROW_NOT_IMPLEMENTED;
  }

  THROW_NOT_IMPLEMENTED;
}

}  // namespace ngn
