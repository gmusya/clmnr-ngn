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

std::shared_ptr<IStream<std::shared_ptr<Batch>>> Execute(std::shared_ptr<Operator> op) {
  ASSERT(op != nullptr);

  switch (op->type) {
    case OperatorType::kAggregate:
      return std::make_shared<AggregationStream>(std::static_pointer_cast<AggregateOperator>(op));
    case OperatorType::kScan:
      return std::make_shared<ScanStream>(std::static_pointer_cast<ScanOperator>(op));
    case OperatorType::kFilter:
      THROW_NOT_IMPLEMENTED;
    case OperatorType::kProject:
      THROW_NOT_IMPLEMENTED;
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
