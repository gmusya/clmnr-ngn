#pragma once

#include <memory>

#include "src/execution/batch.h"

namespace ngn {

enum class OperatorType {
  kScan,
  kFilter,
  kProject,
  kAggregate,
  kSort,
  kLimit,
};

struct Operator {
  OperatorType type;

 protected:
  ~Operator() = default;
};

struct ScanOperator : public Operator {
  ScanOperator() : Operator(OperatorType::kScan) {}
};

struct FilterOperator : public Operator {
  FilterOperator() : Operator(OperatorType::kFilter) {}
};

struct ProjectOperator : public Operator {
  ProjectOperator() : Operator(OperatorType::kProject) {}
};

struct AggregateOperator : public Operator {
  AggregateOperator() : Operator(OperatorType::kAggregate) {}
};

struct SortOperator : public Operator {
  SortOperator() : Operator(OperatorType::kSort) {}
};

struct LimitOperator : public Operator {
  LimitOperator() : Operator(OperatorType::kLimit) {}
};

Batch Execute(std::shared_ptr<Operator> op);

}  // namespace ngn
