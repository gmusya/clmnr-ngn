#pragma once

#include <memory>

#include "src/execution/aggregation.h"
#include "src/execution/batch.h"
#include "src/execution/expression.h"

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
  ScanOperator(std::string input_path, Schema schema)
      : Operator(OperatorType::kScan), input_path(std::move(input_path)), schema(std::move(schema)) {
    ASSERT(!input_path.empty());
    ASSERT(!schema.Fields().empty());
  }

  std::string input_path;
  Schema schema;
};

struct FilterOperator : public Operator {
  FilterOperator(std::shared_ptr<Operator> child, std::shared_ptr<Expression> condition)
      : Operator(OperatorType::kFilter), child(std::move(child)), condition(std::move(condition)) {
    ASSERT(child != nullptr);
    ASSERT(condition != nullptr);
  }

  std::shared_ptr<Operator> child;
  std::shared_ptr<Expression> condition;
};

struct ProjectOperator : public Operator {
  ProjectOperator(std::shared_ptr<Operator> child, std::vector<std::shared_ptr<Expression>> projections)
      : Operator(OperatorType::kProject), child(std::move(child)), projections(std::move(projections)) {
    ASSERT(child != nullptr);
    ASSERT(!projections.empty());
  }

  std::shared_ptr<Operator> child;
  std::vector<std::shared_ptr<Expression>> projections;
};

struct AggregateOperator : public Operator {
  AggregateOperator(std::shared_ptr<Operator> child, std::shared_ptr<Aggregation> aggregation)
      : Operator(OperatorType::kAggregate), child(std::move(child)), aggregation(std::move(aggregation)) {
    ASSERT(child != nullptr);
    ASSERT(aggregation != nullptr);
    ASSERT(!aggregation->aggregations.empty());
    ASSERT(!aggregation->group_by_expressions.empty());
  }

  std::shared_ptr<Operator> child;
  std::shared_ptr<Aggregation> aggregation;
};

struct SortOperator : public Operator {
  SortOperator(std::shared_ptr<Operator> child, std::vector<std::shared_ptr<Expression>> sort_keys)
      : Operator(OperatorType::kSort), child(std::move(child)), sort_keys(std::move(sort_keys)) {
    ASSERT(child != nullptr);
    ASSERT(!sort_keys.empty());
  }

  std::shared_ptr<Operator> child;
  std::vector<std::shared_ptr<Expression>> sort_keys;
};

struct LimitOperator : public Operator {
  LimitOperator(std::shared_ptr<Operator> child, int64_t limit)
      : Operator(OperatorType::kLimit), child(std::move(child)), limit(limit) {
    ASSERT(child != nullptr);
    ASSERT(limit > 0);
  }

  std::shared_ptr<Operator> child;
  int64_t limit;
};

inline std::shared_ptr<ScanOperator> MakeScan(std::string input_path, Schema schema) {
  return std::make_shared<ScanOperator>(std::move(input_path), std::move(schema));
}

inline std::shared_ptr<FilterOperator> MakeFilter(std::shared_ptr<Operator> child,
                                                  std::shared_ptr<Expression> condition) {
  return std::make_shared<FilterOperator>(std::move(child), std::move(condition));
}

inline std::shared_ptr<ProjectOperator> MakeProject(std::shared_ptr<Operator> child,
                                                    std::vector<std::shared_ptr<Expression>> projections) {
  return std::make_shared<ProjectOperator>(std::move(child), std::move(projections));
}

inline std::shared_ptr<AggregateOperator> MakeAggregate(std::shared_ptr<Operator> child,
                                                        std::shared_ptr<Aggregation> aggregation) {
  return std::make_shared<AggregateOperator>(std::move(child), std::move(aggregation));
}

inline std::shared_ptr<SortOperator> MakeSort(std::shared_ptr<Operator> child,
                                              std::vector<std::shared_ptr<Expression>> sort_keys) {
  return std::make_shared<SortOperator>(std::move(child), std::move(sort_keys));
}

inline std::shared_ptr<LimitOperator> MakeLimit(std::shared_ptr<Operator> child, int64_t limit) {
  return std::make_shared<LimitOperator>(std::move(child), std::move(limit));
}

Batch Execute(std::shared_ptr<Operator> op);

}  // namespace ngn
