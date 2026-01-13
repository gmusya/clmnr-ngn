#pragma once

#include <memory>

#include "src/execution/aggregation.h"
#include "src/execution/batch.h"
#include "src/execution/expression.h"
#include "src/execution/stream.h"

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
  ScanOperator(std::string i, Schema s)
      : Operator(OperatorType::kScan), input_path(std::move(i)), schema(std::move(s)) {
    ASSERT(!input_path.empty());
    ASSERT(!schema.Fields().empty());
  }

  std::string input_path;
  Schema schema;
};

struct FilterOperator : public Operator {
  FilterOperator(std::shared_ptr<Operator> chi, std::shared_ptr<Expression> cond)
      : Operator(OperatorType::kFilter), child(std::move(chi)), condition(std::move(cond)) {
    ASSERT(child != nullptr);
    ASSERT(condition != nullptr);
  }

  std::shared_ptr<Operator> child;
  std::shared_ptr<Expression> condition;
};

struct ProjectOperator : public Operator {
  ProjectOperator(std::shared_ptr<Operator> chi, std::vector<std::shared_ptr<Expression>> proj)
      : Operator(OperatorType::kProject), child(std::move(chi)), projections(std::move(proj)) {
    ASSERT(child != nullptr);
    ASSERT(!projections.empty());
  }

  std::shared_ptr<Operator> child;
  std::vector<std::shared_ptr<Expression>> projections;
};

struct AggregateOperator : public Operator {
  AggregateOperator(std::shared_ptr<Operator> chi, std::shared_ptr<Aggregation> aggr)
      : Operator(OperatorType::kAggregate), child(std::move(chi)), aggregation(std::move(aggr)) {
    ASSERT(child != nullptr);
    ASSERT(aggregation != nullptr);
    ASSERT(!aggregation->aggregations.empty());
  }

  std::shared_ptr<Operator> child;
  std::shared_ptr<Aggregation> aggregation;
};

struct SortOperator : public Operator {
  SortOperator(std::shared_ptr<Operator> chi, std::vector<std::shared_ptr<Expression>> sk)
      : Operator(OperatorType::kSort), child(std::move(chi)), sort_keys(std::move(sk)) {
    ASSERT(child != nullptr);
    ASSERT(!sort_keys.empty());
  }

  std::shared_ptr<Operator> child;
  std::vector<std::shared_ptr<Expression>> sort_keys;
};

struct LimitOperator : public Operator {
  LimitOperator(std::shared_ptr<Operator> chi, int64_t lim)
      : Operator(OperatorType::kLimit), child(std::move(chi)), limit(lim) {
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

std::shared_ptr<IStream<std::shared_ptr<Batch>>> Execute(std::shared_ptr<Operator> op);

}  // namespace ngn
