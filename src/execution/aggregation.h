#pragma once

#include <memory>
#include <vector>

#include "src/execution/expression.h"

namespace ngn {

enum class AggregationType {
  kCount,
  kSum,
  kDistinct,
  kMin,
  kMax,
};

struct AggregationUnit {
  AggregationType type;
  std::shared_ptr<Expression> expression;
  std::string name;
};

struct GroupByUnit {
  std::shared_ptr<Expression> expression;
  std::string name;
};

struct Aggregation {
  explicit Aggregation(std::vector<AggregationUnit> aggrs, std::vector<GroupByUnit> group_by)
      : aggregations(std::move(aggrs)), group_by_expressions(std::move(group_by)) {}

  std::vector<AggregationUnit> aggregations;
  std::vector<GroupByUnit> group_by_expressions;
};

inline std::shared_ptr<Aggregation> MakeAggregation(std::vector<AggregationUnit> aggregations,
                                                    std::vector<GroupByUnit> group_by) {
  return std::make_shared<Aggregation>(std::move(aggregations), std::move(group_by));
}

}  // namespace ngn
