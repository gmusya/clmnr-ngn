#pragma once

#include <memory>
#include <vector>

#include "src/execution/expression.h"

namespace ngn {

enum class AggregationType {
  kCount,
  kSum,
  kAverage,
  kMin,
  kMax,
};

struct Aggregation {
  struct AggregationInfo {
    AggregationType type;
    std::shared_ptr<Expression> expression;
    std::string name;
  };

  explicit Aggregation(std::vector<AggregationInfo> aggrs, std::vector<std::shared_ptr<Expression>> group_by)
      : aggregations(std::move(aggrs)), group_by_expressions(std::move(group_by)) {}

  std::vector<AggregationInfo> aggregations;
  std::vector<std::shared_ptr<Expression>> group_by_expressions;
};

inline std::shared_ptr<Aggregation> MakeAggregation(std::vector<Aggregation::AggregationInfo> aggregations,
                                                    std::vector<std::shared_ptr<Expression>> group_by) {
  return std::make_shared<Aggregation>(std::move(aggregations), std::move(group_by));
}

}  // namespace ngn
