#pragma once

#include <memory>
#include <vector>

#include "src/execution/expression.h"
#include "src/execution/stream.h"

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

  explicit Aggregation(std::vector<AggregationInfo> aggrs, std::vector<std::shared_ptr<Expression>> exprs)
      : aggregations(std::move(aggrs)), group_by_expressions(std::move(exprs)) {}

  std::vector<AggregationInfo> aggregations;
  std::vector<std::shared_ptr<Expression>> group_by_expressions;
};

std::shared_ptr<Batch> Evaluate(std::shared_ptr<IStream<std::shared_ptr<Batch>>> batch,
                                std::shared_ptr<Aggregation> aggregation);

}  // namespace ngn
