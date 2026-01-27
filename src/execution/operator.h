#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "src/execution/aggregation.h"
#include "src/execution/batch.h"
#include "src/execution/expression.h"
#include "src/execution/stream.h"

namespace ngn {

enum class OperatorType {
  kScan,
  kCountTable,
  kFilter,
  kProject,
  kAggregate,
  kAggregateCompact,
  kSort,
  kTopK,
};

struct ZoneMapPredicate {
  std::string column_name;

  std::optional<Value> range_min;
  std::optional<Value> range_max;

  static ZoneMapPredicate Equal(std::string col, const Value& val) {
    return ZoneMapPredicate{std::move(col), val, val};
  }

  static ZoneMapPredicate Range(std::string col, const Value& min_val, const Value& max_val) {
    return ZoneMapPredicate{std::move(col), min_val, max_val};
  }
};

struct Operator {
  OperatorType type;

 protected:
  ~Operator() = default;
};

struct ScanOperator : public Operator {
  ScanOperator(std::string i, Schema s, std::vector<ZoneMapPredicate> preds = {})
      : Operator(OperatorType::kScan),
        input_path(std::move(i)),
        schema(std::move(s)),
        zone_map_predicates(std::move(preds)) {
    ASSERT(!input_path.empty());
  }

  std::string input_path;
  Schema schema;
  std::vector<ZoneMapPredicate> zone_map_predicates;  // Predicates for zone map filtering
};

// Returns a single-row, single-column batch containing the number of rows in the table.
// Intended as a metadata-only fast path for queries like: SELECT COUNT(*) FROM hits;
struct CountTableOperator : public Operator {
  explicit CountTableOperator(std::string i, std::string out_name = "count")
      : Operator(OperatorType::kCountTable), input_path(std::move(i)), output_name(std::move(out_name)) {
    ASSERT(!input_path.empty());
    ASSERT(!output_name.empty());
  }

  std::string input_path;
  std::string output_name;
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

struct ProjectionUnit {
  std::shared_ptr<Expression> expression;
  std::string name;
};

struct ProjectOperator : public Operator {
  ProjectOperator(std::shared_ptr<Operator> chi, std::vector<ProjectionUnit> proj)
      : Operator(OperatorType::kProject), child(std::move(chi)), projections(std::move(proj)) {
    ASSERT(child != nullptr);
    ASSERT(!projections.empty());
  }

  std::shared_ptr<Operator> child;
  std::vector<ProjectionUnit> projections;
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

// Memory-lean alternative to AggregateOperator. Intended to be used selectively for
// very high-cardinality aggregations (e.g. ClickBench Q32).
struct CompactAggregateOperator : public Operator {
  CompactAggregateOperator(std::shared_ptr<Operator> chi, std::shared_ptr<Aggregation> aggr)
      : Operator(OperatorType::kAggregateCompact), child(std::move(chi)), aggregation(std::move(aggr)) {
    ASSERT(child != nullptr);
    ASSERT(aggregation != nullptr);
    ASSERT(!aggregation->aggregations.empty());
  }

  std::shared_ptr<Operator> child;
  std::shared_ptr<Aggregation> aggregation;
};

struct SortUnit {
  std::shared_ptr<Expression> expression;
  bool is_ascending;
};

struct SortOperator : public Operator {
  SortOperator(std::shared_ptr<Operator> chi, std::vector<SortUnit> sk)
      : Operator(OperatorType::kSort), child(std::move(chi)), sort_keys(std::move(sk)) {
    ASSERT(child != nullptr);
    ASSERT(!sort_keys.empty());
  }

  std::shared_ptr<Operator> child;
  std::vector<SortUnit> sort_keys;
};

struct TopKOperator : public Operator {
  TopKOperator(std::shared_ptr<Operator> chi, std::vector<SortUnit> sk, uint32_t lim, uint32_t off = 0)
      : Operator(OperatorType::kTopK), child(std::move(chi)), sort_keys(std::move(sk)), limit(lim), offset(off) {
    ASSERT(child != nullptr);
    ASSERT(!sort_keys.empty());
  }

  std::shared_ptr<Operator> child;
  std::vector<SortUnit> sort_keys;
  uint32_t limit;
  uint32_t offset;
};

#if 0
struct LimitOperator : public Operator {
  LimitOperator(std::shared_ptr<Operator> chi, int64_t lim)
      : Operator(OperatorType::kLimit), child(std::move(chi)), limit(lim) {
    ASSERT(child != nullptr);
    ASSERT(limit > 0);
  }

  std::shared_ptr<Operator> child;
  int64_t limit;
};
#endif

inline std::shared_ptr<ScanOperator> MakeScan(std::string input_path, Schema schema,
                                              std::vector<ZoneMapPredicate> predicates = {}) {
  return std::make_shared<ScanOperator>(std::move(input_path), std::move(schema), std::move(predicates));
}

inline std::shared_ptr<CountTableOperator> MakeCountTable(std::string input_path, std::string output_name = "count") {
  return std::make_shared<CountTableOperator>(std::move(input_path), std::move(output_name));
}

inline std::shared_ptr<FilterOperator> MakeFilter(std::shared_ptr<Operator> child,
                                                  std::shared_ptr<Expression> condition) {
  return std::make_shared<FilterOperator>(std::move(child), std::move(condition));
}

inline std::shared_ptr<ProjectOperator> MakeProject(std::shared_ptr<Operator> child,
                                                    std::vector<ProjectionUnit> projections) {
  return std::make_shared<ProjectOperator>(std::move(child), std::move(projections));
}

inline std::shared_ptr<AggregateOperator> MakeAggregate(std::shared_ptr<Operator> child,
                                                        std::shared_ptr<Aggregation> aggregation) {
  return std::make_shared<AggregateOperator>(std::move(child), std::move(aggregation));
}

inline std::shared_ptr<CompactAggregateOperator> MakeAggregateCompact(std::shared_ptr<Operator> child,
                                                                      std::shared_ptr<Aggregation> aggregation) {
  return std::make_shared<CompactAggregateOperator>(std::move(child), std::move(aggregation));
}

inline std::shared_ptr<SortOperator> MakeSort(std::shared_ptr<Operator> child, std::vector<SortUnit> sort_keys) {
  return std::make_shared<SortOperator>(std::move(child), std::move(sort_keys));
}

inline std::shared_ptr<TopKOperator> MakeTopK(std::shared_ptr<Operator> child, std::vector<SortUnit> sort_keys,
                                              uint32_t limit, uint32_t offset = 0) {
  return std::make_shared<TopKOperator>(std::move(child), std::move(sort_keys), limit, offset);
}

#if 0
inline std::shared_ptr<LimitOperator> MakeLimit(std::shared_ptr<Operator> child, int64_t limit) {
  return std::make_shared<LimitOperator>(std::move(child), std::move(limit));
}
#endif

std::shared_ptr<IStream<std::shared_ptr<Batch>>> Execute(std::shared_ptr<Operator> op);

}  // namespace ngn
