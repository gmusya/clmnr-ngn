#include "src/execution/aggregation.h"

#include <unordered_map>

#include "src/core/value.h"
#include "src/execution/expression.h"

namespace ngn {

namespace {

struct ValueHash {
  std::size_t operator()(const ngn::Value& value) const {
    return std::visit([]<typename T>(const T& val) { return std::hash<T>{}(val); }, value.GetValue());
  }
};

struct VectorValueHash {
  std::size_t operator()(const std::vector<ngn::Value>& value) const {
    std::size_t result = 0;
    for (const auto& elem : value) {
      result = (result << 17) + ValueHash{}(elem);
    }
    return result;
  }
};

struct State {
  void Update(const Value& value);
  Value Finalize();
};

class Aggregator {
 public:
  void Consume(std::shared_ptr<Batch> batch) {
    std::vector<Column> group_by_columns;
    for (const auto& expr : aggregation_.group_by_expressions) {
      group_by_columns.emplace_back(Evaluate(batch, expr));
    }

    std::vector<Column> value_columns;
    for (const auto& aggr : aggregation_.aggregations) {
      value_columns.emplace_back(Evaluate(batch, aggr.expression));
    }

    for (int64_t i = 0; i < batch->Rows(); ++i) {
      std::vector<Value> group_by;
      for (size_t j = 0; j < group_by_columns.size(); ++j) {
        group_by.emplace_back(group_by_columns[j][i]);
      }

      auto it = state_.find(group_by);
      if (it == state_.end()) {
        std::vector<State> state(value_columns.size());
        state_[group_by] = state;
        it = state_.find(group_by);
      }

      auto& state = it->second;
      for (size_t j = 0; j < value_columns.size(); ++j) {
        state[j].Update(value_columns[j][i]);
      }
    }
  }

 private:
  std::unordered_map<std::vector<Value>, std::vector<State>, VectorValueHash> state_;
  Aggregation aggregation_;
};

}  // namespace

std::shared_ptr<Batch> Evaluate(std::shared_ptr<IStream<std::shared_ptr<Batch>>> batch,
                                std::shared_ptr<Aggregation> aggregation) {}

}  // namespace ngn
