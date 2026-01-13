#include "src/execution/aggregation_executor.h"

#include <cstdint>
#include <optional>
#include <unordered_map>
#include <vector>

#include "src/core/value.h"
#include "src/execution/expression.h"
#include "src/execution/stream.h"
#include "src/util/assert.h"
#include "src/util/macro.h"

namespace ngn {
namespace {

struct ValueHash {
  std::size_t operator()(const ngn::Value& value) const {
    return std::visit(
        []<typename T>(const T& val) -> size_t {
          if constexpr (std::is_same_v<T, Date>) {
            return std::hash<int64_t>{}(val.value);
          } else if constexpr (std::is_same_v<T, Timestamp>) {
            return std::hash<int64_t>{}(val.value);
          } else {
            return std::hash<T>{}(val);
          }
        },
        value.GetValue());
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

class IState {
 public:
  virtual ~IState() = default;

  virtual void Update(const Value& value) = 0;
  virtual Value Finalize() = 0;
};

class CountState : public IState {
 public:
  void Update(const Value&) override { ++count_; }

  Value Finalize() override { return Value(count_); }

 private:
  int64_t count_;
};

std::shared_ptr<IState> MakeCountState() { return std::make_shared<CountState>(); }

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
        std::vector<std::shared_ptr<IState>> state;
        state.reserve(value_columns.size());
        for (size_t j = 0; j < value_columns.size(); ++j) {
          state.emplace_back(MakeCountState());
        }
        state_[group_by] = std::move(state);
        it = state_.find(group_by);
      }

      auto& state = it->second;
      for (size_t j = 0; j < value_columns.size(); ++j) {
        state[j]->Update(value_columns[j][i]);
      }
    }
  }

  Batch Finalize() {
    std::vector<Column> columns;
    columns.reserve(aggregation_.aggregations.size());

    std::vector<Field> fields;
    fields.reserve(aggregation_.aggregations.size());
    for (const auto& aggr : aggregation_.aggregations) {
      fields.emplace_back(Field(aggr.name, Type::kInt64));
      columns.emplace_back(Column(ArrayType<Type::kInt64>{}));
    }

    for (const auto& [group_by, state] : state_) {
      std::vector<Value> values = group_by;
      values.reserve(values.size() + state.size());
      for (const auto& s : state) {
        values.emplace_back(s->Finalize());
      }
      for (size_t i = 0; i < values.size(); ++i) {
        std::visit(
            [&values, i]<Type type>(ArrayType<type>& column) -> void {
              if (std::holds_alternative<PhysicalType<type>>(values[i].GetValue())) {
                column.emplace_back(std::get<PhysicalType<type>>(values[i].GetValue()));
              } else {
                THROW_RUNTIME_ERROR("Type mismatch");
              }
            },
            columns[i].Values());
      }
    }
    return Batch(std::move(columns), Schema(fields));
  }

 private:
  std::unordered_map<std::vector<Value>, std::vector<std::shared_ptr<IState>>, VectorValueHash> state_;
  Aggregation aggregation_;
};

}  // namespace

std::shared_ptr<Batch> Evaluate(std::shared_ptr<IStream<std::shared_ptr<Batch>>> batch,
                                std::shared_ptr<Aggregation> aggregation) {
  ASSERT(batch != nullptr);
  ASSERT(aggregation != nullptr);

  return batch->Next().value();
}

}  // namespace ngn
