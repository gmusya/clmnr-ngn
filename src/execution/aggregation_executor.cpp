#include "src/execution/aggregation_executor.h"

#include <cstdint>
#include <limits>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "src/core/type.h"
#include "src/core/value.h"
#include "src/execution/aggregation.h"
#include "src/execution/expression.h"
#include "src/execution/int128.h"
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
          } else if constexpr (std::is_same_v<T, Boolean>) {
            return std::hash<bool>{}(val.value);
          } else if constexpr (std::is_same_v<T, Int128>) {
            const unsigned __int128 uval = static_cast<unsigned __int128>(val);
            const uint64_t low = static_cast<uint64_t>(uval);
            const uint64_t high = static_cast<uint64_t>(uval >> 64);
            size_t h1 = std::hash<uint64_t>{}(low);
            size_t h2 = std::hash<uint64_t>{}(high);
            return h1 ^ (h2 + 0x9e3779b97f4a7c15ULL + (h1 << 6) + (h1 >> 2));
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
  CountState() : count_(0) {}

  void Update(const Value&) override { ++count_; }

  Value Finalize() override { return Value(count_); }

 private:
  int64_t count_;
};

std::shared_ptr<IState> MakeCountState() { return std::make_shared<CountState>(); }

class SumState : public IState {
 public:
  explicit SumState(Type output_type) : sum_(static_cast<Int128>(0)), output_type_(output_type) {}

  void Update(const Value& value) override {
    if (auto ptr = std::get_if<int64_t>(&value.GetValue()); ptr != nullptr) {
      sum_ += static_cast<Int128>(*ptr);
    } else if (auto ptr = std::get_if<int16_t>(&value.GetValue()); ptr != nullptr) {
      sum_ += static_cast<Int128>(*ptr);
    } else if (auto ptr = std::get_if<int32_t>(&value.GetValue()); ptr != nullptr) {
      sum_ += static_cast<Int128>(*ptr);
    } else if (auto ptr = std::get_if<Int128>(&value.GetValue()); ptr != nullptr) {
      sum_ += *ptr;
    } else {
      THROW_NOT_IMPLEMENTED;
    }
  }

  Value Finalize() override {
    if (output_type_ == Type::kInt128) {
      return Value(sum_);
    }
    if (sum_ > static_cast<Int128>(std::numeric_limits<int64_t>::max()) ||
        sum_ < static_cast<Int128>(std::numeric_limits<int64_t>::min())) {
      THROW_RUNTIME_ERROR("Overlflow");
    }
    return Value(static_cast<int64_t>(sum_));
  }

 private:
  Int128 sum_;
  Type output_type_;
};

std::shared_ptr<IState> MakeSumState(Type output_type) { return std::make_shared<SumState>(output_type); }

template <bool is_min>
class MinMaxState : public IState {
 public:
  explicit MinMaxState([[maybe_unused]] Type output_type) {}

  void Update(const Value& value) override {
    if (result_value_.has_value()) {
      if (auto ptr = std::get_if<PhysicalType<Type::kDate>>(&value.GetValue()); ptr != nullptr) {
        const auto& current_value = std::get<PhysicalType<Type::kDate>>(result_value_->GetValue());
        if constexpr (is_min) {
          if (*ptr < current_value) {
            result_value_ = Value(*ptr);
          }
        } else {
          if (*ptr > current_value) {
            result_value_ = Value(*ptr);
          }
        }
      } else {
        THROW_NOT_IMPLEMENTED;
      }
    } else {
      result_value_ = value;
    }
  }

  Value Finalize() override {
    ASSERT(result_value_.has_value());

    return *result_value_;
  }

 private:
  std::optional<Value> result_value_;
};

std::shared_ptr<IState> MakeMinState(Type output_type) { return std::make_shared<MinMaxState<true>>(output_type); }

std::shared_ptr<IState> MakeMaxState(Type output_type) { return std::make_shared<MinMaxState<false>>(output_type); }

class DistinctState : public IState {
 public:
  explicit DistinctState() {}

  void Update(const Value& value) override { result_.insert(value); }

  Value Finalize() override { return Value(static_cast<int64_t>(result_.size())); }

 private:
  std::unordered_set<Value, ValueHash> result_;
};

std::shared_ptr<IState> MakeDistinctState() { return std::make_shared<DistinctState>(); }

class Aggregator {
 public:
  explicit Aggregator(Aggregation aggregation) : aggregation_(std::move(aggregation)) {
    ASSERT(!aggregation_.aggregations.empty());
  }

  void Consume(std::shared_ptr<Batch> batch) {
    std::vector<Column> group_by_columns;
    for (const auto& expr : aggregation_.group_by_expressions) {
      group_by_columns.emplace_back(Evaluate(batch, expr.expression));
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
          if (aggregation_.aggregations[j].type == AggregationType::kCount) {
            state.emplace_back(MakeCountState());
          } else if (aggregation_.aggregations[j].type == AggregationType::kSum) {
            state.emplace_back(MakeSumState(GetAggregationType(aggregation_.aggregations[j])));
          } else if (aggregation_.aggregations[j].type == AggregationType::kDistinct) {
            state.emplace_back(MakeDistinctState());
          } else if (aggregation_.aggregations[j].type == AggregationType::kMin) {
            state.emplace_back(MakeMinState(GetAggregationType(aggregation_.aggregations[j])));
          } else if (aggregation_.aggregations[j].type == AggregationType::kMax) {
            state.emplace_back(MakeMaxState(GetAggregationType(aggregation_.aggregations[j])));
          } else {
            THROW_NOT_IMPLEMENTED;
          }
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
    columns.reserve(aggregation_.aggregations.size() + aggregation_.group_by_expressions.size());

    std::vector<Field> fields;
    fields.reserve(aggregation_.aggregations.size() + aggregation_.group_by_expressions.size());
    for (const auto& group_by : aggregation_.group_by_expressions) {
      Type type = GetExpressionType(group_by.expression);
      fields.emplace_back(Field(group_by.name, type));
    }

    for (const auto& aggr : aggregation_.aggregations) {
      Type type = GetAggregationType(aggr);
      fields.emplace_back(Field(aggr.name, type));
    }

    for (const auto& field : fields) {
      switch (field.type) {
        case Type::kInt16:
          columns.emplace_back(Column(ArrayType<Type::kInt16>{}));
          break;
        case Type::kInt32:
          columns.emplace_back(Column(ArrayType<Type::kInt32>{}));
          break;
        case Type::kInt64:
          columns.emplace_back(Column(ArrayType<Type::kInt64>{}));
          break;
        case Type::kInt128:
          columns.emplace_back(Column(ArrayType<Type::kInt128>{}));
          break;
        case Type::kDate:
          columns.emplace_back(Column(ArrayType<Type::kDate>{}));
          break;
        default:
          THROW_NOT_IMPLEMENTED;
      }
    }

    for (const auto& [group_by, state] : state_) {
      std::vector<Value> values = group_by;
      values.reserve(values.size() + state.size());
      for (const auto& s : state) {
        values.emplace_back(s->Finalize());
      }
      for (size_t i = 0; i < values.size(); ++i) {
        Column& column = columns[i];
        Value::GenericValue value = values[i].GetValue();

        std::visit(
            [value]<Type type>(ArrayType<type>& column) -> void {
              if (std::holds_alternative<PhysicalType<type>>(value)) {
                column.emplace_back(std::get<PhysicalType<type>>(value));
              } else {
                THROW_RUNTIME_ERROR("Type mismatch");
              }
            },
            column.Values());
      }
    }
    return Batch(std::move(columns), Schema(fields));
  }

 private:
  static Type GetExpressionType(const std::shared_ptr<Expression>& expression) {
    switch (expression->expr_type) {
      case ExpressionType::kVariable:
        return std::static_pointer_cast<Variable>(expression)->type;
      case ExpressionType::kConst:
        return std::visit(
            []<typename T>(const T&) -> Type {
              if constexpr (std::is_same_v<T, PhysicalType<Type::kBool>>) {
                return Type::kBool;
              } else if constexpr (std::is_same_v<T, PhysicalType<Type::kInt16>>) {
                return Type::kInt16;
              } else if constexpr (std::is_same_v<T, PhysicalType<Type::kInt32>>) {
                return Type::kInt32;
              } else if constexpr (std::is_same_v<T, PhysicalType<Type::kInt64>>) {
                return Type::kInt64;
              } else if constexpr (std::is_same_v<T, PhysicalType<Type::kInt128>>) {
                return Type::kInt128;
              } else if constexpr (std::is_same_v<T, PhysicalType<Type::kString>>) {
                return Type::kString;
              } else if constexpr (std::is_same_v<T, PhysicalType<Type::kDate>>) {
                return Type::kDate;
              } else if constexpr (std::is_same_v<T, PhysicalType<Type::kTimestamp>>) {
                return Type::kTimestamp;
              } else if constexpr (std::is_same_v<T, PhysicalType<Type::kChar>>) {
                return Type::kChar;
              } else {
                static_assert(false, "Unknown type");
              }
            },
            std::static_pointer_cast<Const>(expression)->value.GetValue());
      default:
        THROW_NOT_IMPLEMENTED;
    }
  }

  static Type GetSumOutputType(Type input_type) {
    switch (input_type) {
      case Type::kInt16:
      case Type::kInt32:
        return Type::kInt64;
      case Type::kInt64:
      case Type::kInt128:
        return Type::kInt128;
      default:
        THROW_NOT_IMPLEMENTED;
    }
  }

  static Type GetAggregationType(const AggregationUnit& unit) {
    if (unit.type == AggregationType::kCount || unit.type == AggregationType::kDistinct) {
      return Type::kInt64;
    }
    if (unit.type == AggregationType::kSum) {
      return GetSumOutputType(GetExpressionType(unit.expression));
    }
    if (unit.type == AggregationType::kMin || unit.type == AggregationType::kMax) {
      return GetExpressionType(unit.expression);
    }
    THROW_NOT_IMPLEMENTED;
  }

  std::unordered_map<std::vector<Value>, std::vector<std::shared_ptr<IState>>, VectorValueHash> state_;
  Aggregation aggregation_;
};

}  // namespace

std::shared_ptr<Batch> Evaluate(std::shared_ptr<IStream<std::shared_ptr<Batch>>> stream,
                                std::shared_ptr<Aggregation> aggregation) {
  Aggregator aggregator(*aggregation);

  while (const auto& batch = stream->Next()) {
    aggregator.Consume(batch.value());
  }

  return std::make_shared<Batch>(aggregator.Finalize());
}

}  // namespace ngn
