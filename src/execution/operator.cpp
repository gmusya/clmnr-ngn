#include "src/execution/operator.h"

#include <algorithm>
#include <numeric>
#include <queue>
#include <unordered_map>
#include <unordered_set>

#include "src/core/columnar.h"
#include "src/execution/aggregation_executor.h"
#include "src/execution/aggregation_executor_compact.h"
#include "src/execution/batch.h"
#include "src/execution/kernel.h"
#include "src/execution/stream.h"
#include "src/util/assert.h"
#include "src/util/macro.h"

namespace ngn {

class AggregationStream : public IStream<std::shared_ptr<Batch>> {
 public:
  AggregationStream(std::shared_ptr<AggregateOperator> aggregation) : op_(std::move(aggregation)) {}

  std::optional<std::shared_ptr<Batch>> Next() override {
    if (first_) {
      first_ = false;

      auto stream = Execute(op_->child);
      return Evaluate(stream, op_->aggregation);
    }

    return std::nullopt;
  }

 private:
  bool first_ = true;

  std::shared_ptr<AggregateOperator> op_;
};

class CompactAggregationStream : public IStream<std::shared_ptr<Batch>> {
 public:
  CompactAggregationStream(std::shared_ptr<CompactAggregateOperator> aggregation) : op_(std::move(aggregation)) {}

  std::optional<std::shared_ptr<Batch>> Next() override {
    if (first_) {
      first_ = false;

      auto stream = Execute(op_->child);
      return EvaluateCompact(stream, op_->aggregation);
    }

    return std::nullopt;
  }

 private:
  bool first_ = true;
  std::shared_ptr<CompactAggregateOperator> op_;
};

class ScanStream : public IStream<std::shared_ptr<Batch>> {
 public:
  ScanStream(std::shared_ptr<ScanOperator> scan) : reader_(scan->input_path), op_(std::move(scan)) {
    // Build mapping from column name to index in file schema
    const auto& file_fields = reader_.GetSchema().Fields();
    for (size_t i = 0; i < file_fields.size(); ++i) {
      column_name_to_index_[file_fields[i].name] = i;
    }

    // Precompute indices of columns to read
    for (const auto& field : op_->schema.Fields()) {
      auto it = column_name_to_index_.find(field.name);
      ASSERT_WITH_MESSAGE(it != column_name_to_index_.end(), "Column not found in file: " + field.name);
      columns_to_read_.push_back(it->second);
    }

    for (const auto& pred : op_->zone_map_predicates) {
      auto it = column_name_to_index_.find(pred.column_name);
      if (it != column_name_to_index_.end()) {
        resolved_predicates_.push_back({it->second, pred});
      }
    }
  }

  std::optional<std::shared_ptr<Batch>> Next() override {
    while (row_group_index_ < reader_.RowGroupCount()) {
      if (CanSkipCurrentRowGroup()) {
        ++row_group_index_;
        continue;
      }
      break;
    }

    if (row_group_index_ >= reader_.RowGroupCount()) {
      return std::nullopt;
    }

    if (columns_to_read_.empty()) {
      int64_t row_count = reader_.RowGroupRowCount(row_group_index_);
      ++row_group_index_;
      return std::make_shared<Batch>(row_count, op_->schema);
    }

    std::vector<Column> columns;
    columns.reserve(columns_to_read_.size());
    for (size_t col_idx : columns_to_read_) {
      columns.push_back(reader_.ReadRowGroupColumn(row_group_index_, col_idx));
    }
    ++row_group_index_;

    return std::make_shared<Batch>(std::move(columns), op_->schema);
  }

 private:
  bool CanSkipCurrentRowGroup() const {
    if (!reader_.HasZoneMaps() || resolved_predicates_.empty()) {
      return false;
    }

    for (const auto& [col_idx, pred] : resolved_predicates_) {
      if (reader_.CanSkipRowGroupForRange(row_group_index_, col_idx, *pred.range_min, *pred.range_max)) {
        return true;
      }
    }

    return false;
  }

  FileReader reader_;

  std::shared_ptr<ScanOperator> op_;

  std::unordered_map<std::string, size_t> column_name_to_index_;
  std::vector<size_t> columns_to_read_;

  std::vector<std::pair<size_t, ZoneMapPredicate>> resolved_predicates_;

  size_t row_group_index_ = 0;
};

class CountTableStream : public IStream<std::shared_ptr<Batch>> {
 public:
  explicit CountTableStream(std::shared_ptr<CountTableOperator> op) : reader_(op->input_path), op_(std::move(op)) {}

  std::optional<std::shared_ptr<Batch>> Next() override {
    if (returned_) {
      return std::nullopt;
    }
    returned_ = true;

    int64_t total_rows = 0;
    for (uint64_t i = 0; i < reader_.RowGroupCount(); ++i) {
      total_rows += reader_.RowGroupRowCount(i);
    }

    ArrayType<Type::kInt64> values;
    values.emplace_back(total_rows);

    std::vector<Column> columns;
    columns.emplace_back(std::move(values));

    Schema schema({Field{.name = op_->output_name, .type = Type::kInt64}});
    return std::make_shared<Batch>(std::move(columns), std::move(schema));
  }

 private:
  bool returned_ = false;
  FileReader reader_;
  std::shared_ptr<CountTableOperator> op_;
};

class ConcatStream : public IStream<std::shared_ptr<Batch>> {
 public:
  explicit ConcatStream(std::shared_ptr<ConcatOperator> op) : op_(std::move(op)) {
    streams_.reserve(op_->children.size());
    for (const auto& child : op_->children) {
      streams_.emplace_back(Execute(child));
    }
  }

  std::optional<std::shared_ptr<Batch>> Next() override {
    if (returned_) {
      return std::nullopt;
    }
    returned_ = true;

    std::optional<int64_t> expected_rows;
    std::vector<Column> out_columns;
    std::vector<Field> out_fields;

    for (size_t i = 0; i < streams_.size(); ++i) {
      auto batch_opt = streams_[i]->Next();
      ASSERT_WITH_MESSAGE(batch_opt.has_value(), "Concat child produced no batches");
      ASSERT_WITH_MESSAGE(!streams_[i]->Next().has_value(), "Concat child produced more than one batch");

      std::shared_ptr<Batch> batch = batch_opt.value();
      if (!expected_rows.has_value()) {
        expected_rows = batch->Rows();
      } else {
        ASSERT_WITH_MESSAGE(batch->Rows() == expected_rows.value(), "Concat children must have equal row count");
      }

      for (const auto& f : batch->GetSchema().Fields()) {
        out_fields.emplace_back(f);
      }
      for (const auto& c : batch->Columns()) {
        out_columns.emplace_back(c);
      }
    }

    ASSERT(expected_rows.has_value());
    return std::make_shared<Batch>(std::move(out_columns), Schema(std::move(out_fields)));
  }

 private:
  bool returned_ = false;
  std::shared_ptr<ConcatOperator> op_;
  std::vector<std::shared_ptr<IStream<std::shared_ptr<Batch>>>> streams_;
};

namespace {

struct ValueHash {
  size_t operator()(const Value& v) const {
    return std::visit(
        []<typename T>(const T& x) -> size_t {
          using V = std::decay_t<T>;
          if constexpr (std::is_same_v<V, Boolean>) {
            return std::hash<bool>{}(x.value);
          } else if constexpr (std::is_same_v<V, int16_t>) {
            return std::hash<int16_t>{}(x);
          } else if constexpr (std::is_same_v<V, int32_t>) {
            return std::hash<int32_t>{}(x);
          } else if constexpr (std::is_same_v<V, int64_t>) {
            return std::hash<int64_t>{}(x);
          } else if constexpr (std::is_same_v<V, Int128>) {
            const auto u = static_cast<__uint128_t>(x);
            const uint64_t lo = static_cast<uint64_t>(u);
            const uint64_t hi = static_cast<uint64_t>(u >> 64);
            size_t h = std::hash<uint64_t>{}(lo);
            h ^= std::hash<uint64_t>{}(hi) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
            return h;
          } else if constexpr (std::is_same_v<V, std::string>) {
            return std::hash<std::string>{}(x);
          } else if constexpr (std::is_same_v<V, Date>) {
            return std::hash<int64_t>{}(x.value);
          } else if constexpr (std::is_same_v<V, Timestamp>) {
            return std::hash<int64_t>{}(x.value);
          } else if constexpr (std::is_same_v<V, char>) {
            return std::hash<char>{}(x);
          } else {
            static_assert(sizeof(V) == 0, "Unhandled Value variant alternative");
          }
        },
        v.GetValue());
  }
};

Type GetExpressionType(const std::shared_ptr<Expression>& expression) {
  switch (expression->expr_type) {
    case ExpressionType::kVariable:
      return std::static_pointer_cast<Variable>(expression)->type;
    case ExpressionType::kConst:
      return std::static_pointer_cast<Const>(expression)->value.GetType();
    default:
      THROW_NOT_IMPLEMENTED;
  }
}

Type GetSumOutputType(Type input_type) {
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

Type GetAggregationOutputType(const AggregationUnit& unit) {
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

}  // namespace

class GlobalAggregationStream : public IStream<std::shared_ptr<Batch>> {
 public:
  explicit GlobalAggregationStream(std::shared_ptr<GlobalAggregationOperator> op) : op_(std::move(op)) {
    stream_ = Execute(op_->child);
  }

  std::optional<std::shared_ptr<Batch>> Next() override {
    if (returned_) {
      return std::nullopt;
    }
    returned_ = true;

    const size_t n = op_->aggregations.size();

    std::vector<Type> out_types;
    out_types.reserve(n);
    for (const auto& a : op_->aggregations) {
      out_types.emplace_back(GetAggregationOutputType(a));
    }

    std::vector<int64_t> counts(n, 0);
    std::vector<Int128> sum_acc(n, static_cast<Int128>(0));
    std::vector<std::optional<Value>> minmax_acc(n);
    std::vector<std::unordered_set<Value, ValueHash>> distinct_sets(n);

    bool saw_any_rows = false;

    while (auto batch_opt = stream_->Next()) {
      std::shared_ptr<Batch> batch = batch_opt.value();
      if (batch->Rows() == 0) {
        continue;
      }
      saw_any_rows = true;

      for (size_t i = 0; i < n; ++i) {
        const auto& unit = op_->aggregations[i];
        switch (unit.type) {
          case AggregationType::kCount: {
            counts[i] += batch->Rows();
            break;
          }
          case AggregationType::kSum: {
            Column col = Evaluate(batch, unit.expression);
            // Always accumulate in Int128, then cast/validate at the end if needed.
            Value part = ReduceSum(col, Type::kInt128);
            sum_acc[i] += std::get<Int128>(part.GetValue());
            break;
          }
          case AggregationType::kMin: {
            Column col = Evaluate(batch, unit.expression);
            Value part = ReduceMin(col);
            if (!minmax_acc[i].has_value() || part < *minmax_acc[i]) {
              minmax_acc[i] = part;
            }
            break;
          }
          case AggregationType::kMax: {
            Column col = Evaluate(batch, unit.expression);
            Value part = ReduceMax(col);
            if (!minmax_acc[i].has_value() || part > *minmax_acc[i]) {
              minmax_acc[i] = part;
            }
            break;
          }
          case AggregationType::kDistinct: {
            Column col = Evaluate(batch, unit.expression);
            for (size_t r = 0; r < col.Size(); ++r) {
              distinct_sets[i].insert(col[r]);
            }
            break;
          }
          default:
            THROW_NOT_IMPLEMENTED;
        }
      }
    }

    std::vector<Field> fields;
    fields.reserve(n);
    for (size_t i = 0; i < n; ++i) {
      fields.emplace_back(Field(op_->aggregations[i].name, out_types[i]));
    }

    std::vector<Column> columns;
    columns.reserve(n);
    for (const auto& f : fields) {
      Dispatch([&]<Type type>(Tag<type>) { columns.emplace_back(Column(ArrayType<type>{})); }, f.type);
    }

    for (size_t i = 0; i < n; ++i) {
      const auto& unit = op_->aggregations[i];
      if (!saw_any_rows && (unit.type == AggregationType::kMin || unit.type == AggregationType::kMax)) {
        THROW_RUNTIME_ERROR("MIN/MAX on empty input");
      }

      Value out_v = [&]() -> Value {
        if (unit.type == AggregationType::kCount) {
          return Value(static_cast<int64_t>(counts[i]));
        }
        if (unit.type == AggregationType::kDistinct) {
          return Value(static_cast<int64_t>(distinct_sets[i].size()));
        }
        if (unit.type == AggregationType::kSum) {
          if (out_types[i] == Type::kInt128) {
            return Value(sum_acc[i]);
          }
          if (sum_acc[i] > static_cast<Int128>(std::numeric_limits<int64_t>::max()) ||
              sum_acc[i] < static_cast<Int128>(std::numeric_limits<int64_t>::min())) {
            THROW_RUNTIME_ERROR("Overlflow");
          }
          return Value(static_cast<int64_t>(sum_acc[i]));
        }
        if (unit.type == AggregationType::kMin || unit.type == AggregationType::kMax) {
          return *minmax_acc[i];
        }
        THROW_NOT_IMPLEMENTED;
      }();

      Column::GenericColumn& column = columns[i].Values();
      Value::GenericValue value = out_v.GetValue();
      std::visit(
          [value]<Type type>(ArrayType<type>& col) -> void {
            if (std::holds_alternative<PhysicalType<type>>(value)) {
              col.emplace_back(std::get<PhysicalType<type>>(value));
            } else {
              THROW_RUNTIME_ERROR("Type mismatch");
            }
          },
          column);
    }

    return std::make_shared<Batch>(std::move(columns), Schema(std::move(fields)));
  }

 private:
  bool returned_ = false;
  std::shared_ptr<GlobalAggregationOperator> op_;
  std::shared_ptr<IStream<std::shared_ptr<Batch>>> stream_;
};

class FilterStream : public IStream<std::shared_ptr<Batch>> {
 public:
  FilterStream(std::shared_ptr<FilterOperator> filter) : op_(std::move(filter)) { stream_ = Execute(op_->child); }

  std::optional<std::shared_ptr<Batch>> Next() override {
    std::optional<std::shared_ptr<Batch>> batch = stream_->Next();
    if (batch) {
      Column condition_result = Evaluate(batch.value(), op_->condition);
      ASSERT(condition_result.GetType() == Type::kBool);

      return ApplyFilterColumn(batch.value(), std::get<ArrayType<Type::kBool>>(condition_result.Values()));
    }
    return std::nullopt;
  }

 private:
  std::shared_ptr<Batch> ApplyFilterColumn(std::shared_ptr<Batch> batch, ArrayType<Type::kBool> filter) {
    ASSERT(batch->Rows() == static_cast<int64_t>(filter.size()));

    std::vector<Column> filtered_columns;

    filtered_columns.reserve(batch->Columns().size());
    for (const auto& column : batch->Columns()) {
      std::visit([&filtered_columns]<typename T>(const T&) -> void { filtered_columns.emplace_back(T{}); },
                 column.Values());
    }

    for (int64_t i = 0; i < batch->Rows(); ++i) {
      if (filter[i].value) {
        for (size_t j = 0; j < batch->Columns().size(); ++j) {
          Column::GenericColumn& column = filtered_columns.at(j).Values();
          Value::GenericValue value = batch->Columns().at(j)[i].GetValue();

          std::visit(
              [&value]<Type type>(ArrayType<type>& column) -> void {
                if (std::holds_alternative<PhysicalType<type>>(value)) {
                  column.emplace_back(std::get<PhysicalType<type>>(value));
                } else {
                  THROW_RUNTIME_ERROR("Type mismatch, type = " + std::to_string(static_cast<int>(type)) +
                                      ", vs index " + std::to_string(value.index()));
                }
              },
              column);
        }
      }
    }

    return std::make_shared<Batch>(std::move(filtered_columns), batch->GetSchema());
  }

  std::shared_ptr<FilterOperator> op_;
  std::shared_ptr<IStream<std::shared_ptr<Batch>>> stream_;
};

class ProjectStream : public IStream<std::shared_ptr<Batch>> {
 public:
  ProjectStream(std::shared_ptr<ProjectOperator> project) : op_(std::move(project)) { stream_ = Execute(op_->child); }

  std::optional<std::shared_ptr<Batch>> Next() override {
    std::optional<std::shared_ptr<Batch>> batch = stream_->Next();
    if (batch) {
      std::vector<Column> projected_columns;
      projected_columns.reserve(op_->projections.size());

      std::vector<Field> fields;
      for (const auto& projection : op_->projections) {
        projected_columns.emplace_back(Evaluate(batch.value(), projection.expression));

        fields.emplace_back(Field(projection.name, projected_columns.back().GetType()));
      }
      return std::make_shared<Batch>(std::move(projected_columns), Schema(fields));
    }
    return std::nullopt;
  }

 private:
  std::shared_ptr<ProjectOperator> op_;
  std::shared_ptr<IStream<std::shared_ptr<Batch>>> stream_;
};

class SortStream : public IStream<std::shared_ptr<Batch>> {
 public:
  SortStream(std::shared_ptr<SortOperator> sort) : op_(sort) { stream_ = Execute(sort->child); }

  std::optional<std::shared_ptr<Batch>> Next() override {
    if (returned_) {
      return std::nullopt;
    }
    returned_ = true;

    std::vector<std::shared_ptr<Batch>> batches;
    while (auto batch = stream_->Next()) {
      batches.emplace_back(batch.value());
    }

    if (batches.empty()) {
      return std::nullopt;
    }

    std::shared_ptr<Batch> merged = MergeBatches(batches);
    const int64_t num_rows = merged->Rows();

    if (num_rows == 0) {
      return merged;
    }

    std::vector<Column> sort_columns;
    sort_columns.reserve(op_->sort_keys.size());
    for (const auto& sort_key : op_->sort_keys) {
      sort_columns.emplace_back(Evaluate(merged, sort_key.expression));
    }

    std::vector<int64_t> indices(num_rows);
    std::iota(indices.begin(), indices.end(), 0);

    std::sort(indices.begin(), indices.end(), [&](int64_t a, int64_t b) {
      for (size_t k = 0; k < sort_columns.size(); ++k) {
        std::strong_ordering cmp = sort_columns[k][a] <=> sort_columns[k][b];
        if (cmp != 0) {
          return op_->sort_keys[k].is_ascending ? (cmp < 0) : (cmp > 0);
        }
      }
      return false;
    });

    return std::make_shared<Batch>(ReorderColumns(merged->Columns(), indices), merged->GetSchema());
  }

 private:
  static std::shared_ptr<Batch> MergeBatches(const std::vector<std::shared_ptr<Batch>>& batches) {
    ASSERT(!batches.empty());

    const Schema& schema = batches[0]->GetSchema();
    const size_t num_columns = batches[0]->Columns().size();

    int64_t total_rows = 0;
    for (const auto& batch : batches) {
      total_rows += batch->Rows();
    }

    std::vector<Column> merged_columns;
    merged_columns.reserve(num_columns);
    for (size_t col_idx = 0; col_idx < num_columns; ++col_idx) {
      std::visit(
          [&merged_columns, total_rows]<Type type>(const ArrayType<type>&) {
            ArrayType<type> arr;
            arr.reserve(total_rows);
            merged_columns.emplace_back(std::move(arr));
          },
          batches[0]->Columns()[col_idx].Values());
    }

    for (const auto& batch : batches) {
      for (size_t col_idx = 0; col_idx < num_columns; ++col_idx) {
        std::visit(
            [&batch, col_idx]<Type type>(ArrayType<type>& dest) {
              const auto& src = std::get<ArrayType<type>>(batch->Columns()[col_idx].Values());
              dest.insert(dest.end(), src.begin(), src.end());
            },
            merged_columns[col_idx].Values());
      }
    }

    return std::make_shared<Batch>(std::move(merged_columns), schema);
  }

  static std::vector<Column> ReorderColumns(const std::vector<Column>& columns, const std::vector<int64_t>& indices) {
    std::vector<Column> result;
    result.reserve(columns.size());

    for (const auto& column : columns) {
      std::visit(
          [&result, &indices]<Type type>(const ArrayType<type>& src) {
            ArrayType<type> dest;
            dest.reserve(indices.size());
            for (int64_t idx : indices) {
              dest.emplace_back(src[idx]);
            }
            result.emplace_back(std::move(dest));
          },
          column.Values());
    }

    return result;
  }

  bool returned_ = false;
  std::shared_ptr<SortOperator> op_;
  std::shared_ptr<IStream<std::shared_ptr<Batch>>> stream_;
};

class TopKStream : public IStream<std::shared_ptr<Batch>> {
 private:
  struct HeapEntry {
    std::vector<Value> row_data;
    std::vector<Value> sort_keys;
  };

 public:
  TopKStream(std::shared_ptr<TopKOperator> topk) : op_(topk) { stream_ = Execute(topk->child); }

  std::optional<std::shared_ptr<Batch>> Next() override {
    if (returned_) {
      return std::nullopt;
    }
    returned_ = true;

    auto is_better = [this](const HeapEntry& a, const HeapEntry& b) -> bool {
      for (size_t i = 0; i < a.sort_keys.size(); ++i) {
        std::strong_ordering cmp = a.sort_keys[i] <=> b.sort_keys[i];
        if (cmp != 0) {
          return op_->sort_keys[i].is_ascending ? (cmp < 0) : (cmp > 0);
        }
      }
      return false;
    };

    auto heap_cmp = [&is_better](const HeapEntry& a, const HeapEntry& b) -> bool { return is_better(a, b); };

    std::priority_queue<HeapEntry, std::vector<HeapEntry>, decltype(heap_cmp)> heap(heap_cmp);

    const uint32_t k = op_->limit;
    std::optional<Schema> schema;

    while (auto batch_opt = stream_->Next()) {
      std::shared_ptr<Batch> batch = batch_opt.value();
      if (!schema.has_value()) {
        schema = batch->GetSchema();
      }

      std::vector<Column> sort_columns;
      sort_columns.reserve(op_->sort_keys.size());
      for (const auto& sort_key : op_->sort_keys) {
        sort_columns.emplace_back(Evaluate(batch, sort_key.expression));
      }

      for (int64_t row_idx = 0; row_idx < batch->Rows(); ++row_idx) {
        std::vector<Value> sort_keys;
        sort_keys.reserve(sort_columns.size());
        for (const auto& col : sort_columns) {
          sort_keys.emplace_back(col[row_idx]);
        }

        bool should_insert = false;
        if (heap.size() < k) {
          should_insert = true;
        } else {
          HeapEntry candidate{.row_data = {}, .sort_keys = std::move(sort_keys)};
          if (is_better(candidate, heap.top())) {
            should_insert = true;
            heap.pop();
          }
          sort_keys = std::move(candidate.sort_keys);
        }

        if (should_insert) {
          std::vector<Value> row_data;
          row_data.reserve(batch->Columns().size());
          for (const auto& col : batch->Columns()) {
            row_data.emplace_back(col[row_idx]);
          }

          heap.push(HeapEntry{.row_data = std::move(row_data), .sort_keys = std::move(sort_keys)});
        }
      }
    }

    if (!schema.has_value() || heap.empty()) {
      return std::nullopt;
    }

    std::vector<HeapEntry> entries;
    entries.reserve(heap.size());
    while (!heap.empty()) {
      entries.emplace_back(heap.top());
      heap.pop();
    }

    std::sort(entries.begin(), entries.end(), is_better);

    return std::make_shared<Batch>(BuildColumns(schema.value(), entries), schema.value());
  }

 private:
  static std::vector<Column> BuildColumns(const Schema& schema, const std::vector<HeapEntry>& entries) {
    std::vector<Column> columns;
    columns.reserve(schema.Fields().size());

    for (size_t col_idx = 0; col_idx < schema.Fields().size(); ++col_idx) {
      Type type = schema.Fields()[col_idx].type;
      Dispatch(
          [&]<Type type>(Tag<type>) {
            ArrayType<type> arr;
            arr.reserve(entries.size());
            for (const auto& entry : entries) {
              arr.emplace_back(std::get<PhysicalType<type>>(entry.row_data[col_idx].GetValue()));
            }
            columns.emplace_back(std::move(arr));
          },
          type);
    }

    return columns;
  }

  bool returned_ = false;
  std::shared_ptr<TopKOperator> op_;
  std::shared_ptr<IStream<std::shared_ptr<Batch>>> stream_;
};

std::shared_ptr<IStream<std::shared_ptr<Batch>>> Execute(std::shared_ptr<Operator> op) {
  ASSERT(op != nullptr);

  switch (op->type) {
    case OperatorType::kAggregate:
      return std::make_shared<AggregationStream>(std::static_pointer_cast<AggregateOperator>(op));
    case OperatorType::kAggregateCompact:
      return std::make_shared<CompactAggregationStream>(std::static_pointer_cast<CompactAggregateOperator>(op));
    case OperatorType::kScan:
      return std::make_shared<ScanStream>(std::static_pointer_cast<ScanOperator>(op));
    case OperatorType::kCountTable:
      return std::make_shared<CountTableStream>(std::static_pointer_cast<CountTableOperator>(op));
    case OperatorType::kGlobalAggregation:
      return std::make_shared<GlobalAggregationStream>(std::static_pointer_cast<GlobalAggregationOperator>(op));
    case OperatorType::kConcat:
      return std::make_shared<ConcatStream>(std::static_pointer_cast<ConcatOperator>(op));
    case OperatorType::kFilter:
      return std::make_shared<FilterStream>(std::static_pointer_cast<FilterOperator>(op));
    case OperatorType::kProject:
      return std::make_shared<ProjectStream>(std::static_pointer_cast<ProjectOperator>(op));
    case OperatorType::kSort:
      return std::make_shared<SortStream>(std::static_pointer_cast<SortOperator>(op));
    case OperatorType::kTopK:
      return std::make_shared<TopKStream>(std::static_pointer_cast<TopKOperator>(op));
    default:
      THROW_NOT_IMPLEMENTED;
  }

  THROW_NOT_IMPLEMENTED;
}

}  // namespace ngn
