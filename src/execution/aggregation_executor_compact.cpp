#include "src/execution/aggregation_executor_compact.h"

#include <cstdint>
#include <cstring>
#include <optional>
#include <type_traits>
#include <vector>

#include "src/core/column.h"
#include "src/core/type.h"
#include "src/execution/aggregation_executor.h"  // fallback Evaluate()
#include "src/execution/expression.h"
#include "src/execution/int128.h"
#include "src/util/assert.h"
#include "src/util/macro.h"

namespace ngn {
namespace {

static inline uint64_t Rotl64(uint64_t x, int k) { return (x << k) | (x >> (64 - k)); }

static inline uint64_t Mix64(uint64_t x) {
  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  return x ^ (x >> 31);
}

static inline uint64_t HashBytes(const uint8_t* data, size_t len) {
  // Simple 64-bit hash over bytes, optimized for fixed-width keys.
  uint64_t h = Mix64(static_cast<uint64_t>(len));
  size_t i = 0;
  while (i + sizeof(uint64_t) <= len) {
    uint64_t word = 0;
    std::memcpy(&word, data + i, sizeof(uint64_t));
    h ^= Mix64(word);
    h = Rotl64(h, 27) * 0x3c79ac492ba7b653ULL + 0x1c69b3f74ac4ae35ULL;
    i += sizeof(uint64_t);
  }
  uint64_t tail = 0;
  if (i < len) {
    std::memcpy(&tail, data + i, len - i);
    h ^= Mix64(tail);
  }
  return Mix64(h);
}

static inline size_t SizeOfFixedWidthType(Type type) {
  switch (type) {
    case Type::kBool:
      return sizeof(Boolean);
    case Type::kInt16:
      return sizeof(int16_t);
    case Type::kInt32:
      return sizeof(int32_t);
    case Type::kInt64:
      return sizeof(int64_t);
    case Type::kInt128:
      return sizeof(Int128);
    case Type::kDate:
      return sizeof(Date);
    case Type::kTimestamp:
      return sizeof(Timestamp);
    case Type::kChar:
      return sizeof(char);
    case Type::kString:
      return 0;
  }
  return 0;
}

static inline bool IsFixedWidthType(Type type) { return SizeOfFixedWidthType(type) != 0; }

struct KeyPart {
  Type type;
  size_t offset;
  size_t size;
};

enum class StateKind { kCount, kSum, kMin, kMax };

struct StatePart {
  StateKind kind;
  Type input_type;
  Type output_type;
  size_t has_value_offset;  // for min/max
  size_t value_offset;
  size_t value_size;
};

struct CompactPlan {
  std::vector<KeyPart> key_parts;
  size_t key_size = 0;

  std::vector<StatePart> state_parts;  // same order as aggregation->aggregations
  size_t state_size = 0;
};

static inline Type GetExpressionType(const std::shared_ptr<Expression>& expression) {
  switch (expression->expr_type) {
    case ExpressionType::kVariable:
      return std::static_pointer_cast<Variable>(expression)->type;
    case ExpressionType::kConst:
      return std::static_pointer_cast<Const>(expression)->value.GetType();
    default:
      return Type::kString;  // sentinel for "unsupported" (non-fixed-width in our compact impl)
  }
}

static inline Type GetSumOutputType(Type input_type) {
  switch (input_type) {
    case Type::kInt16:
    case Type::kInt32:
      return Type::kInt64;
    case Type::kInt64:
    case Type::kInt128:
      return Type::kInt128;
    default:
      return Type::kString;  // unsupported sentinel
  }
}

static std::optional<CompactPlan> TryBuildCompactPlan(const Aggregation& aggregation) {
  CompactPlan plan;

  // Group-by: only fixed-width types for compact representation.
  size_t key_offset = 0;
  plan.key_parts.reserve(aggregation.group_by_expressions.size());
  for (const auto& g : aggregation.group_by_expressions) {
    Type t = GetExpressionType(g.expression);
    if (!IsFixedWidthType(t) || t == Type::kString) {
      return std::nullopt;
    }
    const size_t sz = SizeOfFixedWidthType(t);
    plan.key_parts.push_back(KeyPart{.type = t, .offset = key_offset, .size = sz});
    key_offset += sz;
  }
  plan.key_size = key_offset;

  // Aggregations: support COUNT/SUM/MIN/MAX. DISTINCT unsupported (too memory heavy by design).
  size_t state_offset = 0;
  plan.state_parts.reserve(aggregation.aggregations.size());
  for (const auto& a : aggregation.aggregations) {
    if (a.type == AggregationType::kDistinct) {
      return std::nullopt;
    }

    if (a.type == AggregationType::kCount) {
      plan.state_parts.push_back(StatePart{.kind = StateKind::kCount,
                                           .input_type = Type::kInt64,
                                           .output_type = Type::kInt64,
                                           .has_value_offset = 0,
                                           .value_offset = state_offset,
                                           .value_size = sizeof(int64_t)});
      state_offset += sizeof(int64_t);
      continue;
    }

    Type input_type = GetExpressionType(a.expression);
    if (!IsFixedWidthType(input_type) || input_type == Type::kString) {
      return std::nullopt;
    }

    if (a.type == AggregationType::kSum) {
      Type out_t = GetSumOutputType(input_type);
      if (out_t == Type::kString) {
        return std::nullopt;
      }

      // Store sums internally as Int128 to avoid overflow and avoid per-agg variants.
      plan.state_parts.push_back(StatePart{.kind = StateKind::kSum,
                                           .input_type = input_type,
                                           .output_type = out_t,
                                           .has_value_offset = 0,
                                           .value_offset = state_offset,
                                           .value_size = sizeof(Int128)});
      state_offset += sizeof(Int128);
      continue;
    }

    if (a.type == AggregationType::kMin || a.type == AggregationType::kMax) {
      const size_t has_value_off = state_offset;
      state_offset += sizeof(uint8_t);
      const size_t val_off = state_offset;
      const size_t val_sz = SizeOfFixedWidthType(input_type);
      state_offset += val_sz;

      plan.state_parts.push_back(StatePart{
          .kind = (a.type == AggregationType::kMin) ? StateKind::kMin : StateKind::kMax,
          .input_type = input_type,
          .output_type = input_type,
          .has_value_offset = has_value_off,
          .value_offset = val_off,
          .value_size = val_sz,
      });
      continue;
    }

    return std::nullopt;
  }

  plan.state_size = state_offset;
  return plan;
}

class FlatHashAggCompact {
 public:
  FlatHashAggCompact(size_t key_size, size_t state_size) : key_size_(key_size), state_size_(state_size) {
    Rehash(1u << 20);  // ~1M slots initial
  }

  uint8_t* GetOrInsert(const uint8_t* key_bytes) {
    if (size_ + 1 > static_cast<size_t>(static_cast<double>(capacity_) * kMaxLoadFactor)) {
      Rehash(capacity_ * 2);
    }

    uint64_t h = HashBytes(key_bytes, key_size_);
    size_t mask = capacity_ - 1;
    size_t idx = static_cast<size_t>(h) & mask;

    while (occupied_[idx]) {
      const uint8_t* existing_key = &keys_[idx * key_size_];
      if (std::memcmp(existing_key, key_bytes, key_size_) == 0) {
        return &states_[idx * state_size_];
      }
      idx = (idx + 1) & mask;
    }

    occupied_[idx] = 1;
    std::memcpy(&keys_[idx * key_size_], key_bytes, key_size_);
    std::memset(&states_[idx * state_size_], 0, state_size_);
    ++size_;
    return &states_[idx * state_size_];
  }

  template <typename Fn>
  void ForEach(Fn&& fn) const {
    for (size_t i = 0; i < capacity_; ++i) {
      if (occupied_[i]) {
        fn(&keys_[i * key_size_], &states_[i * state_size_]);
      }
    }
  }

  size_t Size() const { return size_; }

 private:
  static constexpr double kMaxLoadFactor = 0.70;

  void Rehash(size_t new_capacity) {
    ASSERT(new_capacity >= 8);
    // round up to power of two
    size_t cap = 1;
    while (cap < new_capacity) {
      cap <<= 1;
    }

    std::vector<uint8_t> old_keys;
    std::vector<uint8_t> old_states;
    std::vector<uint8_t> old_occ;
    size_t old_cap = capacity_;

    if (capacity_ != 0) {
      old_keys = std::move(keys_);
      old_states = std::move(states_);
      old_occ = std::move(occupied_);
    }

    capacity_ = cap;
    size_ = 0;
    keys_.assign(capacity_ * key_size_, 0);
    states_.assign(capacity_ * state_size_, 0);
    occupied_.assign(capacity_, 0);

    if (old_cap == 0) {
      return;
    }

    size_t mask = capacity_ - 1;
    for (size_t i = 0; i < old_cap; ++i) {
      if (!old_occ[i]) {
        continue;
      }

      const uint8_t* key_ptr = &old_keys[i * key_size_];
      const uint8_t* state_ptr = &old_states[i * state_size_];
      uint64_t h = HashBytes(key_ptr, key_size_);
      size_t idx = static_cast<size_t>(h) & mask;
      while (occupied_[idx]) {
        idx = (idx + 1) & mask;
      }

      occupied_[idx] = 1;
      std::memcpy(&keys_[idx * key_size_], key_ptr, key_size_);
      std::memcpy(&states_[idx * state_size_], state_ptr, state_size_);
      ++size_;
    }
  }

  const size_t key_size_;
  const size_t state_size_;

  size_t capacity_ = 0;  // power of two
  size_t size_ = 0;

  std::vector<uint8_t> keys_;
  std::vector<uint8_t> states_;
  std::vector<uint8_t> occupied_;
};

struct ColAccessor {
  Type type;
  const void* data;  // points to contiguous PhysicalType<type> array
};

static ColAccessor MakeAccessor(const Column& col) {
  Type t = col.GetType();
  ColAccessor a{.type = t, .data = nullptr};
  Dispatch(
      [&]<Type type>(Tag<type>) {
        const auto& arr = std::get<ArrayType<type>>(col.Values());
        a.data = static_cast<const void*>(arr.data());
      },
      t);
  return a;
}

static inline void PackValue(uint8_t* dst, const ColAccessor& acc, int64_t row) {
  switch (acc.type) {
    case Type::kBool:
      std::memcpy(dst, static_cast<const Boolean*>(acc.data) + row, sizeof(Boolean));
      return;
    case Type::kInt16:
      std::memcpy(dst, static_cast<const int16_t*>(acc.data) + row, sizeof(int16_t));
      return;
    case Type::kInt32:
      std::memcpy(dst, static_cast<const int32_t*>(acc.data) + row, sizeof(int32_t));
      return;
    case Type::kInt64:
      std::memcpy(dst, static_cast<const int64_t*>(acc.data) + row, sizeof(int64_t));
      return;
    case Type::kInt128:
      std::memcpy(dst, static_cast<const Int128*>(acc.data) + row, sizeof(Int128));
      return;
    case Type::kDate:
      std::memcpy(dst, static_cast<const Date*>(acc.data) + row, sizeof(Date));
      return;
    case Type::kTimestamp:
      std::memcpy(dst, static_cast<const Timestamp*>(acc.data) + row, sizeof(Timestamp));
      return;
    case Type::kChar:
      std::memcpy(dst, static_cast<const char*>(acc.data) + row, sizeof(char));
      return;
    case Type::kString:
      THROW_NOT_IMPLEMENTED;
  }
  THROW_NOT_IMPLEMENTED;
}

template <Type type>
static inline PhysicalType<type> LoadAt(const void* data, int64_t row) {
  return static_cast<const PhysicalType<type>*>(data)[row];
}

static inline Int128 ToInt128(Type type, const void* data, int64_t row) {
  switch (type) {
    case Type::kInt16:
      return static_cast<Int128>(LoadAt<Type::kInt16>(data, row));
    case Type::kInt32:
      return static_cast<Int128>(LoadAt<Type::kInt32>(data, row));
    case Type::kInt64:
      return static_cast<Int128>(LoadAt<Type::kInt64>(data, row));
    case Type::kInt128:
      return LoadAt<Type::kInt128>(data, row);
    default:
      THROW_NOT_IMPLEMENTED;
  }
}

static inline bool Less(Type type, const void* a, const void* b) {
  switch (type) {
    case Type::kBool:
      return static_cast<const Boolean*>(a)->value < static_cast<const Boolean*>(b)->value;
    case Type::kInt16:
      return *static_cast<const int16_t*>(a) < *static_cast<const int16_t*>(b);
    case Type::kInt32:
      return *static_cast<const int32_t*>(a) < *static_cast<const int32_t*>(b);
    case Type::kInt64:
      return *static_cast<const int64_t*>(a) < *static_cast<const int64_t*>(b);
    case Type::kInt128:
      return *static_cast<const Int128*>(a) < *static_cast<const Int128*>(b);
    case Type::kDate:
      return static_cast<const Date*>(a)->value < static_cast<const Date*>(b)->value;
    case Type::kTimestamp:
      return static_cast<const Timestamp*>(a)->value < static_cast<const Timestamp*>(b)->value;
    case Type::kChar:
      return *static_cast<const char*>(a) < *static_cast<const char*>(b);
    case Type::kString:
      THROW_NOT_IMPLEMENTED;
  }
  THROW_NOT_IMPLEMENTED;
}

static inline bool Greater(Type type, const void* a, const void* b) { return Less(type, b, a); }

static std::shared_ptr<Batch> EvaluateCompactGeneral(std::shared_ptr<IStream<std::shared_ptr<Batch>>> stream,
                                                     std::shared_ptr<Aggregation> aggregation,
                                                     const CompactPlan& plan) {
  FlatHashAggCompact ht(plan.key_size, plan.state_size);

  // Pre-create expressions vectors to evaluate.
  std::vector<std::shared_ptr<Expression>> group_exprs;
  group_exprs.reserve(aggregation->group_by_expressions.size());
  for (const auto& g : aggregation->group_by_expressions) {
    group_exprs.emplace_back(g.expression);
  }

  // For aggregations, evaluate only for SUM/MIN/MAX; COUNT ignores its input.
  std::vector<std::optional<std::shared_ptr<Expression>>> agg_exprs;
  agg_exprs.reserve(aggregation->aggregations.size());
  for (const auto& a : aggregation->aggregations) {
    if (a.type == AggregationType::kCount) {
      agg_exprs.emplace_back(std::nullopt);
    } else {
      agg_exprs.emplace_back(a.expression);
    }
  }

  std::vector<uint8_t> key_buf(plan.key_size);

  while (auto batch_opt = stream->Next()) {
    std::shared_ptr<Batch> batch = batch_opt.value();
    const int64_t rows = batch->Rows();

    // Evaluate group-by columns once per batch.
    std::vector<ColAccessor> group_cols;
    group_cols.reserve(group_exprs.size());
    for (const auto& expr : group_exprs) {
      Column c = Evaluate(batch, expr);
      group_cols.emplace_back(MakeAccessor(c));
    }

    // Evaluate aggregation input columns once per batch (for non-count aggs).
    std::vector<std::optional<ColAccessor>> agg_cols;
    agg_cols.reserve(agg_exprs.size());
    for (const auto& maybe_expr : agg_exprs) {
      if (!maybe_expr.has_value()) {
        agg_cols.emplace_back(std::nullopt);
      } else {
        Column c = Evaluate(batch, maybe_expr.value());
        agg_cols.emplace_back(MakeAccessor(c));
      }
    }

    for (int64_t r = 0; r < rows; ++r) {
      // Pack group-by key for this row.
      for (size_t i = 0; i < plan.key_parts.size(); ++i) {
        const auto& kp = plan.key_parts[i];
        PackValue(&key_buf[kp.offset], group_cols[i], r);
      }

      uint8_t* state = ht.GetOrInsert(key_buf.data());

      // Update states.
      for (size_t ai = 0; ai < plan.state_parts.size(); ++ai) {
        const auto& sp = plan.state_parts[ai];
        uint8_t* value_ptr = state + sp.value_offset;

        switch (sp.kind) {
          case StateKind::kCount: {
            auto* cnt = reinterpret_cast<int64_t*>(value_ptr);
            *cnt += 1;
            break;
          }
          case StateKind::kSum: {
            ASSERT(agg_cols[ai].has_value());
            const auto& acc = agg_cols[ai].value();
            auto* sum = reinterpret_cast<Int128*>(value_ptr);
            *sum += ToInt128(sp.input_type, acc.data, r);
            break;
          }
          case StateKind::kMin:
          case StateKind::kMax: {
            ASSERT(agg_cols[ai].has_value());
            const auto& acc = agg_cols[ai].value();

            uint8_t* has_value = state + sp.has_value_offset;
            uint8_t* stored = state + sp.value_offset;

            // Load candidate into a temporary small buffer on stack.
            // value_size is <= 16 for supported types.
            uint8_t tmp[sizeof(Int128)] = {};
            PackValue(tmp, acc, r);

            if (*has_value == 0) {
              *has_value = 1;
              std::memcpy(stored, tmp, sp.value_size);
            } else {
              const bool better =
                  (sp.kind == StateKind::kMin) ? Less(sp.input_type, tmp, stored) : Greater(sp.input_type, tmp, stored);
              if (better) {
                std::memcpy(stored, tmp, sp.value_size);
              }
            }
            break;
          }
        }
      }
    }
  }

  // Build output schema.
  std::vector<Field> fields;
  fields.reserve(aggregation->group_by_expressions.size() + aggregation->aggregations.size());
  for (const auto& g : aggregation->group_by_expressions) {
    fields.emplace_back(Field(g.name, GetExpressionType(g.expression)));
  }
  for (size_t i = 0; i < aggregation->aggregations.size(); ++i) {
    const auto& a = aggregation->aggregations[i];
    Type out_t = Type::kInt64;
    if (a.type == AggregationType::kCount) {
      out_t = Type::kInt64;
    } else if (a.type == AggregationType::kSum) {
      out_t = plan.state_parts[i].output_type;
    } else if (a.type == AggregationType::kMin || a.type == AggregationType::kMax) {
      out_t = plan.state_parts[i].output_type;
    } else {
      THROW_NOT_IMPLEMENTED;
    }
    fields.emplace_back(Field(a.name, out_t));
  }

  std::vector<Column> columns;
  columns.reserve(fields.size());
  const size_t n = ht.Size();
  for (const auto& f : fields) {
    Dispatch([&]<Type type>(Tag<type>) { columns.emplace_back(Column(ArrayType<type>{})); }, f.type);
    std::visit([&]<Type type>(ArrayType<type>& arr) { arr.reserve(n); }, columns.back().Values());
  }

  // Helper: append a fixed-width value from bytes to a column.
  auto append_from_bytes = [&](size_t col_idx, Type type, const uint8_t* src) {
    Column& col = columns[col_idx];
    Dispatch(
        [&]<Type t>(Tag<t>) {
          auto& arr = std::get<ArrayType<t>>(col.Values());
          if constexpr (std::is_trivially_copyable_v<PhysicalType<t>>) {
            PhysicalType<t> v{};
            std::memcpy(&v, src, sizeof(PhysicalType<t>));
            arr.emplace_back(v);
          } else {
            // e.g. std::string - compact aggregation doesn't support non-trivial types
            THROW_NOT_IMPLEMENTED;
          }
        },
        type);
  };

  // Materialize output.
  ht.ForEach([&](const uint8_t* key_bytes, const uint8_t* state_bytes) {
    size_t out_col = 0;

    // Group-by key columns.
    for (const auto& kp : plan.key_parts) {
      append_from_bytes(out_col, kp.type, key_bytes + kp.offset);
      ++out_col;
    }

    // Aggregations.
    for (size_t ai = 0; ai < plan.state_parts.size(); ++ai) {
      const auto& sp = plan.state_parts[ai];
      const uint8_t* value_ptr = state_bytes + sp.value_offset;

      switch (sp.kind) {
        case StateKind::kCount: {
          append_from_bytes(out_col, Type::kInt64, value_ptr);
          break;
        }
        case StateKind::kSum: {
          const Int128 sum = *reinterpret_cast<const Int128*>(value_ptr);
          if (sp.output_type == Type::kInt64) {
            const int64_t v = static_cast<int64_t>(sum);
            append_from_bytes(out_col, Type::kInt64, reinterpret_cast<const uint8_t*>(&v));
          } else {
            append_from_bytes(out_col, Type::kInt128, reinterpret_cast<const uint8_t*>(&sum));
          }
          break;
        }
        case StateKind::kMin:
        case StateKind::kMax: {
          const uint8_t has_value = *(state_bytes + sp.has_value_offset);
          ASSERT(has_value != 0);
          append_from_bytes(out_col, sp.output_type, value_ptr);
          break;
        }
      }

      ++out_col;
    }
  });

  return std::make_shared<Batch>(std::move(columns), Schema(fields));
}

}  // namespace

std::shared_ptr<Batch> EvaluateCompact(std::shared_ptr<IStream<std::shared_ptr<Batch>>> stream,
                                       std::shared_ptr<Aggregation> aggregation) {
  ASSERT(stream != nullptr);
  ASSERT(aggregation != nullptr);

  auto plan = TryBuildCompactPlan(*aggregation);
  if (!plan.has_value()) {
    return Evaluate(std::move(stream), std::move(aggregation));
  }

  return EvaluateCompactGeneral(std::move(stream), std::move(aggregation), plan.value());
}

}  // namespace ngn
