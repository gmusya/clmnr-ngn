#pragma once

#include <optional>
#include <sstream>
#include <variant>
#include <vector>

#include "src/core/column.h"
#include "src/core/serde.h"
#include "src/core/type.h"
#include "src/core/value.h"
#include "src/util/assert.h"

namespace ngn {

struct ZoneMapEntry {
  bool has_stats = false;

  std::optional<Type> type;
  std::optional<Value> min_value;
  std::optional<Value> max_value;

  bool CanSkipForEqual(const Value& value) const {
    if (!has_stats) {
      return false;
    }
    ASSERT(min_value->GetType() == value.GetType());
    ASSERT(max_value->GetType() == value.GetType());
    return value < min_value || value > max_value;
  }

  bool CanSkipForRange(const Value& filter_min, const Value& filter_max) const {
    if (!has_stats) {
      return false;
    }
    ASSERT(min_value->GetType() == filter_min.GetType());
    ASSERT(filter_min.GetType() == filter_max.GetType());
    ASSERT(filter_max.GetType() == max_value->GetType());

    if (!has_stats) {
      return false;
    }
    return max_value < filter_min || min_value > filter_max;
  }

  std::string Serialize() const {
    std::stringstream out;
    Write(Boolean{.value = has_stats}, out);
    if (has_stats) {
      Write<int16_t>(static_cast<int16_t>(*type), out);
      Dispatch(
          [&]<Type type>(Tag<type>) {
            Write<PhysicalType<type>>(std::get<PhysicalType<type>>(min_value->GetValue()), out);
            Write<PhysicalType<type>>(std::get<PhysicalType<type>>(max_value->GetValue()), out);
          },
          *type);
    }
    return out.str();
  }

  static ZoneMapEntry Deserialize(std::istream& in) {
    ZoneMapEntry entry;
    entry.has_stats = Read<Boolean>(in).value;
    if (entry.has_stats) {
      entry.type = static_cast<Type>(Read<int16_t>(in));
      Dispatch(
          [&]<Type type>(Tag<type>) {
            entry.min_value.emplace(Read<PhysicalType<type>>(in));
            entry.max_value.emplace(Read<PhysicalType<type>>(in));
          },
          *entry.type);
    }
    return entry;
  }
};

struct RowGroupZoneMap {
  std::vector<ZoneMapEntry> columns;

  std::string Serialize() const {
    std::stringstream out;
    int64_t sz = columns.size();
    Write(sz, out);
    for (const auto& entry : columns) {
      std::string serialized = entry.Serialize();
      Write(serialized, out);
    }
    return out.str();
  }

  static RowGroupZoneMap Deserialize(std::istream& in) {
    RowGroupZoneMap zm;
    int64_t sz = Read<int64_t>(in);
    zm.columns.reserve(sz);
    for (int64_t i = 0; i < sz; ++i) {
      std::string serialized = Read<std::string>(in);
      std::stringstream ss(serialized);
      zm.columns.push_back(ZoneMapEntry::Deserialize(ss));
    }
    return zm;
  }
};

template <Type type>
ZoneMapEntry ComputeZoneMapEntry(const ArrayType<type>& values) {
  ZoneMapEntry entry;

  if (values.empty()) {
    entry.has_stats = false;
    return entry;
  }

  entry.has_stats = true;
  entry.type = type;

  std::optional<PhysicalType<type>> min_value;
  std::optional<PhysicalType<type>> max_value;

  for (const auto& v : values) {
    ASSERT(min_value.has_value() == max_value.has_value());

    if (!min_value.has_value()) {
      min_value = v;
      max_value = v;
    } else {
      min_value = std::min(*min_value, v);
      max_value = std::max(*max_value, v);
    }
  }

  ASSERT(min_value.has_value() && max_value.has_value());
  entry.min_value = Value(std::move(*min_value));
  entry.max_value = Value(std::move(*max_value));

  return entry;
}

inline RowGroupZoneMap ComputeRowGroupZoneMap(const std::vector<Column>& columns) {
  RowGroupZoneMap zm;
  zm.columns.reserve(columns.size());

  for (const auto& col : columns) {
    ZoneMapEntry entry = std::visit(
        []<Type type>(const ArrayType<type>& values) { return ComputeZoneMapEntry<type>(values); }, col.Values());
    zm.columns.push_back(entry);
  }

  return zm;
}

}  // namespace ngn
