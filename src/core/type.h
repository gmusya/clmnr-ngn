#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "src/execution/int128.h"

namespace ngn {

enum class Type {
  kBool,
  kInt16,
  kInt32,
  kInt64,
  kInt128,
  kDate,
  kTimestamp,
  kChar,
  kString,
};

////////////////////////////////////////////////////////////////////////////////

struct Date {
  int64_t value;

  auto operator<=>(const Date& other) const = default;
};

struct Timestamp {
  int64_t value;

  auto operator<=>(const Timestamp& other) const = default;
};

struct Boolean {
  bool value;

  auto operator<=>(const Boolean& other) const = default;
};

////////////////////////////////////////////////////////////////////////////////

namespace internal {
template <Type type>
struct PhysicalTypeTrait {};

template <>
struct PhysicalTypeTrait<Type::kBool> {
  using PhysicalType = Boolean;
};

template <>
struct PhysicalTypeTrait<Type::kInt16> {
  using PhysicalType = int16_t;
};

template <>
struct PhysicalTypeTrait<Type::kInt32> {
  using PhysicalType = int32_t;
};

template <>
struct PhysicalTypeTrait<Type::kInt64> {
  using PhysicalType = int64_t;
};

template <>
struct PhysicalTypeTrait<Type::kString> {
  using PhysicalType = std::string;
};

template <>
struct PhysicalTypeTrait<Type::kDate> {
  using PhysicalType = Date;
};

template <>
struct PhysicalTypeTrait<Type::kTimestamp> {
  using PhysicalType = Timestamp;
};

template <>
struct PhysicalTypeTrait<Type::kChar> {
  using PhysicalType = char;
};

template <>
struct PhysicalTypeTrait<Type::kInt128> {
  using PhysicalType = Int128;
};
}  // namespace internal

template <Type type>
using PhysicalType = internal::PhysicalTypeTrait<type>::PhysicalType;

////////////////////////////////////////////////////////////////////////////////

// Temporary solution
// TODO(gmusya): use some format
template <Type type>
struct ArrayType : public std::vector<PhysicalType<type>> {
  using std::vector<PhysicalType<type>>::vector;

  ArrayType() = default;
  ArrayType(std::vector<PhysicalType<type>> vals) : std::vector<PhysicalType<type>>(std::move(vals)) {}
};

////////////////////////////////////////////////////////////////////////////////

}  // namespace ngn
