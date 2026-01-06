#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace ngn {

enum class Type {
  kInt64,
  kString,
};

////////////////////////////////////////////////////////////////////////////////

namespace internal {
template <Type type>
struct PhysicalTypeTrait {};

template <>
struct PhysicalTypeTrait<Type::kInt64> {
  using PhysicalType = int64_t;
};

template <>
struct PhysicalTypeTrait<Type::kString> {
  using PhysicalType = std::string;
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
