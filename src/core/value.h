#pragma once

#include <variant>

#include "src/core/type.h"

namespace ngn {

class Value {
 public:
  using GenericValue = std::variant<PhysicalType<Type::kInt64>, PhysicalType<Type::kString>>;

  explicit Value(PhysicalType<Type::kInt64> value) : value_(std::move(value)) {}
  explicit Value(PhysicalType<Type::kString> value) : value_(std::move(value)) {}

  GenericValue& Values() { return value_; }
  const GenericValue& Values() const { return value_; }

  std::string ToString() const {
    return std::visit(
        []<typename T>(const T& value) -> std::string {
          if constexpr (std::is_same_v<T, PhysicalType<Type::kInt64>>) {
            return std::to_string(value);
          } else if constexpr (std::is_same_v<T, PhysicalType<Type::kString>>) {
            return value;
          } else {
            static_assert(false);
          }
        },
        value_);
  }

 private:
  GenericValue value_;
};

}  // namespace ngn
