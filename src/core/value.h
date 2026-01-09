#pragma once

#include <variant>

#include "src/core/type.h"

namespace ngn {

class Value {
 public:
  using GenericValue = std::variant<PhysicalType<Type::kInt16>, PhysicalType<Type::kInt32>, PhysicalType<Type::kInt64>,
                                    PhysicalType<Type::kString>, PhysicalType<Type::kDate>,
                                    PhysicalType<Type::kTimestamp>, PhysicalType<Type::kChar>>;

  explicit Value(PhysicalType<Type::kInt16> value) : value_(std::move(value)) {}
  explicit Value(PhysicalType<Type::kInt32> value) : value_(std::move(value)) {}
  explicit Value(PhysicalType<Type::kInt64> value) : value_(std::move(value)) {}
  explicit Value(PhysicalType<Type::kString> value) : value_(std::move(value)) {}
  explicit Value(PhysicalType<Type::kDate> value) : value_(std::move(value)) {}
  explicit Value(PhysicalType<Type::kTimestamp> value) : value_(std::move(value)) {}
  explicit Value(PhysicalType<Type::kChar> value) : value_(std::move(value)) {}

  GenericValue& GetValue() { return value_; }
  const GenericValue& GetValue() const { return value_; }

  std::string ToString() const {
    return std::visit(
        []<typename T>(const T& value) -> std::string {
          if constexpr (std::is_same_v<T, PhysicalType<Type::kInt16>>) {
            return std::to_string(value);
          } else if constexpr (std::is_same_v<T, PhysicalType<Type::kInt32>>) {
            return std::to_string(value);
          } else if constexpr (std::is_same_v<T, PhysicalType<Type::kInt64>>) {
            return std::to_string(value);
          } else if constexpr (std::is_same_v<T, PhysicalType<Type::kString>>) {
            return value;
          } else if constexpr (std::is_same_v<T, PhysicalType<Type::kDate>>) {
            return std::to_string(value.value);
          } else if constexpr (std::is_same_v<T, PhysicalType<Type::kTimestamp>>) {
            return std::to_string(value.value);
          } else if constexpr (std::is_same_v<T, PhysicalType<Type::kChar>>) {
            return std::string(1, value);
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
