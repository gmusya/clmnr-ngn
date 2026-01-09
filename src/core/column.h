#pragma once

#include <cstdint>
#include <variant>
#include <vector>

#include "src/core/type.h"
#include "src/core/value.h"
#include "src/util/macro.h"

namespace ngn {

class Column {
 public:
  using GenericColumn =
      std::variant<ArrayType<Type::kInt16>, ArrayType<Type::kInt32>, ArrayType<Type::kInt64>, ArrayType<Type::kString>,
                   ArrayType<Type::kDate>, ArrayType<Type::kTimestamp>, ArrayType<Type::kChar>>;

  explicit Column(ArrayType<Type::kInt16> values) : values_(std::move(values)) {}
  explicit Column(ArrayType<Type::kInt32> values) : values_(std::move(values)) {}
  explicit Column(ArrayType<Type::kInt64> values) : values_(std::move(values)) {}
  explicit Column(ArrayType<Type::kDate> values) : values_(std::move(values)) {}
  explicit Column(ArrayType<Type::kTimestamp> values) : values_(std::move(values)) {}
  explicit Column(ArrayType<Type::kChar> values) : values_(std::move(values)) {}
  explicit Column(ArrayType<Type::kString> values) : values_(std::move(values)) {}

  GenericColumn& Values() { return values_; }
  const GenericColumn& Values() const { return values_; }

  Value operator[](size_t index) const {
    return std::visit([index]<Type type>(const ArrayType<type>& arr) { return Value(arr[index]); }, values_);
  }

  Type GetType() const {
    return std::visit([]<Type type>(const ArrayType<type>&) { return type; }, values_);
  }

  size_t Size() const {
    return std::visit([](const auto& arr) { return arr.size(); }, values_);
  }

  bool operator==(const Column& other) const = default;

 private:
  GenericColumn values_;
};

}  // namespace ngn
