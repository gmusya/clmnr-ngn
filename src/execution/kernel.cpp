#include "src/execution/kernel.h"

#include "src/util/assert.h"
#include "src/util/macro.h"

namespace ngn {

namespace internal {

ArrayType<Type::kInt64> Add(const ArrayType<Type::kInt64>& lhs, const ArrayType<Type::kInt64>& rhs) {
  ASSERT(lhs.size() == rhs.size());

  ArrayType<Type::kInt64> result(lhs.size());
  for (size_t i = 0; i < lhs.size(); ++i) {
    result[i] = lhs[i] + rhs[i];
  }
  return result;
}

ArrayType<Type::kInt128> Add(const ArrayType<Type::kInt128>& lhs, const ArrayType<Type::kInt128>& rhs) {
  ASSERT(lhs.size() == rhs.size());

  ArrayType<Type::kInt128> result(lhs.size());
  for (size_t i = 0; i < lhs.size(); ++i) {
    result[i] = lhs[i] + rhs[i];
  }
  return result;
}

ArrayType<Type::kInt64> Sub(const ArrayType<Type::kInt64>& lhs, const ArrayType<Type::kInt64>& rhs) {
  ASSERT(lhs.size() == rhs.size());

  ArrayType<Type::kInt64> result(lhs.size());
  for (size_t i = 0; i < lhs.size(); ++i) {
    result[i] = lhs[i] - rhs[i];
  }
  return result;
}

ArrayType<Type::kInt128> Sub(const ArrayType<Type::kInt128>& lhs, const ArrayType<Type::kInt128>& rhs) {
  ASSERT(lhs.size() == rhs.size());

  ArrayType<Type::kInt128> result(lhs.size());
  for (size_t i = 0; i < lhs.size(); ++i) {
    result[i] = lhs[i] - rhs[i];
  }
  return result;
}

ArrayType<Type::kInt64> Mult(const ArrayType<Type::kInt64>& lhs, const ArrayType<Type::kInt64>& rhs) {
  ASSERT(lhs.size() == rhs.size());

  ArrayType<Type::kInt64> result(lhs.size());
  for (size_t i = 0; i < lhs.size(); ++i) {
    result[i] = lhs[i] * rhs[i];
  }
  return result;
}

ArrayType<Type::kInt128> Mult(const ArrayType<Type::kInt128>& lhs, const ArrayType<Type::kInt128>& rhs) {
  ASSERT(lhs.size() == rhs.size());

  ArrayType<Type::kInt128> result(lhs.size());
  for (size_t i = 0; i < lhs.size(); ++i) {
    result[i] = lhs[i] * rhs[i];
  }
  return result;
}

ArrayType<Type::kInt64> Div(const ArrayType<Type::kInt64>& lhs, const ArrayType<Type::kInt64>& rhs) {
  ASSERT(lhs.size() == rhs.size());

  ArrayType<Type::kInt64> result(lhs.size());
  for (size_t i = 0; i < lhs.size(); ++i) {
    result[i] = lhs[i] / rhs[i];
  }
  return result;
}

ArrayType<Type::kInt128> Div(const ArrayType<Type::kInt128>& lhs, const ArrayType<Type::kInt128>& rhs) {
  ASSERT(lhs.size() == rhs.size());

  ArrayType<Type::kInt128> result(lhs.size());
  for (size_t i = 0; i < lhs.size(); ++i) {
    result[i] = lhs[i] / rhs[i];
  }
  return result;
}

ArrayType<Type::kInt128> Div(const ArrayType<Type::kInt128>& lhs, const ArrayType<Type::kInt64>& rhs) {
  ASSERT(lhs.size() == rhs.size());

  ArrayType<Type::kInt128> result(lhs.size());
  for (size_t i = 0; i < lhs.size(); ++i) {
    result[i] = lhs[i] / static_cast<Int128>(rhs[i]);
  }
  return result;
}

ArrayType<Type::kInt128> Div(const ArrayType<Type::kInt64>& lhs, const ArrayType<Type::kInt128>& rhs) {
  ASSERT(lhs.size() == rhs.size());

  ArrayType<Type::kInt128> result(lhs.size());
  for (size_t i = 0; i < lhs.size(); ++i) {
    result[i] = static_cast<Int128>(lhs[i]) / rhs[i];
  }
  return result;
}

template<Type type>
ArrayType<Type::kBool> NotEqual(const ArrayType<type>& lhs, const ArrayType<type>& rhs) {
  ASSERT(lhs.size() == rhs.size());

  ArrayType<Type::kBool> result(lhs.size());
  for (size_t i = 0; i < lhs.size(); ++i) {
    result[i] = Boolean{lhs[i] != rhs[i]};
  }
  return result;
}

}  // namespace internal

Column Add(const Column& lhs, const Column& rhs) {
  ASSERT(lhs.GetType() == rhs.GetType());
  if (lhs.GetType() == Type::kInt64) {
    return Column(internal::Add(std::get<ArrayType<Type::kInt64>>(lhs.Values()),
                                std::get<ArrayType<Type::kInt64>>(rhs.Values())));
  }
  if (lhs.GetType() == Type::kInt128) {
    return Column(internal::Add(std::get<ArrayType<Type::kInt128>>(lhs.Values()),
                                std::get<ArrayType<Type::kInt128>>(rhs.Values())));
  }
  THROW_NOT_IMPLEMENTED;
}

Column Sub(const Column& lhs, const Column& rhs) {
  ASSERT(lhs.GetType() == rhs.GetType());
  if (lhs.GetType() == Type::kInt64) {
    return Column(internal::Sub(std::get<ArrayType<Type::kInt64>>(lhs.Values()),
                                std::get<ArrayType<Type::kInt64>>(rhs.Values())));
  }
  if (lhs.GetType() == Type::kInt128) {
    return Column(internal::Sub(std::get<ArrayType<Type::kInt128>>(lhs.Values()),
                                std::get<ArrayType<Type::kInt128>>(rhs.Values())));
  }
  THROW_NOT_IMPLEMENTED;
}

Column Mult(const Column& lhs, const Column& rhs) {
  ASSERT(lhs.GetType() == rhs.GetType());
  if (lhs.GetType() == Type::kInt64) {
    return Column(internal::Mult(std::get<ArrayType<Type::kInt64>>(lhs.Values()),
                                 std::get<ArrayType<Type::kInt64>>(rhs.Values())));
  }
  if (lhs.GetType() == Type::kInt128) {
    return Column(internal::Mult(std::get<ArrayType<Type::kInt128>>(lhs.Values()),
                                 std::get<ArrayType<Type::kInt128>>(rhs.Values())));
  }
  THROW_NOT_IMPLEMENTED;
}

Column Div(const Column& lhs, const Column& rhs) {
  if (lhs.GetType() == Type::kInt64 && rhs.GetType() == Type::kInt64) {
    return Column(internal::Div(std::get<ArrayType<Type::kInt64>>(lhs.Values()),
                                std::get<ArrayType<Type::kInt64>>(rhs.Values())));
  }
  if (lhs.GetType() == Type::kInt128 && rhs.GetType() == Type::kInt128) {
    return Column(internal::Div(std::get<ArrayType<Type::kInt128>>(lhs.Values()),
                                std::get<ArrayType<Type::kInt128>>(rhs.Values())));
  }
  if (lhs.GetType() == Type::kInt128 && rhs.GetType() == Type::kInt64) {
    return Column(internal::Div(std::get<ArrayType<Type::kInt128>>(lhs.Values()),
                                std::get<ArrayType<Type::kInt64>>(rhs.Values())));
  }
  if (lhs.GetType() == Type::kInt64 && rhs.GetType() == Type::kInt128) {
    return Column(internal::Div(std::get<ArrayType<Type::kInt64>>(lhs.Values()),
                                std::get<ArrayType<Type::kInt128>>(rhs.Values())));
  }
  THROW_NOT_IMPLEMENTED;
}

Column And(const Column&, const Column&) { THROW_NOT_IMPLEMENTED; }
Column Or(const Column&, const Column&) { THROW_NOT_IMPLEMENTED; }
Column Less(const Column&, const Column&) { THROW_NOT_IMPLEMENTED; }
Column Greater(const Column&, const Column&) { THROW_NOT_IMPLEMENTED; }
Column Equal(const Column&, const Column&) { THROW_NOT_IMPLEMENTED; }
Column NotEqual(const Column& lhs, const Column& rhs) {
  ASSERT(lhs.GetType() == rhs.GetType());

  if (lhs.GetType() == Type::kInt16) {
    return Column(internal::NotEqual(std::get<ArrayType<Type::kInt16>>(lhs.Values()),
                                     std::get<ArrayType<Type::kInt16>>(rhs.Values())));
  }

  if (lhs.GetType() == Type::kString) {
    return Column(internal::NotEqual(std::get<ArrayType<Type::kString>>(lhs.Values()),
                                     std::get<ArrayType<Type::kString>>(rhs.Values())));
  }

  THROW_NOT_IMPLEMENTED;
}

Column LessOrEqual(const Column&, const Column&) { THROW_NOT_IMPLEMENTED; }
Column GreaterOrEqual(const Column&, const Column&) { THROW_NOT_IMPLEMENTED; }

}  // namespace ngn
