#include "src/execution/kernel.h"

// #include "simd/vector_types.h"
#include "src/util/assert.h"
#include "src/util/macro.h"

// #define NGN_SIMD

namespace ngn {

namespace internal {

ArrayType<Type::kInt64> Add(const ArrayType<Type::kInt64>& lhs, const ArrayType<Type::kInt64>& rhs) {
  ASSERT(lhs.size() == rhs.size());

  ArrayType<Type::kInt64> result(lhs.size());

  size_t i = 0;

#ifdef NGN_SIMD
  for (; i + 8 < lhs.size(); i += 8) {
    const simd_long8* l = reinterpret_cast<const simd_long8*>(lhs.data());
    const simd_long8* r = reinterpret_cast<const simd_long8*>(rhs.data());

    simd_long8* res = reinterpret_cast<simd_long8*>(result.data());

    *res = *l + *r;
  }
#endif

  for (; i < lhs.size(); ++i) {
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

ArrayType<Type::kInt64> Mult(const ArrayType<Type::kInt64>& lhs, const ArrayType<Type::kInt64>& rhs) {
  ASSERT(lhs.size() == rhs.size());

  ArrayType<Type::kInt64> result(lhs.size());
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

}  // namespace internal

Column Add(const Column& lhs, const Column& rhs) {
  ASSERT(lhs.GetType() == Type::kInt64);
  ASSERT(rhs.GetType() == Type::kInt64);

  return Column(
      internal::Add(std::get<ArrayType<Type::kInt64>>(lhs.Values()), std::get<ArrayType<Type::kInt64>>(rhs.Values())));
}

Column Sub(const Column& lhs, const Column& rhs) {
  ASSERT(lhs.GetType() == Type::kInt64);
  ASSERT(rhs.GetType() == Type::kInt64);

  return Column(
      internal::Sub(std::get<ArrayType<Type::kInt64>>(lhs.Values()), std::get<ArrayType<Type::kInt64>>(rhs.Values())));
}

Column Mult(const Column& lhs, const Column& rhs) {
  ASSERT(lhs.GetType() == Type::kInt64);
  ASSERT(rhs.GetType() == Type::kInt64);

  return Column(
      internal::Mult(std::get<ArrayType<Type::kInt64>>(lhs.Values()), std::get<ArrayType<Type::kInt64>>(rhs.Values())));
}

Column Div(const Column& lhs, const Column& rhs) {
  ASSERT(lhs.GetType() == Type::kInt64);
  ASSERT(rhs.GetType() == Type::kInt64);

  return Column(
      internal::Div(std::get<ArrayType<Type::kInt64>>(lhs.Values()), std::get<ArrayType<Type::kInt64>>(rhs.Values())));
}

Column And(Column, Column) { THROW_NOT_IMPLEMENTED; }
Column Or(Column, Column) { THROW_NOT_IMPLEMENTED; }
Column Less(Column, Column) { THROW_NOT_IMPLEMENTED; }
Column Greater(Column, Column) { THROW_NOT_IMPLEMENTED; }
Column Equal(Column, Column) { THROW_NOT_IMPLEMENTED; }
Column NotEqual(Column, Column) { THROW_NOT_IMPLEMENTED; }
Column LessOrEqual(Column, Column) { THROW_NOT_IMPLEMENTED; }
Column GreaterOrEqual(Column, Column) { THROW_NOT_IMPLEMENTED; }

}  // namespace ngn
