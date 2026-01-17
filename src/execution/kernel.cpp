#include "src/execution/kernel.h"

#include <functional>

#include "src/core/type.h"
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

template <Type type, typename Comparator>
ArrayType<Type::kBool> Compare(const ArrayType<type>& lhs, const ArrayType<type>& rhs) {
  ASSERT(lhs.size() == rhs.size());

  ArrayType<Type::kBool> result(lhs.size());
  for (size_t i = 0; i < lhs.size(); ++i) {
    result[i] = Boolean{Comparator{}(lhs[i], rhs[i])};
  }
  return result;
}

template <Type type>
ArrayType<Type::kBool> NotEqual(const ArrayType<type>& lhs, const ArrayType<type>& rhs) {
  return Compare<type, std::not_equal_to<PhysicalType<type>>>(lhs, rhs);
}

template <Type type>
ArrayType<Type::kBool> Equal(const ArrayType<type>& lhs, const ArrayType<type>& rhs) {
  return Compare<type, std::equal_to<PhysicalType<type>>>(lhs, rhs);
}

template <Type type>
ArrayType<Type::kBool> Less(const ArrayType<type>& lhs, const ArrayType<type>& rhs) {
  return Compare<type, std::less<PhysicalType<type>>>(lhs, rhs);
}

template <Type type>
ArrayType<Type::kBool> LessOrEqual(const ArrayType<type>& lhs, const ArrayType<type>& rhs) {
  return Compare<type, std::less_equal<PhysicalType<type>>>(lhs, rhs);
}

template <Type type>
ArrayType<Type::kBool> Greater(const ArrayType<type>& lhs, const ArrayType<type>& rhs) {
  return Compare<type, std::greater<PhysicalType<type>>>(lhs, rhs);
}

template <Type type>
ArrayType<Type::kBool> GreaterOrEqual(const ArrayType<type>& lhs, const ArrayType<type>& rhs) {
  return Compare<type, std::greater_equal<PhysicalType<type>>>(lhs, rhs);
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

Column Less(const Column& lhs, const Column& rhs) {
  return Column(Dispatch(
      [&]<Type type>(Tag<type>) {
        return internal::Less(std::get<ArrayType<type>>(lhs.Values()), std::get<ArrayType<type>>(rhs.Values()));
      },
      lhs.GetType()));
}

Column Greater(const Column& lhs, const Column& rhs) {
  return Column(Dispatch(
      [&]<Type type>(Tag<type>) {
        return internal::Greater(std::get<ArrayType<type>>(lhs.Values()), std::get<ArrayType<type>>(rhs.Values()));
      },
      lhs.GetType()));
}

Column Equal(const Column& lhs, const Column& rhs) {
  return Column(Dispatch(
      [&]<Type type>(Tag<type>) {
        return internal::Equal(std::get<ArrayType<type>>(lhs.Values()), std::get<ArrayType<type>>(rhs.Values()));
      },
      lhs.GetType()));
}

Column NotEqual(const Column& lhs, const Column& rhs) {
  return Column(Dispatch(
      [&]<Type type>(Tag<type>) {
        return internal::NotEqual(std::get<ArrayType<type>>(lhs.Values()), std::get<ArrayType<type>>(rhs.Values()));
      },
      lhs.GetType()));
}

Column LessOrEqual(const Column& lhs, const Column& rhs) {
  return Column(Dispatch(
      [&]<Type type>(Tag<type>) {
        return internal::LessOrEqual(std::get<ArrayType<type>>(lhs.Values()), std::get<ArrayType<type>>(rhs.Values()));
      },
      lhs.GetType()));
}

Column GreaterOrEqual(const Column& lhs, const Column& rhs) {
  return Column(Dispatch(
      [&]<Type type>(Tag<type>) {
        return internal::GreaterOrEqual(std::get<ArrayType<type>>(lhs.Values()),
                                        std::get<ArrayType<type>>(rhs.Values()));
      },
      lhs.GetType()));
}

Column LikeMatch(const Column&, const std::string&, bool) { THROW_NOT_IMPLEMENTED; }

Column Not(const Column&) { THROW_NOT_IMPLEMENTED; }
Column ExtractMinute(const Column&) { THROW_NOT_IMPLEMENTED; }

}  // namespace ngn
