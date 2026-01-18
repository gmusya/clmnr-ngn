#include "src/execution/kernel.h"

#include <functional>

#include "src/core/type.h"
#include "src/util/assert.h"
#include "src/util/macro.h"

namespace ngn {

namespace internal {

template <Type type>
ArrayType<type> Add(const ArrayType<type>& lhs, const ArrayType<type>& rhs) {
  ASSERT(lhs.size() == rhs.size());

  ArrayType<type> result(lhs.size());
  for (size_t i = 0; i < lhs.size(); ++i) {
    result[i] = lhs[i] + rhs[i];
  }
  return result;
}

template <Type type>
ArrayType<type> Sub(const ArrayType<type>& lhs, const ArrayType<type>& rhs) {
  ASSERT(lhs.size() == rhs.size());

  ArrayType<type> result(lhs.size());
  for (size_t i = 0; i < lhs.size(); ++i) {
    result[i] = lhs[i] - rhs[i];
  }
  return result;
}

template <Type type>
ArrayType<type> Mult(const ArrayType<type>& lhs, const ArrayType<type>& rhs) {
  ASSERT(lhs.size() == rhs.size());

  ArrayType<type> result(lhs.size());
  for (size_t i = 0; i < lhs.size(); ++i) {
    result[i] = lhs[i] * rhs[i];
  }
  return result;
}

template <Type type>
ArrayType<type> Div(const ArrayType<type>& lhs, const ArrayType<type>& rhs) {
  ASSERT(lhs.size() == rhs.size());

  ArrayType<type> result(lhs.size());
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

ArrayType<Type::kBool> And(const ArrayType<Type::kBool>& lhs, const ArrayType<Type::kBool>& rhs) {
  ASSERT(lhs.size() == rhs.size());

  ArrayType<Type::kBool> result(lhs.size());
  for (size_t i = 0; i < lhs.size(); ++i) {
    result[i] = Boolean{lhs[i].value && rhs[i].value};
  }
  return result;
}

ArrayType<Type::kBool> Or(const ArrayType<Type::kBool>& lhs, const ArrayType<Type::kBool>& rhs) {
  ASSERT(lhs.size() == rhs.size());

  ArrayType<Type::kBool> result(lhs.size());
  for (size_t i = 0; i < lhs.size(); ++i) {
    result[i] = Boolean{lhs[i].value || rhs[i].value};
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

Column And(const Column& lhs, const Column& rhs) {
  ASSERT(lhs.GetType() == Type::kBool);
  ASSERT(rhs.GetType() == Type::kBool);

  return Column(
      internal::And(std::get<ArrayType<Type::kBool>>(lhs.Values()), std::get<ArrayType<Type::kBool>>(rhs.Values())));
}

Column Or(const Column& lhs, const Column& rhs) {
  ASSERT(lhs.GetType() == Type::kBool);
  ASSERT(rhs.GetType() == Type::kBool);

  return Column(
      internal::Or(std::get<ArrayType<Type::kBool>>(lhs.Values()), std::get<ArrayType<Type::kBool>>(rhs.Values())));
}

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

Column StrContains(const Column& operand, const std::string& substring, bool negated) {
  ASSERT(operand.GetType() == Type::kString);
  const auto& values = std::get<ArrayType<Type::kString>>(operand.Values());

  ArrayType<Type::kBool> result(values.size());
  for (size_t i = 0; i < values.size(); ++i) {
    bool found = values[i].find(substring) != std::string::npos;
    result[i] = Boolean{negated ? !found : found};
  }
  return Column(std::move(result));
}

Column Not(const Column& operand) {
  ASSERT(operand.GetType() == Type::kBool);
  const auto& values = std::get<ArrayType<Type::kBool>>(operand.Values());
  ArrayType<Type::kBool> result(values.size());
  for (size_t i = 0; i < values.size(); ++i) {
    result[i] = Boolean{!values[i].value};
  }
  return Column(std::move(result));
}

Column ExtractMinute(const Column& operand) {
  ASSERT(operand.GetType() == Type::kTimestamp);
  const auto& values = std::get<ArrayType<Type::kTimestamp>>(operand.Values());

  static constexpr int64_t kMicrosecondsPerMinute = 60000000LL;
  static constexpr int64_t kMicrosecondsPerHour = 3600000000LL;

  ArrayType<Type::kInt16> result(values.size());
  for (size_t i = 0; i < values.size(); ++i) {
    int64_t total_us = values[i].value;
    // Get microseconds within the current hour (always positive)
    int64_t us_within_hour = total_us % kMicrosecondsPerHour;
    if (us_within_hour < 0) {
      us_within_hour += kMicrosecondsPerHour;
    }
    int64_t minute = us_within_hour / kMicrosecondsPerMinute;
    result[i] = static_cast<int16_t>(minute);
  }
  return Column(std::move(result));
}

Column StrLen(const Column&) { THROW_NOT_IMPLEMENTED; }
Column DateTruncMinute(const Column&) { THROW_NOT_IMPLEMENTED; }

}  // namespace ngn
