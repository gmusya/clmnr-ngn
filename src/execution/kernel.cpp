#include "src/execution/kernel.h"

#include <functional>
#include <limits>
#include <regex>

#include "simde/x86/avx2.h"
#include "simde/x86/avx512.h"
#include "simde/x86/sse2.h"
#include "src/core/type.h"
#include "src/util/assert.h"
#include "src/util/macro.h"

namespace ngn {

namespace internal {

template <Type type>
PhysicalType<type> Min(const ArrayType<type>& arr) {
  ASSERT(!arr.empty());
  PhysicalType<type> best = arr[0];
  for (size_t i = 1; i < arr.size(); ++i) {
    if (arr[i] < best) {
      best = arr[i];
    }
  }
  return best;
}

template <Type type>
PhysicalType<type> Max(const ArrayType<type>& arr) {
  ASSERT(!arr.empty());
  PhysicalType<type> best = arr[0];
  for (size_t i = 1; i < arr.size(); ++i) {
    if (arr[i] > best) {
      best = arr[i];
    }
  }
  return best;
}

template <Type type>
Int128 SumToInt128(const ArrayType<type>& arr) {
  Int128 sum = static_cast<Int128>(0);
  for (const auto& v : arr) {
    if constexpr (type == Type::kInt128) {
      sum += v;
    } else {
      sum += static_cast<Int128>(v);
    }
  }
  return sum;
}

// (removed) previous SIMD helper that accumulated to Int128

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

Value ReduceSum(const Column& operand, Type output_type) {
  return Dispatch(
      [&]<Type type>(Tag<type>) -> Value {
        if constexpr (type == Type::kInt16 || type == Type::kInt32 || type == Type::kInt64 || type == Type::kInt128) {
          Int128 sum = internal::SumToInt128(std::get<ArrayType<type>>(operand.Values()));
          if (output_type == Type::kInt128) {
            return Value(sum);
          }
          if (output_type == Type::kInt64) {
            if (sum > static_cast<Int128>(std::numeric_limits<int64_t>::max()) ||
                sum < static_cast<Int128>(std::numeric_limits<int64_t>::min())) {
              THROW_RUNTIME_ERROR("Overlflow");
            }
            return Value(static_cast<int64_t>(sum));
          }
          THROW_NOT_IMPLEMENTED;
        } else {
          THROW_NOT_IMPLEMENTED;
        }
      },
      operand.GetType());
}

Value ReduceSumSimd256(const Column& operand, Type output_type) {
  if (operand.GetType() == Type::kInt64) {
    const auto& arr = std::get<ArrayType<Type::kInt64>>(operand.Values());
    const int64_t* ptr = arr.data();
    const size_t n = arr.size();

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wpsabi"
#elif defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpsabi"
#endif
    simde__m256i vacc = simde_mm256_setzero_si256();
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
      simde__m256i v = simde_mm256_loadu_si256(reinterpret_cast<const simde__m256i*>(ptr + i));
      vacc = simde_mm256_add_epi64(vacc, v);
    }
    alignas(32) uint64_t lanes[4] = {0, 0, 0, 0};
    simde_mm256_store_si256(reinterpret_cast<simde__m256i*>(lanes), vacc);
#if defined(__clang__)
#pragma clang diagnostic pop
#elif defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

    uint64_t sum_u = lanes[0] + lanes[1] + lanes[2] + lanes[3];
    for (; i < n; ++i) {
      sum_u += static_cast<uint64_t>(ptr[i]);
    }

    int64_t sum64 = static_cast<int64_t>(sum_u);
    if (output_type == Type::kInt128) {
      return Value(static_cast<Int128>(sum64));
    }
    if (output_type == Type::kInt64) {
      return Value(sum64);
    }
    THROW_NOT_IMPLEMENTED;
  } else if (operand.GetType() == Type::kInt16) {
    const auto& arr = std::get<ArrayType<Type::kInt16>>(operand.Values());
    const int16_t* ptr = arr.data();
    const size_t n = arr.size();

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wpsabi"
#elif defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpsabi"
#endif
    simde__m256i vacc32 = simde_mm256_setzero_si256();
    size_t i = 0;
    for (; i + 16 <= n; i += 16) {
      simde__m256i v16 = simde_mm256_loadu_si256(reinterpret_cast<const simde__m256i*>(ptr + i));
      const simde__m256i ones = simde_mm256_set1_epi16(1);
      simde__m256i sum32 = simde_mm256_madd_epi16(v16, ones);  // sums pairs to 32-bit
      vacc32 = simde_mm256_add_epi32(vacc32, sum32);
    }
    alignas(32) int32_t lanes32[8];
    simde_mm256_store_si256(reinterpret_cast<simde__m256i*>(lanes32), vacc32);
#if defined(__clang__)
#pragma clang diagnostic pop
#elif defined(__GNUC__)
#pragma GCC diagnostic pop
#endif
    int64_t sum64 = 0;
    for (int k = 0; k < 8; ++k) {
      sum64 += static_cast<int64_t>(lanes32[k]);
    }
    for (; i < n; ++i) {
      sum64 += static_cast<int64_t>(ptr[i]);
    }
    if (output_type == Type::kInt64) {
      return Value(sum64);
    }
    if (output_type == Type::kInt128) {
      return Value(static_cast<Int128>(sum64));
    }
    THROW_NOT_IMPLEMENTED;
  } else {
    THROW_NOT_IMPLEMENTED;
  }
}

Value ReduceMin(const Column& operand) {
  ASSERT(operand.Size() > 0);
  return Dispatch(
      [&]<Type type>(Tag<type>) -> Value { return Value(internal::Min(std::get<ArrayType<type>>(operand.Values()))); },
      operand.GetType());
}

Value ReduceMax(const Column& operand) {
  ASSERT(operand.Size() > 0);
  return Dispatch(
      [&]<Type type>(Tag<type>) -> Value { return Value(internal::Max(std::get<ArrayType<type>>(operand.Values()))); },
      operand.GetType());
}

Column Add(const Column& lhs, const Column& rhs) {
  ASSERT(lhs.GetType() == rhs.GetType());

  return Dispatch(
      [&]<Type type>(Tag<type>) -> Column {
        if constexpr (type == Type::kInt16 || type == Type::kInt32 || type == Type::kInt64 || type == Type::kInt128) {
          return Column(
              internal::Add(std::get<ArrayType<type>>(lhs.Values()), std::get<ArrayType<type>>(rhs.Values())));
        } else {
          THROW_NOT_IMPLEMENTED;
        }
      },
      lhs.GetType());
}

Column Sub(const Column& lhs, const Column& rhs) {
  ASSERT(lhs.GetType() == rhs.GetType());

  return Dispatch(
      [&]<Type type>(Tag<type>) -> Column {
        if constexpr (type == Type::kInt16 || type == Type::kInt32 || type == Type::kInt64 || type == Type::kInt128) {
          return Column(
              internal::Sub(std::get<ArrayType<type>>(lhs.Values()), std::get<ArrayType<type>>(rhs.Values())));
        } else {
          THROW_NOT_IMPLEMENTED;
        }
      },
      lhs.GetType());
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

Column StrLen(const Column& operand) {
  ASSERT(operand.GetType() == Type::kString);
  const auto& values = std::get<ArrayType<Type::kString>>(operand.Values());

  ArrayType<Type::kInt64> result(values.size());
  for (size_t i = 0; i < values.size(); ++i) {
    result[i] = static_cast<int64_t>(values[i].size());
  }
  return Column(std::move(result));
}

Column DateTruncMinute(const Column& operand) {
  ASSERT(operand.GetType() == Type::kTimestamp);
  const auto& values = std::get<ArrayType<Type::kTimestamp>>(operand.Values());

  static constexpr int64_t kMicrosecondsPerMinute = 60000000LL;

  ArrayType<Type::kTimestamp> result(values.size());
  for (size_t i = 0; i < values.size(); ++i) {
    int64_t total_us = values[i].value;
    // Floor division to truncate to minute boundary
    int64_t minutes = total_us / kMicrosecondsPerMinute;
    if (total_us < 0 && total_us % kMicrosecondsPerMinute != 0) {
      --minutes;  // Adjust for negative timestamps to get floor behavior
    }
    result[i] = Timestamp{minutes * kMicrosecondsPerMinute};
  }
  return Column(std::move(result));
}

Column StrRegexReplace(const Column& operand, const std::string& pattern, const std::string& replacement) {
  ASSERT(operand.GetType() == Type::kString);
  const auto& values = std::get<ArrayType<Type::kString>>(operand.Values());

  std::regex re(pattern);

  ArrayType<Type::kString> result(values.size());
  for (size_t i = 0; i < values.size(); ++i) {
    result[i] = std::regex_replace(values[i], re, replacement);
  }
  return Column(std::move(result));
}

}  // namespace ngn
