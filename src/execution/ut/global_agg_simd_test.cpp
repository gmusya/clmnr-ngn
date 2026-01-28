#include <random>

#include "gtest/gtest.h"
#include "src/core/type.h"
#include "src/execution/kernel.h"

namespace ngn {

static Column MakeSequentialInt64(int64_t n, int64_t start = 0) {
  ArrayType<Type::kInt64> data;
  data.reserve(n);
  for (int64_t i = 0; i < n; ++i) {
    data.emplace_back(start + i);
  }
  return Column(std::move(data));
}

TEST(GlobalAggSimd, ReduceSumInt64Simple) {
  // Small size, covers tails and correctness
  Column col = MakeSequentialInt64(7, 1);  // 1..7 sum = 28
  Value v = ReduceSumSimd256(col, Type::kInt128);
  EXPECT_EQ(std::get<Int128>(v.GetValue()), static_cast<Int128>(28));
}

TEST(GlobalAggSimd, ReduceSumInt64LargeRandom) {
  const int64_t n = 12345;
  std::mt19937_64 rng(2101);
  ArrayType<Type::kInt64> data;
  data.reserve(n);
  for (int64_t i = 0; i < n; ++i) {
    data.emplace_back(static_cast<int64_t>(rng()));
  }
  Column col(std::move(data));

  uint64_t expected_u = 0;
  for (int64_t i = 0; i < n; ++i) {
    expected_u += static_cast<uint64_t>(std::get<ArrayType<Type::kInt64>>(col.Values())[i]);
  }
  Int128 expected = static_cast<Int128>(static_cast<int64_t>(expected_u));

  Value v256 = ReduceSumSimd256(col, Type::kInt128);
  EXPECT_EQ(std::get<Int128>(v256.GetValue()), expected);
}

}  // namespace ngn
