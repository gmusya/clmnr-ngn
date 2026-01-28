#include <random>

#include "benchmark/benchmark.h"
#include "src/core/column.h"
#include "src/core/type.h"
#include "src/execution/kernel.h"

namespace ngn {

constexpr int64_t kValues = 1'000'000;

// Sum int64
static void BenchmarkReduceSumInt64(benchmark::State& state) {
  std::mt19937_64 rng(2101);
  ArrayType<Type::kInt64> values(kValues);
  for (int64_t i = 0; i < kValues; ++i) {
    values[i] = static_cast<int64_t>(rng());
  }
  Column col(values);
  for (auto _ : state) {
    auto v = ReduceSum(col, Type::kInt128);
    benchmark::DoNotOptimize(v.GetValue());
  }
}
BENCHMARK(BenchmarkReduceSumInt64);

// AVX2 256-bit Sum int64
static void BenchmarkReduceSumInt64Simd256(benchmark::State& state) {
  std::mt19937_64 rng(2101);
  ArrayType<Type::kInt64> values(kValues);
  for (int64_t i = 0; i < kValues; ++i) {
    values[i] = static_cast<int64_t>(rng());
  }
  Column col(values);
  for (auto _ : state) {
    auto v = ReduceSumSimd256(col, Type::kInt128);
    benchmark::DoNotOptimize(v.GetValue());
  }
}
BENCHMARK(BenchmarkReduceSumInt64Simd256);

// Baseline (scalar) Sum int64
static void BenchmarkReduceSumInt64Baseline(benchmark::State& state) {
  std::mt19937_64 rng(2101);
  ArrayType<Type::kInt64> values(kValues);
  for (int64_t i = 0; i < kValues; ++i) {
    values[i] = static_cast<int64_t>(rng());
  }
  // Scalar baseline: accumulate into 128-bit
  for (auto _ : state) {
    __int128 sum = 0;
    for (int64_t i = 0; i < kValues; ++i) {
      sum += values[i];
    }
    benchmark::DoNotOptimize(sum);
  }
}
BENCHMARK(BenchmarkReduceSumInt64Baseline);

// Sum int16
static void BenchmarkReduceSumInt16(benchmark::State& state) {
  std::mt19937 rng(42);
  std::uniform_int_distribution<int16_t> dist(std::numeric_limits<int16_t>::min(), std::numeric_limits<int16_t>::max());
  ArrayType<Type::kInt16> values(kValues);
  for (int64_t i = 0; i < kValues; ++i) {
    values[i] = dist(rng);
  }
  Column col(values);
  for (auto _ : state) {
    auto v = ReduceSum(col, Type::kInt64);
    benchmark::DoNotOptimize(v.GetValue());
  }
}
BENCHMARK(BenchmarkReduceSumInt16);

// SIMD Sum int16
static void BenchmarkReduceSumInt16Simd256(benchmark::State& state) {
  std::mt19937 rng(42);
  std::uniform_int_distribution<int16_t> dist(std::numeric_limits<int16_t>::min(), std::numeric_limits<int16_t>::max());
  ArrayType<Type::kInt16> values(kValues);
  for (int64_t i = 0; i < kValues; ++i) {
    values[i] = dist(rng);
  }
  Column col(values);
  for (auto _ : state) {
    auto v = ReduceSumSimd256(col, Type::kInt64);
    benchmark::DoNotOptimize(v.GetValue());
  }
}
BENCHMARK(BenchmarkReduceSumInt16Simd256);

// Baseline (scalar) Sum int16
static void BenchmarkReduceSumInt16Baseline(benchmark::State& state) {
  std::mt19937 rng(42);
  std::uniform_int_distribution<int16_t> dist(std::numeric_limits<int16_t>::min(), std::numeric_limits<int16_t>::max());
  ArrayType<Type::kInt16> values(kValues);
  for (int64_t i = 0; i < kValues; ++i) {
    values[i] = dist(rng);
  }
  for (auto _ : state) {
    int64_t sum = 0;
    for (int64_t i = 0; i < kValues; ++i) {
      sum += values[i];
    }
    benchmark::DoNotOptimize(sum);
  }
}
BENCHMARK(BenchmarkReduceSumInt16Baseline);

// Min int64
static void BenchmarkReduceMinInt64(benchmark::State& state) {
  std::mt19937_64 rng(2101);
  ArrayType<Type::kInt64> values(kValues);
  for (int64_t i = 0; i < kValues; ++i) {
    values[i] = static_cast<int64_t>(rng());
  }
  Column col(values);
  for (auto _ : state) {
    auto v = ReduceMin(col);
    benchmark::DoNotOptimize(v.GetValue());
  }
}
BENCHMARK(BenchmarkReduceMinInt64);

// Baseline (scalar) Min int64
static void BenchmarkReduceMinInt64Baseline(benchmark::State& state) {
  std::mt19937_64 rng(2101);
  ArrayType<Type::kInt64> values(kValues);
  for (int64_t i = 0; i < kValues; ++i) {
    values[i] = static_cast<int64_t>(rng());
  }
  for (auto _ : state) {
    int64_t mv = values[0];
    for (int64_t i = 1; i < kValues; ++i) {
      if (values[i] < mv) {
        mv = values[i];
      }
    }
    benchmark::DoNotOptimize(mv);
  }
}
BENCHMARK(BenchmarkReduceMinInt64Baseline);

// Max int64
static void BenchmarkReduceMaxInt64(benchmark::State& state) {
  std::mt19937_64 rng(2101);
  ArrayType<Type::kInt64> values(kValues);
  for (int64_t i = 0; i < kValues; ++i) {
    values[i] = static_cast<int64_t>(rng());
  }
  Column col(values);
  for (auto _ : state) {
    auto v = ReduceMax(col);
    benchmark::DoNotOptimize(v.GetValue());
  }
}
BENCHMARK(BenchmarkReduceMaxInt64);

// Baseline (scalar) Max int64
static void BenchmarkReduceMaxInt64Baseline(benchmark::State& state) {
  std::mt19937_64 rng(2101);
  ArrayType<Type::kInt64> values(kValues);
  for (int64_t i = 0; i < kValues; ++i) {
    values[i] = static_cast<int64_t>(rng());
  }
  for (auto _ : state) {
    int64_t mv = values[0];
    for (int64_t i = 1; i < kValues; ++i) {
      if (values[i] > mv) {
        mv = values[i];
      }
    }
    benchmark::DoNotOptimize(mv);
  }
}
BENCHMARK(BenchmarkReduceMaxInt64Baseline);

}  // namespace ngn
