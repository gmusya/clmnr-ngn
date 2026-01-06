#include <random>

#include "benchmark/benchmark.h"
#include "src/core/column.h"
#include "src/core/type.h"
#include "src/execution/kernel.h"

namespace ngn {

static void BenchmarkAdd(benchmark::State& state) {
  std::mt19937_64 rnd(2101);

  constexpr int64_t kValues = 1'000'000;
  ArrayType<Type::kInt64> values(kValues);
  for (int i = 0; i < kValues; ++i) {
    values[i] = rnd();
  }

  Column col(values);

  for (auto _ : state) {
    Column result = Add(col, col);
  }
}

BENCHMARK(BenchmarkAdd);

////////////////////////////////////////////////////////////////////////////////

static void BenchmarkSub(benchmark::State& state) {
  std::mt19937_64 rnd(2101);

  constexpr int64_t kValues = 1'000'000;
  ArrayType<Type::kInt64> values(kValues);
  for (int i = 0; i < kValues; ++i) {
    values[i] = rnd();
  }

  Column col(values);

  for (auto _ : state) {
    Column result = Sub(col, col);
  }
}

BENCHMARK(BenchmarkSub);

////////////////////////////////////////////////////////////////////////////////

static void BenchmarkMult(benchmark::State& state) {
  std::mt19937_64 rnd(2101);

  constexpr int64_t kValues = 1'000'000;
  ArrayType<Type::kInt64> values(kValues);
  for (int i = 0; i < kValues; ++i) {
    values[i] = rnd();
  }

  Column col(values);

  for (auto _ : state) {
    Column result = Mult(col, col);
  }
}

BENCHMARK(BenchmarkMult);

}  // namespace ngn

BENCHMARK_MAIN();
