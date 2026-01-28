#include <filesystem>
#include <random>

#include "gtest/gtest.h"
#include "src/core/columnar.h"
#include "src/execution/operator.h"

namespace ngn {

TEST(GlobalAggregation, SumCountDistinct) {
  std::mt19937 rnd(2101);
  std::filesystem::path path =
      std::filesystem::temp_directory_path() / ("ngn_global_agg_" + std::to_string(rnd() % 10000) + ".clmnr");

  Schema schema({Field{"UserID", Type::kInt64}});
  FileWriter writer(path.string(), schema);

  Column user_ids(std::vector<int64_t>{1, 2, 2, 3});
  writer.AppendRowGroup({user_ids});
  std::move(writer).Finalize();

  auto scan = MakeScan(path.string(), schema);

  auto plan = MakeGlobalAggregation(
      scan, {AggregationUnit{AggregationType::kSum, MakeVariable("UserID", Type::kInt64), "sum"},
             AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "count"},
             AggregationUnit{AggregationType::kDistinct, MakeVariable("UserID", Type::kInt64), "distinct"}});

  auto stream = Execute(plan);
  auto batch_opt = stream->Next();
  ASSERT_TRUE(batch_opt.has_value());

  auto batch = batch_opt.value();
  EXPECT_EQ(batch->Rows(), 1);

  EXPECT_EQ(batch->ColumnByName("sum")[0], Value(static_cast<Int128>(8)));
  EXPECT_EQ(batch->ColumnByName("count")[0], Value(static_cast<int64_t>(4)));
  EXPECT_EQ(batch->ColumnByName("distinct")[0], Value(static_cast<int64_t>(3)));
}

}  // namespace ngn
