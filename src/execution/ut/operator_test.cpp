#include "src/execution/operator.h"

#include <filesystem>
#include <random>

#include "gtest/gtest.h"
#include "src/core/columnar.h"

namespace ngn {

TEST(Operator, ConcatCountTable) {
  std::mt19937 rnd(2101);
  std::filesystem::path path = std::filesystem::temp_directory_path() / ("ngn_concat_" + std::to_string(rnd() % 10000));

  Schema schema({Field{"a", Type::kInt64}});
  FileWriter writer(path.string(), schema);

  Column rg0_a(std::vector<int64_t>{1, 2});
  writer.AppendRowGroup({rg0_a});

  Column rg1_a(std::vector<int64_t>{3, 4, 5});
  writer.AppendRowGroup({rg1_a});

  std::move(writer).Finalize();

  auto plan = MakeConcat({MakeCountTable(path.string(), "count1"), MakeCountTable(path.string(), "count2")});
  auto stream = Execute(plan);

  auto batch_opt = stream->Next();
  ASSERT_TRUE(batch_opt.has_value());

  auto batch = batch_opt.value();
  EXPECT_EQ(batch->Rows(), 1);
  EXPECT_EQ(batch->Columns().size(), 2);
  EXPECT_EQ(batch->GetSchema().Fields().size(), 2);

  EXPECT_EQ(batch->ColumnByName("count1")[0], Value(static_cast<int64_t>(5)));
  EXPECT_EQ(batch->ColumnByName("count2")[0], Value(static_cast<int64_t>(5)));

  EXPECT_FALSE(stream->Next().has_value());
}

}  // namespace ngn
