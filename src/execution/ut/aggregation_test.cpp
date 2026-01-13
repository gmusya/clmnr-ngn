#include "gtest/gtest.h"
#include "src/execution/aggregation_executor.h"

namespace ngn {

TEST(Aggregation, Simple) {
  std::shared_ptr<Aggregation> aggregation = MakeAggregation(
      {AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "count"}}, {});

  Batch batch(std::vector<Column>{Column(ArrayType<Type::kInt64>{1, 2, 3}), Column(ArrayType<Type::kInt64>{4, 5, 6})},
              Schema({Field{"a", Type::kInt64}, Field{"b", Type::kInt64}}));

  auto stream = std::make_shared<VectorStream<std::shared_ptr<Batch>>>(
      std::vector<std::shared_ptr<Batch>>{std::make_shared<Batch>(batch)});

  std::shared_ptr<Batch> result = Evaluate(stream, aggregation);

  EXPECT_EQ(result->Rows(), 1);
  EXPECT_EQ(result->ColumnByName("count")[0], Value(static_cast<int64_t>(3)));
}

}  // namespace ngn
