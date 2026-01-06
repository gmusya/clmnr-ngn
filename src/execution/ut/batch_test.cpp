#include "src/execution/batch.h"

#include "gtest/gtest.h"
#include "src/core/column.h"
#include "src/core/schema.h"
#include "src/core/type.h"

namespace ngn {

TEST(Batch, Simple) {
  Column col1(ArrayType<Type::kInt64>{1, 2, 3});
  Column col2(ArrayType<Type::kString>{"abc", "qwe", "def"});

  Schema schema({Field{.name = "a", .type = Type::kInt64}, Field{.name = "c", .type = Type::kString}});

  Batch batch({col1, col2}, schema);

  EXPECT_EQ(batch.Rows(), 3);
  EXPECT_EQ(batch.ColumnByName("a"), col1);
  EXPECT_EQ(batch.ColumnByName("c"), col2);
}

}  // namespace ngn
