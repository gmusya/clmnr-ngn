#include "src/execution/kernel.h"

#include "gtest/gtest.h"
#include "src/core/column.h"
#include "src/core/type.h"

namespace ngn {

TEST(Kernel, Add) {
  Column col1(ArrayType<Type::kInt64>{1, 2, 3});
  Column col2(ArrayType<Type::kInt64>{4, 5, 6});

  Column result = Add(col1, col2);

  Column expected(ArrayType<Type::kInt64>{5, 7, 9});
  EXPECT_EQ(result, expected);
}

}  // namespace ngn
