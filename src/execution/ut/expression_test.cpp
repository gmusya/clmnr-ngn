#include "src/execution/expression.h"

#include "gtest/gtest.h"
#include "src/core/schema.h"
#include "src/execution/batch.h"

namespace ngn {

TEST(Expression, Simple) { EXPECT_EQ(2 + 2, 4); }

TEST(Expression, CaseWhen) {
  auto batch = std::make_shared<Batch>(
      std::vector<Column>{
          Column(ArrayType<Type::kBool>{Boolean{true}, Boolean{false}, Boolean{true}, Boolean{false}}),
          Column(ArrayType<Type::kInt64>{10, 20, 30, 40}),
          Column(ArrayType<Type::kInt64>{1, 2, 3, 4}),
      },
      Schema({Field{"cond", Type::kBool}, Field{"a", Type::kInt64}, Field{"b", Type::kInt64}}));

  auto case_expr =
      MakeCase(MakeVariable("cond", Type::kBool), MakeVariable("a", Type::kInt64), MakeVariable("b", Type::kInt64));

  Column result = Evaluate(batch, case_expr);

  Column expected(ArrayType<Type::kInt64>{10, 2, 30, 4});  // true->a, false->b, true->a, false->b
  EXPECT_EQ(result, expected);
}

TEST(Expression, CaseWhenWithStrings) {
  auto batch = std::make_shared<Batch>(
      std::vector<Column>{
          Column(ArrayType<Type::kBool>{Boolean{true}, Boolean{false}}),
          Column(ArrayType<Type::kString>{"hello", "world"}),
      },
      Schema({Field{"cond", Type::kBool}, Field{"s", Type::kString}}));

  auto case_expr =
      MakeCase(MakeVariable("cond", Type::kBool), MakeVariable("s", Type::kString), MakeConst(Value(std::string(""))));

  Column result = Evaluate(batch, case_expr);

  Column expected(ArrayType<Type::kString>{"hello", ""});
  EXPECT_EQ(result, expected);
}

}  // namespace ngn
