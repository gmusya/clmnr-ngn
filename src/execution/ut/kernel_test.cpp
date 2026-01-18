#include "src/execution/kernel.h"

#include "gtest/gtest.h"
#include "src/core/column.h"
#include "src/core/datetime.h"
#include "src/core/type.h"

namespace ngn {

TEST(Kernel, Add) {
  Column col1(ArrayType<Type::kInt64>{1, 2, 3});
  Column col2(ArrayType<Type::kInt64>{4, 5, 6});

  Column result = Add(col1, col2);

  Column expected(ArrayType<Type::kInt64>{5, 7, 9});
  EXPECT_EQ(result, expected);
}

TEST(Kernel, Not) {
  Column col(ArrayType<Type::kBool>{Boolean{true}, Boolean{false}, Boolean{true}});

  Column result = Not(col);

  Column expected(ArrayType<Type::kBool>{Boolean{false}, Boolean{true}, Boolean{false}});
  EXPECT_EQ(result, expected);
}

TEST(Kernel, ExtractMinute) {
  Column col(ArrayType<Type::kTimestamp>{
      Timestamp{0},                     // 00:00:00
      Timestamp{30 * 60 * 1000000LL},   // 00:30:00
      Timestamp{105 * 60 * 1000000LL},  // 01:45:00
      Timestamp{3599 * 1000000LL},      // 00:59:59
  });

  Column result = ExtractMinute(col);

  Column expected(ArrayType<Type::kInt16>{0, 30, 45, 59});
  EXPECT_EQ(result, expected);
}

TEST(Kernel, ExtractMinuteFromParsedTimestamps) {
  Column col(ArrayType<Type::kTimestamp>{
      ParseTimestamp("2013-07-15 10:30:45"),
      ParseTimestamp("2023-12-31 23:59:00"),
      ParseTimestamp("1970-01-01 00:00:00"),
  });

  Column result = ExtractMinute(col);

  Column expected(ArrayType<Type::kInt16>{30, 59, 0});
  EXPECT_EQ(result, expected);
}

TEST(Kernel, ExtractMinuteBeforeEpoch) {
  Column col(ArrayType<Type::kTimestamp>{
      Timestamp{-60 * 1000000LL},                        // -1 minute from epoch = 23:59
      Timestamp{-30 * 60 * 1000000LL + 30 * 1000000LL},  // -29.5 minutes = 23:30:30
  });

  Column result = ExtractMinute(col);

  Column expected(ArrayType<Type::kInt16>{59, 30});
  EXPECT_EQ(result, expected);
}

TEST(Kernel, StrContains) {
  Column col(ArrayType<Type::kString>{"hello world", "google.com", "example.org", ""});

  Column result = StrContains(col, "google", false);

  Column expected(ArrayType<Type::kBool>{Boolean{false}, Boolean{true}, Boolean{false}, Boolean{false}});
  EXPECT_EQ(result, expected);
}

TEST(Kernel, StrContainsNegated) {
  Column col(ArrayType<Type::kString>{"hello world", "google.com", "example.org", ""});

  Column result = StrContains(col, "google", true);

  Column expected(ArrayType<Type::kBool>{Boolean{true}, Boolean{false}, Boolean{true}, Boolean{true}});
  EXPECT_EQ(result, expected);
}

TEST(Kernel, StrLen) {
  Column col(ArrayType<Type::kString>{"hello", "", "test string", "x"});

  Column result = StrLen(col);

  Column expected(ArrayType<Type::kInt64>{5, 0, 11, 1});
  EXPECT_EQ(result, expected);
}

}  // namespace ngn
